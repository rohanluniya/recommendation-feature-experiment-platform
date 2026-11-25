"""Valkey-compatible low-latency online feature store, materializer, freshness tracker, and offline-vs-online parity validator."""

from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from typing import Any

import valkey

from recsys_platform.config import PlatformConfig
from recsys_platform.lakehouse import IcebergLakehouse
from recsys_platform.observability import TELEMETRY

USER_PARITY_NUMERIC_FIELDS = (
    "user_watch_seconds_total",
    "user_play_count",
    "user_completion_count",
    "user_completion_rate",
    "user_skip_rate",
    "user_rebuffer_rate",
    "user_device_switches",
)
ITEM_PARITY_NUMERIC_FIELDS = (
    "duration_seconds",
    "release_year",
    "popularity_prior",
    "item_impressions_so_far",
    "item_plays_so_far",
    "item_completion_rate_so_far",
)


@dataclass(slots=True)
class ParityReport:
    """Result of comparing exact online feature vectors against offline Iceberg Gold snapshots."""

    users_checked: int
    items_checked: int
    user_mismatches: int
    item_mismatches: int
    total_mismatches: int
    mismatch_ratio: float
    max_abs_numeric_diff: float
    freshness_lag_seconds: float
    passed: bool
    sample_diffs: list[dict[str, Any]]


class OnlineFeatureStore:
    """Low-latency key-value online feature store backed by Valkey (containerized) or SQLite WAL (local_native)."""

    def __init__(self, config: PlatformConfig) -> None:
        self.config = config
        self.config.ensure_dirs()
        self.db_path = config.online_store_db_path
        self._valkey_client: valkey.Valkey | None = None
        if config.valkey_enabled:
            try:
                client = valkey.Valkey.from_url(config.valkey_url, decode_responses=True, socket_timeout=0.5)
                client.ping()
                self._valkey_client = client
            except Exception:
                self._valkey_client = None
        self._init_sqlite()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=10.0)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        return conn

    def _init_sqlite(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS online_features (
                    entity_type TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    feature_json TEXT NOT NULL,
                    event_ts_ms INTEGER NOT NULL,
                    materialized_ts_ms INTEGER NOT NULL,
                    expires_at_ms INTEGER NOT NULL,
                    PRIMARY KEY (entity_type, entity_id)
                )
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_online_entity_type ON online_features (entity_type)"
            )

    @property
    def backend_mode(self) -> str:
        return "valkey" if self._valkey_client is not None else "sqlite_wal_valkey_compat"

    def clear_all(self) -> None:
        """Wipe all online feature entries (used for chaos wipe & rebuild drills)."""
        if self._valkey_client is not None:
            for pattern in ("recsys:user:*", "recsys:item:*"):
                keys = list(self._valkey_client.scan_iter(match=pattern))
                if keys:
                    self._valkey_client.delete(*keys)
        with self._connect() as conn:
            conn.execute("DELETE FROM online_features")

    def upsert_batch(
        self,
        entity_type: str,
        records: list[dict[str, Any]],
        *,
        entity_key: str,
        now_ms: int | None = None,
    ) -> int:
        """Materialize latest feature vectors per entity into online store with TTL."""
        mat_ms = now_ms if now_ms is not None else int(time.time() * 1000)
        latest: dict[str, dict[str, Any]] = {}
        for rec in records:
            eid = str(rec[entity_key])
            prev = latest.get(eid)
            if prev is None or int(rec["event_ts_ms"]) >= int(prev["event_ts_ms"]):
                latest[eid] = rec

        rows_to_write: list[tuple[str, str, str, int, int, int]] = []
        pipe = self._valkey_client.pipeline() if self._valkey_client is not None else None
        for eid, rec in latest.items():
            ttl_sec = max(60, int(rec.get("ttl_seconds", 86400)))
            expires_ms = mat_ms + ttl_sec * 1000
            payload = dict(rec)
            payload["materialized_ts_ms"] = mat_ms
            encoded = json.dumps(payload, sort_keys=True)
            rows_to_write.append(
                (entity_type, eid, encoded, int(rec["event_ts_ms"]), mat_ms, expires_ms)
            )
            if pipe is not None:
                pipe.setex(f"recsys:{entity_type}:{eid}", ttl_sec, encoded)

        if pipe is not None and rows_to_write:
            pipe.execute()

        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO online_features
                    (entity_type, entity_id, feature_json, event_ts_ms, materialized_ts_ms, expires_at_ms)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(entity_type, entity_id) DO UPDATE SET
                    feature_json=excluded.feature_json,
                    event_ts_ms=excluded.event_ts_ms,
                    materialized_ts_ms=excluded.materialized_ts_ms,
                    expires_at_ms=excluded.expires_at_ms
                """,
                rows_to_write,
            )
        return len(rows_to_write)

    def get_features(
        self,
        entity_type: str,
        entity_id: str,
        *,
        now_ms: int | None = None,
    ) -> dict[str, Any] | None:
        """Fetch a single entity's online feature vector, respecting TTL and tracking serving latency."""
        t0 = time.perf_counter()
        check_ms = now_ms if now_ms is not None else int(time.time() * 1000)
        result: dict[str, Any] | None = None

        if self._valkey_client is not None:
            raw = self._valkey_client.get(f"recsys:{entity_type}:{entity_id}")
            if isinstance(raw, str):
                result = json.loads(raw)
        else:
            with self._connect() as conn:
                row = conn.execute(
                    """
                    SELECT feature_json, expires_at_ms
                    FROM online_features
                    WHERE entity_type = ? AND entity_id = ?
                    """,
                    (entity_type, entity_id),
                ).fetchone()
                if row is not None and int(row[1]) >= check_ms:
                    result = json.loads(str(row[0]))

        elapsed_ms = (time.perf_counter() - t0) * 1000.0
        TELEMETRY.feature_serving_latency_ms.observe(elapsed_ms)
        return result

    def get_user_features(self, user_id: str) -> dict[str, Any] | None:
        return self.get_features("user", user_id)

    def get_item_features(self, item_id: str) -> dict[str, Any] | None:
        return self.get_features("item", item_id)

    def list_entities(self, entity_type: str, limit: int = 500) -> list[dict[str, Any]]:
        now_ms = int(time.time() * 1000)
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT feature_json
                FROM online_features
                WHERE entity_type = ? AND expires_at_ms >= ?
                ORDER BY entity_id
                LIMIT ?
                """,
                (entity_type, now_ms, limit),
            ).fetchall()
        return [json.loads(str(r[0])) for r in rows]

    def stats(self) -> dict[str, Any]:
        now_ms = int(time.time() * 1000)
        with self._connect() as conn:
            u_cnt = conn.execute(
                "SELECT COUNT(*), MAX(event_ts_ms), MAX(materialized_ts_ms) FROM online_features WHERE entity_type='user' AND expires_at_ms >= ?",
                (now_ms,),
            ).fetchone()
            i_cnt = conn.execute(
                "SELECT COUNT(*), MAX(event_ts_ms), MAX(materialized_ts_ms) FROM online_features WHERE entity_type='item' AND expires_at_ms >= ?",
                (now_ms,),
            ).fetchone()
        max_mat_ms = max(int(u_cnt[2] or 0), int(i_cnt[2] or 0))
        freshness_age_sec = round(max(0.0, (now_ms - max_mat_ms) / 1000.0), 3) if max_mat_ms else 0.0
        return {
            "backend_mode": self.backend_mode,
            "active_users": int(u_cnt[0] or 0),
            "active_items": int(i_cnt[0] or 0),
            "latest_user_event_ts_ms": int(u_cnt[1] or 0),
            "latest_item_event_ts_ms": int(i_cnt[1] or 0),
            "latest_materialized_ts_ms": max_mat_ms,
            "seconds_since_materialization": freshness_age_sec,
        }

    def materialize_from_lakehouse(self, lakehouse: IcebergLakehouse) -> dict[str, Any]:
        """Materialize latest user and item feature snapshots from Iceberg Gold into online store."""
        with TELEMETRY.span("online_store.materialize"):
            user_snaps = lakehouse.read_records("gold.user_features_pit")
            item_snaps = lakehouse.read_records("gold.item_features_pit")
            now_ms = int(time.time() * 1000)
            u_written = self.upsert_batch("user", user_snaps, entity_key="user_id", now_ms=now_ms)
            i_written = self.upsert_batch("item", item_snaps, entity_key="item_id", now_ms=now_ms)
            return {
                "users_materialized": u_written,
                "items_materialized": i_written,
                "materialized_ts_ms": now_ms,
                "backend_mode": self.backend_mode,
            }

    def validate_parity_against_lakehouse(
        self,
        lakehouse: IcebergLakehouse,
        *,
        tolerance: float = 1e-6,
    ) -> ParityReport:
        """Compare every online user & item feature vector against latest offline Iceberg Gold snapshot."""
        with TELEMETRY.span("online_store.validate_parity"):
            offline_users: dict[str, dict[str, Any]] = {}
            for rec in lakehouse.read_records("gold.user_features_pit"):
                uid = str(rec["user_id"])
                prev = offline_users.get(uid)
                if prev is None or int(rec["event_ts_ms"]) >= int(prev["event_ts_ms"]):
                    offline_users[uid] = rec

            offline_items: dict[str, dict[str, Any]] = {}
            for rec in lakehouse.read_records("gold.item_features_pit"):
                iid = str(rec["item_id"])
                prev = offline_items.get(iid)
                if prev is None or int(rec["event_ts_ms"]) >= int(prev["event_ts_ms"]):
                    offline_items[iid] = rec

            user_mismatches = 0
            item_mismatches = 0
            max_diff = 0.0
            sample_diffs: list[dict[str, Any]] = []
            max_lag_ms = 0

            for uid, off in offline_users.items():
                onl = self.get_user_features(uid)
                if onl is None:
                    user_mismatches += 1
                    if len(sample_diffs) < 5:
                        sample_diffs.append({"entity_type": "user", "entity_id": uid, "reason": "missing_online"})
                    continue
                lag = abs(int(off["event_ts_ms"]) - int(onl["event_ts_ms"]))
                max_lag_ms = max(max_lag_ms, lag)
                row_bad = False
                for col in USER_PARITY_NUMERIC_FIELDS:
                    diff = abs(float(off[col]) - float(onl[col]))
                    max_diff = max(max_diff, diff)
                    if diff > tolerance:
                        row_bad = True
                        if len(sample_diffs) < 5:
                            sample_diffs.append(
                                {"entity_type": "user", "entity_id": uid, "field": col, "offline": off[col], "online": onl[col]}
                            )
                if str(off["user_top_genre"]) != str(onl["user_top_genre"]):
                    row_bad = True
                if row_bad:
                    user_mismatches += 1

            for iid, off in offline_items.items():
                onl = self.get_item_features(iid)
                if onl is None:
                    item_mismatches += 1
                    if len(sample_diffs) < 5:
                        sample_diffs.append({"entity_type": "item", "entity_id": iid, "reason": "missing_online"})
                    continue
                lag = abs(int(off["event_ts_ms"]) - int(onl["event_ts_ms"]))
                max_lag_ms = max(max_lag_ms, lag)
                row_bad = False
                for col in ITEM_PARITY_NUMERIC_FIELDS:
                    diff = abs(float(off[col]) - float(onl[col]))
                    max_diff = max(max_diff, diff)
                    if diff > tolerance:
                        row_bad = True
                        if len(sample_diffs) < 5:
                            sample_diffs.append(
                                {"entity_type": "item", "entity_id": iid, "field": col, "offline": off[col], "online": onl[col]}
                            )
                if str(off["genre"]) != str(onl["genre"]) or bool(off["is_active"]) != bool(onl["is_active"]):
                    row_bad = True
                if row_bad:
                    item_mismatches += 1

            total_checked = len(offline_users) + len(offline_items)
            total_mismatches = user_mismatches + item_mismatches
            mismatch_ratio = round(total_mismatches / max(1, total_checked), 6)
            freshness_lag_sec = round(max_lag_ms / 1000.0, 3)

            TELEMETRY.online_offline_parity_mismatch_ratio.set(mismatch_ratio)
            TELEMETRY.feature_freshness_lag_seconds.set(freshness_lag_sec)

            return ParityReport(
                users_checked=len(offline_users),
                items_checked=len(offline_items),
                user_mismatches=user_mismatches,
                item_mismatches=item_mismatches,
                total_mismatches=total_mismatches,
                mismatch_ratio=mismatch_ratio,
                max_abs_numeric_diff=round(max_diff, 8),
                freshness_lag_seconds=freshness_lag_sec,
                passed=(total_mismatches == 0),
                sample_diffs=sample_diffs,
            )

    def wipe_and_rebuild_from_lakehouse(self, lakehouse: IcebergLakehouse) -> dict[str, Any]:
        """Wipe derived online store completely and rebuild deterministically from durable Iceberg Gold."""
        self.clear_all()
        after_wipe = self.stats()
        mat = self.materialize_from_lakehouse(lakehouse)
        parity = self.validate_parity_against_lakehouse(lakehouse)
        return {
            "active_users_after_wipe": after_wipe["active_users"],
            "active_items_after_wipe": after_wipe["active_items"],
            "rebuilt": mat,
            "parity_passed": parity.passed,
            "parity_mismatches": parity.total_mismatches,
            "max_abs_numeric_diff": parity.max_abs_numeric_diff,
        }
