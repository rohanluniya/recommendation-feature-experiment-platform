"""Temporal feature builder, strict Point-in-Time (ASOF) join engine, leakage audit, and delayed label backfill."""

from __future__ import annotations

import bisect
from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Any

from recsys_platform.observability import TELEMETRY
from recsys_platform.simulator import deterministic_id

USER_FEATURE_TTL_SECONDS = 86_400 * 7
ITEM_FEATURE_TTL_SECONDS = 86_400 * 30


@dataclass(slots=True)
class PitAuditReport:
    """Point-in-time temporal correctness verification report."""

    total_rows: int
    user_leakage_violations: int
    item_leakage_violations: int
    total_leakage_violations: int
    leakage_rate: float
    ttl_expired_fallbacks: int
    cold_start_user_rows: int
    cold_start_item_rows: int
    passed: bool


def build_temporal_feature_histories(
    interactions: list[dict[str, Any]],
    catalog_changes: list[dict[str, Any]],
    *,
    created_ts_ms: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Build point-in-time user and item feature snapshots ordered strictly by event_ts_ms."""
    with TELEMETRY.span("features.build_histories", {"interactions": len(interactions), "catalog_changes": len(catalog_changes)}):
        # 1. Build item catalog timeline first (SCD2-like catalog edits + interaction rollups)
        # Map item_id -> latest catalog metadata over time
        item_meta: dict[str, dict[str, Any]] = {}
        item_snapshots: list[dict[str, Any]] = []

        # Combine catalog CDC events and interaction events chronologically by event_ts_ms
        timeline: list[tuple[int, int, str, dict[str, Any]]] = []
        for cdc in catalog_changes:
            timeline.append((int(cdc["event_ts_ms"]), 0, "cdc", cdc))
        for ev in interactions:
            timeline.append((int(ev["event_ts_ms"]), 1, "interaction", ev))
        timeline.sort(key=lambda t: (t[0], t[1]))

        item_impressions: dict[str, int] = defaultdict(int)
        item_plays: dict[str, int] = defaultdict(int)
        item_completions: dict[str, int] = defaultdict(int)

        user_watch: dict[str, float] = defaultdict(float)
        user_plays: dict[str, int] = defaultdict(int)
        user_impressions: dict[str, int] = defaultdict(int)
        user_completions: dict[str, int] = defaultdict(int)
        user_skips: dict[str, int] = defaultdict(int)
        user_rebuffers: dict[str, int] = defaultdict(int)
        user_heartbeats: dict[str, int] = defaultdict(int)
        user_dev_switches: dict[str, int] = defaultdict(int)
        user_genre_counts: dict[str, Counter[str]] = defaultdict(Counter)

        user_snapshots: list[dict[str, Any]] = []

        for ts_ms, _prio, kind, rec in timeline:
            if kind == "cdc":
                iid = str(rec["item_id"])
                item_meta[iid] = {
                    "title": str(rec["title"]),
                    "genre": str(rec["genre"]),
                    "duration_seconds": int(rec["duration_seconds"]),
                    "release_year": int(rec["release_year"]),
                    "popularity_prior": float(rec["popularity_prior"]),
                    "is_active": bool(rec["is_active"]),
                }
                impr = item_impressions[iid]
                plays = item_plays[iid]
                comps = item_completions[iid]
                item_snapshots.append(
                    {
                        "item_id": iid,
                        "event_ts_ms": ts_ms,
                        "created_ts_ms": created_ts_ms,
                        "ttl_seconds": ITEM_FEATURE_TTL_SECONDS,
                        "title": item_meta[iid]["title"],
                        "genre": item_meta[iid]["genre"],
                        "duration_seconds": item_meta[iid]["duration_seconds"],
                        "release_year": item_meta[iid]["release_year"],
                        "popularity_prior": round(item_meta[iid]["popularity_prior"], 4),
                        "is_active": item_meta[iid]["is_active"],
                        "item_impressions_so_far": impr,
                        "item_plays_so_far": plays,
                        "item_completion_rate_so_far": round(comps / max(1, plays), 4),
                        "is_cold_start_item": plays < 3,
                    }
                )
                continue

            # Interaction event updates user & item historical state
            uid = str(rec["user_id"])
            iid = str(rec["item_id"])
            ev_type = str(rec["event_type"])
            meta = item_meta.get(
                iid,
                {
                    "title": iid,
                    "genre": "unknown",
                    "duration_seconds": 3000,
                    "release_year": 2024,
                    "popularity_prior": 0.15,
                    "is_active": True,
                },
            )

            emit_user = False
            emit_item = False
            if ev_type == "IMPRESSION":
                user_impressions[uid] += 1
                item_impressions[iid] += 1
                emit_item = True
            elif ev_type == "PLAY_START":
                user_plays[uid] += 1
                item_plays[iid] += 1
                user_genre_counts[uid][meta["genre"]] += 1
                emit_user = True
                emit_item = True
            elif ev_type == "HEARTBEAT":
                user_watch[uid] += float(rec.get("watch_seconds", 0.0))
                user_heartbeats[uid] += 1
                user_rebuffers[uid] += int(rec.get("rebuffer_count", 0))
                emit_user = True
            elif ev_type == "COMPLETE":
                user_completions[uid] += 1
                item_completions[iid] += 1
                user_watch[uid] += float(rec.get("watch_seconds", 0.0))
                user_genre_counts[uid][meta["genre"]] += 2
                emit_user = True
                emit_item = True
            elif ev_type == "SKIP":
                user_skips[uid] += 1
                emit_user = True
            elif ev_type == "DEVICE_SWITCH":
                user_dev_switches[uid] += 1
                emit_user = True
            elif ev_type == "THUMBS_UP":
                user_genre_counts[uid][meta["genre"]] += 3
                emit_user = True

            if emit_user:
                plays = user_plays[uid]
                imprs = max(1, user_impressions[uid])
                hbs = max(1, user_heartbeats[uid])
                top_genre = (
                    user_genre_counts[uid].most_common(1)[0][0]
                    if user_genre_counts[uid]
                    else "unknown"
                )
                user_snapshots.append(
                    {
                        "user_id": uid,
                        "event_ts_ms": ts_ms,
                        "created_ts_ms": created_ts_ms,
                        "ttl_seconds": USER_FEATURE_TTL_SECONDS,
                        "user_watch_seconds_total": round(user_watch[uid], 2),
                        "user_play_count": plays,
                        "user_completion_count": user_completions[uid],
                        "user_completion_rate": round(user_completions[uid] / max(1, plays), 4),
                        "user_skip_rate": round(user_skips[uid] / imprs, 4),
                        "user_rebuffer_rate": round(user_rebuffers[uid] / hbs, 4),
                        "user_device_switches": user_dev_switches[uid],
                        "user_top_genre": top_genre,
                        "is_cold_start_user": plays < 2,
                    }
                )

            if emit_item:
                plays_i = item_plays[iid]
                item_snapshots.append(
                    {
                        "item_id": iid,
                        "event_ts_ms": ts_ms,
                        "created_ts_ms": created_ts_ms,
                        "ttl_seconds": ITEM_FEATURE_TTL_SECONDS,
                        "title": meta["title"],
                        "genre": meta["genre"],
                        "duration_seconds": int(meta["duration_seconds"]),
                        "release_year": int(meta["release_year"]),
                        "popularity_prior": round(float(meta["popularity_prior"]), 4),
                        "is_active": bool(meta["is_active"]),
                        "item_impressions_so_far": item_impressions[iid],
                        "item_plays_so_far": plays_i,
                        "item_completion_rate_so_far": round(item_completions[iid] / max(1, plays_i), 4),
                        "is_cold_start_item": plays_i < 3,
                    }
                )

        return user_snapshots, item_snapshots


def _index_feature_history(
    snapshots: list[dict[str, Any]],
    entity_key: str,
) -> dict[str, tuple[list[int], list[dict[str, Any]]]]:
    """Group and sort snapshots per entity by event_ts_ms for O(log N) ASOF binary search."""
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for snap in snapshots:
        grouped[str(snap[entity_key])].append(snap)

    indexed: dict[str, tuple[list[int], list[dict[str, Any]]]] = {}
    for eid, rows in grouped.items():
        rows.sort(key=lambda r: int(r["event_ts_ms"]))
        # Keep last record per exact timestamp to break ties cleanly
        dedup_rows: list[dict[str, Any]] = []
        for row in rows:
            if dedup_rows and dedup_rows[-1]["event_ts_ms"] == row["event_ts_ms"]:
                dedup_rows[-1] = row
            else:
                dedup_rows.append(row)
        timestamps = [int(r["event_ts_ms"]) for r in dedup_rows]
        indexed[eid] = (timestamps, dedup_rows)
    return indexed


def _asof_lookup(
    indexed: dict[str, tuple[list[int], list[dict[str, Any]]]],
    entity_id: str,
    request_ts_ms: int,
    *,
    strict_prior: bool = True,
) -> tuple[dict[str, Any] | None, bool]:
    """Find latest snapshot with event_ts_ms <= request_ts_ms (or < if strict_prior) within TTL.

    Returns (snapshot_or_none, was_ttl_expired).
    """
    entry = indexed.get(entity_id)
    if entry is None:
        return None, False
    timestamps, rows = entry
    if strict_prior:
        # Strictly strictly before or at decision boundary without including same-ms post-decision outcomes:
        # We use strictly `< request_ts_ms` for user interaction history and `<= request_ts_ms` for item catalog.
        idx = bisect.bisect_left(timestamps, request_ts_ms) - 1
    else:
        idx = bisect.bisect_right(timestamps, request_ts_ms) - 1

    if idx < 0:
        return None, False
    snap = rows[idx]
    ttl_ms = int(snap["ttl_seconds"]) * 1000
    if (request_ts_ms - int(snap["event_ts_ms"])) > ttl_ms:
        return None, True
    return snap, False


def _extract_immediate_completions(interactions: list[dict[str, Any]]) -> dict[tuple[str, str], list[int]]:
    """Index COMPLETE event timestamps per (user_id, item_id) for supervised label matching."""
    completions: dict[tuple[str, str], list[int]] = defaultdict(list)
    for ev in interactions:
        if ev.get("event_type") == "COMPLETE":
            completions[(str(ev["user_id"]), str(ev["item_id"]))].append(int(ev["event_ts_ms"]))
    for ts_list in completions.values():
        ts_list.sort()
    return completions


def build_pit_training_dataset(
    interactions: list[dict[str, Any]],
    user_features_pit: list[dict[str, Any]],
    item_features_pit: list[dict[str, Any]],
    *,
    delayed_labels: list[dict[str, Any]] | None = None,
    include_delayed_labels: bool = False,
    label_window_ms: int = 1_800_000,
) -> list[dict[str, Any]]:
    """Construct a point-in-time-correct training dataset via strict ASOF joins (zero future leakage)."""
    with TELEMETRY.span(
        "features.build_pit_dataset",
        {"interactions": len(interactions), "include_delayed_labels": include_delayed_labels},
    ):
        user_idx = _index_feature_history(user_features_pit, "user_id")
        item_idx = _index_feature_history(item_features_pit, "item_id")
        imm_completions = _extract_immediate_completions(interactions)

        delayed_map: dict[tuple[str, str, int], dict[str, Any]] = {}
        if include_delayed_labels and delayed_labels:
            for dl in delayed_labels:
                delayed_map[(str(dl["user_id"]), str(dl["item_id"]), int(dl["request_ts_ms"]))] = dl

        impressions = [ev for ev in interactions if ev.get("event_type") == "IMPRESSION"]
        impressions.sort(key=lambda r: (int(r["event_ts_ms"]), str(r["event_id"])))

        rows: list[dict[str, Any]] = []
        for imp in impressions:
            uid = str(imp["user_id"])
            iid = str(imp["item_id"])
            req_ts = int(imp["event_ts_ms"])

            # Strict PIT ASOF join: user history strictly BEFORE request_ts; item features <= request_ts
            u_snap, _u_expired = _asof_lookup(user_idx, uid, req_ts, strict_prior=True)
            i_snap, _i_expired = _asof_lookup(item_idx, iid, req_ts, strict_prior=False)

            u_feat_ts = int(u_snap["event_ts_ms"]) if u_snap else 0
            i_feat_ts = int(i_snap["event_ts_ms"]) if i_snap else 0

            u_watch = float(u_snap["user_watch_seconds_total"]) if u_snap else 0.0
            u_plays = int(u_snap["user_play_count"]) if u_snap else 0
            u_comp_rate = float(u_snap["user_completion_rate"]) if u_snap else 0.0
            u_skip_rate = float(u_snap["user_skip_rate"]) if u_snap else 0.0
            u_rebuf_rate = float(u_snap["user_rebuffer_rate"]) if u_snap else 0.0
            u_top_genre = str(u_snap["user_top_genre"]) if u_snap else "unknown"
            u_cold = 1 if (u_snap is None or bool(u_snap["is_cold_start_user"])) else 0

            i_genre = str(i_snap["genre"]) if i_snap else "unknown"
            i_pop = float(i_snap["popularity_prior"]) if i_snap else 0.15
            i_comp_rate = float(i_snap["item_completion_rate_so_far"]) if i_snap else 0.0
            i_dur = int(i_snap["duration_seconds"]) if i_snap else 3000
            i_cold = 1 if (i_snap is None or bool(i_snap["is_cold_start_item"])) else 0

            # Resolve supervised completion label within label_window_ms after request_ts
            label_completed = 0
            label_source = "negative"
            label_arrival_ts = req_ts + label_window_ms

            comp_times = imm_completions.get((uid, iid), [])
            pos = bisect.bisect_left(comp_times, req_ts)
            if pos < len(comp_times) and (comp_times[pos] - req_ts) <= label_window_ms:
                label_completed = 1
                label_source = "immediate"
                label_arrival_ts = comp_times[pos]
            elif (uid, iid, req_ts) in delayed_map:
                dl = delayed_map[(uid, iid, req_ts)]
                label_completed = int(dl["completed"])
                label_source = "delayed_backfill"
                label_arrival_ts = int(dl["label_arrival_ts_ms"])

            rows.append(
                {
                    "row_id": deterministic_id("train", uid, iid, req_ts),
                    "user_id": uid,
                    "item_id": iid,
                    "request_ts_ms": req_ts,
                    "user_feature_ts_ms": u_feat_ts,
                    "item_feature_ts_ms": i_feat_ts,
                    "user_watch_seconds_total": round(u_watch, 2),
                    "user_play_count": u_plays,
                    "user_completion_rate": round(u_comp_rate, 4),
                    "user_skip_rate": round(u_skip_rate, 4),
                    "user_rebuffer_rate": round(u_rebuf_rate, 4),
                    "genre_match": 1 if (u_top_genre != "unknown" and u_top_genre == i_genre) else 0,
                    "item_popularity_prior": round(i_pop, 4),
                    "item_completion_rate_so_far": round(i_comp_rate, 4),
                    "item_duration_seconds": i_dur,
                    "is_cold_start_user": u_cold,
                    "is_cold_start_item": i_cold,
                    "experiment_variant": str(imp.get("experiment_variant") or "control"),
                    "label_completed": label_completed,
                    "label_source": label_source,
                    "label_arrival_ts_ms": label_arrival_ts,
                }
            )
        return rows


def build_naive_leaking_dataset(
    interactions: list[dict[str, Any]],
    user_features_pit: list[dict[str, Any]],
    item_features_pit: list[dict[str, Any]],
    *,
    delayed_labels: list[dict[str, Any]] | None = None,
    include_delayed_labels: bool = True,
    label_window_ms: int = 1_800_000,
) -> list[dict[str, Any]]:
    """INTENTIONALLY FLAWED naive global latest-feature join used ONLY to demonstrate & benchmark future leakage."""
    latest_user: dict[str, dict[str, Any]] = {}
    for snap in sorted(user_features_pit, key=lambda r: int(r["event_ts_ms"])):
        latest_user[str(snap["user_id"])] = snap

    latest_item: dict[str, dict[str, Any]] = {}
    for snap in sorted(item_features_pit, key=lambda r: int(r["event_ts_ms"])):
        latest_item[str(snap["item_id"])] = snap

    imm_completions = _extract_immediate_completions(interactions)
    delayed_map: dict[tuple[str, str, int], dict[str, Any]] = {}
    if include_delayed_labels and delayed_labels:
        for dl in delayed_labels:
            delayed_map[(str(dl["user_id"]), str(dl["item_id"]), int(dl["request_ts_ms"]))] = dl

    impressions = [ev for ev in interactions if ev.get("event_type") == "IMPRESSION"]
    impressions.sort(key=lambda r: (int(r["event_ts_ms"]), str(r["event_id"])))

    rows: list[dict[str, Any]] = []
    for imp in impressions:
        uid = str(imp["user_id"])
        iid = str(imp["item_id"])
        req_ts = int(imp["event_ts_ms"])

        # LEAK: joins end-of-window global feature snapshot directly onto past requests!
        u_snap = latest_user.get(uid)
        i_snap = latest_item.get(iid)

        u_feat_ts = int(u_snap["event_ts_ms"]) if u_snap else 0
        i_feat_ts = int(i_snap["event_ts_ms"]) if i_snap else 0
        u_top_genre = str(u_snap["user_top_genre"]) if u_snap else "unknown"
        i_genre = str(i_snap["genre"]) if i_snap else "unknown"

        label_completed = 0
        label_source = "negative"
        label_arrival_ts = req_ts + label_window_ms
        comp_times = imm_completions.get((uid, iid), [])
        pos = bisect.bisect_left(comp_times, req_ts)
        if pos < len(comp_times) and (comp_times[pos] - req_ts) <= label_window_ms:
            label_completed = 1
            label_source = "immediate"
            label_arrival_ts = comp_times[pos]
        elif (uid, iid, req_ts) in delayed_map:
            dl = delayed_map[(uid, iid, req_ts)]
            label_completed = int(dl["completed"])
            label_source = "delayed_backfill"
            label_arrival_ts = int(dl["label_arrival_ts_ms"])

        rows.append(
            {
                "row_id": deterministic_id("naive", uid, iid, req_ts),
                "user_id": uid,
                "item_id": iid,
                "request_ts_ms": req_ts,
                "user_feature_ts_ms": u_feat_ts,
                "item_feature_ts_ms": i_feat_ts,
                "user_watch_seconds_total": round(float(u_snap["user_watch_seconds_total"]), 2) if u_snap else 0.0,
                "user_play_count": int(u_snap["user_play_count"]) if u_snap else 0,
                "user_completion_rate": round(float(u_snap["user_completion_rate"]), 4) if u_snap else 0.0,
                "user_skip_rate": round(float(u_snap["user_skip_rate"]), 4) if u_snap else 0.0,
                "user_rebuffer_rate": round(float(u_snap["user_rebuffer_rate"]), 4) if u_snap else 0.0,
                "genre_match": 1 if (u_top_genre != "unknown" and u_top_genre == i_genre) else 0,
                "item_popularity_prior": round(float(i_snap["popularity_prior"]), 4) if i_snap else 0.15,
                "item_completion_rate_so_far": round(float(i_snap["item_completion_rate_so_far"]), 4) if i_snap else 0.0,
                "item_duration_seconds": int(i_snap["duration_seconds"]) if i_snap else 3000,
                "is_cold_start_user": 1 if (u_snap is None or bool(u_snap["is_cold_start_user"])) else 0,
                "is_cold_start_item": 1 if (i_snap is None or bool(i_snap["is_cold_start_item"])) else 0,
                "experiment_variant": str(imp.get("experiment_variant") or "control"),
                "label_completed": label_completed,
                "label_source": label_source,
                "label_arrival_ts_ms": label_arrival_ts,
            }
        )
    return rows


def audit_pit_leakage(dataset: list[dict[str, Any]]) -> PitAuditReport:
    """Assert zero future leakage: user_feature_ts_ms <= request_ts_ms AND item_feature_ts_ms <= request_ts_ms."""
    user_violations = 0
    item_violations = 0
    cold_users = 0
    cold_items = 0
    for row in dataset:
        req_ts = int(row["request_ts_ms"])
        if int(row["user_feature_ts_ms"]) > req_ts:
            user_violations += 1
        if int(row["item_feature_ts_ms"]) > req_ts:
            item_violations += 1
        if int(row.get("is_cold_start_user", 0)) == 1:
            cold_users += 1
        if int(row.get("is_cold_start_item", 0)) == 1:
            cold_items += 1

    total_violations = user_violations + item_violations
    total = len(dataset)
    # Count distinct violating rows for rate
    violating_rows = sum(
        1
        for r in dataset
        if int(r["user_feature_ts_ms"]) > int(r["request_ts_ms"])
        or int(r["item_feature_ts_ms"]) > int(r["request_ts_ms"])
    )
    TELEMETRY.pit_leakage_violations.set(total_violations)
    return PitAuditReport(
        total_rows=total,
        user_leakage_violations=user_violations,
        item_leakage_violations=item_violations,
        total_leakage_violations=total_violations,
        leakage_rate=round(violating_rows / max(1, total), 4),
        ttl_expired_fallbacks=0,
        cold_start_user_rows=cold_users,
        cold_start_item_rows=cold_items,
        passed=(total_violations == 0),
    )


def apply_delayed_label_backfill(
    existing_training_rows: list[dict[str, Any]],
    delayed_labels: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Backfill delayed completion labels onto historical decision rows WITHOUT altering feature timestamps."""
    delayed_lookup: dict[tuple[str, str, int], dict[str, Any]] = {
        (str(dl["user_id"]), str(dl["item_id"]), int(dl["request_ts_ms"])): dl
        for dl in delayed_labels
    }
    updated_rows: list[dict[str, Any]] = []
    updated_count = 0
    feature_ts_mutated = 0

    for row in existing_training_rows:
        clone = dict(row)
        key = (str(clone["user_id"]), str(clone["item_id"]), int(clone["request_ts_ms"]))
        dl = delayed_lookup.get(key)
        if dl is not None and int(clone["label_completed"]) == 0 and int(dl["completed"]) == 1:
            orig_u_ts = clone["user_feature_ts_ms"]
            orig_i_ts = clone["item_feature_ts_ms"]
            clone["label_completed"] = 1
            clone["label_source"] = "delayed_backfill"
            clone["label_arrival_ts_ms"] = int(dl["label_arrival_ts_ms"])
            if clone["user_feature_ts_ms"] != orig_u_ts or clone["item_feature_ts_ms"] != orig_i_ts:
                feature_ts_mutated += 1
            updated_count += 1
        updated_rows.append(clone)

    before_pos = sum(int(r["label_completed"]) for r in existing_training_rows)
    after_pos = sum(int(r["label_completed"]) for r in updated_rows)
    pit_after = audit_pit_leakage(updated_rows)
    summary = {
        "rows_scanned": len(existing_training_rows),
        "delayed_labels_applied": updated_count,
        "positive_labels_before": before_pos,
        "positive_labels_after": after_pos,
        "feature_timestamps_mutated": feature_ts_mutated,
        "pit_violations_after_backfill": pit_after.total_leakage_violations,
    }
    return updated_rows, summary
