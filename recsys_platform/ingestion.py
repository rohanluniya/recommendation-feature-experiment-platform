"""Idempotent ingestion gateway, durable local topic spool with replay offsets, and DLQ quarantine."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from recsys_platform.logging_utils import get_logger, log_event
from recsys_platform.observability import TELEMETRY
from recsys_platform.schemas import SCHEMA_REGISTRY

logger = get_logger("recsys_platform.ingestion")


@dataclass(slots=True)
class IngestionResult:
    """Counts and split outputs from an idempotent ingestion pass."""

    valid_events: list[dict[str, Any]]
    dlq_events: list[dict[str, Any]]
    duplicates_dropped: int
    total_received: int
    topic_offsets: dict[str, int] = field(default_factory=dict)


class LocalTopicSpool:
    """Append-only partitioned JSONL spool mimicking Kafka topic logs with replay offsets.

    Supports simulated broker outage mode where records buffer in a pending WAL
    and drain idempotently once broker connectivity recovers.
    """

    def __init__(self, spool_dir: Path) -> None:
        self.spool_dir = spool_dir
        self.spool_dir.mkdir(parents=True, exist_ok=True)
        self.broker_available = True
        self._pending_buffer: dict[str, list[dict[str, Any]]] = {}

    def _topic_path(self, topic: str) -> Path:
        return self.spool_dir / f"{topic}.jsonl"

    def _checkpoint_path(self, consumer_group: str, topic: str) -> Path:
        return self.spool_dir / f"offset_{consumer_group}_{topic}.json"

    def current_offset(self, topic: str) -> int:
        path = self._topic_path(topic)
        if not path.exists():
            return 0
        with path.open("r", encoding="utf-8") as fh:
            return sum(1 for _ in fh)

    def produce(self, topic: str, records: list[dict[str, Any]], key_field: str = "user_id") -> int:
        """Append records to durable topic log (or local outage WAL if broker is down)."""
        if not self.broker_available:
            self._pending_buffer.setdefault(topic, []).extend(records)
            return 0
        offset = self.current_offset(topic)
        path = self._topic_path(topic)
        with path.open("a", encoding="utf-8") as fh:
            for rec in records:
                envelope = {
                    "topic": topic,
                    "offset": offset,
                    "partition_key": str(rec.get(key_field, "unknown")),
                    "produced_ts_ms": int(time.time() * 1000),
                    "payload": rec,
                }
                fh.write(json.dumps(envelope, default=str) + "\n")
                offset += 1
        return len(records)

    def drain_outage_buffer(self, topic: str, key_field: str = "user_id") -> int:
        """Flush buffered events after simulated broker recovery."""
        self.broker_available = True
        buffered = self._pending_buffer.pop(topic, [])
        if not buffered:
            return 0
        return self.produce(topic, buffered, key_field=key_field)

    def read_from_offset(self, topic: str, start_offset: int = 0) -> list[dict[str, Any]]:
        """Replay events deterministically from any historical offset."""
        path = self._topic_path(topic)
        if not path.exists():
            return []
        envelopes: list[dict[str, Any]] = []
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                if not line.strip():
                    continue
                env = json.loads(line)
                if env["offset"] >= start_offset:
                    envelopes.append(env)
        return envelopes

    def commit_offset(self, consumer_group: str, topic: str, next_offset: int) -> None:
        cp = self._checkpoint_path(consumer_group, topic)
        cp.write_text(json.dumps({"consumer_group": consumer_group, "topic": topic, "next_offset": next_offset}))

    def get_committed_offset(self, consumer_group: str, topic: str) -> int:
        cp = self._checkpoint_path(consumer_group, topic)
        if not cp.exists():
            return 0
        data = json.loads(cp.read_text(encoding="utf-8"))
        return int(data.get("next_offset", 0))


def validate_and_quarantine_interactions(
    raw_events: list[dict[str, Any]],
    *,
    seen_event_ids: set[str] | None = None,
    spool: LocalTopicSpool | None = None,
) -> IngestionResult:
    """Validate Avro v1/v2 contracts + semantic invariants, deduplicate by event_id, and route bad events to DLQ."""
    seen = seen_event_ids if seen_event_ids is not None else set()
    valid: list[dict[str, Any]] = []
    dlq: list[dict[str, Any]] = []
    dups = 0

    with TELEMETRY.span("ingestion.validate_interactions", {"batch_size": len(raw_events)}):
        for raw in raw_events:
            schema_ver = raw.get("schema_version", 1)
            schema_name = "playback_interaction_v2" if schema_ver == 2 else "playback_interaction_v1"

            # Normalize optional fields for validation cleanly
            candidate = dict(raw)
            if schema_name == "playback_interaction_v2":
                candidate.setdefault("device_os_version", None)
                candidate.setdefault("playback_bitrate_kbps", None)

            ok, err = SCHEMA_REGISTRY.validate_record(schema_name, candidate)
            reason: str | None = None
            if not ok:
                reason = f"avro_schema_error:{err}"
            else:
                # Semantic guardrails beyond Avro structural typing
                if float(candidate.get("watch_seconds", 0.0)) < 0.0:
                    reason = "semantic_error:negative_watch_seconds"
                elif float(candidate.get("position_seconds", 0.0)) < 0.0:
                    reason = "semantic_error:negative_position_seconds"
                elif int(candidate.get("event_ts_ms", 0)) <= 0:
                    reason = "semantic_error:invalid_event_ts_ms"
                elif not str(candidate.get("user_id", "")).strip():
                    reason = "semantic_error:empty_user_id"

            if reason is not None:
                dlq_rec = {
                    "dlq_id": f"dlq_{len(dlq)}_{candidate.get('event_id', 'missing')}",
                    "event_id": str(candidate.get("event_id", "unknown")),
                    "reason": reason,
                    "schema_version": int(schema_ver) if isinstance(schema_ver, int) else -1,
                    "source_system": str(candidate.get("source_system", "playback-gateway")),
                    "quarantined_ts_ms": int(time.time() * 1000),
                    "raw_payload_json": json.dumps(raw, default=str),
                }
                dlq.append(dlq_rec)
                TELEMETRY.dlq_events.labels(reason=reason.split(":")[0]).inc()
                TELEMETRY.events_ingested.labels(stream="interactions", status="quarantined").inc()
                continue

            ev_id = str(candidate["event_id"])
            if ev_id in seen:
                dups += 1
                TELEMETRY.events_ingested.labels(stream="interactions", status="duplicate_dropped").inc()
                continue
            seen.add(ev_id)

            # Promote v1 records smoothly to unified v2 canonical shape (null defaults)
            candidate.setdefault("device_os_version", None)
            candidate.setdefault("playback_bitrate_kbps", None)
            valid.append(candidate)
            TELEMETRY.events_ingested.labels(stream="interactions", status="accepted").inc()

        offsets: dict[str, int] = {}
        if spool is not None:
            spool.produce("playback.interactions.raw", valid, key_field="user_id")
            spool.produce("playback.interactions.dlq", dlq, key_field="event_id")
            offsets["playback.interactions.raw"] = spool.current_offset("playback.interactions.raw")
            offsets["playback.interactions.dlq"] = spool.current_offset("playback.interactions.dlq")

    log_event(
        logger,
        20,
        "Ingested interaction batch",
        total_received=len(raw_events),
        valid_count=len(valid),
        duplicates_dropped=dups,
        dlq_count=len(dlq),
    )
    return IngestionResult(
        valid_events=valid,
        dlq_events=dlq,
        duplicates_dropped=dups,
        total_received=len(raw_events),
        topic_offsets=offsets,
    )
