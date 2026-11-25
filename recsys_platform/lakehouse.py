"""Apache Iceberg V2 Medallion Lakehouse (Bronze/Silver/Gold/Quality) with DuckDB SQL, compaction, and time travel."""

from __future__ import annotations

import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
)

from recsys_platform.config import PlatformConfig
from recsys_platform.observability import TELEMETRY


def _ms_to_day(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=UTC).strftime("%Y-%m-%d")


TABLE_SCHEMAS: dict[str, tuple[Schema, pa.Schema, PartitionSpec | None]] = {
    "bronze.interaction_events": (
        Schema(
            NestedField(1, "event_id", StringType(), required=True),
            NestedField(2, "event_type", StringType(), required=True),
            NestedField(3, "user_id", StringType(), required=True),
            NestedField(4, "item_id", StringType(), required=True),
            NestedField(5, "device_id", StringType(), required=True),
            NestedField(6, "device_type", StringType(), required=True),
            NestedField(7, "event_ts_ms", LongType(), required=True),
            NestedField(8, "ingest_ts_ms", LongType(), required=True),
            NestedField(9, "event_day", StringType(), required=True),
            NestedField(10, "watch_seconds", DoubleType(), required=True),
            NestedField(11, "position_seconds", DoubleType(), required=True),
            NestedField(12, "rebuffer_count", IntegerType(), required=True),
            NestedField(13, "search_query", StringType(), required=False),
            NestedField(14, "experiment_id", StringType(), required=False),
            NestedField(15, "experiment_variant", StringType(), required=False),
            NestedField(16, "source_system", StringType(), required=True),
            NestedField(17, "schema_version", IntegerType(), required=True),
            NestedField(18, "device_os_version", StringType(), required=False),
            NestedField(19, "playback_bitrate_kbps", IntegerType(), required=False),
        ),
        pa.schema(
            [
                pa.field("event_id", pa.string(), nullable=False),
                pa.field("event_type", pa.string(), nullable=False),
                pa.field("user_id", pa.string(), nullable=False),
                pa.field("item_id", pa.string(), nullable=False),
                pa.field("device_id", pa.string(), nullable=False),
                pa.field("device_type", pa.string(), nullable=False),
                pa.field("event_ts_ms", pa.int64(), nullable=False),
                pa.field("ingest_ts_ms", pa.int64(), nullable=False),
                pa.field("event_day", pa.string(), nullable=False),
                pa.field("watch_seconds", pa.float64(), nullable=False),
                pa.field("position_seconds", pa.float64(), nullable=False),
                pa.field("rebuffer_count", pa.int32(), nullable=False),
                pa.field("search_query", pa.string(), nullable=True),
                pa.field("experiment_id", pa.string(), nullable=True),
                pa.field("experiment_variant", pa.string(), nullable=True),
                pa.field("source_system", pa.string(), nullable=False),
                pa.field("schema_version", pa.int32(), nullable=False),
                pa.field("device_os_version", pa.string(), nullable=True),
                pa.field("playback_bitrate_kbps", pa.int32(), nullable=True),
            ]
        ),
        PartitionSpec(
            PartitionField(source_id=9, field_id=1000, transform=IdentityTransform(), name="event_day")
        ),
    ),
    "bronze.catalog_changes": (
        Schema(
            NestedField(1, "change_id", StringType(), required=True),
            NestedField(2, "op", StringType(), required=True),
            NestedField(3, "item_id", StringType(), required=True),
            NestedField(4, "title", StringType(), required=True),
            NestedField(5, "genre", StringType(), required=True),
            NestedField(6, "duration_seconds", IntegerType(), required=True),
            NestedField(7, "release_year", IntegerType(), required=True),
            NestedField(8, "maturity_rating", StringType(), required=True),
            NestedField(9, "popularity_prior", DoubleType(), required=True),
            NestedField(10, "is_active", BooleanType(), required=True),
            NestedField(11, "event_ts_ms", LongType(), required=True),
            NestedField(12, "ingest_ts_ms", LongType(), required=True),
            NestedField(13, "source_system", StringType(), required=True),
        ),
        pa.schema(
            [
                pa.field("change_id", pa.string(), nullable=False),
                pa.field("op", pa.string(), nullable=False),
                pa.field("item_id", pa.string(), nullable=False),
                pa.field("title", pa.string(), nullable=False),
                pa.field("genre", pa.string(), nullable=False),
                pa.field("duration_seconds", pa.int32(), nullable=False),
                pa.field("release_year", pa.int32(), nullable=False),
                pa.field("maturity_rating", pa.string(), nullable=False),
                pa.field("popularity_prior", pa.float64(), nullable=False),
                pa.field("is_active", pa.bool_(), nullable=False),
                pa.field("event_ts_ms", pa.int64(), nullable=False),
                pa.field("ingest_ts_ms", pa.int64(), nullable=False),
                pa.field("source_system", pa.string(), nullable=False),
            ]
        ),
        None,
    ),
    "bronze.quarantine_dlq": (
        Schema(
            NestedField(1, "dlq_id", StringType(), required=True),
            NestedField(2, "event_id", StringType(), required=True),
            NestedField(3, "reason", StringType(), required=True),
            NestedField(4, "schema_version", IntegerType(), required=True),
            NestedField(5, "source_system", StringType(), required=True),
            NestedField(6, "quarantined_ts_ms", LongType(), required=True),
            NestedField(7, "raw_payload_json", StringType(), required=True),
        ),
        pa.schema(
            [
                pa.field("dlq_id", pa.string(), nullable=False),
                pa.field("event_id", pa.string(), nullable=False),
                pa.field("reason", pa.string(), nullable=False),
                pa.field("schema_version", pa.int32(), nullable=False),
                pa.field("source_system", pa.string(), nullable=False),
                pa.field("quarantined_ts_ms", pa.int64(), nullable=False),
                pa.field("raw_payload_json", pa.string(), nullable=False),
            ]
        ),
        None,
    ),
    "silver.viewing_sessions": (
        Schema(
            NestedField(1, "session_id", StringType(), required=True),
            NestedField(2, "user_id", StringType(), required=True),
            NestedField(3, "session_start_ts_ms", LongType(), required=True),
            NestedField(4, "session_end_ts_ms", LongType(), required=True),
            NestedField(5, "session_duration_seconds", DoubleType(), required=True),
            NestedField(6, "event_count", IntegerType(), required=True),
            NestedField(7, "heartbeat_count", IntegerType(), required=True),
            NestedField(8, "total_watch_seconds", DoubleType(), required=True),
            NestedField(9, "rebuffer_count", IntegerType(), required=True),
            NestedField(10, "completions", IntegerType(), required=True),
            NestedField(11, "skips", IntegerType(), required=True),
            NestedField(12, "searches", IntegerType(), required=True),
            NestedField(13, "thumbs_up", IntegerType(), required=True),
            NestedField(14, "thumbs_down", IntegerType(), required=True),
            NestedField(15, "device_switch_count", IntegerType(), required=True),
            NestedField(16, "distinct_devices", IntegerType(), required=True),
            NestedField(17, "distinct_items", IntegerType(), required=True),
            NestedField(18, "revision", IntegerType(), required=True),
            NestedField(19, "changelog_op", StringType(), required=True),
            NestedField(20, "had_late_retraction", BooleanType(), required=True),
            NestedField(21, "emitted_watermark_ms", LongType(), required=True),
            NestedField(22, "experiment_variant", StringType(), required=True),
        ),
        pa.schema(
            [
                pa.field("session_id", pa.string(), nullable=False),
                pa.field("user_id", pa.string(), nullable=False),
                pa.field("session_start_ts_ms", pa.int64(), nullable=False),
                pa.field("session_end_ts_ms", pa.int64(), nullable=False),
                pa.field("session_duration_seconds", pa.float64(), nullable=False),
                pa.field("event_count", pa.int32(), nullable=False),
                pa.field("heartbeat_count", pa.int32(), nullable=False),
                pa.field("total_watch_seconds", pa.float64(), nullable=False),
                pa.field("rebuffer_count", pa.int32(), nullable=False),
                pa.field("completions", pa.int32(), nullable=False),
                pa.field("skips", pa.int32(), nullable=False),
                pa.field("searches", pa.int32(), nullable=False),
                pa.field("thumbs_up", pa.int32(), nullable=False),
                pa.field("thumbs_down", pa.int32(), nullable=False),
                pa.field("device_switch_count", pa.int32(), nullable=False),
                pa.field("distinct_devices", pa.int32(), nullable=False),
                pa.field("distinct_items", pa.int32(), nullable=False),
                pa.field("revision", pa.int32(), nullable=False),
                pa.field("changelog_op", pa.string(), nullable=False),
                pa.field("had_late_retraction", pa.bool_(), nullable=False),
                pa.field("emitted_watermark_ms", pa.int64(), nullable=False),
                pa.field("experiment_variant", pa.string(), nullable=False),
            ]
        ),
        None,
    ),
    "silver.delayed_labels": (
        Schema(
            NestedField(1, "label_id", StringType(), required=True),
            NestedField(2, "user_id", StringType(), required=True),
            NestedField(3, "item_id", StringType(), required=True),
            NestedField(4, "request_ts_ms", LongType(), required=True),
            NestedField(5, "label_event_ts_ms", LongType(), required=True),
            NestedField(6, "label_arrival_ts_ms", LongType(), required=True),
            NestedField(7, "completed", IntegerType(), required=True),
            NestedField(8, "converted_subscription", IntegerType(), required=True),
            NestedField(9, "delayed_watch_seconds", DoubleType(), required=True),
            NestedField(10, "experiment_id", StringType(), required=True),
            NestedField(11, "experiment_variant", StringType(), required=True),
        ),
        pa.schema(
            [
                pa.field("label_id", pa.string(), nullable=False),
                pa.field("user_id", pa.string(), nullable=False),
                pa.field("item_id", pa.string(), nullable=False),
                pa.field("request_ts_ms", pa.int64(), nullable=False),
                pa.field("label_event_ts_ms", pa.int64(), nullable=False),
                pa.field("label_arrival_ts_ms", pa.int64(), nullable=False),
                pa.field("completed", pa.int32(), nullable=False),
                pa.field("converted_subscription", pa.int32(), nullable=False),
                pa.field("delayed_watch_seconds", pa.float64(), nullable=False),
                pa.field("experiment_id", pa.string(), nullable=False),
                pa.field("experiment_variant", pa.string(), nullable=False),
            ]
        ),
        None,
    ),
    "gold.user_features_pit": (
        Schema(
            NestedField(1, "user_id", StringType(), required=True),
            NestedField(2, "event_ts_ms", LongType(), required=True),
            NestedField(3, "created_ts_ms", LongType(), required=True),
            NestedField(4, "ttl_seconds", IntegerType(), required=True),
            NestedField(5, "user_watch_seconds_total", DoubleType(), required=True),
            NestedField(6, "user_play_count", IntegerType(), required=True),
            NestedField(7, "user_completion_count", IntegerType(), required=True),
            NestedField(8, "user_completion_rate", DoubleType(), required=True),
            NestedField(9, "user_skip_rate", DoubleType(), required=True),
            NestedField(10, "user_rebuffer_rate", DoubleType(), required=True),
            NestedField(11, "user_device_switches", IntegerType(), required=True),
            NestedField(12, "user_top_genre", StringType(), required=True),
            NestedField(13, "is_cold_start_user", BooleanType(), required=True),
        ),
        pa.schema(
            [
                pa.field("user_id", pa.string(), nullable=False),
                pa.field("event_ts_ms", pa.int64(), nullable=False),
                pa.field("created_ts_ms", pa.int64(), nullable=False),
                pa.field("ttl_seconds", pa.int32(), nullable=False),
                pa.field("user_watch_seconds_total", pa.float64(), nullable=False),
                pa.field("user_play_count", pa.int32(), nullable=False),
                pa.field("user_completion_count", pa.int32(), nullable=False),
                pa.field("user_completion_rate", pa.float64(), nullable=False),
                pa.field("user_skip_rate", pa.float64(), nullable=False),
                pa.field("user_rebuffer_rate", pa.float64(), nullable=False),
                pa.field("user_device_switches", pa.int32(), nullable=False),
                pa.field("user_top_genre", pa.string(), nullable=False),
                pa.field("is_cold_start_user", pa.bool_(), nullable=False),
            ]
        ),
        None,
    ),
    "gold.item_features_pit": (
        Schema(
            NestedField(1, "item_id", StringType(), required=True),
            NestedField(2, "event_ts_ms", LongType(), required=True),
            NestedField(3, "created_ts_ms", LongType(), required=True),
            NestedField(4, "ttl_seconds", IntegerType(), required=True),
            NestedField(5, "title", StringType(), required=True),
            NestedField(6, "genre", StringType(), required=True),
            NestedField(7, "duration_seconds", IntegerType(), required=True),
            NestedField(8, "release_year", IntegerType(), required=True),
            NestedField(9, "popularity_prior", DoubleType(), required=True),
            NestedField(10, "is_active", BooleanType(), required=True),
            NestedField(11, "item_impressions_so_far", IntegerType(), required=True),
            NestedField(12, "item_plays_so_far", IntegerType(), required=True),
            NestedField(13, "item_completion_rate_so_far", DoubleType(), required=True),
            NestedField(14, "is_cold_start_item", BooleanType(), required=True),
        ),
        pa.schema(
            [
                pa.field("item_id", pa.string(), nullable=False),
                pa.field("event_ts_ms", pa.int64(), nullable=False),
                pa.field("created_ts_ms", pa.int64(), nullable=False),
                pa.field("ttl_seconds", pa.int32(), nullable=False),
                pa.field("title", pa.string(), nullable=False),
                pa.field("genre", pa.string(), nullable=False),
                pa.field("duration_seconds", pa.int32(), nullable=False),
                pa.field("release_year", pa.int32(), nullable=False),
                pa.field("popularity_prior", pa.float64(), nullable=False),
                pa.field("is_active", pa.bool_(), nullable=False),
                pa.field("item_impressions_so_far", pa.int32(), nullable=False),
                pa.field("item_plays_so_far", pa.int32(), nullable=False),
                pa.field("item_completion_rate_so_far", pa.float64(), nullable=False),
                pa.field("is_cold_start_item", pa.bool_(), nullable=False),
            ]
        ),
        None,
    ),
    "gold.training_dataset": (
        Schema(
            NestedField(1, "row_id", StringType(), required=True),
            NestedField(2, "user_id", StringType(), required=True),
            NestedField(3, "item_id", StringType(), required=True),
            NestedField(4, "request_ts_ms", LongType(), required=True),
            NestedField(5, "user_feature_ts_ms", LongType(), required=True),
            NestedField(6, "item_feature_ts_ms", LongType(), required=True),
            NestedField(7, "user_watch_seconds_total", DoubleType(), required=True),
            NestedField(8, "user_play_count", IntegerType(), required=True),
            NestedField(9, "user_completion_rate", DoubleType(), required=True),
            NestedField(10, "user_skip_rate", DoubleType(), required=True),
            NestedField(11, "user_rebuffer_rate", DoubleType(), required=True),
            NestedField(12, "genre_match", IntegerType(), required=True),
            NestedField(13, "item_popularity_prior", DoubleType(), required=True),
            NestedField(14, "item_completion_rate_so_far", DoubleType(), required=True),
            NestedField(15, "item_duration_seconds", IntegerType(), required=True),
            NestedField(16, "is_cold_start_user", IntegerType(), required=True),
            NestedField(17, "is_cold_start_item", IntegerType(), required=True),
            NestedField(18, "experiment_variant", StringType(), required=True),
            NestedField(19, "label_completed", IntegerType(), required=True),
            NestedField(20, "label_source", StringType(), required=True),
            NestedField(21, "label_arrival_ts_ms", LongType(), required=True),
        ),
        pa.schema(
            [
                pa.field("row_id", pa.string(), nullable=False),
                pa.field("user_id", pa.string(), nullable=False),
                pa.field("item_id", pa.string(), nullable=False),
                pa.field("request_ts_ms", pa.int64(), nullable=False),
                pa.field("user_feature_ts_ms", pa.int64(), nullable=False),
                pa.field("item_feature_ts_ms", pa.int64(), nullable=False),
                pa.field("user_watch_seconds_total", pa.float64(), nullable=False),
                pa.field("user_play_count", pa.int32(), nullable=False),
                pa.field("user_completion_rate", pa.float64(), nullable=False),
                pa.field("user_skip_rate", pa.float64(), nullable=False),
                pa.field("user_rebuffer_rate", pa.float64(), nullable=False),
                pa.field("genre_match", pa.int32(), nullable=False),
                pa.field("item_popularity_prior", pa.float64(), nullable=False),
                pa.field("item_completion_rate_so_far", pa.float64(), nullable=False),
                pa.field("item_duration_seconds", pa.int32(), nullable=False),
                pa.field("is_cold_start_user", pa.int32(), nullable=False),
                pa.field("is_cold_start_item", pa.int32(), nullable=False),
                pa.field("experiment_variant", pa.string(), nullable=False),
                pa.field("label_completed", pa.int32(), nullable=False),
                pa.field("label_source", pa.string(), nullable=False),
                pa.field("label_arrival_ts_ms", pa.int64(), nullable=False),
            ]
        ),
        None,
    ),
    "gold.experiment_exposures": (
        Schema(
            NestedField(1, "exposure_id", StringType(), required=True),
            NestedField(2, "experiment_id", StringType(), required=True),
            NestedField(3, "variant", StringType(), required=True),
            NestedField(4, "user_id", StringType(), required=True),
            NestedField(5, "request_id", StringType(), required=True),
            NestedField(6, "exposure_ts_ms", LongType(), required=True),
            NestedField(7, "pre_experiment_watch_minutes", DoubleType(), required=True),
            NestedField(8, "assigned_bucket_hash", IntegerType(), required=True),
            NestedField(9, "ranker_model_version", StringType(), required=True),
            NestedField(10, "serving_latency_ms", DoubleType(), required=True),
        ),
        pa.schema(
            [
                pa.field("exposure_id", pa.string(), nullable=False),
                pa.field("experiment_id", pa.string(), nullable=False),
                pa.field("variant", pa.string(), nullable=False),
                pa.field("user_id", pa.string(), nullable=False),
                pa.field("request_id", pa.string(), nullable=False),
                pa.field("exposure_ts_ms", pa.int64(), nullable=False),
                pa.field("pre_experiment_watch_minutes", pa.float64(), nullable=False),
                pa.field("assigned_bucket_hash", pa.int32(), nullable=False),
                pa.field("ranker_model_version", pa.string(), nullable=False),
                pa.field("serving_latency_ms", pa.float64(), nullable=False),
            ]
        ),
        None,
    ),
    "quality.dq_results": (
        Schema(
            NestedField(1, "check_id", StringType(), required=True),
            NestedField(2, "run_id", StringType(), required=True),
            NestedField(3, "table_name", StringType(), required=True),
            NestedField(4, "check_name", StringType(), required=True),
            NestedField(5, "status", StringType(), required=True),
            NestedField(6, "observed_value", DoubleType(), required=True),
            NestedField(7, "threshold", DoubleType(), required=True),
            NestedField(8, "details", StringType(), required=True),
            NestedField(9, "checked_ts_ms", LongType(), required=True),
        ),
        pa.schema(
            [
                pa.field("check_id", pa.string(), nullable=False),
                pa.field("run_id", pa.string(), nullable=False),
                pa.field("table_name", pa.string(), nullable=False),
                pa.field("check_name", pa.string(), nullable=False),
                pa.field("status", pa.string(), nullable=False),
                pa.field("observed_value", pa.float64(), nullable=False),
                pa.field("threshold", pa.float64(), nullable=False),
                pa.field("details", pa.string(), nullable=False),
                pa.field("checked_ts_ms", pa.int64(), nullable=False),
            ]
        ),
        None,
    ),
}


class IcebergLakehouse:
    """Manages Apache Iceberg V2 Medallion tables, snapshots, compaction, and DuckDB queries."""

    def __init__(self, config: PlatformConfig) -> None:
        self.config = config
        self.config.ensure_dirs()
        self.warehouse_path: Path = self.config.warehouse_dir.resolve()
        self.catalog_uri = f"sqlite:///{self.config.catalog_db_path.resolve()}"
        self.catalog = SqlCatalog(
            "recsys_iceberg",
            **{
                "uri": self.catalog_uri,
                "warehouse": f"file://{self.warehouse_path}",
            },
        )
        self._init_namespaces_and_tables()

    def _init_namespaces_and_tables(self) -> None:
        for ns in ("bronze", "silver", "gold", "quality"):
            try:
                self.catalog.create_namespace(ns)
            except NamespaceAlreadyExistsError:
                pass

        for table_id, (ice_schema, _pa_schema, part_spec) in TABLE_SCHEMAS.items():
            try:
                self.catalog.load_table(table_id)
            except NoSuchTableError:
                kwargs: dict[str, Any] = {
                    "identifier": table_id,
                    "schema": ice_schema,
                    "properties": {
                        "format-version": "2",
                        "write.parquet.compression-codec": "zstd",
                    },
                }
                if part_spec is not None:
                    kwargs["partition_spec"] = part_spec
                self.catalog.create_table(**kwargs)

    def get_table(self, table_id: str) -> Table:
        return self.catalog.load_table(table_id)

    def _records_to_arrow(self, table_id: str, records: list[dict[str, Any]]) -> pa.Table:
        _, pa_schema, _ = TABLE_SCHEMAS[table_id]
        if not records:
            empty_cols = {f.name: pa.array([], type=f.type) for f in pa_schema}
            return pa.table(empty_cols, schema=pa_schema)

        # Derive event_day automatically for bronze.interaction_events
        if table_id == "bronze.interaction_events":
            prepared = []
            for r in records:
                row = dict(r)
                row.setdefault("event_day", _ms_to_day(int(row["event_ts_ms"])))
                row.setdefault("search_query", None)
                row.setdefault("experiment_id", None)
                row.setdefault("experiment_variant", None)
                row.setdefault("device_os_version", None)
                row.setdefault("playback_bitrate_kbps", None)
                prepared.append(row)
            records = prepared

        columns: dict[str, pa.Array] = {}
        for field in pa_schema:
            vals = [r.get(field.name) for r in records]
            columns[field.name] = pa.array(vals, type=field.type)
        return pa.table(columns, schema=pa_schema)

    def write_table(self, table_id: str, records: list[dict[str, Any]], *, mode: str = "overwrite") -> int:
        """Write records into an Iceberg V2 table using atomic overwrite or append."""
        with TELEMETRY.span("iceberg.write", {"table": table_id, "mode": mode, "rows": len(records)}):
            arrow_tbl = self._records_to_arrow(table_id, records)
            table = self.get_table(table_id)
            if mode == "append":
                if arrow_tbl.num_rows > 0:
                    table.append(arrow_tbl)
            elif mode == "overwrite":
                if table.current_snapshot() is None:
                    if arrow_tbl.num_rows > 0:
                        table.append(arrow_tbl)
                else:
                    table.overwrite(arrow_tbl)
            else:
                raise ValueError(f"Unsupported write mode: {mode}")
            return int(arrow_tbl.num_rows)

    def read_arrow(self, table_id: str, snapshot_id: int | None = None) -> pa.Table:
        """Read current or historical (time-travel) Iceberg table snapshot as PyArrow Table."""
        table = self.get_table(table_id)
        scan = table.scan(snapshot_id=snapshot_id) if snapshot_id is not None else table.scan()
        return scan.to_arrow()

    def read_records(self, table_id: str, snapshot_id: int | None = None) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = self.read_arrow(table_id, snapshot_id=snapshot_id).to_pylist()
        return rows

    def snapshot_history(self, table_id: str) -> list[dict[str, Any]]:
        table = self.get_table(table_id)
        snaps: list[dict[str, Any]] = []
        for snap in table.snapshots():
            summary_map = dict(snap.summary.additional_properties) if snap.summary else {}
            snaps.append(
                {
                    "snapshot_id": snap.snapshot_id,
                    "parent_snapshot_id": snap.parent_snapshot_id,
                    "timestamp_ms": snap.timestamp_ms,
                    "operation": str(snap.summary.operation.value) if snap.summary and snap.summary.operation else "unknown",
                    "total_records": int(summary_map.get("total-records", 0)),
                    "total_data_files": int(summary_map.get("total-data-files", 0)),
                }
            )
        return snaps

    def data_file_count(self, table_id: str) -> int:
        table = self.get_table(table_id)
        return sum(1 for _ in table.scan().plan_files())

    def compact_table(self, table_id: str) -> dict[str, Any]:
        """Compact small Iceberg Parquet files into consolidated files and measure before/after scan time."""
        with TELEMETRY.span("iceberg.compact", {"table": table_id}):
            files_before = self.data_file_count(table_id)
            t0 = time.perf_counter()
            arrow_tbl = self.read_arrow(table_id)
            scan_before_ms = (time.perf_counter() - t0) * 1000.0

            # Combine chunks and atomically overwrite into compacted file(s)
            combined = arrow_tbl.combine_chunks()
            table = self.get_table(table_id)
            table.overwrite(combined)

            files_after = self.data_file_count(table_id)
            t1 = time.perf_counter()
            _ = self.read_arrow(table_id)
            scan_after_ms = (time.perf_counter() - t1) * 1000.0

            return {
                "table_id": table_id,
                "row_count": combined.num_rows,
                "files_before": files_before,
                "files_after": files_after,
                "scan_before_ms": round(scan_before_ms, 3),
                "scan_after_ms": round(scan_after_ms, 3),
                "file_reduction_pct": round(
                    ((files_before - files_after) / max(1, files_before)) * 100.0, 2
                ),
            }

    def table_inventory(self) -> list[dict[str, Any]]:
        """Summarize row counts, file counts, and snapshots across all Medallion tables."""
        inv: list[dict[str, Any]] = []
        for table_id in TABLE_SCHEMAS:
            tbl = self.read_arrow(table_id)
            snaps = self.snapshot_history(table_id)
            inv.append(
                {
                    "table_id": table_id,
                    "row_count": tbl.num_rows,
                    "data_files": self.data_file_count(table_id),
                    "snapshot_count": len(snaps),
                    "latest_snapshot_id": snaps[-1]["snapshot_id"] if snaps else None,
                }
            )
        return inv

    def query_sql(self, sql: str, tables: list[str] | None = None) -> list[dict[str, Any]]:
        """Run DuckDB analytical SQL over live Iceberg V2 Arrow scans."""
        target_tables = tables or list(TABLE_SCHEMAS.keys())
        con = duckdb.connect(":memory:")
        try:
            for tbl_id in target_tables:
                alias = tbl_id.replace(".", "_")
                con.register(alias, self.read_arrow(tbl_id))
            rel = con.execute(sql)
            cols = [desc[0] for desc in (rel.description or [])]
            return [dict(zip(cols, row, strict=False)) for row in rel.fetchall()]
        finally:
            con.close()
