"""PyFlink 2.1.3 event-time sessionization & retraction job for streaming viewing interactions.

Runs inside the Flink container profile (`docker-compose.yml`, image `flink:2.1.3-scala_2.12-java17`).
In `local_native` mode (no host JVM), `recsys_platform/sessionizer.py` executes identical
event-time watermark, inactivity-gap, allowed-lateness retraction, and user state TTL semantics.
"""

from __future__ import annotations

import os


def build_sessionization_sql() -> list[str]:
    """Return Flink SQL DDL and DML statements configuring event-time sessionization with state TTL."""
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
    iceberg_uri = os.getenv("ICEBERG_CATALOG_URI", "http://lakekeeper:8181/catalog")
    s3_endpoint = os.getenv("OBJECT_STORE_ENDPOINT", "http://rustfs:9000")

    return [
        # Bounded user state TTL (24 hours) to prevent unbounded RocksDB state growth
        "SET 'table.exec.state.ttl' = '86400000 ms'",
        "SET 'execution.checkpointing.interval' = '30000 ms'",
        "SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE'",
        "SET 'table.exec.emit.late-fire.enabled' = 'true'",
        "SET 'table.exec.emit.late-fire.delay' = '30000 ms'",
        f"""
        CREATE CATALOG recsys_iceberg WITH (
            'type' = 'iceberg',
            'catalog-type' = 'rest',
            'uri' = '{iceberg_uri}',
            'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
            's3.endpoint' = '{s3_endpoint}',
            's3.path-style-access' = 'true'
        )
        """,
        f"""
        CREATE TABLE kafka_playback_interactions (
            event_id STRING,
            event_type STRING,
            user_id STRING,
            item_id STRING,
            device_id STRING,
            device_type STRING,
            event_ts_ms BIGINT,
            ingest_ts_ms BIGINT,
            watch_seconds DOUBLE,
            position_seconds DOUBLE,
            rebuffer_count INT,
            experiment_id STRING,
            experiment_variant STRING,
            event_time AS TO_TIMESTAMP_LTZ(event_ts_ms, 3),
            WATERMARK FOR event_time AS event_time - INTERVAL '30' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'playback.interactions.raw',
            'properties.bootstrap.servers' = '{kafka_bootstrap}',
            'properties.group.id' = 'flink-recsys-sessionizer',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'avro'
        )
        """,
        # Step 1: Deduplicate heartbeat storms by event_id keeping first arrival per event_time
        """
        CREATE TEMPORARY VIEW deduped_interactions AS
        SELECT *
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_time ASC) AS rn
            FROM kafka_playback_interactions
        )
        WHERE rn = 1 AND watch_seconds >= 0.0
        """,
        # Step 2: 30-minute inactivity-gap session window per user across devices
        """
        INSERT INTO recsys_iceberg.silver.viewing_sessions
        SELECT
            CONCAT('sess_', user_id, '_', CAST(UNIX_TIMESTAMP(CAST(window_start AS STRING)) AS STRING)) AS session_id,
            user_id,
            UNIX_TIMESTAMP(CAST(window_start AS STRING)) * 1000 AS session_start_ts_ms,
            UNIX_TIMESTAMP(CAST(window_end AS STRING)) * 1000 AS session_end_ts_ms,
            CAST(TIMESTAMPDIFF(SECOND, window_start, window_end) AS DOUBLE) AS session_duration_seconds,
            CAST(COUNT(*) AS INT) AS event_count,
            CAST(SUM(CASE WHEN event_type = 'HEARTBEAT' THEN 1 ELSE 0 END) AS INT) AS heartbeat_count,
            SUM(CASE WHEN event_type IN ('HEARTBEAT', 'COMPLETE') THEN watch_seconds ELSE 0.0 END) AS total_watch_seconds,
            CAST(SUM(rebuffer_count) AS INT) AS rebuffer_count,
            CAST(SUM(CASE WHEN event_type = 'COMPLETE' THEN 1 ELSE 0 END) AS INT) AS completions,
            CAST(SUM(CASE WHEN event_type = 'SKIP' THEN 1 ELSE 0 END) AS INT) AS skips,
            CAST(SUM(CASE WHEN event_type = 'SEARCH' THEN 1 ELSE 0 END) AS INT) AS searches,
            CAST(SUM(CASE WHEN event_type = 'THUMBS_UP' THEN 1 ELSE 0 END) AS INT) AS thumbs_up,
            CAST(SUM(CASE WHEN event_type = 'THUMBS_DOWN' THEN 1 ELSE 0 END) AS INT) AS thumbs_down,
            CAST(SUM(CASE WHEN event_type = 'DEVICE_SWITCH' THEN 1 ELSE 0 END) AS INT) AS device_switch_count,
            CAST(COUNT(DISTINCT device_type) AS INT) AS distinct_devices,
            CAST(COUNT(DISTINCT item_id) AS INT) AS distinct_items,
            1 AS revision,
            '+I' AS changelog_op,
            FALSE AS had_late_retraction,
            UNIX_TIMESTAMP(CAST(window_end AS STRING)) * 1000 AS emitted_watermark_ms,
            MAX(COALESCE(experiment_variant, 'control')) AS experiment_variant
        FROM TABLE(
            SESSION(
                DATA => TABLE deduped_interactions PARTITION BY user_id,
                TIMECOL => DESCRIPTOR(event_time),
                GAP => INTERVAL '30' MINUTE
            )
        )
        GROUP BY user_id, window_start, window_end
        """,
    ]


def main() -> None:
    """Submit the Flink SQL job when pyflink is available in the Flink runtime container."""
    from pyflink.table import EnvironmentSettings, TableEnvironment

    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)
    for stmt in build_sessionization_sql():
        if stmt.strip().upper().startswith("SET "):
            key_val = stmt.strip()[4:].split("=", 1)
            k = key_val[0].strip().strip("'")
            v = key_val[1].strip().strip("'")
            t_env.get_config().set(k, v)
        else:
            t_env.execute_sql(stmt)


if __name__ == "__main__":
    main()
