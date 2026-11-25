"""PySpark 4.1.4 + Apache Iceberg V2 Point-in-Time ASOF training builder and file compaction job.

Runs inside the Spark container profile (`docker-compose.yml`, image `apache/spark:4.1.4-scala2.13-java17-python3-ubuntu`).
In `local_native` mode (no host JVM), `recsys_platform/features.py` and `recsys_platform/lakehouse.py`
execute equivalent PIT ASOF joins and Iceberg V2 compaction locally.
"""

from __future__ import annotations

import os

PIT_TRAINING_SQL = """
WITH impressions AS (
    SELECT
        event_id,
        user_id,
        item_id,
        event_ts_ms AS request_ts_ms,
        COALESCE(experiment_variant, 'control') AS experiment_variant
    FROM recsys_iceberg.bronze.interaction_events
    WHERE event_type = 'IMPRESSION'
),
user_asof AS (
    SELECT
        imp.event_id,
        u.event_ts_ms AS user_feature_ts_ms,
        u.user_watch_seconds_total,
        u.user_play_count,
        u.user_completion_rate,
        u.user_skip_rate,
        u.user_rebuffer_rate,
        u.user_top_genre,
        u.is_cold_start_user,
        ROW_NUMBER() OVER (
            PARTITION BY imp.event_id
            ORDER BY u.event_ts_ms DESC
        ) AS rn
    FROM impressions imp
    LEFT JOIN recsys_iceberg.gold.user_features_pit u
      ON imp.user_id = u.user_id
     AND u.event_ts_ms < imp.request_ts_ms
     AND (imp.request_ts_ms - u.event_ts_ms) <= (u.ttl_seconds * 1000)
),
item_asof AS (
    SELECT
        imp.event_id,
        i.event_ts_ms AS item_feature_ts_ms,
        i.genre AS item_genre,
        i.popularity_prior AS item_popularity_prior,
        i.item_completion_rate_so_far,
        i.duration_seconds AS item_duration_seconds,
        i.is_cold_start_item,
        ROW_NUMBER() OVER (
            PARTITION BY imp.event_id
            ORDER BY i.event_ts_ms DESC
        ) AS rn
    FROM impressions imp
    LEFT JOIN recsys_iceberg.gold.item_features_pit i
      ON imp.item_id = i.item_id
     AND i.event_ts_ms <= imp.request_ts_ms
     AND (imp.request_ts_ms - i.event_ts_ms) <= (i.ttl_seconds * 1000)
)
SELECT
    imp.event_id AS row_id,
    imp.user_id,
    imp.item_id,
    imp.request_ts_ms,
    COALESCE(u.user_feature_ts_ms, 0) AS user_feature_ts_ms,
    COALESCE(i.item_feature_ts_ms, 0) AS item_feature_ts_ms,
    COALESCE(u.user_watch_seconds_total, 0.0) AS user_watch_seconds_total,
    COALESCE(u.user_play_count, 0) AS user_play_count,
    COALESCE(u.user_completion_rate, 0.0) AS user_completion_rate,
    COALESCE(u.user_skip_rate, 0.0) AS user_skip_rate,
    COALESCE(u.user_rebuffer_rate, 0.0) AS user_rebuffer_rate,
    CASE WHEN u.user_top_genre IS NOT NULL AND u.user_top_genre = i.item_genre THEN 1 ELSE 0 END AS genre_match,
    COALESCE(i.item_popularity_prior, 0.15) AS item_popularity_prior,
    COALESCE(i.item_completion_rate_so_far, 0.0) AS item_completion_rate_so_far,
    COALESCE(i.item_duration_seconds, 3000) AS item_duration_seconds,
    CASE WHEN u.is_cold_start_user IS NULL OR u.is_cold_start_user THEN 1 ELSE 0 END AS is_cold_start_user,
    CASE WHEN i.is_cold_start_item IS NULL OR i.is_cold_start_item THEN 1 ELSE 0 END AS is_cold_start_item,
    imp.experiment_variant
FROM impressions imp
LEFT JOIN user_asof u ON imp.event_id = u.event_id AND u.rn = 1
LEFT JOIN item_asof i ON imp.event_id = i.event_id AND i.rn = 1
"""


def main() -> None:
    """Execute PIT ASOF dataset build and Iceberg V2 bin-pack compaction in Spark 4.1.4."""
    from pyspark.sql import SparkSession

    catalog_uri = os.getenv("ICEBERG_CATALOG_URI", "http://lakekeeper:8181/catalog")
    s3_endpoint = os.getenv("OBJECT_STORE_ENDPOINT", "http://rustfs:9000")

    spark = (
        SparkSession.builder.appName("recsys-pit-training-builder")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.recsys_iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.recsys_iceberg.type", "rest")
        .config("spark.sql.catalog.recsys_iceberg.uri", catalog_uri)
        .config("spark.sql.catalog.recsys_iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.recsys_iceberg.s3.endpoint", s3_endpoint)
        .config("spark.sql.catalog.recsys_iceberg.s3.path-style-access", "true")
        .getOrCreate()
    )
    try:
        pit_df = spark.sql(PIT_TRAINING_SQL)
        # Hard PIT safety assertion prior to publish
        leak_count = pit_df.filter(
            "user_feature_ts_ms > request_ts_ms OR item_feature_ts_ms > request_ts_ms"
        ).count()
        if leak_count > 0:
            raise RuntimeError(f"PIT violation guard tripped: {leak_count} rows leaked future features.")

        pit_df.writeTo("recsys_iceberg.gold.training_dataset").overwritePartitions()
        spark.sql(
            "CALL recsys_iceberg.system.rewrite_data_files(table => 'gold.training_dataset', strategy => 'binpack')"
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
