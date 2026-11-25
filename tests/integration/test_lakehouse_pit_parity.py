"""Integration tests for Iceberg V2 medallion tables, time travel, compaction, DuckDB SQL, PIT parity, and Feast."""

from __future__ import annotations

import pytest

from feast_repo.features import item_catalog_fv, recsys_ranking_v2_service, user_engagement_fv
from recsys_platform.lakehouse import IcebergLakehouse
from recsys_platform.online_store import OnlineFeatureStore


@pytest.mark.integration
def test_iceberg_time_travel_and_compaction_and_duckdb(module_bootstrapped_env) -> None:
    cfg, _summary = module_bootstrapped_env
    lh = IcebergLakehouse(cfg)

    inv = {row["table_id"]: row for row in lh.table_inventory()}
    assert len(inv) == 10
    assert inv["bronze.interaction_events"]["row_count"] > 0
    assert inv["gold.training_dataset"]["snapshot_count"] >= 2  # pre-backfill + post-backfill

    # Time travel: verify earlier snapshot has fewer positive labels than post-backfill snapshot
    snaps = lh.snapshot_history("gold.training_dataset")
    pre_rows = lh.read_records("gold.training_dataset", snapshot_id=snaps[0]["snapshot_id"])
    post_rows = lh.read_records("gold.training_dataset", snapshot_id=snaps[-1]["snapshot_id"])
    assert sum(r["label_completed"] for r in post_rows) > sum(r["label_completed"] for r in pre_rows)

    # Fragment gold.user_features_pit with multiple small appends, then compact and verify reduction
    sample_u = lh.read_records("gold.user_features_pit")[:10]
    for _ in range(4):
        lh.write_table("gold.user_features_pit", sample_u, mode="append")
    comp = lh.compact_table("gold.user_features_pit")
    assert comp["files_before"] >= 5
    assert comp["files_after"] == 1
    assert comp["file_reduction_pct"] >= 80.0

    # DuckDB SQL over live Iceberg Arrow scans
    sql_rows = lh.query_sql(
        """
        SELECT
            COUNT(*) AS n_rows,
            SUM(CASE WHEN user_feature_ts_ms > request_ts_ms OR item_feature_ts_ms > request_ts_ms THEN 1 ELSE 0 END) AS leaks
        FROM gold_training_dataset
        """,
        tables=["gold.training_dataset"],
    )
    assert sql_rows[0]["n_rows"] == len(post_rows)
    assert sql_rows[0]["leaks"] == 0


@pytest.mark.integration
def test_online_offline_parity_and_feast_definitions(module_bootstrapped_env) -> None:
    cfg, _summary = module_bootstrapped_env
    lh = IcebergLakehouse(cfg)
    store = OnlineFeatureStore(cfg)

    parity = store.validate_parity_against_lakehouse(lh)
    assert parity.passed is True
    assert parity.total_mismatches == 0
    assert parity.max_abs_numeric_diff == 0.0

    # Verify Feast 0.68.0 FeatureView & FeatureService objects
    assert user_engagement_fv.name == "user_engagement_features"
    assert item_catalog_fv.name == "item_catalog_features"
    assert recsys_ranking_v2_service.name == "recsys_ranking_v2"
    assert user_engagement_fv.online is True
