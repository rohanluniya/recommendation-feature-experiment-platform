-- Trino 484 SQL: Strict Point-in-Time (ASOF) Training Dataset Verification & Leak Audit
-- Verifies zero future feature leakage across gold.training_dataset and reproduces PIT join semantics.

SELECT
    COUNT(*) AS total_training_rows,
    SUM(CASE WHEN user_feature_ts_ms > request_ts_ms THEN 1 ELSE 0 END) AS user_future_leak_rows,
    SUM(CASE WHEN item_feature_ts_ms > request_ts_ms THEN 1 ELSE 0 END) AS item_future_leak_rows,
    SUM(CASE WHEN label_source = 'immediate' THEN 1 ELSE 0 END) AS immediate_positive_labels,
    SUM(CASE WHEN label_source = 'delayed_backfill' THEN 1 ELSE 0 END) AS delayed_backfill_labels,
    ROUND(AVG(CAST(label_completed AS DOUBLE)), 4) AS overall_completion_label_rate,
    SUM(is_cold_start_user) AS cold_start_user_rows,
    SUM(is_cold_start_item) AS cold_start_item_rows
FROM iceberg.gold.training_dataset;
