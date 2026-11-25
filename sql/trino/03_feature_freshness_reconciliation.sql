-- Trino 484 SQL: Feature Freshness, Quarantine Conservation & DQ Gate Summary

SELECT
    table_name,
    check_name,
    status,
    observed_value,
    threshold,
    details,
    FROM_UNIXTIME(checked_ts_ms / 1000.0) AS checked_at_utc
FROM iceberg.quality.dq_results
ORDER BY status DESC, table_name, check_name;
