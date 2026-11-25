-- Trino 484 SQL: Experiment Exposure, SRM Counts & CUPED Covariate Preparation
-- Joins sticky user exposures with session watch time and delayed labels for CUPED theta estimation.

WITH sticky_exposures AS (
    SELECT
        experiment_id,
        user_id,
        MIN(variant) AS variant,
        COUNT(DISTINCT variant) AS distinct_variants,
        MIN(pre_experiment_watch_minutes) AS pre_exp_watch_min,
        APPROX_PERCENTILE(serving_latency_ms, 0.95) AS serving_latency_p95_ms
    FROM iceberg.gold.experiment_exposures
    GROUP BY experiment_id, user_id
),
user_outcomes AS (
    SELECT
        user_id,
        SUM(total_watch_seconds) / 60.0 AS post_exp_watch_min,
        SUM(completions) AS immediate_completions,
        SUM(skips) AS skips,
        SUM(rebuffer_count) AS rebuffers,
        SUM(heartbeat_count) AS heartbeats
    FROM iceberg.silver.viewing_sessions
    GROUP BY user_id
),
user_level AS (
    SELECT
        e.experiment_id,
        e.variant,
        e.user_id,
        e.pre_exp_watch_min AS x,
        COALESCE(o.post_exp_watch_min, 0.0) AS y,
        e.serving_latency_p95_ms
    FROM sticky_exposures e
    LEFT JOIN user_outcomes o ON e.user_id = o.user_id
    WHERE e.distinct_variants = 1
),
cuped_stats AS (
    SELECT
        experiment_id,
        COVAR_SAMP(y, x) / NULLIF(VAR_SAMP(x), 0.0) AS theta,
        AVG(x) AS mean_x
    FROM user_level
    GROUP BY experiment_id
)
SELECT
    u.experiment_id,
    u.variant,
    COUNT(*) AS exposed_users,
    ROUND(AVG(u.y), 3) AS unadjusted_mean_watch_min,
    ROUND(VAR_SAMP(u.y), 3) AS unadjusted_var,
    ROUND(AVG(u.y - c.theta * (u.x - c.mean_x)), 3) AS cuped_mean_watch_min,
    ROUND(VAR_SAMP(u.y - c.theta * (u.x - c.mean_x)), 3) AS cuped_var,
    ROUND(AVG(u.serving_latency_p95_ms), 3) AS avg_p95_serving_latency_ms
FROM user_level u
JOIN cuped_stats c ON u.experiment_id = c.experiment_id
GROUP BY u.experiment_id, u.variant
ORDER BY u.experiment_id, u.variant;
