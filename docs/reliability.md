# Reliability, Alerting & Operator Runbooks

Every Prometheus alert in [`infra/observability/alerts.yml`](../infra/observability/alerts.yml) links directly to an operational runbook anchor below.

## Core SLO Summary

- **Online Feature Serving p99 Latency:** `<= 10.0 ms`
- **Feature Freshness Lag:** `<= 300 s`
- **Online/Offline Feature Parity:** `0` mismatches (`mismatch_ratio == 0.0`)
- **PIT Training Leakage:** `0` violations
- **Experiment Allocation Integrity:** SRM `p >= 0.001` and `0` cross-bucket contaminations

---

## Alert Runbooks

### Alert: FeatureFreshnessSloBreach {#alert-feature-freshness-slo-breach}
- **Trigger:** `recsys_feature_freshness_lag_seconds > 300` for 5m (`critical`).
- **Triage:**
  1. Check online store status via `GET /health` (`online_store.seconds_since_materialization`).
  2. Inspect Airflow DAG `recsys_feature_experiment_pipeline` or run `uv run python services/feature_materializer.py`.
  3. Confirm latest snapshot timestamp on `gold.user_features_pit` via `GET /v1/lineage`.
- **Mitigation:** Re-trigger materialization (`POST /v1/admin/rebuild-online-store`) and verify `freshness_lag_seconds <= 300`.

### Alert: OnlineOfflineFeatureParityDrift {#alert-online-offline-parity-drift}
- **Trigger:** `recsys_online_offline_parity_mismatch_ratio > 0` for 1m (`critical`).
- **Triage:**
  1. Call `GET /v1/parity` with an `analyst` API key and inspect `sample_diffs`.
  2. Verify whether online store experienced partial writes, TTL expiry, or an unmaterialized Iceberg backfill.
- **Mitigation:** Execute deterministic full rebuild from durable Iceberg Gold:
  ```bash
  uv run python services/feature_materializer.py --wipe-first
  ```
  Verify `parity.total_mismatches == 0` and `max_abs_numeric_diff == 0.0`.

### Alert: PitFutureFeatureLeakageDetected {#alert-pit-future-leakage}
- **Trigger:** `recsys_pit_leakage_violations > 0` immediately (`critical`).
- **Triage:**
  1. Block model promotion immediately; do not deploy any ranker trained on the affected Iceberg snapshot.
  2. Call `GET /v1/pit/audit` and inspect `live_gold_training_audit` and `training_snapshots`.
  3. Inspect recent edits to `recsys_platform/features.py` or `processing/spark/pit_training_builder.py` for non-ASOF join predicates.
- **Mitigation:** Roll back `gold.training_dataset` to the last clean Iceberg snapshot ID and re-run `tests/unit/test_pit_properties.py`.

### Alert: ExperimentSampleRatioMismatch {#alert-experiment-srm}
- **Trigger:** `recsys_experiment_srm_pvalue < 0.001` for 5m (`page`).
- **Triage:**
  1. Pause experiment readout and halt automated rollout decisions (`INVALID_STOP_SRM_OR_CONTAMINATION`).
  2. Call `GET /v1/experiments/{experiment_id}/evaluate` and check `control_users`, `treatment_users`, `observed_treatment_ratio`, and `cross_bucket_contaminated_users`.
  3. Audit routing logic, client-side telemetry drops (e.g., treatment crashes preventing exposure logs), and bot filters.
- **Mitigation:** Fix upstream assignment/logging defect, rotate experiment salt, and start a clean experiment run.

### Alert: OnlineFeatureServingLatencyHigh {#alert-online-serving-latency}
- **Trigger:** `p99(recsys_online_feature_latency_ms) > 10` for 5m (`warning`).
- **Triage:**
  1. Inspect `GET /metrics` histogram buckets and `GET /v1/observability/traces` spans.
  2. Check Valkey memory eviction / CPU saturation or local SQLite WAL checkpoint contention.
- **Mitigation:** Scale Valkey read replicas or move hot item catalog features into local in-process TTL cache.

### Alert: QuarantineDlqSpike {#alert-quarantine-dlq-spike}
- **Trigger:** `increase(recsys_dlq_events_total[15m]) > 500` (`warning`).
- **Triage:**
  1. Query `bronze.quarantine_dlq` grouped by `reason` and `schema_version`.
  2. Determine whether a client release introduced an incompatible schema change (violating backward compatibility) or corrupt telemetry values.
- **Mitigation:** Coordinate schema rollback or register a backward-compatible Avro schema (nullable fields with defaults), then replay quarantined payloads once remediated.
