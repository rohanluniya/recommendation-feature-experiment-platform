# Incident & Chaos Engineering Report — INC-001

## Drill Overview

- **Incident ID:** `INC-001` (Scheduled Chaos Game Day)
- **Environment:** `local_native` (Python 3.12.8, Apache Iceberg V2 via PyIceberg 0.12.0, SQLite WAL online store)
- **Trigger:** Automated chaos suite `uv run pytest -v tests/chaos/test_chaos_drills.py`
- **Raw Evidence Artifact:** [`results/chaos/chaos_results.json`](../../results/chaos/chaos_results.json)
- **Outcome:** **8 / 8 drills PASS**, zero silent data corruption, zero unhandled exceptions, and deterministic recovery across all four failure domains.

## Scenario Breakdown & Observed Recovery Behavior

| Drill ID | Fault Injected | Expected Guarantee | Observed Metrics | Recovery Time | Status |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `CHAOS-01` | 5x duplicate playback heartbeat flood (100 events = 20 unique x 5) | Exact `event_id` deduplication with zero watch-time inflation | 80 duplicates dropped; 20 retained; observed `600.0s` == expected `600.0s` | `1.64 ms` | **PASS** |
| `CHAOS-02` | Late arrival inside 5m allowed lateness + late arrival past cleanup horizon | Emit `-U`/`+U` retraction for valid late event; drop expired event to side output | `1` retraction (`revision=2`), `1` side-output drop, watch time reconciled to `100.0s` | `0.07 ms` | **PASS** |
| `CHAOS-03` | Kafka/Spool broker outage mid-batch (`broker_available=False`) | Local WAL buffers 25 events without loss; drains contiguously on recovery | 15 pre-outage + 25 drained = 40 contiguous offsets (`0..39`) replayed | `0.62 ms` | **PASS** |
| `CHAOS-04` | Complete online feature store wipe (`active_users=0`, `active_items=0`) | Deterministic rebuild from Iceberg Gold with 0% offline/online parity drift | Rebuilt 80 users & 24 items; `0` parity mismatches; `max_abs_numeric_diff = 0.0` | `360.74 ms` | **PASS** |
| `CHAOS-05` | Broken experiment router skewing 50/50 split to 57/43 (`N=500` users) | Chi-squared SRM test (`alpha=0.001`) detects skew and blocks ship decision | `209 C / 291 T` (`58.2%`), `chi2=13.448`, `p=0.000245`, decision=`INVALID_STOP_SRM_OR_CONTAMINATION` | `487.89 ms` | **PASS** |
| `CHAOS-06` | Delayed completion labels arriving 2-6h late backfilled onto `gold.training_dataset` | Update positive labels cleanly without mutating historical feature timestamps; preserve time-travel snapshot | `+90` delayed labels applied (`320 -> 410` positives); `0` feature timestamps mutated; `0` PIT leaks; pre-backfill snapshot verified via time travel | `179.60 ms` | **PASS** |
| `CHAOS-07` | Schema drift & corrupt payloads (invalid enum, missing keys, negative watch times) | 100% quarantined to `bronze.quarantine_dlq`; zero negative watch times in Bronze; exact conservation | `2,380 raw = 2,074 valid + 134 DLQ + 172 dups`; `0` negative watch times in Bronze | `96.10 ms` | **PASS** |
| `CHAOS-08` | 4 control users leaked into treatment + `+15ms` serving latency regression | Sticky contamination detector + latency p95 guardrail block rollout | `4` contaminated users detected; latency delta `+15.996 ms` failed `<=5.0ms` guardrail; decision=`INVALID_STOP_SRM_OR_CONTAMINATION` | `164.76 ms` | **PASS** |

## Root-Cause Walkthrough: Primary Drill (`CHAOS-04` — Online Store Total Loss)

1. **Detection:** Online feature requests return cache misses / 404s, and `OnlineOfflineFeatureParityDrift` fires because `recsys_online_offline_parity_mismatch_ratio` spikes to `1.0`.
2. **Containment:** Recommendation API degrades gracefully to cold-start priors (`is_cold_start_user=True`, popularity + exploration candidates) rather than throwing HTTP 500s.
3. **Recovery:** Operator (or automated CronJob) executes `POST /v1/admin/rebuild-online-store` or `uv run python services/feature_materializer.py --wipe-first`.
4. **Verification:** Materializer scans the latest committed Iceberg V2 snapshots of `gold.user_features_pit` and `gold.item_features_pit`, repopulates every entity with fresh TTL timestamps, and runs full-entity parity validation confirming `0` mismatches and `0.0` numeric difference.

## Follow-Up Action Items

- Add automated canary parity check inside CI for containerized Valkey 9.2.0 when running on a Docker-enabled runner.
- Automate pager notification webhook routing for `ExperimentSampleRatioMismatch` and `PitFutureFeatureLeakageDetected` in production Alertmanager.
