# Requirements, SLOs & Workload Assumptions

## Functional Requirements

1. **Deterministic Streaming & Catalog Source:** Seeded generation of viewers, devices, content catalog CDC edits, viewing interactions (`IMPRESSION`, `PLAY_START`, `HEARTBEAT`, `PAUSE`, `COMPLETE`, `SKIP`, `SEARCH`, `THUMBS_UP`, `THUMBS_DOWN`, `DEVICE_SWITCH`, `SUBSCRIPTION_UPGRADE`), cold-start entities, duplicate storms, late events, delayed labels, and corrupt payloads.
2. **Contract Validation & Quarantine:** Enforce Avro v1/v2 contracts (`fastavro`) plus semantic constraints (`watch_seconds >= 0`, non-empty keys). Quarantine all invalid payloads to `bronze.quarantine_dlq` with 100% conservation accounting.
3. **Event-Time Sessionization:** 30-minute inactivity-gap session windows with 30s out-of-order bound, 5-minute allowed lateness emitting `-U`/`+U` retractions, cross-device switch tracking, heartbeat deduplication, and 24-hour user state TTL eviction.
4. **Point-in-Time (ASOF) Feature Store & Parity:** Build Iceberg V2 feature histories (`gold.user_features_pit`, `gold.item_features_pit`) with explicit `event_ts_ms`, `created_ts_ms`, and `ttl_seconds`. Enforce zero future leakage (`feature_ts <= request_ts`), delayed label backfill without mutating feature timestamps, Feast 0.68.0 definitions, and exact offline-vs-online parity.
5. **Experimentation & Statistical Rigor:** Deterministic SHA-256 bucketing, sticky exposure verification (zero cross-bucket contamination), chi-squared SRM detection (`alpha = 0.001`), CUPED covariate variance reduction, latency/rebuffer/skip guardrails, and catalog interference diagnostics.
6. **Live Serving & Control Tower:** RBAC-protected FastAPI API (`/v1/features/online/{user_id}`, `/v1/recommend`, `/v1/pit/audit`, `/v1/parity`, `/v1/experiments/{id}/evaluate`, `/v1/admin/rebuild-online-store`), Prometheus `/metrics`, OpenTelemetry traces, and an interactive dark-mode Control Tower UI.

## Service-Level Objectives (SLOs)

| Domain | SLI | Target SLO | Measurement Source |
| :--- | :--- | :--- | :--- |
| Online Feature Serving | Single-user feature lookup latency (p99) | `<= 10.0 ms` | `recsys_online_feature_latency_ms` |
| Recommendation Serving | Live candidate gen + bucketing + ranking (p99) | `<= 25.0 ms` | `recsys_recommendation_latency_ms` |
| Feature Freshness | Lag between latest Gold event_ts and online store | `<= 300 s` | `recsys_feature_freshness_lag_seconds` |
| Feature Correctness | Online-vs-offline feature mismatch ratio | `0.0` (0 mismatches) | `recsys_online_offline_parity_mismatch_ratio` |
| Temporal Integrity | PIT future feature leakage violations | `0` | `recsys_pit_leakage_violations` |
| Experiment Integrity | SRM goodness-of-fit p-value & cross-bucket leaks | `p >= 0.001` & `0` leaks | `recsys_experiment_srm_pvalue` |
| Ingestion Integrity | Conservation delta (`raw - valid - dlq - dups`) | `0` | `quality.dq_results` |

## Workload Assumptions

- **Local Verification Profile (`local_native`):** 100 to 800 synthetic viewers, 30 to 150 catalog titles, ~6k to ~94k raw interactions per run, single-node Apple M4 Pro workstation (48 GiB RAM).
- **Production Scale Target (Architectural Reference):** 10M daily active viewers, 50k active catalog titles, 50k events/sec peak heartbeat ingress across 32 Kafka partitions, Flink RocksDB state sharded across 16 task slots, and Valkey cluster with 64 GiB memory.
