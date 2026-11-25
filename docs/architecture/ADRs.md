# Architecture Decision Records (ADRs)

## ADR-001: Apache Kafka 4.3.2 (Distributed) + Local WAL Topic Spool (`local_native`)
- **Context:** The platform needs durable, replayable event streaming with deterministic offset checkpoints both in multi-container deployments and on developer workstations without Docker/JVM runtimes.
- **Decision:** Pin `apache/kafka:4.3.2` (KRaft mode, zero ZooKeeper) + `apicurio/apicurio-registry:3.3.5` in `docker-compose.yml`, and provide `LocalTopicSpool` (`recsys_platform/ingestion.py`) backed by append-only partitioned JSONL logs with explicit offsets, consumer checkpoints, and outage WAL buffering for `local_native`.
- **Consequences:** Identical Avro v1/v2 contracts (`fastavro`) and offset replay tests execute cleanly in sub-second CI without requiring a 2 GiB Kafka broker JVM.

## ADR-002: Event-Time Sessionization Semantics (PyFlink 2.1.3 + Deterministic Python K-Way Engine)
- **Context:** Playback heartbeats arrive out of order, duplicate during mobile/TV reconnect storms, and span cross-device handoffs. Processing-time windows corrupt watch-time metrics.
- **Decision:** Implement Flink-exact event-time sessionization (`30m` inactivity gap, `30s` out-of-order bound, `5m` allowed lateness emitting `-U`/`+U` changelog retractions, `24h` user state TTL eviction) in `recsys_platform/sessionizer.py` alongside the deployable PyFlink 2.1.3 job in `processing/flink/sessionization_job.py`.
- **Consequences:** Late arrivals within allowed lateness reconcile cleanly while stale state stays bounded; unit and chaos drills verify retraction counts and drop boundaries deterministically.

## ADR-003: Apache Iceberg V2 Medallion Lakehouse via PyIceberg 0.12.0 + DuckDB 2.0.0
- **Context:** Offline feature histories and delayed-label backfills require ACID snapshot isolation, schema enforcement, time-travel auditing, and small-file compaction.
- **Decision:** Standardize all 10 Bronze/Silver/Gold/Quality tables on Iceberg V2 (`format-version=2`, Zstd Parquet) using PyIceberg's SQLite catalog locally and Lakekeeper (`v0.15.1`) + RustFS (`1.0.6`) in Docker Compose, queried via DuckDB 2.0.0 locally and Trino 484 in the analytics profile.
- **Consequences:** Pre-backfill and post-backfill training datasets are provably comparable via Iceberg snapshot time travel; compaction collapses streaming micro-batches cleanly.

## ADR-004: Strict Point-in-Time (ASOF) Joins Over Naive Feature Tables
- **Context:** Joining latest user/item feature snapshots onto historical recommendation impressions leaks future watch behavior and completions into training features, inflating offline AUC and causing severe online serving degradation.
- **Decision:** Store immutable timestamped feature histories (`gold.user_features_pit`, `gold.item_features_pit`) with explicit `event_ts_ms`, `created_ts_ms`, and `ttl_seconds`, and construct training sets exclusively via binary-search ASOF joins enforcing `user_feature_ts_ms < request_ts_ms` and `item_feature_ts_ms <= request_ts_ms`. Pair every run with an automated naive-join leakage comparator.
- **Consequences:** Zero future feature leakage is enforced by DQ gates, Hypothesis property tests, and explicit AUC-inflation benchmarks.

## ADR-005: Feast 0.68.0 Feature Definitions + Valkey 9.2.0 / SQLite WAL Online Store
- **Context:** Feature definitions require declarative entity keys, TTLs, and feature services alongside low-latency (<10ms p99) online serving with verified offline/online parity.
- **Decision:** Author real `feast==0.68.0` entities, `FileSource`s, `FeatureView`s, and `FeatureService` in `feast_repo/` (applied and verified via `feast apply`), paired with `OnlineFeatureStore` supporting both Valkey 9.2.0 (`valkey==6.2.0`) and a zero-dependency SQLite WAL TTL backend in `local_native`, plus an automated full-entity parity validator.
- **Consequences:** Derived online state can be wiped completely and rebuilt deterministically from Iceberg Gold with zero numeric drift.

## ADR-006: Deterministic SHA-256 Bucketing, Chi-Squared SRM Gate & CUPED Variance Reduction
- **Context:** Flaky assignment routing invalidates experiments (SRM), while raw post-exposure watch time exhibits heavy user-level variance that masks realistic 4-7% recommendation lifts.
- **Decision:** Hash `SHA-256(experiment_id + ":" + salt + ":" + user_id) mod 10000` for sticky user assignment; enforce SciPy chi-squared SRM gating (`alpha = 0.001`); and compute CUPED adjustment `Y_cuped = Y - theta * (X - mean(X))` using pre-experiment 7-day watch minutes `X`. Report unadjusted and CUPED statistics side-by-side alongside latency/rebuffer/skip guardrails and catalog overlap diagnostics.
- **Consequences:** Variance drops markedly without biasing treatment estimates, while broken traffic splits or cross-bucket leaks automatically block rollout decisions.

## ADR-007: Lightweight Deterministic scikit-learn Rankers (Zero External Pretrained Weights)
- **Context:** The core focus is the recommendation data platform (feature freshness, PIT correctness, experimentation) rather than large neural model hosting, and external pretrained model weights introduce license and reproducibility overhead.
- **Decision:** Train deterministic `LogisticRegression` (control baseline) and regularized `HistGradientBoostingClassifier` (treatment ranker) strictly on chronological 75/25 splits of PIT-verified `gold.training_dataset`, logging metrics, params, and Iceberg snapshot lineage into MLflow-compatible `results/mlruns/`.
- **Consequences:** 100% Apache-2.0 / BSD-3-Clause compliance, zero external weight downloads, reproducible sub-second retraining, and realistic online candidate scoring.
