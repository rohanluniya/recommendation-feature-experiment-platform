# Engineering Case Study: Preventing Silent Bias in Real-Time Recommendation Platforms

## Problem Statement

Recommendation systems rarely fail loudly. Instead, they fail **silently** through four compounding data-engineering bugs:
1. **Duplicate heartbeat inflation:** Flaky TV/mobile playback clients replay identical heartbeat batches during reconnects, inflating user watch-time features and biasing experiment outcomes.
2. **Temporal feature leakage:** Joining current/latest user or item aggregates onto historical recommendation impressions lets models peek at future completions, inflating offline ROC-AUC while degrading live serving.
3. **Training-serving skew:** Offline feature pipelines and online key-value stores drift apart after backfills, TTL evictions, or partial writes.
4. **Experimentation noise and routing bias:** Unchecked Sample Ratio Mismatch (SRM) corrupts causal inference, while high-variance watch-time metrics drown out real ranking improvements unless pre-experiment covariates are leveraged via CUPED.

## Architectural Solution

We built an end-to-end, reproducible recommendation data backbone organized around strict temporal invariants:

### 1. Contract-First Idempotent Ingestion & Quarantine
Every interaction passes through FastAvro v1/v2 contract validation and semantic checks before entering Bronze (`recsys_platform/ingestion.py`). Invalid payloads (corrupt opcodes, missing keys, negative watch times) are isolated in `bronze.quarantine_dlq`, while duplicate `event_id` bursts are dropped idempotently. A hard Data Quality assertion enforces exact conservation (`raw == valid + dlq + duplicates_dropped`) on every pipeline run.

### 2. Flink-Exact Event-Time Sessionization with Retractions & Timer Coalescing
Processing-time windows break whenever mobile clients buffer offline events. Our event-time sessionizer (`recsys_platform/sessionizer.py`, mirrored by PyFlink 2.1.3 in `processing/flink/sessionization_job.py`) advances a monotonic watermark (`max_event_ts - 30s`), closes sessions after 30 minutes of inactivity across device switches, emits `-U`/`+U` changelog retractions when late arrivals land within the 5-minute allowed-lateness window, drops expired stragglers to a side-output counter, and evicts idle user state past 24-hour TTL. Profiling the `stress` workload (800 users, 96k events) exposed an `O(N * U)` per-event sweep bottleneck; introducing 10-second watermark timer coalescing raised sessionization throughput from `13.2k` to **`228.6k events/sec` (17.34x vs. uncoalesced sweep)** without changing a single session output.

### 3. Strict Point-in-Time (ASOF) Feature Store, Delayed Backfill & Online Parity
User and item features are stored as immutable event-time histories in Apache Iceberg V2 (`gold.user_features_pit`, `gold.item_features_pit`) with explicit `event_ts_ms`, `created_ts_ms`, and `ttl_seconds`, paired with validated Feast 0.68.0 definitions (`feast_repo/features.py`). Training rows are constructed via binary-search ASOF joins guaranteeing `user_feature_ts_ms < request_ts_ms` and `item_feature_ts_ms <= request_ts_ms`.

- **Quantified Leakage Exposure:** Comparing our strict PIT join against a naive global join on the `local` profile (5,430 impressions) exposed `10,837` feature-timestamp leaks and **+0.1277 artificial ROC-AUC inflation** (`0.6913` leaking vs. `0.5636` strict PIT).
- **Leak-Free Delayed Label Backfill:** Completions arriving 2-6 hours late update `label_completed` and `label_source="delayed_backfill"` in a new Iceberg V2 snapshot without mutating historical feature timestamps (`0` timestamp mutations, `0` PIT violations), preserving pre-backfill snapshots for time-travel auditing.
- **Zero-Skew Online Serving:** Derived serving state (`OnlineFeatureStore`) serves lookups at **`0.070 ms` p50 / `0.683 ms` p99** locally, validates 100% numeric parity (`0` mismatches) against Iceberg Gold, and rebuilds cleanly from zero after a complete store wipe (`CHAOS-04`).

### 4. Statistically Rigorous Experimentation Engine
Traffic assignment uses deterministic `SHA-256(experiment_id + salt + user_id) mod 10000` with sticky exposure auditing (`0` cross-bucket contamination). Every readout runs a SciPy chi-squared SRM gate (`alpha = 0.001`), computes CUPED-adjusted watch time (`Y_cuped = Y - theta * (X - mean(X))`), checks latency/rebuffer/skip guardrails, and reports catalog item Jaccard overlap as an explicit interference caveat. On the 240-user seed-42 vertical slice, the unadjusted Welch t-test was inconclusive (`p = 0.1263`, CI width `45.42 min`), whereas CUPED reduced variance by **`76.23%`**, cut CI width to `21.23 min`, and cleanly identified the true positive lift (`p = 0.000001`).

## Key Tradeoffs

- **Local Native vs. Distributed Container Stack:** Rather than mocking unavailable JVM/Docker services on an ARM64 workstation without Docker, we implemented full native Python engines (PyIceberg V2, DuckDB, FastAvro, Feast 0.68.0, SQLite WAL) alongside pinned multi-profile Docker Compose, PyFlink, PySpark, Trino, and Airflow artifacts.
- **Full-Entity vs. Sampled Parity Validation:** Checking 100% of entities guarantees zero blind spots up to tens of thousands of entities; at 10M-user production scale, stratified sampling keeps validation cost bounded.
