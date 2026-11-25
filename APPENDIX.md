# Appendix: Ablations, Optimization Progression & Acceptance Matrix

This appendix holds extended engineering evidence, optimization progressions, statistical derivations, chaos drill details, and the 15-point acceptance matrix so [`README.md`](README.md) remains concise and scannable.

---

## 1. Optimization Progression & Profiling Ablations

### 1.1 Event-Time Sessionizer Watermark Timer Coalescing
During initial `stress` benchmarking (800 users, 96,211 raw events), profiling showed sessionization throughput degrading nonlinearly as active user count rose because `_maybe_close_and_evict()` scanned the entire `_user_sessions` dictionary on every incoming event (`O(N * U)`).

Coalescing close and TTL eviction sweeps onto `10,000 ms` event-time watermark ticks (mirroring Apache Flink's timer coalescing service) eliminated per-event full-map sweeps without altering window boundaries or retraction semantics:

| Load Profile | Valid Events | Sessionizer Baseline (ev/s) | Sessionizer Coalesced (ev/s) | Sessionizer Speedup (vs. Baseline) | Full E2E Baseline (s) | Full E2E Coalesced (s) | E2E Speedup (vs. Baseline) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `smoke` (100u) | 4,478 | 125,570.2 | **453,774.7** | **3.61x** | 0.655 | **0.634** | 1.03x |
| `local` (300u) | 25,833 | 37,416.2 | **307,905.2** | **8.23x** | 2.128 | **1.521** | 1.40x |
| `stress` (800u) | 86,462 | 13,181.4 | **228,614.4** | **17.34x** | 10.634 | **4.394** | **2.42x** |

### 1.2 Apache Iceberg V2 Small-File Compaction (`gold.user_features_pit`)
Streaming micro-batch appends fragment Iceberg tables into many small Parquet files. On `gold.user_features_pit` (17,676 rows written across 12 micro-batch appends), atomic `compact_table()` consolidated chunks into a single Zstd Parquet file:

| Metric | Before Compaction (12 micro-batches) | After Compaction | Delta / Speedup |
| :--- | ---: | ---: | :--- |
| Active Parquet Data Files | 12 | **1** | **-91.67%** vs. fragmented table |
| Median Full Arrow Scan Time (5 runs) | 12.180 ms | **2.090 ms** | **5.83x faster** vs. fragmented table |

### 1.3 Regularized HistGBDT vs. Unregularized Depth-4 HistGBDT on PIT Dataset
- **Initial (`max_iter=80, max_depth=4, min_samples_leaf=15, l2=0`):** On the 240-user seed-42 vertical slice (`4,308` training rows), unregularized GBDT slightly overfit the chronological training window (`ROC-AUC = 0.5465`, `PR-AUC = 0.3748` vs. `baseline_logreg` `ROC-AUC = 0.5915`, `PR-AUC = 0.3939`).
- **Tuned (`max_iter=50, max_depth=3, learning_rate=0.05, min_samples_leaf=30, l2_regularization=5.0`):** Held-out `treatment_gbdt` improved to **`ROC-AUC = 0.5835` (+0.0370)** and **`PR-AUC = 0.4170` (+0.0422, outperforming `baseline_logreg` PR-AUC `0.3939`)** while maintaining honest zero-leakage evaluation.

---

## 2. Point-in-Time Leakage Analysis & Naive Join Comparison

| Profile | Training Rows | Strict PIT Violations | Naive Global Join Violations | Naive GBDT ROC-AUC | Strict PIT GBDT ROC-AUC | Artificial AUC Inflation (Naive vs. Strict PIT) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| `smoke` (100u) | 956 | **0** | 1,900 | 0.7550 | 0.5288 | **+0.2262** |
| `local` (300u) | 5,430 | **0** | 10,837 | 0.6913 | 0.5636 | **+0.1277** |
| `stress` (800u) | 18,052 | **0** | 36,068 | 0.6805 | 0.5996 | **+0.0809** |

**Why it happens:** The naive join attaches each user's end-of-window cumulative watch time and completion rate to earlier impression timestamps, allowing tree splits to infer whether the impression succeeded. Strict PIT ASOF joins enforce `user_feature_ts_ms < request_ts_ms` and `item_feature_ts_ms <= request_ts_ms`, verified both at runtime (`audit_pit_leakage`) and via Hypothesis property testing (`tests/unit/test_pit_properties.py`).

---

## 3. CUPED Statistical Formulation & Measured Scaling

Given post-exposure user outcome $Y_i$ (watch minutes) and pre-experiment covariate $X_i$ (7-day prior watch minutes, independent of treatment assignment):

$$\hat{\theta} = \frac{\widehat{\text{Cov}}(Y, X)}{\widehat{\text{Var}}(X)}, \qquad Y_i^{\text{cuped}} = Y_i - \hat{\theta}\,(X_i - \bar{X})$$

Because $\mathbb{E}[X_i - \bar{X} \mid T_i = 1] = \mathbb{E}[X_i - \bar{X} \mid T_i = 0] = 0$ under randomized assignment, $\mathbb{E}[Y^{\text{cuped}}_T - Y^{\text{cuped}}_C] = \mathbb{E}[Y_T - Y_C]$ (unbiased lift) while $\text{Var}(Y^{\text{cuped}}) = (1 - \rho_{XY}^2)\,\text{Var}(Y) \le \text{Var}(Y)$.

On the default 240-user vertical slice (`seed=42`):
- **Unadjusted Welch t-test:** Lift `+17.687 min`, 95% CI width `45.422 min`, `p = 0.126302` (inconclusive).
- **CUPED-adjusted Welch t-test:** `theta = 4.8104`, `Corr(Y, X) = 0.8731`, **variance reduced by `-76.23%`**, 95% CI width narrowed to `21.234 min` (**-53.25%**), `p = 0.000001` (conclusive ship decision).

Full scaling results (`N = 160, 300, 800`) are stored in [`results/benchmarks/cuped_scaling.csv`](results/benchmarks/cuped_scaling.csv).

---

## 4. Chaos Engineering Drill Ledger (8 / 8 Passed)

Raw evidence: [`results/chaos/chaos_results.json`](results/chaos/chaos_results.json) and [`docs/incidents/INC-001-chaos-drill.md`](docs/incidents/INC-001-chaos-drill.md).

| Drill ID | Fault Scenario | Key Measured Assertion | Status |
| :--- | :--- | :--- | :--- |
| `CHAOS-01` | 5x duplicate heartbeat flood | 80 duplicates dropped; exact `600.0s` watch time | **PASS** |
| `CHAOS-02` | Late event within/beyond allowed lateness | `1` `-U/+U` retraction (`v2`), `1` side-output drop | **PASS** |
| `CHAOS-03` | Broker outage WAL spool & drain | 25 buffered events drained; 40 contiguous offsets | **PASS** |
| `CHAOS-04` | Complete online feature store wipe & rebuild | Rebuilt from Iceberg Gold; `0` parity mismatches | **PASS** |
| `CHAOS-05` | Intentional SRM injection (`50/50 -> 57/43`) | `chi2=13.448`, `p=0.000245`, rollout halted | **PASS** |
| `CHAOS-06` | Delayed label backfill without leakage | `+90` labels; `0` feature ts mutated; `0` PIT leaks | **PASS** |
| `CHAOS-07` | Schema drift & corrupt payload quarantine | `2,380 raw == 2,074 valid + 134 DLQ + 172 dups` | **PASS** |
| `CHAOS-08` | Cross-bucket leak + `+15ms` latency regression | `4` leaks & `+15.996ms` guardrail breach caught | **PASS** |

---

## 5. Mandatory Acceptance-Test Matrix (15 / 15 Verified)

| # | Mandatory Acceptance Criterion | Verification Mechanism & Evidence | Status |
| :--- | :--- | :--- | :--- |
| 1 | Fresh-clone setup starts core stack & verifies health | `scripts/run_portable.sh`, `uv sync --extra dev --python 3.12`, `GET /health` -> HTTP 200 (`iceberg_tables: 10`) | **PASS** |
| 2 | Deterministic input generation produces identical IDs/outcomes | `tests/unit/test_simulator.py::test_simulation_is_strictly_deterministic_for_same_seed` | **PASS** |
| 3 | Real ingestion persists source data with metadata & replay | `LocalTopicSpool` durable WAL offsets + `tests/e2e/test_vertical_slice_e2e.py` full offset replay | **PASS** |
| 4 | Core domain transformation yields correct results for fixtures | `tests/unit/test_sessionizer.py`, `tests/unit/test_experimentation.py` | **PASS** |
| 5 | Advanced domain features operational end-to-end with integration tests | PIT ASOF join, delayed backfill, online/offline parity, Feast 0.68.0 `apply`, CUPED/SRM (`tests/integration/`) | **PASS** |
| 6 | Duplicates, late events, and corrections follow documented semantics | `CHAOS-01`, `CHAOS-02`, `CHAOS-06`, `tests/unit/test_sessionizer.py` | **PASS** |
| 7 | Service crash/wipe recovers according to tested guarantee | `CHAOS-03` (broker outage WAL drain) & `CHAOS-04` (online store wipe & Iceberg Gold rebuild) | **PASS** |
| 8 | Backfill/replay produces reconcilable results | `CHAOS-06` Iceberg V2 time-travel pre/post backfill + `LocalTopicSpool` offset replay | **PASS** |
| 9 | Primary API & dashboard return real computed outputs + freshness | Live `http://127.0.0.1:8030/` verified visually (`docs/assets/recsys_control_tower_*.png`) + `POST /v1/recommend` | **PASS** |
| 10 | DQ checks catch injected invalid inputs & retain quarantine records | `bronze.quarantine_dlq` + `quality.dq_results` (9/9 PASS) + `CHAOS-07` | **PASS** |
| 11 | Observability contains actual logs, metrics & traces (or documented limit) | Structured JSON logs with redaction, Prometheus `/metrics`, OpenTelemetry `X-Trace-Id` & `/v1/observability/traces`; OTLP/Marquez Docker scope disclosed in `docs/limitations.md` | **PASS** |
| 12 | Access control rejects unauthorized test request | `tests/integration/test_api_security.py` asserts HTTP 401 & 403 + `audit_events.jsonl` entries | **PASS** |
| 13 | Controlled failure produces incident report with observed recovery | [`docs/incidents/INC-001-chaos-drill.md`](docs/incidents/INC-001-chaos-drill.md) + `results/chaos/chaos_results.json` | **PASS** |
| 14 | Benchmark records hardware, volume, commands, throughput/latency & raw outputs | [`docs/benchmarks/benchmark-report.md`](docs/benchmarks/benchmark-report.md) + `results/benchmarks/*` + Matplotlib plot | **PASS** |
| 15 | CI and tests execute cleanly; limitations disclosed honestly | 30/30 pytest PASS, ruff 0 errors, mypy 0 errors, static audit PASS, `.github/workflows/ci.yml`, `docs/limitations.md` | **PASS** |
