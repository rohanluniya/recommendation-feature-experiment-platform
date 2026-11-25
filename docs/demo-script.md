# 5-Minute Live Engineering Demo Script

This script walks through the complete end-to-end platform from a fresh clone with copy-pasteable commands.

## 1. Fresh Setup & Vertical Slice Run (30 seconds)

```bash
uv sync --extra dev --python 3.12
uv run python scripts/run_vertical_slice.py --seed 42
```

**What to point out:**
- Deterministic Avro v1/v2 ingestion drops duplicate heartbeats and quarantines corrupt payloads into `bronze.quarantine_dlq` with 100% conservation accounting.
- Event-time sessionization emits late-arrival retractions (`-U`/`+U`) and drops events past cleanup horizon.
- Strict PIT ASOF join produces **0 future leakage violations**, whereas the naive global join leaks 100% of rows and artificially inflates GBDT ROC-AUC.
- Delayed completion labels backfill cleanly onto a new Iceberg V2 snapshot with **0 feature timestamp mutations**.
- CUPED reduces watch-time variance by ~75% and cuts 95% CI width in half.

## 2. Feast 0.68.0 Feature Repository & Parity Verification (20 seconds)

```bash
uv run python services/feature_materializer.py --export-feast
uv run feast -c feast_repo apply
uv run feast -c feast_repo feature-views list
```

## 3. Start Live FastAPI Serving API & Control Tower Dashboard (30 seconds)

```bash
uv run uvicorn apps.recsys_api.main:app --host 127.0.0.1 --port 8030
```

Open `http://127.0.0.1:8030/` in a browser:
1. Show the 6 live KPI cards (0 PIT leaks, 100% online/offline parity, CUPED variance reduction, SRM healthy, sessions/retractions, 9/9 DQ checks).
2. In the **Interactive Live Recommendation & Experiment Simulator**, select a warm user (e.g. `user_00000`) and then a cold-start user (`user_00235`) and click **Score & Bucket** to observe deterministic SHA-256 variant assignment, online feature vector lookup, and model switching (`baseline_logreg` vs `treatment_gbdt`).
3. Inspect the **CUPED vs Unadjusted 95% CI** forest plot and **PIT vs Naive Join Leakage** AUC bar chart.

## 4. Demonstrate RBAC Security & Live Online Serving via `curl` (30 seconds)

```bash
# Unauthorized call -> HTTP 401
curl -i http://127.0.0.1:8030/v1/features/online/user_00000

# Serving key -> HTTP 200 live feature vector
curl -s -H "X-API-Key: dev-serving-key-change-me" \
  http://127.0.0.1:8030/v1/features/online/user_00000 | python3 -m json.tool

# Serving key attempting admin rebuild -> HTTP 403 Forbidden
curl -i -X POST -H "X-API-Key: dev-serving-key-change-me" \
  http://127.0.0.1:8030/v1/admin/rebuild-online-store

# Admin key rebuilding online store from Iceberg Gold -> HTTP 200 with 0 parity skew
curl -s -X POST -H "X-API-Key: dev-admin-key-change-me" \
  http://127.0.0.1:8030/v1/admin/rebuild-online-store | python3 -m json.tool

# Live Prometheus metrics exposition
curl -s http://127.0.0.1:8030/metrics | grep recsys_
```

## 5. Run Automated Chaos Drills & Full Verification Suite (60 seconds)

```bash
uv run pytest -v
uv run python benchmarks/run_benchmarks.py
```
