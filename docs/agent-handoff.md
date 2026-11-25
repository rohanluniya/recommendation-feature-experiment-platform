# Agent Handoff & Operational Resume Guide

## Current Status

All 8 phases of Project 04 (`recommendation-feature-experiment-platform`) are implemented, tested, benchmarked, visually verified, and running live on `127.0.0.1:8030` in `local_native` mode.

## Verified Commands & Evidence

| Check | Exact Command | Latest Verified Result |
| :--- | :--- | :--- |
| Environment Sync | `uv sync --extra dev --python 3.12` | 103 locked packages (`uv.lock`), including `feast==0.68.0`, `pyiceberg==0.12.0`, `duckdb==2.0.0` |
| End-to-End Vertical Slice | `uv run python scripts/run_vertical_slice.py --seed 42` | `1.344s`, 0 PIT leaks, 0 parity skew, 9/9 DQ PASS, CUPED `-76.23%` variance |
| Feast Repo Apply | `uv run python services/feature_materializer.py --export-feast && uv run feast -c feast_repo apply` | 2 entities, 2 FeatureViews, 1 FeatureService (`recsys_ranking_v2`) applied cleanly |
| Full Test Suite (Unit + Property + Contract + Integration + E2E + 8 Chaos + Perf) | `uv run pytest -v --junitxml=results/tests/pytest_report.xml` | 30/30 PASS (`results/chaos/chaos_results.json` = 8/8 PASS) |
| Benchmarks & Chart | `uv run python benchmarks/run_benchmarks.py` | `results/benchmarks/benchmark_summary.json`, `load_profiles.csv`, `cuped_scaling.csv`, `docs/assets/benchmark_recsys_feature_experiment.png` |
| Static & Security Gates | `uv run ruff check . && uv run mypy recsys_platform apps services && uv run python scripts/static_audit.py` | 0 lint errors, 0 mypy errors, 0 unpinned tags, 0 secrets |
| Compliance & SBOM | `uv run python scripts/check_licenses.py && uv run python scripts/generate_sbom.py` | `results/compliance/license_report.json` (`PASS`), `results/compliance/sbom.cdx.json` (CycloneDX 1.5) |
| Live Control Tower | `curl -sf http://127.0.0.1:8030/health` | HTTP 200 (`iceberg_tables: 10`, `active_users: 240`, `active_items: 60`) |

## Next Engineering Steps (When Running on a Docker-Enabled Host)

1. Start the containerized `core` profile: `docker compose --profile core up -d` and set `VALKEY_ENABLED=true`.
2. Register the Debezium PostgreSQL CDC connector: `curl -X POST http://localhost:8083/connectors -H 'Content-Type: application/json' -d @ingestion/cdc/postgres-catalog-connector.json`.
3. Submit `processing/flink/sessionization_job.py` to Flink 2.1.3 and `processing/spark/pit_training_builder.py` to Spark 4.1.4.
4. Enable `observability` profile (`docker compose --profile observability up -d`) to activate live Grafana 13.0.14 dashboards, OTLP Collector ingestion, and Marquez 0.51.1 OpenLineage graph UI.
