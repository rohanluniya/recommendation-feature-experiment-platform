# Component Compatibility & License Matrix

## Host Verification Summary

- **Verified Workstation:** macOS `Darwin 24.6.0` (`arm64`, Apple M4 Pro, 14 logical cores, 48 GiB RAM)
- **Python Runtime:** CPython `3.12.8` managed via `uv 0.12.3` (`uv.lock` pinned)
- **Host Constraints:** No Docker daemon, Podman, Node.js, or host JVM available on the verification workstation. Accordingly, the `local_native` profile runs 100% natively in Python 3.12 (PyIceberg V2, DuckDB, FastAvro, Feast 0.68.0, SQLite WAL online store, scikit-learn, SciPy, FastAPI/Uvicorn, Prometheus, OpenTelemetry), while distributed engines (Kafka, Flink, Spark, Trino, Airflow, Valkey, Lakekeeper, RustFS, Debezium, MLflow, Grafana, Marquez) are fully configured with pinned multi-arch tags in `docker-compose.yml`.

## Verified Core Python Stack (`local_native` + Standalone Container)

| Package | Pinned Version | License | ARM64 macOS | Linux x86_64 | Role |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `pyiceberg` (`sql-sqlite`, `pyiceberg-core`) | `0.12.0` | Apache-2.0 | Verified | Supported | Apache Iceberg V2 catalog, snapshots, compaction |
| `duckdb` | `2.0.0` | MIT | Verified | Supported | Analytical SQL over Iceberg Arrow scans |
| `pyarrow` | `25.0.1` | Apache-2.0 | Verified | Supported | Columnar Parquet I/O & zero-copy interchange |
| `fastavro` | `1.13.1` | MIT | Verified | Supported | Avro v1/v2 schema validation & evolution |
| `feast` | `0.68.0` | Apache-2.0 | Verified | Supported | Declarative FeatureViews, entities & FeatureService |
| `valkey` | `6.2.0` | MIT | Verified | Supported | Valkey wire-protocol client for online store |
| `confluent-kafka` | `2.16.1` | Apache-2.0 | Verified | Supported | Kafka client for containerized streaming profile |
| `fastapi` / `uvicorn` | `0.163.2` / `0.54.0` | MIT / BSD-3 | Verified | Supported | Serving API, RBAC auth, Control Tower UI |
| `scikit-learn` / `scipy` / `numpy` | `1.9.1` / `1.18.1` / `2.5.5` | BSD-3-Clause | Verified | Supported | Baseline/GBDT rankers, chi-squared SRM, CUPED |
| `prometheus-client` | `0.24.1` | Apache-2.0 | Verified | Supported | Prometheus `/metrics` exposition (aligned with Feast 0.68.0 constraint) |
| `opentelemetry-api` / `sdk` | `1.45.2` | Apache-2.0 | Verified | Supported | Distributed span instrumentation |
| `hypothesis` / `pytest` | `6.170.15` / `9.2.0` | MPL-2.0 / MIT | Verified | Supported | Property-based, unit, contract, integration, chaos tests |

> **Compatibility note on `prometheus-client` & Feast:** `feast==0.68.0` resolves cleanly alongside `pyiceberg==0.12.0`, `pyarrow==25.0.1`, `pydantic==2.14.3`, and `numpy==2.5.5` when `prometheus-client` is pinned to `0.24.1`.
> **Compatibility note on PyFlink & PySpark:** `apache-flink==2.1.3` requires `pyarrow<21` and Java 17; `pyspark==4.1.4` requires Java 17. Both are isolated into dedicated container images (`docker-compose.yml`) and `infra/docker/requirements-*.txt` so the core serving/lakehouse Python 3.12 environment stays on `pyarrow==25.0.1`.

## Pinned Container Stack (`docker-compose.yml` Profiles)

| Component | Pinned Image Tag | Profile(s) | License | ARM64 / x86_64 Note |
| :--- | :--- | :--- | :--- | :--- |
| Apache Kafka (KRaft) | `apache/kafka:4.3.2` | `core`, `full` | Apache-2.0 | Multi-arch |
| Apicurio Registry | `apicurio/apicurio-registry:3.3.5` | `core`, `full` | Apache-2.0 | Multi-arch |
| Valkey | `valkey/valkey:9.2.0-alpine3.24` | `core`, `full` | BSD-3-Clause | Multi-arch |
| RustFS (S3 Object Store) | `rustfs/rustfs:1.0.6` | `core`, `analytics`, `full` | Apache-2.0 | Multi-arch |
| Lakekeeper (Iceberg REST) | `quay.io/lakekeeper/catalog:v0.15.1` | `core`, `analytics`, `full` | Apache-2.0 | Multi-arch |
| PostgreSQL / PostGIS | `imresamu/postgis:17-3.6.1-alpine3.23` | `core`, `analytics`, `full` | PostgreSQL / GPL-2 (container service) | Multi-arch |
| Debezium Connect | `quay.io/debezium/connect:3.7.1.Final` | `core`, `full` | Apache-2.0 | Multi-arch |
| Apache Flink | `flink:2.1.3-scala_2.12-java17` | `core`, `full` | Apache-2.0 | Multi-arch |
| Apache Spark | `apache/spark:4.1.4-scala2.13-java17-python3-ubuntu` | `analytics`, `full` | Apache-2.0 | Multi-arch |
| Trino | `trinodb/trino:484` | `analytics`, `full` | Apache-2.0 | Multi-arch |
| Apache Airflow | `apache/airflow:slim-3.4.0-python3.12` | `analytics`, `full` | Apache-2.0 | Multi-arch |
| MLflow Server | `ghcr.io/mlflow/mlflow:v3.18.0` | `analytics`, `full` | Apache-2.0 | Multi-arch |
| Prometheus | `prom/prometheus:v3.16.0` | `observability`, `full` | Apache-2.0 | Multi-arch |
| Grafana | `grafana/grafana:13.0.14` | `observability`, `full` | AGPL-3.0 (standalone observability server) | Multi-arch |
| Grafana Loki | `grafana/loki:3.6.22` | `observability`, `full` | AGPL-3.0 (standalone observability server) | Multi-arch |
| OTel Collector Contrib | `otel/opentelemetry-collector-contrib:0.166.0-${OTEL_ARCH:-arm64}` | `observability`, `full` | Apache-2.0 | Arch parameterized via `OTEL_ARCH` |
| Marquez (OpenLineage) | `marquezproject/marquez:0.51.1` | `observability`, `full` | Apache-2.0 | Pinned `platform: linux/amd64` (Rosetta/QEMU on ARM64) |

## Model & Dataset License Compliance

- **Pretrained Model Weights:** None downloaded or bundled. Both `baseline_logreg` and `treatment_gbdt` are trained from scratch locally using `scikit-learn` (BSD-3-Clause).
- **Datasets:** 100% deterministic seeded synthetic data (`recsys_platform/simulator.py`); zero external copyrighted media metadata or user PII.
