# Known Limitations & Honest Disclosure

This document explicitly records what runs natively on the verification host versus what requires a Docker/Kubernetes host, along with analytical and operational boundaries.

## Runtime & Infrastructure Limitations

1. **No Local Docker/JVM Execution on Verification Host:** The Apple M4 Pro workstation used to build and verify this repository has no Docker daemon, Podman, Node.js, or host JVM. Consequently:
   - The `local_native` profile (Python 3.12.8, PyIceberg V2, DuckDB 2.0.0, FastAvro 1.13.1, Feast 0.68.0, SQLite WAL online store, scikit-learn, SciPy, FastAPI, Uvicorn, Prometheus, OpenTelemetry) is **100% executed and verified end-to-end**.
   - Multi-container distributed services in `docker-compose.yml` (Kafka 4.3.2, Apicurio 3.3.5, Valkey 9.2.0, RustFS 1.0.6, Lakekeeper v0.15.1, Debezium 3.7.1, Flink 2.1.3, Spark 4.1.4, Trino 484, Airflow 3.4.0, MLflow v3.18.0, Grafana 13.0.14, Marquez 0.51.1) are syntax-validated and pinned, but were **not started live** on this Docker-less host.
2. **Automatic OpenLineage / Marquez Ingestion:** In `local_native` mode, OpenLineage v1-compatible events are written durably to `.local_data/lineage/lineage_events.jsonl` and surfaced via `GET /v1/lineage`. Live HTTP push to Marquez (`marquezproject/marquez:0.51.1`, `linux/amd64`) activates only when the Docker `observability` profile is running.
3. **OpenTelemetry Span Export:** Locally, spans are captured via `InMemorySpanExporter` and inspectable via `GET /v1/observability/traces` and `X-Trace-Id` HTTP headers. OTLP gRPC export to `otel-collector` activates in the Docker `observability` profile.

## Domain & Statistical Limitations

1. **Synthetic Data vs. Real Viewer Behavior:** All benchmark and experiment numbers come from the deterministic seeded simulator (`recsys_platform/simulator.py`). CUPED variance reduction (~76%) reflects strong simulated correlation (`r ~ 0.87`) between 7-day pre-experiment watch time and post-exposure watch time; real-world streaming platforms typically observe 20%–55% CUPED variance reduction depending on metric noise and seasonality.
2. **Catalog Popularity Spillover (Interference):** As flagged explicitly in `interference_diagnostics`, control and treatment viewers read shared global item-level features (`item_completion_rate_so_far`, `item_plays_so_far`). In long-running high-traffic production tests, treatment lift on specific titles can spill over into control rankings. Mitigation requires cluster/title-stratified experiments or variant-isolated item feature counters.
3. **Single-Process SQLite Online Store Concurrency:** The `sqlite_wal_valkey_compat` backend delivers sub-millisecond reads (`0.072 ms` p50) for single-host demos and tests, but multi-pod production serving requires switching `VALKEY_ENABLED=true` against a replicated Valkey 9.2.0 cluster.
4. **Security Hardening for Production:** Static API keys from `.env.example`, local HTTP (non-TLS), and the unauthenticated local `/v1/dashboard/overview` summary are intentionally scoped for `127.0.0.1` evaluation only (see [`docs/threat-model.md`](threat-model.md)).
