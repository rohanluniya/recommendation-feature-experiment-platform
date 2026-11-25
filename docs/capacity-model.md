# Capacity Model & Scaling Envelope

## Measured Local Envelope (`local_native`, Apple M4 Pro, 48 GiB RAM)

All local figures are measured directly via `benchmarks/run_benchmarks.py` and backed by `results/benchmarks/benchmark_summary.json` (see `docs/benchmarks/benchmark-report.md` for exact run outputs).

- **Memory Footprint:** Measured single-process Python peak RSS across profiles: `337.7 MiB` (`smoke`, 5,067 events), `486.7 MiB` (`local`, 28,720 events), and `870.7 MiB` (`stress`, 96,211 events) while running simulation, ingestion, event-time sessionization, Iceberg V2 writes, PIT ASOF joins, and dual ranker training in one process.
- **Disk Footprint:** Zstd-compressed Iceberg V2 Parquet files plus SQLite catalog and WAL online store occupy < 25 MiB per full `local` run.

## Production Reference Capacity Model (Projected Sizing — Distinct from Local Measurements)

The following table documents **architectural sizing calculations** for a 10M DAU streaming workload (clearly separated from measured local benchmarks):

| Subsystem | Sizing Formula / Assumption | Estimated Production Footprint |
| :--- | :--- | :--- |
| **Kafka Ingress** | 50,000 events/sec peak, ~350 B Avro binary/event, 3x replication, 7-day retention | `~17.5 MB/s` raw (`52.5 MB/s` replicated), `~31.8 TB` broker disk across 32 partitions |
| **Flink Session State** | 4M concurrent active users within 24h TTL, ~2 KB RocksDB state/user | `~8 GB` active keyed state + incremental checkpoints to S3/RustFS every 30s |
| **Iceberg Bronze & Gold** | 4.3B events/day (~300 B compressed Zstd Parquet ~5x compression -> 60 B/row) | `~258 GB/day` Bronze (`~23 TB` for 90-day retention); daily bin-pack compaction |
| **Valkey Online Store** | 10M active users (~600 B hash/user) + 50k items (~800 B/item), 2x overhead/replication | `~12.1 GB` RAM across a 3-shard Valkey 9.2 cluster (24 GB provisioned with headroom) |
| **FastAPI Serving Fleet** | 20,000 recommendation QPS peak, ~4ms compute/req per worker | ~80-100 stateless API pods behind L7 load balancer with horizontal autoscaling |

## Known Scaling Bottlenecks & Mitigations

1. **Iceberg Small Files from Streaming Micro-Batches:** Frequent appends inflate Parquet file scan overhead. Mitigated via scheduled `compact_table()` / Spark `rewrite_data_files` bin-packing (measured in our compaction benchmark).
2. **High-Cardinality Power Users:** A single user generating 100x normal heartbeat volume can skew partition keys. Mitigated via `event_id` pre-deduplication and bounded user state TTL.
3. **Full-Table Parity Validation at 10M Users:** Checking every entity synchronously works for hundreds/thousands of entities locally; at 10M users, switch parity validation to stratified 1% random sampling + 100% check on top-traffic users every 15 minutes.
