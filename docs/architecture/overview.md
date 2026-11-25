# Architecture Overview

## System Context & Component Topology

The platform converts streaming playback interactions and content catalog Change Data Capture (CDC) updates into point-in-time-correct offline training sets, sub-millisecond online feature vectors, ranked candidate lists, and statistically rigorous A/B experiment readouts.

```mermaid
flowchart LR
    subgraph Sources["Deterministic Event Sources"]
        Sim["Playback & Discovery Simulator\n(v1/v2 Avro, storms, late, cold-start)"]
        CDC["Catalog CDC Source\n(PostgreSQL / Debezium 3.7.1)"]
    end

    subgraph Ingest["Ingestion & Quarantine Layer"]
        GW["Idempotent Gateway\n(FastAvro v1/v2 + event_id dedup)"]
        Spool["Durable Topic Log\n(Kafka 4.3.2 / Local WAL Spool)"]
        DLQ["Quarantine DLQ\n(bronze.quarantine_dlq)"]
    end

    subgraph Stream["Event-Time Streaming Layer"]
        Sess["Event-Time Sessionizer\n(PyFlink 2.1.3 / Local K-Way)\n30m Gap, 5m Allowed Lateness,\n-U/+U Retractions, 24h User TTL"]
    end

    subgraph Lakehouse["Apache Iceberg V2 Medallion Lakehouse"]
        Bronze["Bronze\ninteraction_events (event_day)\ncatalog_changes"]
        Silver["Silver\nviewing_sessions\ndelayed_labels"]
        Gold["Gold\nuser_features_pit\nitem_features_pit\ntraining_dataset (ASOF)\nexperiment_exposures"]
        Qual["Quality\ndq_results"]
    end

    subgraph Serving["Online Feature Store, ML & Experiments"]
        Online["Online Feature Store\n(Valkey 9.2.0 / SQLite WAL TTL)\n+ Feast 0.68.0 Repo"]
        Ranker["Candidate Gen & Ranker\n(LogReg v1 vs HistGBDT v2)\n+ MLflow Tracking Metadata"]
        Exp["Experiment Engine\n(SHA-256 Bucket, SRM Chi2,\nCUPED, Guardrails)"]
        API["FastAPI Serving API & UI\n(RBAC, /metrics, OTel Traces)"]
    end

    Sim --> GW
    CDC --> Spool
    GW -->|Valid| Spool
    GW -->|Schema / Semantic Error| DLQ
    Spool --> Bronze
    Spool --> Sess
    Sess --> Silver
    Bronze --> Gold
    Silver --> Gold
    Gold --> Online
    Gold --> Ranker
    Gold --> Exp
    Silver --> Exp
    Gold --> Qual
    Online --> API
    Ranker --> API
    Exp --> API
```

## Core Workflow Sequence (Online Serving & Delayed Label Backfill)

```mermaid
sequenceDiagram
    participant Client as Playback Client
    participant API as FastAPI /v1/recommend
    participant Exp as SHA-256 Bucketer
    participant Store as Online Feature Store
    participant Lake as Iceberg V2 Gold
    participant Backfill as Delayed Label Backfill

    Client->>API: POST /v1/recommend (X-API-Key, user_id, top_k)
    API->>Exp: SHA-256(experiment_id + salt + user_id) mod 10000
    Exp-->>API: Sticky variant (control or treatment)
    API->>Store: Fetch user & active candidate item features
    Store-->>API: Feature vectors + freshness_age_seconds
    API-->>Client: Ranked top-K titles + variant + trace headers

    Note over Lake,Backfill: Hours later, delayed completion / upgrade labels arrive
    Backfill->>Lake: Read initial gold.training_dataset snapshot
    Backfill->>Lake: Update label_completed & label_source="delayed_backfill"
    Note over Backfill,Lake: Feature timestamps remain untouched (0 future leakage)
    Backfill->>Lake: Commit new Iceberg V2 snapshot (time-travel preserved)
```

## Failure Domains & Recovery Boundaries

```mermaid
flowchart TB
    FD1["Failure Domain 1: Upstream Broker / Gateway\nFault: Heartbeat storms, malformed Avro, broker outage\nGuarantee: event_id dedup, DLQ quarantine, WAL spool & drain"]
    FD2["Failure Domain 2: Streaming Session State\nFault: Out-of-order late arrivals, unbounded user cardinality\nGuarantee: Watermark + 5m allowed lateness retractions (-U/+U), 24h TTL eviction"]
    FD3["Failure Domain 3: Online Serving Store\nFault: Valkey/SQLite wipe, stale materialization\nGuarantee: Ephemeral derived state rebuilt cleanly from Iceberg Gold (0 skew)"]
    FD4["Failure Domain 4: Experiment Routing\nFault: Traffic skew (SRM), sticky leak, latency regression\nGuarantee: Chi-squared SRM gate (p < 0.001), contamination count, guardrail hold"]
    FD1 --> FD2 --> FD3 --> FD4
```

## Data Model, Grain, Keys, Partitioning & Retention

| Table | Layer | Grain | Primary / Natural Key | Partitioning | Temporal Semantics | Retention Policy |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `bronze.interaction_events` | Bronze | 1 deduplicated client interaction | `event_id` | Identity(`event_day`) | `event_ts_ms` vs `ingest_ts_ms` | 90 days |
| `bronze.catalog_changes` | Bronze | 1 catalog CDC change event | `change_id` | Unpartitioned (compact) | `event_ts_ms` SCD2 change log | Indefinite |
| `bronze.quarantine_dlq` | Bronze | 1 rejected/corrupt payload | `dlq_id` | Unpartitioned | `quarantined_ts_ms` | 30 days |
| `silver.viewing_sessions` | Silver | 1 user viewing session window | `session_id` | Unpartitioned | `session_start_ts_ms`..`session_end_ts_ms`, `revision` | 180 days |
| `silver.delayed_labels` | Silver | 1 delayed completion/upgrade | `label_id` | Unpartitioned | `request_ts_ms` vs `label_arrival_ts_ms` | 180 days |
| `gold.user_features_pit` | Gold | 1 user feature state snapshot | `(user_id, event_ts_ms)` | Unpartitioned | `event_ts_ms`, `created_ts_ms`, `ttl_seconds` | 365 days |
| `gold.item_features_pit` | Gold | 1 item feature/catalog state | `(item_id, event_ts_ms)` | Unpartitioned | `event_ts_ms`, `created_ts_ms`, `ttl_seconds` | 365 days |
| `gold.training_dataset` | Gold | 1 recommendation decision row | `row_id` | Unpartitioned | Strict ASOF (`user_feature_ts_ms < request_ts_ms`, `item_feature_ts_ms <= request_ts_ms`) | 365 days (Iceberg snapshots) |
| `gold.experiment_exposures` | Gold | 1 user session exposure | `exposure_id` | Unpartitioned | `exposure_ts_ms` + pre-experiment covariate `X` | 365 days |
| `quality.dq_results` | Quality | 1 DQ assertion per run | `check_id` | Unpartitioned | `checked_ts_ms` | 180 days |

## Explicit Consistency & Delivery Boundaries

- **Ingestion:** At-least-once delivery from upstream clients and topic spool; **idempotent effects** achieved via `event_id` deduplication before Bronze commit and inside the sessionizer state store.
- **Session Windows:** Event-time processing with 30s out-of-order watermark bound, 30m inactivity gap, and 5m allowed lateness emitting `-U`/`+U` retractions. Events arriving past `window_end + gap + allowed_lateness` are explicitly dropped to side-output counters rather than silently mutating expired state.
- **Iceberg Lakehouse:** Atomic snapshot commits via Iceberg V2 metadata (`format-version=2`). Delayed label backfills write a new snapshot without rewriting historical feature timestamps, enabling deterministic time-travel auditing.
- **Online Feature Store:** Eventually consistent derived projection of Iceberg Gold materialized on schedule (or on-demand rebuild). Parity is continuously verified against Iceberg Gold (`mismatch_ratio == 0.0`).
