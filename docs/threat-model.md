# Security Architecture & Threat Model

## Authentication, Authorization & Audit Trail

The FastAPI control plane enforces role-based API key authentication via `X-API-Key` across three privilege tiers:

| Role | Allowed Operations | Rejected Operations |
| :--- | :--- | :--- |
| `serving` | `GET /v1/features/online/{user_id}`, `GET /v1/features/online/item/{item_id}`, `POST /v1/recommend`, `GET /v1/experiments/{id}/bucket/{user_id}` | All analyst & admin routes (`403 Forbidden`) |
| `analyst` | All `serving` routes + `/v1/pit/audit`, `/v1/parity`, `/v1/dq`, `/v1/experiments/{id}/evaluate`, `/v1/lineage`, `/v1/observability/traces` | All admin mutation routes (`403 Forbidden`) |
| `admin` | All routes including `POST /v1/admin/rebuild-online-store` and `POST /v1/admin/pipeline/run` | None |
| `anonymous` | `GET /health`, `GET /metrics`, `GET /`, `GET /v1/dashboard/overview` (sanitized local summary) | All `/v1/features/*`, `/v1/recommend`, `/v1/experiments/*`, `/v1/pit/*`, `/v1/parity`, `/v1/dq`, `/v1/admin/*` (`401 Unauthorized`) |

Every rejected authentication/authorization attempt (`401`/`403`) and every privileged admin mutation is recorded in `.local_data/audit_events.jsonl` with correlation ID, trace ID, actor role, action, resource, and redacted details.

## STRIDE Threat Analysis

| Category | Threat Scenario | Implemented Control | Production Hardening Needed |
| :--- | :--- | :--- | :--- |
| **Spoofing** | Unauthorized caller queries user feature vectors or triggers online store wipe | `X-API-Key` RBAC middleware rejecting missing/invalid keys with `401` | Replace static API keys with short-lived OIDC/JWT tokens and mTLS |
| **Tampering** | Corrupt or negative watch-time events poison training features or A/B metrics | FastAvro v1/v2 validation + semantic checks route bad events to `bronze.quarantine_dlq` | Enforce Kafka ACLs and Apicurio Registry schema-ID wire signing |
| **Repudiation** | Operator runs backfill or rebuild without audit trail | Immutable `audit_events.jsonl` + Iceberg V2 snapshot history + OpenLineage JSONL | Ship audit logs to WORM object storage / SIEM with cryptographic chaining |
| **Information Disclosure** | PII (emails, tokens, credentials) leaked in logs or stack traces | `recsys_platform/logging_utils.py` regex & key-name redactor (`[REDACTED]`) | Field-level encryption (KMS) and hashed pseudonymous viewer IDs |
| **Denial of Service** | Heartbeat replay storm or unbounded user cardinality exhausts streaming state | `event_id` deduplication, bounded 24h user state TTL eviction, side-output drop past allowed lateness | API gateway rate limiting (Envoy/Kong), Flink backpressure alarms |
| **Elevation of Privilege** | Serving key attempts admin rebuild or pipeline execution | Hierarchical RBAC dependency (`require_role("admin")`) returning `403` | Kubernetes RBAC, non-root container (`USER recsys`), NetworkPolicies |

## Explicit Dev-vs-Production Security Limitations

- `.env.example` contains **non-secret local demo defaults** (`dev-*-key-change-me`) strictly for local evaluation. In production, keys must be injected from a secrets manager (Vault, GCP Secret Manager, AWS Secrets Manager) and rotated periodically.
- `/v1/dashboard/overview` and `/metrics` are unauthenticated on `127.0.0.1` in `local_native` mode to allow local browser inspection and Prometheus scraping. In production, place both behind an authenticated internal ingress / IAP and disable demo key pre-fill (`RECSYS_ENV != local_native` automatically omits `demo_serving_api_key`).
- No TLS termination is enabled on local `127.0.0.1:8030`; terminate TLS 1.3 at the ingress load balancer in production.
