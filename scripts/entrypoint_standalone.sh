#!/usr/bin/env bash
set -euo pipefail

echo "[recsys-standalone] Bootstrapping Iceberg V2 Medallion warehouse, PIT features, online store & rankers..."
/app/.venv/bin/python /app/scripts/run_vertical_slice.py --seed "${RECSYS_SEED:-42}"

echo "[recsys-standalone] Starting FastAPI Control Tower on ${RECSYS_API_HOST:-0.0.0.0}:${RECSYS_API_PORT:-8030}..."
exec /app/.venv/bin/uvicorn apps.recsys_api.main:app \
  --host "${RECSYS_API_HOST:-0.0.0.0}" \
  --port "${RECSYS_API_PORT:-8030}"
