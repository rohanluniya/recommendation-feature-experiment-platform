#!/usr/bin/env bash
# Zero-Docker one-command portable runner for macOS ARM64 / Linux x86_64.
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_DIR"

if ! command -v uv >/dev/null 2>&1; then
  echo "ERROR: 'uv' is required. Install from https://docs.astral.sh/uv/getting-started/installation/"
  exit 1
fi

echo "[1/4] Syncing locked Python 3.12 environment..."
uv sync --extra dev --python 3.12

echo "[2/4] Executing end-to-end vertical slice (Iceberg V2 + Event-Time Sessionizer + PIT ASOF + CUPED/SRM)..."
uv run python scripts/run_vertical_slice.py --seed "${RECSYS_SEED:-42}"

echo "[3/4] Exporting Feast snapshots and applying Feast 0.68.0 repository..."
uv run python services/feature_materializer.py --export-feast
uv run feast -c feast_repo apply

echo "[4/4] Starting live FastAPI Serving API & Control Tower at http://${RECSYS_API_HOST:-127.0.0.1}:${RECSYS_API_PORT:-8030} ..."
exec uv run uvicorn apps.recsys_api.main:app \
  --host "${RECSYS_API_HOST:-127.0.0.1}" \
  --port "${RECSYS_API_PORT:-8030}"
