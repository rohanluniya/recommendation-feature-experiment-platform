"""FastAPI Serving API & Control Tower for the Recommendation Feature & Experimentation Platform."""

from __future__ import annotations

import json
import time
from dataclasses import asdict
from typing import Any

from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request, Response
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from recsys_platform.config import REPO_ROOT, PlatformConfig, get_config
from recsys_platform.experimentation import assign_user_variant, evaluate_experiment
from recsys_platform.features import audit_pit_leakage
from recsys_platform.lakehouse import IcebergLakehouse
from recsys_platform.logging_utils import (
    CORRELATION_ID_VAR,
    append_audit_event,
    get_logger,
    log_event,
    new_correlation_id,
)
from recsys_platform.observability import TELEMETRY
from recsys_platform.online_store import OnlineFeatureStore
from recsys_platform.pipeline import run_full_pipeline
from recsys_platform.ranker import generate_and_rank_candidates
from recsys_platform.simulator import SimulationConfig

logger = get_logger("recsys_platform.api")
DASHBOARD_DIR = REPO_ROOT / "apps" / "recsys-dashboard"

ROLE_HIERARCHY = {"serving": 1, "analyst": 2, "admin": 3}


class RecommendRequest(BaseModel):
    user_id: str = Field(..., min_length=1, max_length=128)
    experiment_id: str | None = Field(default=None, max_length=128)
    top_k: int = Field(default=5, ge=1, le=20)


class PipelineRunRequest(BaseModel):
    seed: int = Field(default=42, ge=0, le=100_000)
    num_users: int = Field(default=240, ge=20, le=2000)
    num_items: int = Field(default=60, ge=10, le=500)
    interactions_per_user: int = Field(default=18, ge=4, le=100)


def create_app(config: PlatformConfig | None = None) -> FastAPI:
    """Create a configured FastAPI application instance."""
    cfg = config or get_config()
    app = FastAPI(
        title="Real-Time Recommendation Feature & Experimentation Platform",
        version="0.1.0",
        description="Online feature serving, candidate ranking, PIT leakage audit, offline/online parity, and SRM/CUPED experimentation API.",
    )
    app.state.config = cfg

    if DASHBOARD_DIR.exists():
        app.mount("/static", StaticFiles(directory=str(DASHBOARD_DIR)), name="static")

    def _get_cfg() -> PlatformConfig:
        return app.state.config  # type: ignore[no-any-return]

    def _resolve_role(api_key: str | None, c: PlatformConfig) -> str | None:
        if not api_key:
            return None
        if api_key == c.admin_api_key:
            return "admin"
        if api_key == c.analyst_api_key:
            return "analyst"
        if api_key == c.serving_api_key:
            return "serving"
        return None

    def require_role(min_role: str):
        def _dep(
            request: Request,
            x_api_key: str | None = Header(default=None, alias="X-API-Key"),
        ) -> str:
            c = _get_cfg()
            role = _resolve_role(x_api_key, c)
            if role is None:
                append_audit_event(
                    c.audit_log_path,
                    actor_role="anonymous",
                    action="auth_denied_401",
                    resource=request.url.path,
                    status="DENIED",
                    details={"reason": "missing_or_invalid_api_key"},
                )
                raise HTTPException(status_code=401, detail="Missing or invalid X-API-Key")
            if ROLE_HIERARCHY[role] < ROLE_HIERARCHY[min_role]:
                append_audit_event(
                    c.audit_log_path,
                    actor_role=role,
                    action="auth_denied_403",
                    resource=request.url.path,
                    status="FORBIDDEN",
                    details={"required_role": min_role},
                )
                raise HTTPException(
                    status_code=403,
                    detail=f"Role '{role}' lacks required permission '{min_role}'",
                )
            return role

        return _dep

    @app.middleware("http")
    async def observability_middleware(request: Request, call_next: Any) -> Response:
        corr_id = request.headers.get("X-Correlation-Id") or new_correlation_id()
        token = CORRELATION_ID_VAR.set(corr_id)
        t0 = time.perf_counter()
        try:
            with TELEMETRY.span(
                f"http.{request.method}.{request.url.path}",
                {"http.method": request.method, "http.path": request.url.path},
            ) as span:
                response: Response = await call_next(request)
                elapsed_ms = round((time.perf_counter() - t0) * 1000.0, 3)
                ctx = span.get_span_context()
                trace_hex = f"{ctx.trace_id:032x}" if ctx and ctx.trace_id else ""
                response.headers["X-Correlation-Id"] = corr_id
                response.headers["X-Trace-Id"] = trace_hex
                TELEMETRY.api_requests.labels(
                    route=request.url.path, status_code=str(response.status_code)
                ).inc()
                log_event(
                    logger,
                    20,
                    "HTTP request handled",
                    method=request.method,
                    path=request.url.path,
                    status_code=response.status_code,
                    duration_ms=elapsed_ms,
                )
                return response
        finally:
            CORRELATION_ID_VAR.reset(token)

    def _ensure_bootstrapped(c: PlatformConfig) -> dict[str, Any]:
        summary_path = c.data_dir / "latest_pipeline_summary.json"
        if not summary_path.exists():
            return run_full_pipeline(c)
        return json.loads(summary_path.read_text(encoding="utf-8"))  # type: ignore[no-any-return]

    @app.get("/", include_in_schema=False)
    @app.get("/dashboard", include_in_schema=False)
    def serve_dashboard() -> FileResponse:
        index = DASHBOARD_DIR / "index.html"
        if not index.exists():
            raise HTTPException(status_code=404, detail="Dashboard UI assets not found")
        return FileResponse(str(index))

    @app.get("/health")
    def health() -> dict[str, Any]:
        c = _get_cfg()
        lh = IcebergLakehouse(c)
        store = OnlineFeatureStore(c)
        return {
            "status": "ok",
            "environment": c.env,
            "online_store": store.stats(),
            "iceberg_tables": len(lh.table_inventory()),
            "timestamp_ms": int(time.time() * 1000),
        }

    @app.get("/metrics", response_class=PlainTextResponse)
    def prometheus_metrics() -> Response:
        return PlainTextResponse(
            content=TELEMETRY.render_prometheus().decode("utf-8"),
            media_type="text/plain; version=0.0.4; charset=utf-8",
        )

    @app.get("/v1/dashboard/overview")
    def dashboard_overview() -> JSONResponse:
        """Read-only sanitized platform snapshot for the local Control Tower dashboard."""
        c = _get_cfg()
        summary = _ensure_bootstrapped(c)
        lh = IcebergLakehouse(c)
        store = OnlineFeatureStore(c)

        sessions = lh.read_records("silver.viewing_sessions")
        retracted_first = sorted(
            sessions,
            key=lambda s: (1 if s.get("had_late_retraction") else 0, int(s.get("device_switch_count", 0)), float(s.get("total_watch_seconds", 0.0))),
            reverse=True,
        )[:12]
        dlq_sample = lh.read_records("bronze.quarantine_dlq")[:10]
        sample_users = [u["user_id"] for u in store.list_entities("user", limit=24)]

        payload = {
            "environment": c.env,
            "online_store_stats": store.stats(),
            "summary": summary,
            "sample_users": sample_users,
            "sample_sessions": retracted_first,
            "sample_dlq": dlq_sample,
            "recent_traces": TELEMETRY.recent_traces(limit=12),
            "demo_serving_api_key": c.serving_api_key if c.env == "local_native" else "",
        }
        return JSONResponse(content=payload)

    @app.get("/v1/features/online/{user_id}")
    def get_online_user_features(
        user_id: str,
        role: str = Depends(require_role("serving")),
    ) -> dict[str, Any]:
        c = _get_cfg()
        store = OnlineFeatureStore(c)
        feats = store.get_user_features(user_id)
        if feats is None:
            raise HTTPException(status_code=404, detail=f"Online features not found for user '{user_id}'")
        now_ms = int(time.time() * 1000)
        mat_ms = int(feats.get("materialized_ts_ms", 0))
        return {
            "user_id": user_id,
            "backend_mode": store.backend_mode,
            "role": role,
            "freshness_age_seconds": round(max(0.0, (now_ms - mat_ms) / 1000.0), 3) if mat_ms else 0.0,
            "features": feats,
        }

    @app.get("/v1/features/online/item/{item_id}")
    def get_online_item_features(
        item_id: str,
        role: str = Depends(require_role("serving")),
    ) -> dict[str, Any]:
        c = _get_cfg()
        store = OnlineFeatureStore(c)
        feats = store.get_item_features(item_id)
        if feats is None:
            raise HTTPException(status_code=404, detail=f"Online features not found for item '{item_id}'")
        return {"item_id": item_id, "backend_mode": store.backend_mode, "role": role, "features": feats}

    @app.post("/v1/recommend")
    def recommend(
        body: RecommendRequest,
        role: str = Depends(require_role("serving")),
    ) -> dict[str, Any]:
        c = _get_cfg()
        _ensure_bootstrapped(c)
        store = OnlineFeatureStore(c)
        result = generate_and_rank_candidates(
            user_id=body.user_id,
            online_store=store,
            config=c,
            experiment_id=body.experiment_id,
            top_k=body.top_k,
        )
        result["role"] = role
        return result

    @app.get("/v1/experiments/{experiment_id}/bucket/{user_id}")
    def experiment_bucket(
        experiment_id: str,
        user_id: str,
        _role: str = Depends(require_role("serving")),
    ) -> dict[str, Any]:
        c = _get_cfg()
        return assign_user_variant(
            user_id=user_id,
            experiment_id=experiment_id,
            salt=c.default_experiment_salt,
        )

    @app.get("/v1/experiments/{experiment_id}/evaluate")
    def experiment_evaluate(
        experiment_id: str,
        expected_treatment_ratio: float = Query(default=0.50, gt=0.05, lt=0.95),
        _role: str = Depends(require_role("analyst")),
    ) -> dict[str, Any]:
        c = _get_cfg()
        _ensure_bootstrapped(c)
        lh = IcebergLakehouse(c)
        ev = evaluate_experiment(
            exposures=lh.read_records("gold.experiment_exposures"),
            sessions=lh.read_records("silver.viewing_sessions"),
            delayed_labels=lh.read_records("silver.delayed_labels"),
            interactions=lh.read_records("bronze.interaction_events"),
            experiment_id=experiment_id,
            expected_treatment_ratio=expected_treatment_ratio,
        )
        return asdict(ev)

    @app.get("/v1/pit/audit")
    def pit_audit(_role: str = Depends(require_role("analyst"))) -> dict[str, Any]:
        c = _get_cfg()
        summary = _ensure_bootstrapped(c)
        lh = IcebergLakehouse(c)
        live_rows = lh.read_records("gold.training_dataset")
        live_audit = asdict(audit_pit_leakage(live_rows))
        return {
            "live_gold_training_audit": live_audit,
            "latest_run_pit_vs_naive": summary.get("pit_audit", {}),
            "delayed_label_backfill": summary.get("delayed_label_backfill", {}),
            "training_snapshots": lh.snapshot_history("gold.training_dataset"),
        }

    @app.get("/v1/parity")
    def parity_check(_role: str = Depends(require_role("analyst"))) -> dict[str, Any]:
        c = _get_cfg()
        _ensure_bootstrapped(c)
        lh = IcebergLakehouse(c)
        store = OnlineFeatureStore(c)
        return asdict(store.validate_parity_against_lakehouse(lh))

    @app.get("/v1/dq")
    def dq_status(_role: str = Depends(require_role("analyst"))) -> dict[str, Any]:
        c = _get_cfg()
        _ensure_bootstrapped(c)
        lh = IcebergLakehouse(c)
        checks = lh.read_records("quality.dq_results")
        return {
            "total_checks": len(checks),
            "passed_checks": sum(1 for x in checks if x["status"] == "PASS"),
            "failed_checks": sum(1 for x in checks if x["status"] != "PASS"),
            "checks": checks,
        }

    @app.get("/v1/lineage")
    def lineage(_role: str = Depends(require_role("analyst"))) -> dict[str, Any]:
        c = _get_cfg()
        lh = IcebergLakehouse(c)
        events: list[dict[str, Any]] = []
        lineage_file = c.lineage_dir / "lineage_events.jsonl"
        if lineage_file.exists():
            for line in lineage_file.read_text(encoding="utf-8").splitlines()[-25:]:
                if line.strip():
                    events.append(json.loads(line))
        return {
            "runtime_mode": "local_native_file_sink",
            "marquez_sync_note": "Local OpenLineage JSONL emitted; automatic Marquez ingestion activates in Docker observability profile.",
            "events": events,
            "iceberg_inventory": lh.table_inventory(),
        }

    @app.get("/v1/observability/traces")
    def traces(_role: str = Depends(require_role("analyst"))) -> dict[str, Any]:
        return {"traces": TELEMETRY.recent_traces(limit=30)}

    @app.post("/v1/admin/rebuild-online-store")
    def admin_rebuild_online_store(role: str = Depends(require_role("admin"))) -> dict[str, Any]:
        c = _get_cfg()
        lh = IcebergLakehouse(c)
        store = OnlineFeatureStore(c)
        res = store.wipe_and_rebuild_from_lakehouse(lh)
        append_audit_event(
            c.audit_log_path,
            actor_role=role,
            action="rebuild_online_store",
            resource="/v1/admin/rebuild-online-store",
            status="SUCCESS",
            details={"parity_passed": res["parity_passed"], "mismatches": res["parity_mismatches"]},
        )
        return res

    @app.post("/v1/admin/pipeline/run")
    def admin_run_pipeline(
        body: PipelineRunRequest,
        role: str = Depends(require_role("admin")),
    ) -> dict[str, Any]:
        c = _get_cfg()
        scfg = SimulationConfig(
            seed=body.seed,
            num_users=body.num_users,
            num_items=body.num_items,
            interactions_per_user=body.interactions_per_user,
            experiment_id=c.default_experiment_id,
            experiment_salt=c.default_experiment_salt,
        )
        summary = run_full_pipeline(c, scfg)
        append_audit_event(
            c.audit_log_path,
            actor_role=role,
            action="pipeline_run",
            resource="/v1/admin/pipeline/run",
            status="SUCCESS",
            details={"run_id": summary["run_id"], "seed": body.seed},
        )
        return summary

    return app


app = create_app()
