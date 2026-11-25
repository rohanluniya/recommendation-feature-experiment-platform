"""Integration tests for FastAPI RBAC/API-key auth, audit logging, Prometheus metrics, and serving endpoints."""

from __future__ import annotations

import json

import pytest
from fastapi.testclient import TestClient

from apps.recsys_api.main import create_app


@pytest.mark.integration
def test_api_rbac_rejects_unauthorized_and_forbidden_requests(module_bootstrapped_env) -> None:
    cfg, _summary = module_bootstrapped_env
    client = TestClient(create_app(cfg))

    # 1. Unauthorized (missing key) -> 401
    r_unauth = client.get("/v1/features/online/user_00000")
    assert r_unauth.status_code == 401
    assert "X-Correlation-Id" in r_unauth.headers
    assert "X-Trace-Id" in r_unauth.headers

    # 2. Invalid key -> 401
    r_bad = client.post(
        "/v1/recommend",
        json={"user_id": "user_00000", "top_k": 5},
        headers={"X-API-Key": "wrong-key"},
    )
    assert r_bad.status_code == 401

    # 3. Serving role forbidden on analyst & admin routes -> 403
    r_forbid_analyst = client.get("/v1/pit/audit", headers={"X-API-Key": cfg.serving_api_key})
    assert r_forbid_analyst.status_code == 403
    r_forbid_admin = client.post(
        "/v1/admin/rebuild-online-store", headers={"X-API-Key": cfg.analyst_api_key}
    )
    assert r_forbid_admin.status_code == 403

    # Verify security audit log recorded denials
    audit_lines = cfg.audit_log_path.read_text(encoding="utf-8").strip().splitlines()
    actions = {json.loads(line)["action"] for line in audit_lines}
    assert "auth_denied_401" in actions
    assert "auth_denied_403" in actions


@pytest.mark.integration
def test_api_authorized_serving_analytics_admin_and_metrics(module_bootstrapped_env) -> None:
    cfg, _summary = module_bootstrapped_env
    client = TestClient(create_app(cfg))

    # Health & Prometheus metrics
    assert client.get("/health").status_code == 200
    metrics_resp = client.get("/metrics")
    assert metrics_resp.status_code == 200
    assert "recsys_pit_leakage_violations" in metrics_resp.text
    assert "recsys_online_feature_latency_ms" in metrics_resp.text

    # Serving routes
    feat_resp = client.get("/v1/features/online/user_00000", headers={"X-API-Key": cfg.serving_api_key})
    assert feat_resp.status_code == 200
    assert feat_resp.json()["features"]["user_id"] == "user_00000"

    rec_resp = client.post(
        "/v1/recommend",
        json={"user_id": "user_00000", "top_k": 5},
        headers={"X-API-Key": cfg.serving_api_key},
    )
    assert rec_resp.status_code == 200
    body = rec_resp.json()
    assert len(body["recommendations"]) == 5
    assert body["experiment_assignment"]["variant"] in {"control", "treatment"}

    # Analyst routes
    for path in (
        "/v1/pit/audit",
        "/v1/parity",
        "/v1/dq",
        f"/v1/experiments/{cfg.default_experiment_id}/evaluate",
        "/v1/lineage",
        "/v1/observability/traces",
    ):
        resp = client.get(path, headers={"X-API-Key": cfg.analyst_api_key})
        assert resp.status_code == 200, f"Expected 200 on {path}, got {resp.status_code}"

    # Admin rebuild route
    rb = client.post("/v1/admin/rebuild-online-store", headers={"X-API-Key": cfg.admin_api_key})
    assert rb.status_code == 200
    assert rb.json()["parity_passed"] is True
    assert rb.json()["parity_mismatches"] == 0
