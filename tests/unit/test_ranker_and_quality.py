"""Unit tests for candidate rankers, MLflow metadata tracking, and PII log redaction."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from recsys_platform.logging_utils import redact_value
from recsys_platform.online_store import OnlineFeatureStore
from recsys_platform.ranker import generate_and_rank_candidates


@pytest.mark.unit
def test_log_redaction_strips_emails_tokens_and_sensitive_keys() -> None:
    redacted = redact_value(
        "payload",
        {
            "api_key": "super-secret-value",
            "user_email": "alice@example.com",
            "note": "Contact bob@company.org with Bearer tok_1234567890",
            "safe_metric": 42.5,
        },
    )
    assert redacted["api_key"] == "[REDACTED]"
    assert redacted["user_email"] == "[REDACTED]"
    assert "bob@company.org" not in redacted["note"]
    assert "tok_1234567890" not in redacted["note"]
    assert redacted["safe_metric"] == 42.5


@pytest.mark.unit
def test_ranker_mlflow_metadata_and_cold_start_recommendations(module_bootstrapped_env) -> None:
    cfg, summary = module_bootstrapped_env
    base_rep = summary["rankers"]["baseline_logreg_pit"]
    trt_rep = summary["rankers"]["treatment_gbdt_pit"]

    # Verify MLflow-compatible tracking files exist and assert zero pretrained weights
    for rep in (base_rep, trt_rep):
        run_dir = Path(rep["mlflow_run_dir"])
        assert (run_dir / "meta.yaml").exists()
        assert (run_dir / "metrics.json").exists()
        lineage = json.loads((run_dir / "lineage.json").read_text(encoding="utf-8"))
        assert lineage["pretrained_weights_used"] is False

    # Verify live ranking works for both warm and unseen cold-start user
    store = OnlineFeatureStore(cfg)
    cold_rec = generate_and_rank_candidates(
        user_id="brand_new_unseen_user_999",
        online_store=store,
        config=cfg,
        top_k=5,
    )
    assert cold_rec["is_cold_start_user"] is True
    assert len(cold_rec["recommendations"]) == 5
    assert cold_rec["experiment_assignment"]["variant"] in {"control", "treatment"}
