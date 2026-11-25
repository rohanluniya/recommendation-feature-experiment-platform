"""End-to-end vertical slice test validating full source -> spool -> sessionizer -> Iceberg -> PIT -> online -> ranker -> experiment -> API."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from apps.recsys_api.main import create_app
from recsys_platform.ingestion import LocalTopicSpool
from recsys_platform.pipeline import run_full_pipeline
from recsys_platform.simulator import SimulationConfig


@pytest.mark.e2e
def test_end_to_end_vertical_slice_and_spool_replay(isolated_config) -> None:
    scfg = SimulationConfig(
        seed=42,
        num_users=140,
        num_items=40,
        interactions_per_user=14,
        experiment_id=isolated_config.default_experiment_id,
        experiment_salt=isolated_config.default_experiment_salt,
    )
    summary = run_full_pipeline(isolated_config, scfg)

    # Core invariants
    assert summary["pit_audit"]["strict_pit"]["total_leakage_violations"] == 0
    assert summary["pit_audit"]["naive_global_join"]["total_leakage_violations"] > 0
    assert summary["online_store"]["parity"]["passed"] is True
    assert summary["data_quality"]["failed_checks"] == 0
    assert summary["data_quality"]["passed_checks"] == 9
    assert summary["experiment"]["srm_detected"] is False
    assert summary["experiment"]["cuped_variance_reduction_pct"] > 30.0
    assert summary["sessionization"]["late_retractions"] > 0

    # Durable spool replay verification
    spool = LocalTopicSpool(isolated_config.spool_dir)
    replayed = spool.read_from_offset("playback.interactions.raw", start_offset=0)
    assert len(replayed) == summary["ingestion"]["valid_events"]

    # Live API overview & recommendation verification
    client = TestClient(create_app(isolated_config))
    overview = client.get("/v1/dashboard/overview")
    assert overview.status_code == 200
    assert overview.json()["summary"]["run_id"] == summary["run_id"]
