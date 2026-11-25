"""Unit tests for deterministic streaming & catalog simulation."""

from __future__ import annotations

import pytest

from recsys_platform.simulator import SimulationConfig, generate_simulation


@pytest.mark.unit
def test_simulation_is_strictly_deterministic_for_same_seed() -> None:
    cfg = SimulationConfig(seed=101, num_users=60, num_items=24, interactions_per_user=10)
    run_a = generate_simulation(cfg)
    run_b = generate_simulation(cfg)

    assert run_a.summary == run_b.summary
    assert [e["event_id"] for e in run_a.interaction_events] == [
        e["event_id"] for e in run_b.interaction_events
    ]
    assert [c["change_id"] for c in run_a.catalog_changes] == [
        c["change_id"] for c in run_b.catalog_changes
    ]
    assert [x["exposure_id"] for x in run_a.experiment_exposures] == [
        x["exposure_id"] for x in run_b.experiment_exposures
    ]


@pytest.mark.unit
def test_simulation_generates_all_required_adversarial_scenarios() -> None:
    cfg = SimulationConfig(seed=42, num_users=160, num_items=48, interactions_per_user=15)
    bundle = generate_simulation(cfg)

    assert bundle.summary["injected_duplicate_heartbeats"] > 0
    assert bundle.summary["injected_late_within_allowed"] > 0
    assert bundle.summary["injected_late_beyond_allowed"] > 0
    assert bundle.summary["injected_corrupt_events"] >= 5
    assert bundle.summary["injected_device_switches"] > 0
    assert bundle.summary["v2_schema_events"] > 0
    assert bundle.summary["delayed_labels"] > 0
    assert bundle.summary["num_cold_start_users"] == cfg.num_cold_start_users
    assert bundle.summary["num_cold_start_items"] == cfg.num_cold_start_items
