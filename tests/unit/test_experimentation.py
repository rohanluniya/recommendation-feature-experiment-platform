"""Unit tests for deterministic bucketing, SRM chi-squared detection, CUPED variance reduction, and guardrails."""

from __future__ import annotations

import numpy as np
import pytest

from recsys_platform.experimentation import (
    assign_user_variant,
    compute_cuped_adjustment,
    detect_srm,
    evaluate_experiment,
)
from recsys_platform.ingestion import validate_and_quarantine_interactions
from recsys_platform.sessionizer import run_event_time_sessionization
from recsys_platform.simulator import SimulationConfig, generate_simulation


@pytest.mark.unit
def test_deterministic_bucketing_and_salt_independence() -> None:
    a1 = assign_user_variant("user_00042", "exp_1", "salt_a")
    a2 = assign_user_variant("user_00042", "exp_1", "salt_a")
    assert a1 == a2

    # Different salts re-randomize assignments across 200 users
    same_count = sum(
        1
        for i in range(200)
        if assign_user_variant(f"u_{i}", "exp_1", "salt_a")["variant"]
        == assign_user_variant(f"u_{i}", "exp_1", "salt_b")["variant"]
    )
    assert 70 <= same_count <= 130


@pytest.mark.unit
def test_srm_detector_catches_skewed_split_and_passes_balanced() -> None:
    _, p_ok, srm_ok = detect_srm(504, 496, expected_treatment_ratio=0.50, alpha=0.001)
    assert srm_ok is False
    assert p_ok > 0.5

    # Broken 57/43 split over 1000 users triggers SRM immediately
    _, p_bad, srm_bad = detect_srm(430, 570, expected_treatment_ratio=0.50, alpha=0.001)
    assert srm_bad is True
    assert p_bad < 0.001


@pytest.mark.unit
def test_cuped_reduces_variance_when_covariate_correlated() -> None:
    rng = np.random.default_rng(7)
    x = rng.normal(30.0, 10.0, size=500)
    y = 0.85 * x + rng.normal(0.0, 4.0, size=500)

    y_cuped, theta, corr_xy, var_red_pct = compute_cuped_adjustment(y, x)
    assert theta > 0.7
    assert corr_xy > 0.8
    assert var_red_pct > 60.0
    assert float(np.var(y_cuped, ddof=1)) < float(np.var(y, ddof=1))


@pytest.mark.unit
def test_full_experiment_evaluation_with_cuped_and_guardrails() -> None:
    scfg = SimulationConfig(seed=42, num_users=220, num_items=50, interactions_per_user=16)
    bundle = generate_simulation(scfg)
    ing = validate_and_quarantine_interactions(bundle.interaction_events)
    sess = run_event_time_sessionization(ing.valid_events)

    ev = evaluate_experiment(
        exposures=bundle.experiment_exposures,
        sessions=sess.final_sessions,
        delayed_labels=bundle.delayed_labels,
        interactions=ing.valid_events,
        experiment_id=scfg.experiment_id,
    )
    assert ev.cross_bucket_contaminated_users == 0
    assert ev.srm_detected is False
    assert ev.cuped_variance_reduction_pct > 30.0
    assert ev.cuped_ci_width < ev.unadjusted_ci_width
    assert all(g["passed"] for g in ev.guardrails.values())
