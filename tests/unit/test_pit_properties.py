"""Hypothesis property-based tests for PIT zero-leakage, bucketing determinism, and CUPED variance monotonicity."""

from __future__ import annotations

import numpy as np
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from recsys_platform.experimentation import assign_user_variant, compute_cuped_adjustment
from recsys_platform.features import (
    audit_pit_leakage,
    build_pit_training_dataset,
    build_temporal_feature_histories,
)


@pytest.mark.unit
@settings(max_examples=40, deadline=None)
@given(
    user_id=st.text(min_size=1, max_size=24),
    exp_id=st.text(min_size=1, max_size=24),
    salt=st.text(min_size=1, max_size=24),
)
def test_property_bucketing_is_deterministic_and_bounded(user_id: str, exp_id: str, salt: str) -> None:
    r1 = assign_user_variant(user_id, exp_id, salt)
    r2 = assign_user_variant(user_id, exp_id, salt)
    assert r1 == r2
    assert r1["variant"] in {"control", "treatment"}
    assert 0 <= r1["bucket_hash"] < 10_000


@pytest.mark.unit
@settings(max_examples=30, deadline=None)
@given(
    timestamps=st.lists(
        st.integers(min_value=1_764_000_000_000, max_value=1_764_005_000_000),
        min_size=6,
        max_size=25,
    )
)
def test_property_strict_pit_join_never_leaks_future_features(timestamps: list[int]) -> None:
    """For ANY arbitrary event-time ordering, PIT ASOF join MUST maintain feature_ts <= request_ts."""
    base = 1_763_900_000_000
    catalog = [
        {
            "change_id": "cdc_1",
            "op": "c",
            "item_id": "item_0001",
            "title": "Test Title",
            "genre": "sci_fi",
            "duration_seconds": 3600,
            "release_year": 2025,
            "maturity_rating": "TV-14",
            "popularity_prior": 0.5,
            "is_active": True,
            "event_ts_ms": base,
            "ingest_ts_ms": base + 10,
            "source_system": "postgres-catalog-cdc",
        }
    ]
    interactions = []
    event_types = ["IMPRESSION", "PLAY_START", "HEARTBEAT", "COMPLETE"]
    for idx, ts in enumerate(timestamps):
        interactions.append(
            {
                "event_id": f"ev_{idx}",
                "event_type": event_types[idx % len(event_types)],
                "user_id": "user_prop",
                "item_id": "item_0001",
                "device_id": "dev_1",
                "device_type": "smart_tv",
                "event_ts_ms": ts,
                "ingest_ts_ms": ts + 50,
                "watch_seconds": 40.0 if event_types[idx % len(event_types)] == "HEARTBEAT" else 0.0,
                "position_seconds": 40.0,
                "rebuffer_count": 0,
                "experiment_variant": "control",
            }
        )

    u_hist, i_hist = build_temporal_feature_histories(interactions, catalog, created_ts_ms=base + 999_999)
    pit_rows = build_pit_training_dataset(interactions, u_hist, i_hist)
    report = audit_pit_leakage(pit_rows)

    assert report.passed is True
    assert report.total_leakage_violations == 0
    for row in pit_rows:
        assert row["user_feature_ts_ms"] < row["request_ts_ms"] or row["user_feature_ts_ms"] == 0
        assert row["item_feature_ts_ms"] <= row["request_ts_ms"]


@pytest.mark.unit
@settings(max_examples=40, deadline=None)
@given(
    pairs=st.lists(
        st.tuples(
            st.floats(min_value=-1000.0, max_value=1000.0, allow_nan=False, allow_infinity=False),
            st.floats(min_value=-1000.0, max_value=1000.0, allow_nan=False, allow_infinity=False),
        ),
        min_size=5,
        max_size=80,
    )
)
def test_property_cuped_never_increases_sample_variance(pairs: list[tuple[float, float]]) -> None:
    """Mathematically Var(Y - theta*(X - mean(X))) = (1 - rho^2) * Var(Y) <= Var(Y)."""
    x = np.array([p[0] for p in pairs], dtype=np.float64)
    y = np.array([p[1] for p in pairs], dtype=np.float64)
    y_cuped, _theta, _corr, var_red_pct = compute_cuped_adjustment(y, x)

    var_y = float(np.var(y, ddof=1))
    var_c = float(np.var(y_cuped, ddof=1))
    assert var_c <= var_y + 1e-6 * max(1.0, var_y)
    assert var_red_pct >= 0.0
