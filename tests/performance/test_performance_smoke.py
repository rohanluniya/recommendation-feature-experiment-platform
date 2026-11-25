"""Performance smoke regression test for online feature store lookup latency."""

from __future__ import annotations

import time

import numpy as np
import pytest

from recsys_platform.online_store import OnlineFeatureStore


@pytest.mark.performance
def test_online_feature_lookup_p99_within_slo(module_bootstrapped_env) -> None:
    cfg, _summary = module_bootstrapped_env
    store = OnlineFeatureStore(cfg)
    user_ids = [u["user_id"] for u in store.list_entities("user", limit=100)]
    assert len(user_ids) >= 50

    latencies_ms: list[float] = []
    for idx in range(300):
        uid = user_ids[idx % len(user_ids)]
        t0 = time.perf_counter()
        feat = store.get_user_features(uid)
        latencies_ms.append((time.perf_counter() - t0) * 1000.0)
        assert feat is not None

    p50 = float(np.percentile(latencies_ms, 50))
    p95 = float(np.percentile(latencies_ms, 95))
    p99 = float(np.percentile(latencies_ms, 99))
    assert p50 < 5.0
    assert p95 < 10.0
    assert p99 < 20.0
