"""Contract tests verifying FastAvro serialization and v1 <-> v2 backward-compatible schema evolution."""

from __future__ import annotations

import pytest

from recsys_platform.schemas import SCHEMA_REGISTRY


@pytest.mark.contract
def test_playback_v1_to_v2_backward_compatibility() -> None:
    v1_record = {
        "event_id": "ev_contract_001",
        "event_type": "HEARTBEAT",
        "user_id": "user_00001",
        "item_id": "item_0002",
        "device_id": "dev_tv_1",
        "device_type": "smart_tv",
        "event_ts_ms": 1_764_000_010_000,
        "ingest_ts_ms": 1_764_000_010_250,
        "watch_seconds": 30.0,
        "position_seconds": 120.0,
        "rebuffer_count": 0,
        "search_query": None,
        "experiment_id": "exp_recsys_ranker_v2",
        "experiment_variant": "treatment",
        "source_system": "playback-gateway",
        "schema_version": 1,
    }
    ok_v1, err = SCHEMA_REGISTRY.validate_record("playback_interaction_v1", v1_record)
    assert ok_v1 is True and err is None

    compat_ok, decoded_v2 = SCHEMA_REGISTRY.check_backward_compatibility(
        "playback_interaction_v1", "playback_interaction_v2", v1_record
    )
    assert compat_ok is True
    assert decoded_v2["event_id"] == "ev_contract_001"
    assert decoded_v2["device_os_version"] is None
    assert decoded_v2["playback_bitrate_kbps"] is None


@pytest.mark.contract
def test_invalid_enum_and_missing_required_fields_rejected() -> None:
    bad_enum = {
        "event_id": "ev_bad_1",
        "event_type": "NON_EXISTENT_EVENT",
        "user_id": "u1",
        "item_id": "i1",
        "device_id": "d1",
        "device_type": "web",
        "event_ts_ms": 1_764_000_000_000,
        "ingest_ts_ms": 1_764_000_000_100,
    }
    ok, err = SCHEMA_REGISTRY.validate_record("playback_interaction_v1", bad_enum)
    assert ok is False and err is not None

    catalog_rec = {
        "change_id": "cdc_001",
        "op": "u",
        "item_id": "item_0001",
        "title": "Sci Fi Chronicles #1",
        "genre": "sci_fi",
        "duration_seconds": 3600,
        "release_year": 2025,
        "maturity_rating": "TV-14",
        "popularity_prior": 0.72,
        "is_active": True,
        "event_ts_ms": 1_764_000_000_000,
        "ingest_ts_ms": 1_764_000_000_120,
        "source_system": "postgres-catalog-cdc",
    }
    ok_cdc, err_cdc = SCHEMA_REGISTRY.validate_record("catalog_change_v1", catalog_rec)
    assert ok_cdc is True and err_cdc is None
