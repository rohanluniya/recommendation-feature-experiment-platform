"""Unit tests for Flink-semantics event-time sessionization, retractions, dedup, and state TTL."""

from __future__ import annotations

import pytest

from recsys_platform.sessionizer import run_event_time_sessionization


def _ev(
    event_id: str,
    user_id: str,
    event_ts_ms: int,
    *,
    event_type: str = "HEARTBEAT",
    watch_seconds: float = 30.0,
    device_type: str = "smart_tv",
    item_id: str = "item_0001",
) -> dict:
    return {
        "event_id": event_id,
        "event_type": event_type,
        "user_id": user_id,
        "item_id": item_id,
        "device_id": f"dev_{device_type}",
        "device_type": device_type,
        "event_ts_ms": event_ts_ms,
        "ingest_ts_ms": event_ts_ms + 100,
        "watch_seconds": watch_seconds,
        "position_seconds": watch_seconds,
        "rebuffer_count": 0,
        "experiment_variant": "control",
    }


@pytest.mark.unit
def test_heartbeat_storm_dedup_and_device_switch_continuity() -> None:
    t0 = 1_764_000_000_000
    events = [
        _ev("e1", "u1", t0, event_type="PLAY_START", watch_seconds=0.0, device_type="mobile_ios"),
        _ev("e2", "u1", t0 + 30_000, watch_seconds=60.0, device_type="mobile_ios"),
        # Duplicate heartbeat storm (identical event_id)
        _ev("e2", "u1", t0 + 30_000, watch_seconds=60.0, device_type="mobile_ios"),
        _ev("e2", "u1", t0 + 30_000, watch_seconds=60.0, device_type="mobile_ios"),
        # Cross-device switch within 30m inactivity gap
        _ev("e3", "u1", t0 + 90_000, event_type="HEARTBEAT", watch_seconds=90.0, device_type="smart_tv"),
    ]
    out = run_event_time_sessionization(events)
    assert len(out.final_sessions) == 1
    sess = out.final_sessions[0]
    assert out.duplicates_suppressed == 2
    assert sess["total_watch_seconds"] == pytest.approx(150.0)
    assert sess["distinct_devices"] == 2
    assert sess["device_switch_count"] >= 1


@pytest.mark.unit
def test_late_event_within_allowed_lateness_emits_retraction_and_too_late_drops() -> None:
    t0 = 1_764_000_000_000
    gap = 1_800_000  # 30 min
    allowed = 300_000  # 5 min

    events = [
        # Session 1 for u1 at t0
        _ev("s1_a", "u1", t0, watch_seconds=60.0),
        # Another user advances event-time watermark past t0 + gap + out_of_order_bound (closes u1 session 1)
        _ev("wm_close", "u2", t0 + gap + 60_000, watch_seconds=10.0),
        # Late event for u1 session 1 arriving BEFORE watermark passes (end + gap + allowed_lateness) -> RETRACTION!
        _ev("s1_late_ok", "u1", t0 + 20_000, watch_seconds=45.0),
        # Advance watermark far past (end + gap + allowed_lateness)
        _ev("wm_far", "u2", t0 + gap + allowed + 200_000, watch_seconds=10.0),
        # Too-late event for u1 session 1 -> DROPPED to side output!
        _ev("s1_too_late", "u1", t0 + 25_000, watch_seconds=999.0),
    ]
    out = run_event_time_sessionization(events, inactivity_gap_ms=gap, allowed_lateness_ms=allowed)
    assert out.late_retractions == 1
    assert out.late_dropped_events == 1

    u1_sessions = [s for s in out.final_sessions if s["user_id"] == "u1"]
    assert len(u1_sessions) == 1
    assert u1_sessions[0]["had_late_retraction"] is True
    assert u1_sessions[0]["revision"] == 2
    assert u1_sessions[0]["total_watch_seconds"] == pytest.approx(105.0)
    ops = [c["changelog_op"] for c in out.changelog_stream if c["user_id"] == "u1"]
    assert "-U" in ops and "+U" in ops


@pytest.mark.unit
def test_bounded_user_state_ttl_eviction() -> None:
    t0 = 1_764_000_000_000
    events = [
        _ev("u_old_1", "u_old", t0, watch_seconds=30.0),
        # Advance watermark by > 2 hours while TTL is 1 hour
        _ev("u_new_1", "u_new", t0 + 7_200_000, watch_seconds=30.0),
    ]
    out = run_event_time_sessionization(events, state_ttl_ms=3_600_000)
    assert out.ttl_evicted_users >= 1
    assert len(out.final_sessions) == 2
