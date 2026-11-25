"""Automated chaos engineering drills (8 controlled fault injections) with persisted JSON evidence."""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from recsys_platform.config import REPO_ROOT, PlatformConfig, get_config
from recsys_platform.experimentation import evaluate_experiment
from recsys_platform.features import (
    apply_delayed_label_backfill,
    audit_pit_leakage,
    build_pit_training_dataset,
    build_temporal_feature_histories,
)
from recsys_platform.ingestion import LocalTopicSpool, validate_and_quarantine_interactions
from recsys_platform.lakehouse import IcebergLakehouse
from recsys_platform.online_store import OnlineFeatureStore
from recsys_platform.pipeline import run_full_pipeline
from recsys_platform.sessionizer import run_event_time_sessionization
from recsys_platform.simulator import SimulationConfig, generate_simulation

CHAOS_RESULTS_PATH = REPO_ROOT / "results" / "chaos" / "chaos_results.json"
_COLLECTED_DRILLS: list[dict[str, Any]] = []


def _record_drill(drill_id: str, title: str, passed: bool, elapsed_ms: float, metrics: dict[str, Any]) -> None:
    _COLLECTED_DRILLS.append(
        {
            "drill_id": drill_id,
            "title": title,
            "status": "PASS" if passed else "FAIL",
            "elapsed_ms": round(elapsed_ms, 2),
            "metrics": metrics,
        }
    )
    CHAOS_RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "total_drills": len(_COLLECTED_DRILLS),
        "passed_drills": sum(1 for d in _COLLECTED_DRILLS if d["status"] == "PASS"),
        "drills": _COLLECTED_DRILLS,
    }
    CHAOS_RESULTS_PATH.write_text(json.dumps(payload, indent=2), encoding="utf-8")


@pytest.mark.chaos
def test_chaos_01_heartbeat_storm_duplicate_flood() -> None:
    """Inject 5x duplicate playback heartbeat flood and verify exact idempotent watch-time conservation."""
    t0 = time.perf_counter()
    base = 1_764_000_000_000
    clean = [
        {
            "event_id": f"hb_{i}",
            "event_type": "HEARTBEAT",
            "user_id": "user_storm",
            "item_id": "item_0001",
            "device_id": "dev_tv",
            "device_type": "smart_tv",
            "event_ts_ms": base + i * 30_000,
            "ingest_ts_ms": base + i * 30_000 + 100,
            "watch_seconds": 30.0,
            "position_seconds": float((i + 1) * 30),
            "rebuffer_count": 0,
            "search_query": None,
            "experiment_id": "exp_recsys_ranker_v2",
            "experiment_variant": "treatment",
            "source_system": "playback-gateway",
            "schema_version": 1,
        }
        for i in range(20)
    ]
    storm = clean * 5
    ing = validate_and_quarantine_interactions(storm)
    sess = run_event_time_sessionization(ing.valid_events)
    watch_sec = sess.final_sessions[0]["total_watch_seconds"]

    passed = (
        len(ing.valid_events) == 20
        and ing.duplicates_dropped == 80
        and abs(watch_sec - 600.0) < 1e-6
    )
    _record_drill(
        "CHAOS-01",
        "Heartbeat Storm Duplicate Flood (5x Replay)",
        passed,
        (time.perf_counter() - t0) * 1000.0,
        {
            "raw_events_injected": len(storm),
            "duplicates_dropped": ing.duplicates_dropped,
            "valid_events_retained": len(ing.valid_events),
            "expected_watch_seconds": 600.0,
            "observed_watch_seconds": watch_sec,
        },
    )
    assert passed


@pytest.mark.chaos
def test_chaos_02_late_event_retraction_and_cutoff() -> None:
    """Inject late events both inside allowed lateness (retraction) and past cleanup horizon (dropped)."""
    t0 = time.perf_counter()
    base = 1_764_000_000_000
    gap = 1_800_000
    allowed = 300_000
    events = [
        {"event_id": "e1", "event_type": "HEARTBEAT", "user_id": "u_late", "item_id": "i1", "device_type": "smart_tv", "event_ts_ms": base, "watch_seconds": 60.0},
        {"event_id": "wm1", "event_type": "HEARTBEAT", "user_id": "u_clock", "item_id": "i1", "device_type": "web", "event_ts_ms": base + gap + 60_000, "watch_seconds": 10.0},
        {"event_id": "e_late_ok", "event_type": "HEARTBEAT", "user_id": "u_late", "item_id": "i1", "device_type": "smart_tv", "event_ts_ms": base + 30_000, "watch_seconds": 40.0},
        {"event_id": "wm2", "event_type": "HEARTBEAT", "user_id": "u_clock", "item_id": "i1", "device_type": "web", "event_ts_ms": base + gap + allowed + 240_000, "watch_seconds": 10.0},
        {"event_id": "e_too_late", "event_type": "HEARTBEAT", "user_id": "u_late", "item_id": "i1", "device_type": "smart_tv", "event_ts_ms": base + 35_000, "watch_seconds": 500.0},
    ]
    out = run_event_time_sessionization(events, inactivity_gap_ms=gap, allowed_lateness_ms=allowed)
    u_sess = [s for s in out.final_sessions if s["user_id"] == "u_late"][0]
    passed = (
        out.late_retractions == 1
        and out.late_dropped_events == 1
        and u_sess["revision"] == 2
        and abs(u_sess["total_watch_seconds"] - 100.0) < 1e-6
    )
    _record_drill(
        "CHAOS-02",
        "Out-of-Order Late Event Retraction & Side-Output Cutoff",
        passed,
        (time.perf_counter() - t0) * 1000.0,
        {
            "late_retractions": out.late_retractions,
            "late_dropped_events": out.late_dropped_events,
            "final_session_revision": u_sess["revision"],
            "reconciled_watch_seconds": u_sess["total_watch_seconds"],
        },
    )
    assert passed


@pytest.mark.chaos
def test_chaos_03_broker_outage_spool_drain_recovery(tmp_path: Path) -> None:
    """Simulate broker outage mid-stream, buffer events in local WAL, recover, and verify zero data loss."""
    t0 = time.perf_counter()
    spool = LocalTopicSpool(tmp_path / "spool")
    batch1 = [{"event_id": f"b1_{i}", "user_id": f"u_{i}"} for i in range(15)]
    batch2_outage = [{"event_id": f"b2_{i}", "user_id": f"u_{i}"} for i in range(25)]

    written1 = spool.produce("playback.interactions.raw", batch1)
    spool.broker_available = False
    written_during_outage = spool.produce("playback.interactions.raw", batch2_outage)
    drained = spool.drain_outage_buffer("playback.interactions.raw")
    replayed = spool.read_from_offset("playback.interactions.raw", start_offset=0)

    passed = (
        written1 == 15
        and written_during_outage == 0
        and drained == 25
        and len(replayed) == 40
        and [e["offset"] for e in replayed] == list(range(40))
    )
    _record_drill(
        "CHAOS-03",
        "Broker Outage WAL Spool & Idempotent Drain Recovery",
        passed,
        (time.perf_counter() - t0) * 1000.0,
        {
            "pre_outage_written": written1,
            "during_outage_buffered": len(batch2_outage),
            "drained_after_recovery": drained,
            "total_replayed_contiguous": len(replayed),
        },
    )
    assert passed


@pytest.mark.chaos
def test_chaos_04_online_store_wipe_and_iceberg_rebuild(tmp_path: Path) -> None:
    """Wipe online feature store completely, rebuild deterministically from Iceberg Gold, verify 0 parity skew."""
    t0 = time.perf_counter()
    cfg: PlatformConfig = get_config(data_dir=tmp_path / "data")
    cfg.mlflow_tracking_dir = tmp_path / "mlruns"
    run_full_pipeline(
        cfg,
        SimulationConfig(seed=42, num_users=80, num_items=24, interactions_per_user=10),
        train_models=False,
    )
    lh = IcebergLakehouse(cfg)
    store = OnlineFeatureStore(cfg)

    rebuild = store.wipe_and_rebuild_from_lakehouse(lh)
    passed = (
        rebuild["active_users_after_wipe"] == 0
        and rebuild["active_items_after_wipe"] == 0
        and rebuild["parity_passed"] is True
        and rebuild["parity_mismatches"] == 0
        and rebuild["max_abs_numeric_diff"] == 0.0
    )
    _record_drill(
        "CHAOS-04",
        "Online Feature Store Wipe & Rebuild from Iceberg Gold",
        passed,
        (time.perf_counter() - t0) * 1000.0,
        rebuild,
    )
    assert passed


@pytest.mark.chaos
def test_chaos_05_intentional_srm_fault_injection() -> None:
    """Inject broken 57/43 traffic routing and verify chi-squared SRM detector halts rollout."""
    t0 = time.perf_counter()
    scfg = SimulationConfig(
        seed=99,
        num_users=500,
        num_items=30,
        interactions_per_user=8,
        treatment_ratio=0.57,  # Broken router skew!
    )
    bundle = generate_simulation(scfg)
    ing = validate_and_quarantine_interactions(bundle.interaction_events)
    sess = run_event_time_sessionization(ing.valid_events)

    ev = evaluate_experiment(
        exposures=bundle.experiment_exposures,
        sessions=sess.final_sessions,
        delayed_labels=bundle.delayed_labels,
        interactions=ing.valid_events,
        experiment_id=scfg.experiment_id,
        expected_treatment_ratio=0.50,
    )
    passed = (
        ev.srm_detected is True
        and ev.srm_pvalue < 0.001
        and ev.decision == "INVALID_STOP_SRM_OR_CONTAMINATION"
    )
    _record_drill(
        "CHAOS-05",
        "Intentional SRM Traffic Skew Injection (50/50 -> 57/43)",
        passed,
        (time.perf_counter() - t0) * 1000.0,
        {
            "control_users": ev.control_users,
            "treatment_users": ev.treatment_users,
            "observed_treatment_ratio": ev.observed_treatment_ratio,
            "srm_chi2_stat": ev.srm_chi2_stat,
            "srm_pvalue": ev.srm_pvalue,
            "decision": ev.decision,
        },
    )
    assert passed


@pytest.mark.chaos
def test_chaos_06_delayed_label_backfill_without_leakage(tmp_path: Path) -> None:
    """Backfill delayed completion labels hours later while proving 0 feature timestamp mutations and 0 PIT leaks."""
    t0 = time.perf_counter()
    cfg = get_config(data_dir=tmp_path / "data")
    lh = IcebergLakehouse(cfg)
    bundle = generate_simulation(SimulationConfig(seed=42, num_users=100, num_items=30, interactions_per_user=12))
    ing = validate_and_quarantine_interactions(bundle.interaction_events)
    u_pit, i_pit = build_temporal_feature_histories(ing.valid_events, bundle.catalog_changes, created_ts_ms=int(time.time() * 1000))

    initial_rows = build_pit_training_dataset(ing.valid_events, u_pit, i_pit, include_delayed_labels=False)
    lh.write_table("gold.training_dataset", initial_rows, mode="overwrite")
    snap_before = lh.snapshot_history("gold.training_dataset")[-1]["snapshot_id"]

    backfilled_rows, bf_summary = apply_delayed_label_backfill(initial_rows, bundle.delayed_labels)
    lh.write_table("gold.training_dataset", backfilled_rows, mode="overwrite")
    snap_after = lh.snapshot_history("gold.training_dataset")[-1]["snapshot_id"]

    pit_after = audit_pit_leakage(backfilled_rows)
    pre_pos = sum(r["label_completed"] for r in lh.read_records("gold.training_dataset", snapshot_id=snap_before))
    post_pos = sum(r["label_completed"] for r in lh.read_records("gold.training_dataset", snapshot_id=snap_after))

    passed = (
        bf_summary["delayed_labels_applied"] > 0
        and bf_summary["feature_timestamps_mutated"] == 0
        and pit_after.total_leakage_violations == 0
        and post_pos > pre_pos
        and snap_before != snap_after
    )
    _record_drill(
        "CHAOS-06",
        "Delayed Label Backfill Without Feature Timestamp Mutation",
        passed,
        (time.perf_counter() - t0) * 1000.0,
        {
            **bf_summary,
            "iceberg_snapshot_before": snap_before,
            "iceberg_snapshot_after": snap_after,
            "time_travel_pos_before": pre_pos,
            "time_travel_pos_after": post_pos,
        },
    )
    assert passed


@pytest.mark.chaos
def test_chaos_07_schema_drift_and_corrupt_payload_quarantine(tmp_path: Path) -> None:
    """Inject corrupt payloads, unknown opcodes, and negative watch times -> verify 100% DLQ quarantine & conservation."""
    t0 = time.perf_counter()
    cfg = get_config(data_dir=tmp_path / "data")
    lh = IcebergLakehouse(cfg)
    bundle = generate_simulation(
        SimulationConfig(seed=77, num_users=60, num_items=20, interactions_per_user=8, corrupt_event_rate=0.06)
    )
    ing = validate_and_quarantine_interactions(bundle.interaction_events)
    lh.write_table("bronze.interaction_events", ing.valid_events, mode="overwrite")
    lh.write_table("bronze.quarantine_dlq", ing.dlq_events, mode="overwrite")

    bronze_rows = lh.read_records("bronze.interaction_events")
    dlq_rows = lh.read_records("bronze.quarantine_dlq")
    neg_in_bronze = sum(1 for r in bronze_rows if float(r["watch_seconds"]) < 0.0)
    conserved = (len(bronze_rows) + len(dlq_rows) + ing.duplicates_dropped) == ing.total_received

    passed = len(dlq_rows) > 0 and neg_in_bronze == 0 and conserved
    _record_drill(
        "CHAOS-07",
        "Schema Drift & Corrupt Payload Quarantine Conservation",
        passed,
        (time.perf_counter() - t0) * 1000.0,
        {
            "total_received": ing.total_received,
            "bronze_valid": len(bronze_rows),
            "quarantined_dlq": len(dlq_rows),
            "duplicates_dropped": ing.duplicates_dropped,
            "negative_watch_in_bronze": neg_in_bronze,
            "conservation_exact": conserved,
        },
    )
    assert passed


@pytest.mark.chaos
def test_chaos_08_cross_bucket_contamination_and_guardrail_breach() -> None:
    """Inject sticky-bucketing contamination and +15ms serving latency regression -> verify guardrails block ship."""
    t0 = time.perf_counter()
    scfg = SimulationConfig(seed=42, num_users=160, num_items=30, interactions_per_user=10)
    bundle = generate_simulation(scfg)
    ing = validate_and_quarantine_interactions(bundle.interaction_events)
    sess = run_event_time_sessionization(ing.valid_events)

    # Corrupt exposures: flip 4 control users into treatment on second exposure AND add +15ms latency regression
    corrupted_exposures = [dict(e) for e in bundle.experiment_exposures]
    flipped_users = set()
    for e in corrupted_exposures:
        if e["variant"] == "treatment":
            e["serving_latency_ms"] = float(e["serving_latency_ms"]) + 15.0
        elif len(flipped_users) < 4:
            flipped_users.add(e["user_id"])
    for uid in flipped_users:
        corrupted_exposures.append(
            {
                "exposure_id": f"corrupt_flip_{uid}",
                "experiment_id": scfg.experiment_id,
                "variant": "treatment",
                "user_id": uid,
                "request_id": f"req_flip_{uid}",
                "exposure_ts_ms": scfg.base_ts_ms + 90_000,
                "pre_experiment_watch_minutes": 15.0,
                "assigned_bucket_hash": 1,
                "ranker_model_version": "ranker_gbdt_v2",
                "serving_latency_ms": 22.0,
            }
        )

    ev = evaluate_experiment(
        exposures=corrupted_exposures,
        sessions=sess.final_sessions,
        delayed_labels=bundle.delayed_labels,
        interactions=ing.valid_events,
        experiment_id=scfg.experiment_id,
    )
    passed = (
        ev.cross_bucket_contaminated_users == 4
        and ev.guardrails["serving_latency_p95_ms"]["passed"] is False
        and ev.decision == "INVALID_STOP_SRM_OR_CONTAMINATION"
    )
    _record_drill(
        "CHAOS-08",
        "Cross-Bucket Contamination & Latency Guardrail Regression",
        passed,
        (time.perf_counter() - t0) * 1000.0,
        {
            "contaminated_users_detected": ev.cross_bucket_contaminated_users,
            "latency_delta_ms": ev.guardrails["serving_latency_p95_ms"]["delta_ms"],
            "latency_guardrail_passed": ev.guardrails["serving_latency_p95_ms"]["passed"],
            "decision": ev.decision,
        },
    )
    assert passed
