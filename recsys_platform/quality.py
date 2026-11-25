"""Data quality gate suite, reconciliation checks, and OpenLineage-compatible lineage emission."""

from __future__ import annotations

import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from recsys_platform.experimentation import ExperimentEvaluation
from recsys_platform.features import PitAuditReport
from recsys_platform.lakehouse import IcebergLakehouse
from recsys_platform.online_store import ParityReport
from recsys_platform.simulator import deterministic_id


def run_data_quality_checks(
    *,
    run_id: str,
    lakehouse: IcebergLakehouse,
    total_raw_received: int,
    duplicates_dropped: int,
    pit_audit: PitAuditReport,
    parity_report: ParityReport,
    experiment_eval: ExperimentEvaluation,
    freshness_slo_seconds: int = 300,
) -> list[dict[str, Any]]:
    """Run automated DQ assertions across Bronze, Silver, Gold, Online Store, and Experiments."""
    now_ms = int(time.time() * 1000)
    interactions = lakehouse.read_records("bronze.interaction_events")
    dlq = lakehouse.read_records("bronze.quarantine_dlq")
    sessions = lakehouse.read_records("silver.viewing_sessions")

    null_pk = sum(1 for r in interactions if not r.get("event_id") or not r.get("user_id"))
    neg_watch = sum(1 for r in interactions if float(r.get("watch_seconds", 0.0)) < 0.0)
    reconciled_total = len(interactions) + len(dlq) + duplicates_dropped
    recon_delta = abs(total_raw_received - reconciled_total)
    neg_session_dur = sum(1 for s in sessions if float(s.get("session_duration_seconds", 0.0)) < 0.0)

    checks: list[dict[str, Any]] = [
        {
            "table_name": "bronze.interaction_events",
            "check_name": "zero_null_primary_keys",
            "status": "PASS" if null_pk == 0 else "FAIL",
            "observed_value": float(null_pk),
            "threshold": 0.0,
            "details": f"Scanned {len(interactions)} bronze interactions for null event_id/user_id",
        },
        {
            "table_name": "bronze.interaction_events",
            "check_name": "zero_negative_watch_seconds",
            "status": "PASS" if neg_watch == 0 else "FAIL",
            "observed_value": float(neg_watch),
            "threshold": 0.0,
            "details": "Semantic bad events must be quarantined in bronze.quarantine_dlq",
        },
        {
            "table_name": "bronze.quarantine_dlq",
            "check_name": "ingestion_conservation_reconciliation",
            "status": "PASS" if recon_delta == 0 else "FAIL",
            "observed_value": float(recon_delta),
            "threshold": 0.0,
            "details": (
                f"raw={total_raw_received} vs valid({len(interactions)}) + dlq({len(dlq)}) + dups({duplicates_dropped}) = {reconciled_total}"
            ),
        },
        {
            "table_name": "silver.viewing_sessions",
            "check_name": "non_negative_session_durations",
            "status": "PASS" if neg_session_dur == 0 else "FAIL",
            "observed_value": float(neg_session_dur),
            "threshold": 0.0,
            "details": f"Validated {len(sessions)} event-time viewing sessions",
        },
        {
            "table_name": "gold.training_dataset",
            "check_name": "zero_pit_future_feature_leakage",
            "status": "PASS" if pit_audit.passed else "FAIL",
            "observed_value": float(pit_audit.total_leakage_violations),
            "threshold": 0.0,
            "details": f"Checked {pit_audit.total_rows} training rows for feature_ts <= request_ts",
        },
        {
            "table_name": "online_feature_store",
            "check_name": "online_offline_feature_parity",
            "status": "PASS" if parity_report.passed else "FAIL",
            "observed_value": float(parity_report.mismatch_ratio),
            "threshold": 0.0,
            "details": (
                f"Checked {parity_report.users_checked} users & {parity_report.items_checked} items; "
                f"max_abs_diff={parity_report.max_abs_numeric_diff}"
            ),
        },
        {
            "table_name": "online_feature_store",
            "check_name": "feature_freshness_within_slo",
            "status": "PASS" if parity_report.freshness_lag_seconds <= freshness_slo_seconds else "FAIL",
            "observed_value": float(parity_report.freshness_lag_seconds),
            "threshold": float(freshness_slo_seconds),
            "details": f"Freshness lag {parity_report.freshness_lag_seconds}s vs SLO {freshness_slo_seconds}s",
        },
        {
            "table_name": "gold.experiment_exposures",
            "check_name": "zero_cross_bucket_contamination",
            "status": "PASS" if experiment_eval.cross_bucket_contaminated_users == 0 else "FAIL",
            "observed_value": float(experiment_eval.cross_bucket_contaminated_users),
            "threshold": 0.0,
            "details": f"Checked {experiment_eval.total_users} exposed users for sticky variant assignment",
        },
        {
            "table_name": "gold.experiment_exposures",
            "check_name": "experiment_srm_health",
            "status": "PASS" if not experiment_eval.srm_detected else "FAIL",
            "observed_value": float(experiment_eval.srm_pvalue),
            "threshold": 0.001,
            "details": (
                f"chi2={experiment_eval.srm_chi2_stat}, p={experiment_eval.srm_pvalue}, "
                f"control={experiment_eval.control_users}, treatment={experiment_eval.treatment_users}"
            ),
        },
    ]

    records: list[dict[str, Any]] = []
    for idx, chk in enumerate(checks):
        records.append(
            {
                "check_id": deterministic_id("dq", run_id, chk["table_name"], chk["check_name"], idx),
                "run_id": run_id,
                "table_name": chk["table_name"],
                "check_name": chk["check_name"],
                "status": chk["status"],
                "observed_value": chk["observed_value"],
                "threshold": chk["threshold"],
                "details": chk["details"],
                "checked_ts_ms": now_ms,
            }
        )

    lakehouse.write_table("quality.dq_results", records, mode="overwrite")
    return records


def emit_openlineage_event(
    lineage_dir: Path,
    *,
    run_id: str,
    job_name: str,
    inputs: list[str],
    outputs: list[str],
    facets: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Emit an OpenLineage v1-compatible JSON event locally (Marquez sync active in Docker observability profile)."""
    lineage_dir.mkdir(parents=True, exist_ok=True)
    event = {
        "eventType": "COMPLETE",
        "eventTime": datetime.now(UTC).isoformat(),
        "producer": "https://github.com/ankdeshm/recommendation-feature-experiment-platform",
        "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json",
        "run": {
            "runId": run_id,
            "facets": {
                "runtimeMode": "local_native_file_sink",
                "marquezSyncActive": False,
                **(facets or {}),
            },
        },
        "job": {
            "namespace": "recsys_platform",
            "name": job_name,
        },
        "inputs": [{"namespace": "iceberg://recsys_iceberg", "name": name} for name in inputs],
        "outputs": [{"namespace": "iceberg://recsys_iceberg", "name": name} for name in outputs],
    }
    out_path = lineage_dir / "lineage_events.jsonl"
    with out_path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(event, default=str) + "\n")
    return event
