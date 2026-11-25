"""End-to-end pipeline orchestrator for vertical slice, full run, delayed backfill, and parity audit."""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import asdict
from typing import Any

from recsys_platform.config import PlatformConfig, get_config
from recsys_platform.experimentation import evaluate_experiment
from recsys_platform.features import (
    apply_delayed_label_backfill,
    audit_pit_leakage,
    build_naive_leaking_dataset,
    build_pit_training_dataset,
    build_temporal_feature_histories,
)
from recsys_platform.ingestion import LocalTopicSpool, validate_and_quarantine_interactions
from recsys_platform.lakehouse import IcebergLakehouse
from recsys_platform.logging_utils import RUN_ID_VAR, get_logger, log_event
from recsys_platform.observability import TELEMETRY
from recsys_platform.online_store import OnlineFeatureStore
from recsys_platform.quality import emit_openlineage_event, run_data_quality_checks
from recsys_platform.ranker import train_and_evaluate_ranker
from recsys_platform.sessionizer import run_event_time_sessionization
from recsys_platform.simulator import SimulationConfig, generate_simulation

logger = get_logger("recsys_platform.pipeline")


def run_full_pipeline(
    config: PlatformConfig | None = None,
    sim_config: SimulationConfig | None = None,
    *,
    run_delayed_backfill: bool = True,
    train_models: bool = True,
) -> dict[str, Any]:
    """Execute full source -> spool -> sessionizer -> Iceberg V2 -> PIT ASOF -> online store -> ranker -> experiment -> DQ."""
    cfg = config or get_config()
    scfg = sim_config or SimulationConfig(
        seed=cfg.seed,
        experiment_id=cfg.default_experiment_id,
        experiment_salt=cfg.default_experiment_salt,
    )
    run_id = f"run-{uuid.uuid4().hex[:10]}"
    run_token = RUN_ID_VAR.set(run_id)
    t_start = time.perf_counter()

    try:
        with TELEMETRY.span("pipeline.run_full", {"run_id": run_id, "seed": scfg.seed}):
            lakehouse = IcebergLakehouse(cfg)
            online_store = OnlineFeatureStore(cfg)
            spool = LocalTopicSpool(cfg.spool_dir)

            # 1. Deterministic source generation
            bundle = generate_simulation(scfg)

            # 2. Idempotent ingestion + Avro validation + dedup + DLQ + durable topic spool
            ing = validate_and_quarantine_interactions(bundle.interaction_events, spool=spool)
            spool.produce("catalog.changes.cdc", bundle.catalog_changes, key_field="item_id")
            spool.produce("experiments.exposures", bundle.experiment_exposures, key_field="user_id")

            lakehouse.write_table("bronze.interaction_events", ing.valid_events, mode="overwrite")
            lakehouse.write_table("bronze.catalog_changes", bundle.catalog_changes, mode="overwrite")
            lakehouse.write_table("bronze.quarantine_dlq", ing.dlq_events, mode="overwrite")

            emit_openlineage_event(
                cfg.lineage_dir,
                run_id=run_id,
                job_name="bronze_ingestion",
                inputs=["stream://playback.interactions", "cdc://postgres.catalog"],
                outputs=["bronze.interaction_events", "bronze.catalog_changes", "bronze.quarantine_dlq"],
                facets={"valid_events": len(ing.valid_events), "dlq_events": len(ing.dlq_events)},
            )

            # 3. Event-time sessionization (watermark + allowed lateness + retractions + TTL)
            sess_out = run_event_time_sessionization(
                ing.valid_events,
                inactivity_gap_ms=cfg.session_inactivity_gap_seconds * 1000,
                allowed_lateness_ms=cfg.watermark_allowed_lateness_seconds * 1000,
                state_ttl_ms=cfg.user_state_ttl_seconds * 1000,
            )
            lakehouse.write_table("silver.viewing_sessions", sess_out.final_sessions, mode="overwrite")
            lakehouse.write_table("silver.delayed_labels", bundle.delayed_labels, mode="overwrite")

            emit_openlineage_event(
                cfg.lineage_dir,
                run_id=run_id,
                job_name="silver_event_time_sessionization",
                inputs=["bronze.interaction_events"],
                outputs=["silver.viewing_sessions", "silver.delayed_labels"],
                facets={
                    "sessions": len(sess_out.final_sessions),
                    "late_retractions": sess_out.late_retractions,
                    "late_dropped": sess_out.late_dropped_events,
                },
            )

            # 4. Gold PIT feature histories & strict ASOF training dataset (initial before delayed backfill)
            now_ms = int(time.time() * 1000)
            user_pit, item_pit = build_temporal_feature_histories(
                ing.valid_events, bundle.catalog_changes, created_ts_ms=now_ms
            )
            lakehouse.write_table("gold.user_features_pit", user_pit, mode="overwrite")
            lakehouse.write_table("gold.item_features_pit", item_pit, mode="overwrite")
            lakehouse.write_table("gold.experiment_exposures", bundle.experiment_exposures, mode="overwrite")

            pit_dataset_initial = build_pit_training_dataset(
                ing.valid_events,
                user_pit,
                item_pit,
                delayed_labels=bundle.delayed_labels,
                include_delayed_labels=False,
            )
            lakehouse.write_table("gold.training_dataset", pit_dataset_initial, mode="overwrite")

            # 5. Delayed label backfill (creates a new Iceberg V2 snapshot preserving zero feature leakage!)
            backfill_summary: dict[str, Any] | None = None
            pit_dataset_final = pit_dataset_initial
            if run_delayed_backfill:
                pit_dataset_final, backfill_summary = apply_delayed_label_backfill(
                    pit_dataset_initial, bundle.delayed_labels
                )
                lakehouse.write_table("gold.training_dataset", pit_dataset_final, mode="overwrite")

            pit_audit = audit_pit_leakage(pit_dataset_final)
            naive_dataset = build_naive_leaking_dataset(
                ing.valid_events,
                user_pit,
                item_pit,
                delayed_labels=bundle.delayed_labels,
                include_delayed_labels=run_delayed_backfill,
            )
            naive_audit = audit_pit_leakage(naive_dataset)

            # 6. Materialize online feature store + validate offline/online parity
            mat_summary = online_store.materialize_from_lakehouse(lakehouse)
            parity_report = online_store.validate_parity_against_lakehouse(lakehouse)

            emit_openlineage_event(
                cfg.lineage_dir,
                run_id=run_id,
                job_name="gold_pit_features_and_online_materialization",
                inputs=["bronze.interaction_events", "bronze.catalog_changes", "silver.delayed_labels"],
                outputs=["gold.user_features_pit", "gold.item_features_pit", "gold.training_dataset", "online://valkey_store"],
                facets={"pit_violations": pit_audit.total_leakage_violations, "parity_mismatches": parity_report.total_mismatches},
            )

            # 7. Train deterministic baseline & treatment rankers + leakage comparison
            train_snaps = lakehouse.snapshot_history("gold.training_dataset")
            latest_snap_id = train_snaps[-1]["snapshot_id"] if train_snaps else None
            ranker_reports: dict[str, Any] = {}
            if train_models:
                _, base_rep = train_and_evaluate_ranker(
                    pit_dataset_final,
                    cfg,
                    model_kind="baseline_logreg",
                    dataset_mode="pit_asof",
                    iceberg_snapshot_id=latest_snap_id,
                    seed=scfg.seed,
                )
                _, trt_rep = train_and_evaluate_ranker(
                    pit_dataset_final,
                    cfg,
                    model_kind="treatment_gbdt",
                    dataset_mode="pit_asof",
                    iceberg_snapshot_id=latest_snap_id,
                    seed=scfg.seed,
                )
                _, naive_rep = train_and_evaluate_ranker(
                    naive_dataset,
                    cfg,
                    model_kind="treatment_gbdt",
                    dataset_mode="naive_leaking_audit",
                    iceberg_snapshot_id=latest_snap_id,
                    seed=scfg.seed,
                    save_artifact=False,
                )
                ranker_reports = {
                    "baseline_logreg_pit": asdict(base_rep),
                    "treatment_gbdt_pit": asdict(trt_rep),
                    "naive_leaking_gbdt_audit": asdict(naive_rep),
                    "auc_leakage_inflation": round(naive_rep.roc_auc - trt_rep.roc_auc, 4),
                }

            # 8. Experiment evaluation (SRM + CUPED + guardrails + interference)
            exp_eval = evaluate_experiment(
                exposures=bundle.experiment_exposures,
                sessions=sess_out.final_sessions,
                delayed_labels=bundle.delayed_labels,
                interactions=ing.valid_events,
                experiment_id=scfg.experiment_id,
                expected_treatment_ratio=0.50,
            )

            # 9. Automated Data Quality gate suite
            dq_records = run_data_quality_checks(
                run_id=run_id,
                lakehouse=lakehouse,
                total_raw_received=ing.total_received,
                duplicates_dropped=ing.duplicates_dropped,
                pit_audit=pit_audit,
                parity_report=parity_report,
                experiment_eval=exp_eval,
                freshness_slo_seconds=cfg.feature_freshness_slo_seconds,
            )

            elapsed_sec = round(time.perf_counter() - t_start, 3)
            summary: dict[str, Any] = {
                "run_id": run_id,
                "elapsed_seconds": elapsed_sec,
                "simulation": bundle.summary,
                "ingestion": {
                    "total_received": ing.total_received,
                    "valid_events": len(ing.valid_events),
                    "duplicates_dropped": ing.duplicates_dropped,
                    "dlq_events": len(ing.dlq_events),
                    "topic_offsets": ing.topic_offsets,
                },
                "sessionization": {
                    "final_sessions": len(sess_out.final_sessions),
                    "changelog_events": len(sess_out.changelog_stream),
                    "late_retractions": sess_out.late_retractions,
                    "late_dropped_events": sess_out.late_dropped_events,
                    "duplicates_suppressed": sess_out.duplicates_suppressed,
                    "ttl_evicted_users": sess_out.ttl_evicted_users,
                    "peak_active_users_in_state": sess_out.peak_active_users_in_state,
                    "final_watermark_ms": sess_out.final_watermark_ms,
                },
                "pit_audit": {
                    "strict_pit": asdict(pit_audit),
                    "naive_global_join": asdict(naive_audit),
                },
                "delayed_label_backfill": backfill_summary,
                "online_store": {
                    "materialization": mat_summary,
                    "parity": asdict(parity_report),
                },
                "rankers": ranker_reports,
                "experiment": asdict(exp_eval),
                "data_quality": {
                    "total_checks": len(dq_records),
                    "passed_checks": sum(1 for c in dq_records if c["status"] == "PASS"),
                    "failed_checks": sum(1 for c in dq_records if c["status"] != "PASS"),
                    "checks": dq_records,
                },
                "iceberg_inventory": lakehouse.table_inventory(),
            }
            # Persist latest run summary for instant API/dashboard startup inspection
            summary_path = cfg.data_dir / "latest_pipeline_summary.json"
            summary_path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")

            log_event(
                logger,
                20,
                "Completed end-to-end pipeline run",
                run_id=run_id,
                elapsed_seconds=elapsed_sec,
                dq_passed=summary["data_quality"]["passed_checks"],
                dq_total=summary["data_quality"]["total_checks"],
                pit_violations=pit_audit.total_leakage_violations,
                parity_mismatches=parity_report.total_mismatches,
                cuped_var_red_pct=exp_eval.cuped_variance_reduction_pct,
            )
            return summary
    finally:
        RUN_ID_VAR.reset(run_token)
