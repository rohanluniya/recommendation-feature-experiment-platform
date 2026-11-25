"""Apache Airflow 3.4.0 DAG orchestrating Bronze -> Silver -> Gold PIT -> Materialize -> Parity -> Train -> Experiment Eval."""

from __future__ import annotations

from datetime import datetime, timedelta

try:
    from airflow.sdk import DAG, task
except ImportError:  # Fallback for Airflow 2.x/3.x shim compatibility
    from airflow import DAG
    from airflow.decorators import task  # type: ignore[no-redef]


default_args = {
    "owner": "recsys-data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="recsys_feature_experiment_pipeline",
    default_args=default_args,
    description="Hourly PIT feature materialization, delayed label backfill, parity check, ranker retrain, and SRM/CUPED evaluation",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["recsys", "iceberg", "feast", "experimentation", "pit"],
) as dag:

    @task(task_id="ingest_and_sessionize")
    def ingest_and_sessionize() -> dict:
        from recsys_platform.config import get_config
        from recsys_platform.pipeline import run_full_pipeline

        summary = run_full_pipeline(get_config(), run_delayed_backfill=True, train_models=True)
        return {
            "run_id": summary["run_id"],
            "valid_events": summary["ingestion"]["valid_events"],
            "dlq_events": summary["ingestion"]["dlq_events"],
            "sessions": summary["sessionization"]["final_sessions"],
            "pit_violations": summary["pit_audit"]["strict_pit"]["total_leakage_violations"],
        }

    @task(task_id="verify_online_offline_parity")
    def verify_online_offline_parity(upstream: dict) -> dict:
        from recsys_platform.config import get_config
        from recsys_platform.lakehouse import IcebergLakehouse
        from recsys_platform.online_store import OnlineFeatureStore

        cfg = get_config()
        lakehouse = IcebergLakehouse(cfg)
        store = OnlineFeatureStore(cfg)
        report = store.validate_parity_against_lakehouse(lakehouse)
        if not report.passed:
            raise ValueError(f"Online/offline parity gate failed: {report.total_mismatches} mismatches")
        return {"run_id": upstream["run_id"], "parity_passed": report.passed, "users_checked": report.users_checked}

    @task(task_id="evaluate_ab_experiment_srm_cuped")
    def evaluate_ab_experiment_srm_cuped(upstream: dict) -> dict:
        from recsys_platform.config import get_config
        from recsys_platform.experimentation import evaluate_experiment
        from recsys_platform.lakehouse import IcebergLakehouse

        cfg = get_config()
        lh = IcebergLakehouse(cfg)
        res = evaluate_experiment(
            exposures=lh.read_records("gold.experiment_exposures"),
            sessions=lh.read_records("silver.viewing_sessions"),
            delayed_labels=lh.read_records("silver.delayed_labels"),
            interactions=lh.read_records("bronze.interaction_events"),
            experiment_id=cfg.default_experiment_id,
        )
        if res.srm_detected:
            raise ValueError(f"SRM detected (p={res.srm_pvalue}); halting automated rollout.")
        return {
            "run_id": upstream["run_id"],
            "decision": res.decision,
            "cuped_variance_reduction_pct": res.cuped_variance_reduction_pct,
        }

    step1 = ingest_and_sessionize()
    step2 = verify_online_offline_parity(step1)
    evaluate_ab_experiment_srm_cuped(step2)
