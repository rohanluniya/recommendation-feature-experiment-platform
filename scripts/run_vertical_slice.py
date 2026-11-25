#!/usr/bin/env python3
"""Run the end-to-end vertical slice (source -> sessionizer -> Iceberg V2 -> PIT ASOF -> online store -> ranker -> experiment -> DQ)."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from recsys_platform.config import get_config
from recsys_platform.pipeline import run_full_pipeline
from recsys_platform.simulator import SimulationConfig


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Project 04 end-to-end vertical slice.")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--users", type=int, default=240)
    parser.add_argument("--items", type=int, default=60)
    parser.add_argument("--interactions-per-user", type=int, default=18)
    parser.add_argument("--data-dir", type=Path, default=None)
    args = parser.parse_args()

    cfg = get_config(data_dir=args.data_dir)
    scfg = SimulationConfig(
        seed=args.seed,
        num_users=args.users,
        num_items=args.items,
        interactions_per_user=args.interactions_per_user,
        experiment_id=cfg.default_experiment_id,
        experiment_salt=cfg.default_experiment_salt,
    )
    summary = run_full_pipeline(cfg, scfg)
    print(
        json.dumps(
            {
                "run_id": summary["run_id"],
                "elapsed_seconds": summary["elapsed_seconds"],
                "ingestion": summary["ingestion"],
                "sessionization": summary["sessionization"],
                "pit_strict_violations": summary["pit_audit"]["strict_pit"]["total_leakage_violations"],
                "pit_naive_violations": summary["pit_audit"]["naive_global_join"]["total_leakage_violations"],
                "pit_naive_leakage_rate": summary["pit_audit"]["naive_global_join"]["leakage_rate"],
                "delayed_label_backfill": summary["delayed_label_backfill"],
                "online_parity_passed": summary["online_store"]["parity"]["passed"],
                "online_parity_mismatches": summary["online_store"]["parity"]["total_mismatches"],
                "rankers": {
                    k: (
                        {
                            "roc_auc": v["roc_auc"],
                            "pr_auc": v["pr_auc"],
                            "ndcg_at_5": v["ndcg_at_5"],
                        }
                        if isinstance(v, dict)
                        else v
                    )
                    for k, v in summary["rankers"].items()
                },
                "experiment": {
                    "control_users": summary["experiment"]["control_users"],
                    "treatment_users": summary["experiment"]["treatment_users"],
                    "srm_pvalue": summary["experiment"]["srm_pvalue"],
                    "srm_detected": summary["experiment"]["srm_detected"],
                    "unadjusted_lift": summary["experiment"]["unadjusted_lift"],
                    "unadjusted_ci_width": summary["experiment"]["unadjusted_ci_width"],
                    "unadjusted_pvalue": summary["experiment"]["unadjusted_pvalue"],
                    "cuped_lift": summary["experiment"]["cuped_lift"],
                    "cuped_ci_width": summary["experiment"]["cuped_ci_width"],
                    "cuped_pvalue": summary["experiment"]["cuped_pvalue"],
                    "cuped_variance_reduction_pct": summary["experiment"]["cuped_variance_reduction_pct"],
                    "decision": summary["experiment"]["decision"],
                },
                "dq_passed": f"{summary['data_quality']['passed_checks']}/{summary['data_quality']['total_checks']}",
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
