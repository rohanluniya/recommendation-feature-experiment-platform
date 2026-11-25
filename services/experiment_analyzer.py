#!/usr/bin/env python3
"""Service CLI: Evaluate A/B experiment SRM, CUPED variance reduction, guardrails, and interference from Iceberg tables."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path

from recsys_platform.config import get_config
from recsys_platform.experimentation import evaluate_experiment
from recsys_platform.lakehouse import IcebergLakehouse


def main() -> None:
    parser = argparse.ArgumentParser(description="Run SRM + CUPED + guardrail evaluation on Iceberg tables.")
    parser.add_argument("--experiment-id", type=str, default=None)
    parser.add_argument("--expected-treatment-ratio", type=float, default=0.50)
    parser.add_argument("--data-dir", type=Path, default=None)
    args = parser.parse_args()

    cfg = get_config(data_dir=args.data_dir)
    lakehouse = IcebergLakehouse(cfg)
    exp_id = args.experiment_id or cfg.default_experiment_id

    report = evaluate_experiment(
        exposures=lakehouse.read_records("gold.experiment_exposures"),
        sessions=lakehouse.read_records("silver.viewing_sessions"),
        delayed_labels=lakehouse.read_records("silver.delayed_labels"),
        interactions=lakehouse.read_records("bronze.interaction_events"),
        experiment_id=exp_id,
        expected_treatment_ratio=args.expected_treatment_ratio,
    )
    print(json.dumps(asdict(report), indent=2, default=str))


if __name__ == "__main__":
    main()
