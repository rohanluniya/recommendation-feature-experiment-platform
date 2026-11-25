"""Shared pytest fixtures for isolated lakehouse, online store, and pipeline testing."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from recsys_platform.config import PlatformConfig, get_config
from recsys_platform.pipeline import run_full_pipeline
from recsys_platform.simulator import SimulationConfig


@pytest.fixture()
def isolated_config(tmp_path: Path) -> PlatformConfig:
    cfg = get_config(data_dir=tmp_path / "data")
    cfg.mlflow_tracking_dir = tmp_path / "mlruns"
    cfg.ensure_dirs()
    return cfg


@pytest.fixture(scope="module")
def module_bootstrapped_env(tmp_path_factory: pytest.TempPathFactory) -> tuple[PlatformConfig, dict[str, Any]]:
    base = tmp_path_factory.mktemp("recsys_mod")
    cfg = get_config(data_dir=base / "data")
    cfg.mlflow_tracking_dir = base / "mlruns"
    cfg.ensure_dirs()
    scfg = SimulationConfig(
        seed=42,
        num_users=120,
        num_items=36,
        interactions_per_user=12,
        experiment_id=cfg.default_experiment_id,
        experiment_salt=cfg.default_experiment_salt,
    )
    summary = run_full_pipeline(cfg, scfg)
    return cfg, summary
