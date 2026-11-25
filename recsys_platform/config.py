"""Centralized configuration for the Recommendation Feature & Experimentation Platform."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


@dataclass(slots=True)
class PlatformConfig:
    """Runtime settings loaded from environment variables with safe local defaults."""

    env: str = field(default_factory=lambda: os.getenv("RECSYS_ENV", "local_native"))
    data_dir: Path = field(
        default_factory=lambda: Path(os.getenv("RECSYS_DATA_DIR", str(REPO_ROOT / ".local_data")))
    )
    api_host: str = field(default_factory=lambda: os.getenv("RECSYS_API_HOST", "127.0.0.1"))
    api_port: int = field(default_factory=lambda: int(os.getenv("RECSYS_API_PORT", "8030")))
    seed: int = field(default_factory=lambda: int(os.getenv("RECSYS_SEED", "42")))

    # RBAC API keys (non-secret dev defaults; override via secrets manager in production)
    admin_api_key: str = field(
        default_factory=lambda: os.getenv("RECSYS_ADMIN_API_KEY", "dev-admin-key-change-me")
    )
    analyst_api_key: str = field(
        default_factory=lambda: os.getenv("RECSYS_ANALYST_API_KEY", "dev-analyst-key-change-me")
    )
    serving_api_key: str = field(
        default_factory=lambda: os.getenv("RECSYS_SERVING_API_KEY", "dev-serving-key-change-me")
    )

    # Distributed connectors (active when containerized stack is running)
    kafka_bootstrap_servers: str = field(
        default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    valkey_url: str = field(default_factory=lambda: os.getenv("VALKEY_URL", "redis://localhost:6379/0"))
    valkey_enabled: bool = field(
        default_factory=lambda: os.getenv("VALKEY_ENABLED", "false").lower() == "true"
    )
    mlflow_tracking_dir: Path = field(
        default_factory=lambda: Path(
            os.getenv("MLFLOW_TRACKING_DIR", str(REPO_ROOT / "results" / "mlruns"))
        )
    )

    # Streaming, Feature Store, and Experimentation parameters
    feature_freshness_slo_seconds: int = field(
        default_factory=lambda: int(os.getenv("FEATURE_FRESHNESS_SLO_SECONDS", "300"))
    )
    session_inactivity_gap_seconds: int = field(
        default_factory=lambda: int(os.getenv("SESSION_INACTIVITY_GAP_SECONDS", "1800"))
    )
    watermark_allowed_lateness_seconds: int = field(
        default_factory=lambda: int(os.getenv("WATERMARK_ALLOWED_LATENESS_SECONDS", "300"))
    )
    user_state_ttl_seconds: int = field(
        default_factory=lambda: int(os.getenv("USER_STATE_TTL_SECONDS", "86400"))
    )
    default_experiment_id: str = field(
        default_factory=lambda: os.getenv("DEFAULT_EXPERIMENT_ID", "exp_recsys_ranker_v2")
    )
    default_experiment_salt: str = field(
        default_factory=lambda: os.getenv("DEFAULT_EXPERIMENT_SALT", "recsys-platform-salt-2026")
    )

    @property
    def warehouse_dir(self) -> Path:
        return self.data_dir / "iceberg_warehouse"

    @property
    def catalog_db_path(self) -> Path:
        return self.data_dir / "iceberg_catalog.db"

    @property
    def spool_dir(self) -> Path:
        return self.data_dir / "spool"

    @property
    def online_store_db_path(self) -> Path:
        return self.data_dir / "online_feature_store.db"

    @property
    def audit_log_path(self) -> Path:
        return self.data_dir / "audit_events.jsonl"

    @property
    def lineage_dir(self) -> Path:
        return self.data_dir / "lineage"

    @property
    def models_dir(self) -> Path:
        return self.data_dir / "models"

    def ensure_dirs(self) -> None:
        for path in (
            self.data_dir,
            self.warehouse_dir,
            self.spool_dir,
            self.lineage_dir,
            self.models_dir,
            self.mlflow_tracking_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)


def get_config(data_dir: Path | None = None) -> PlatformConfig:
    """Create a PlatformConfig, optionally overriding data_dir for isolated runs/tests."""
    cfg = PlatformConfig()
    if data_dir is not None:
        cfg.data_dir = Path(data_dir)
    cfg.ensure_dirs()
    return cfg
