#!/usr/bin/env python3
"""Service CLI: Materialize Iceberg Gold PIT features to the online feature store and validate parity + Feast export."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from recsys_platform.config import get_config
from recsys_platform.lakehouse import IcebergLakehouse
from recsys_platform.online_store import OnlineFeatureStore


def export_feast_parquet(lakehouse: IcebergLakehouse, export_dir: Path) -> dict[str, int]:
    """Export Iceberg Gold PIT tables with UTC timestamp columns for Feast offline/online materialization."""
    export_dir.mkdir(parents=True, exist_ok=True)
    counts: dict[str, int] = {}
    for table_id, filename in (
        ("gold.user_features_pit", "user_features_pit.parquet"),
        ("gold.item_features_pit", "item_features_pit.parquet"),
    ):
        df = lakehouse.read_arrow(table_id).to_pandas()
        df["event_timestamp"] = pd.to_datetime(df["event_ts_ms"], unit="ms", utc=True)
        df["created_timestamp"] = pd.to_datetime(df["created_ts_ms"], unit="ms", utc=True)
        df.to_parquet(export_dir / filename, index=False)
        counts[table_id] = len(df)
    return counts


def main() -> None:
    parser = argparse.ArgumentParser(description="Materialize Iceberg Gold features into Online Store and verify parity.")
    parser.add_argument("--data-dir", type=Path, default=None)
    parser.add_argument("--wipe-first", action="store_true", help="Wipe online store before rebuilding from Iceberg Gold.")
    parser.add_argument("--export-feast", action="store_true", help="Also export Feast-compatible Parquet snapshots.")
    args = parser.parse_args()

    cfg = get_config(data_dir=args.data_dir)
    lakehouse = IcebergLakehouse(cfg)
    online = OnlineFeatureStore(cfg)

    if args.wipe_first:
        result = online.wipe_and_rebuild_from_lakehouse(lakehouse)
    else:
        mat = online.materialize_from_lakehouse(lakehouse)
        parity = online.validate_parity_against_lakehouse(lakehouse)
        result = {"materialization": mat, "parity": asdict(parity)}

    if args.export_feast:
        result["feast_exports"] = export_feast_parquet(lakehouse, cfg.data_dir / "feast_exports")
        result["feast_exported_at"] = datetime.now(UTC).isoformat()

    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
