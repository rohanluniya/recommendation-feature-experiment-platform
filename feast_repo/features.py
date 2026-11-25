"""Feast 0.68.0 feature definitions for real-time recommendation serving and PIT training."""

from __future__ import annotations

from datetime import timedelta

from feast import Entity, FeatureService, FeatureView, Field, FileSource, ValueType
from feast.types import Bool, Float64, Int32, String

user_entity = Entity(
    name="user_id",
    join_keys=["user_id"],
    value_type=ValueType.STRING,
    description="Deterministic streaming viewer identifier.",
)

item_entity = Entity(
    name="item_id",
    join_keys=["item_id"],
    value_type=ValueType.STRING,
    description="Content catalog title identifier.",
)

user_features_source = FileSource(
    name="gold_user_features_pit_source",
    path="../.local_data/feast_exports/user_features_pit.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
)

item_features_source = FileSource(
    name="gold_item_features_pit_source",
    path="../.local_data/feast_exports/item_features_pit.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created_timestamp",
)

user_engagement_fv = FeatureView(
    name="user_engagement_features",
    entities=[user_entity],
    ttl=timedelta(days=7),
    schema=[
        Field(name="user_watch_seconds_total", dtype=Float64),
        Field(name="user_play_count", dtype=Int32),
        Field(name="user_completion_count", dtype=Int32),
        Field(name="user_completion_rate", dtype=Float64),
        Field(name="user_skip_rate", dtype=Float64),
        Field(name="user_rebuffer_rate", dtype=Float64),
        Field(name="user_device_switches", dtype=Int32),
        Field(name="user_top_genre", dtype=String),
        Field(name="is_cold_start_user", dtype=Bool),
    ],
    online=True,
    source=user_features_source,
    tags={"layer": "gold", "owner": "recsys-data-platform", "pit_verified": "true"},
)

item_catalog_fv = FeatureView(
    name="item_catalog_features",
    entities=[item_entity],
    ttl=timedelta(days=30),
    schema=[
        Field(name="genre", dtype=String),
        Field(name="duration_seconds", dtype=Int32),
        Field(name="release_year", dtype=Int32),
        Field(name="popularity_prior", dtype=Float64),
        Field(name="is_active", dtype=Bool),
        Field(name="item_impressions_so_far", dtype=Int32),
        Field(name="item_plays_so_far", dtype=Int32),
        Field(name="item_completion_rate_so_far", dtype=Float64),
        Field(name="is_cold_start_item", dtype=Bool),
    ],
    online=True,
    source=item_features_source,
    tags={"layer": "gold", "owner": "recsys-data-platform", "scd2_catalog": "true"},
)

recsys_ranking_v2_service = FeatureService(
    name="recsys_ranking_v2",
    features=[user_engagement_fv, item_catalog_fv],
    tags={"experiment_id": "exp_recsys_ranker_v2", "sla_p99_ms": "10"},
)
