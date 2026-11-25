"""Candidate generation, deterministic scikit-learn baseline/treatment rankers (zero pretrained weights), and MLflow-compatible run tracking."""

from __future__ import annotations

import json
import pickle
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, log_loss, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from recsys_platform.config import PlatformConfig
from recsys_platform.experimentation import assign_user_variant
from recsys_platform.observability import TELEMETRY
from recsys_platform.online_store import OnlineFeatureStore

FEATURE_COLUMNS = (
    "user_watch_seconds_total",
    "user_play_count",
    "user_completion_rate",
    "user_skip_rate",
    "user_rebuffer_rate",
    "genre_match",
    "item_popularity_prior",
    "item_completion_rate_so_far",
    "item_duration_seconds",
    "is_cold_start_user",
    "is_cold_start_item",
)


@dataclass(slots=True)
class RankerTrainingReport:
    """Offline evaluation metrics and MLflow-compatible run metadata."""

    run_id: str
    model_name: str
    dataset_mode: str
    train_rows: int
    test_rows: int
    positive_rate_test: float
    roc_auc: float
    pr_auc: float
    logloss: float
    ndcg_at_5: float
    artifact_path: str
    mlflow_run_dir: str


def _rows_to_matrix(rows: list[dict[str, Any]]) -> tuple[np.ndarray, np.ndarray]:
    x = np.array(
        [[float(r[col]) for col in FEATURE_COLUMNS] for r in rows],
        dtype=np.float64,
    )
    y = np.array([int(r["label_completed"]) for r in rows], dtype=np.int32)
    return x, y


def _compute_ndcg_at_k(rows: list[dict[str, Any]], scores: np.ndarray, k: int = 5) -> float:
    """Compute mean NDCG@k grouped by user over held-out impressions."""
    grouped: dict[str, list[tuple[float, int]]] = {}
    for row, sc in zip(rows, scores, strict=False):
        grouped.setdefault(str(row["user_id"]), []).append((float(sc), int(row["label_completed"])))

    ndcgs: list[float] = []
    for pairs in grouped.values():
        if not any(lbl == 1 for _, lbl in pairs):
            continue
        ranked = sorted(pairs, key=lambda p: p[0], reverse=True)[:k]
        ideal = sorted(pairs, key=lambda p: p[1], reverse=True)[:k]
        dcg = sum(lbl / np.log2(idx + 2) for idx, (_, lbl) in enumerate(ranked))
        idcg = sum(lbl / np.log2(idx + 2) for idx, (_, lbl) in enumerate(ideal))
        if idcg > 0:
            ndcgs.append(float(dcg / idcg))
    return round(float(np.mean(ndcgs)) if ndcgs else 0.0, 4)


def _write_mlflow_metadata(
    tracking_dir: Path,
    *,
    run_id: str,
    model_name: str,
    params: dict[str, Any],
    metrics: dict[str, float],
    tags: dict[str, Any],
    lineage: dict[str, Any],
) -> Path:
    """Write standard MLflow-compatible file-store metadata + Iceberg snapshot lineage."""
    run_dir = tracking_dir / "exp_recsys_ranker" / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    meta_yaml = (
        "artifact_uri: ./artifacts\n"
        f"end_time: {int(time.time() * 1000)}\n"
        "experiment_id: exp_recsys_ranker\n"
        "lifecycle_stage: active\n"
        f"run_id: {run_id}\n"
        f"run_name: {model_name}\n"
        "status: FINISHED\n"
    )
    (run_dir / "meta.yaml").write_text(meta_yaml, encoding="utf-8")
    (run_dir / "params.json").write_text(json.dumps(params, indent=2, sort_keys=True), encoding="utf-8")
    (run_dir / "metrics.json").write_text(json.dumps(metrics, indent=2, sort_keys=True), encoding="utf-8")
    (run_dir / "tags.json").write_text(json.dumps(tags, indent=2, sort_keys=True), encoding="utf-8")
    (run_dir / "lineage.json").write_text(json.dumps(lineage, indent=2, sort_keys=True), encoding="utf-8")
    return run_dir


def train_and_evaluate_ranker(
    dataset: list[dict[str, Any]],
    config: PlatformConfig,
    *,
    model_kind: str = "baseline_logreg",
    dataset_mode: str = "pit_asof",
    iceberg_snapshot_id: int | None = None,
    seed: int = 42,
    save_artifact: bool = True,
) -> tuple[Any, RankerTrainingReport]:
    """Train deterministic scikit-learn ranker using strict chronological 75/25 train/test split."""
    with TELEMETRY.span("ranker.train", {"model_kind": model_kind, "dataset_mode": dataset_mode, "rows": len(dataset)}):
        ordered = sorted(dataset, key=lambda r: (int(r["request_ts_ms"]), str(r["row_id"])))
        split_idx = max(10, int(len(ordered) * 0.75))
        train_rows = ordered[:split_idx]
        test_rows = ordered[split_idx:]

        x_train, y_train = _rows_to_matrix(train_rows)
        x_test, y_test = _rows_to_matrix(test_rows)

        if model_kind == "baseline_logreg":
            model: Any = Pipeline(
                [
                    ("scaler", StandardScaler()),
                    ("clf", LogisticRegression(max_iter=300, C=1.0, random_state=seed)),
                ]
            )
            params: dict[str, Any] = {"algorithm": "LogisticRegression", "C": 1.0, "max_iter": 300, "seed": seed}
        elif model_kind == "treatment_gbdt":
            model = HistGradientBoostingClassifier(
                max_iter=50,
                max_depth=3,
                learning_rate=0.05,
                min_samples_leaf=30,
                l2_regularization=5.0,
                random_state=seed,
            )
            params = {
                "algorithm": "HistGradientBoostingClassifier",
                "max_iter": 50,
                "max_depth": 3,
                "learning_rate": 0.05,
                "min_samples_leaf": 30,
                "l2_regularization": 5.0,
                "seed": seed,
            }
        else:
            raise ValueError(f"Unsupported model_kind: {model_kind}")

        # Guard edge case if tiny synthetic fixture has single class
        if len(set(y_train.tolist())) < 2:
            probs = np.full(len(y_test), float(np.mean(y_train)), dtype=np.float64)
            roc = 0.5
            pr = float(np.mean(y_test))
            ll = 0.6931
        else:
            model.fit(x_train, y_train)
            probs = model.predict_proba(x_test)[:, 1]
            if len(set(y_test.tolist())) < 2:
                roc = 0.5
                pr = float(np.mean(y_test))
            else:
                roc = float(roc_auc_score(y_test, probs))
                pr = float(average_precision_score(y_test, probs))
            ll = float(log_loss(y_test, np.clip(probs, 1e-6, 1.0 - 1e-6), labels=[0, 1]))

        ndcg5 = _compute_ndcg_at_k(test_rows, probs, k=5)
        run_id = f"{model_kind}_{dataset_mode}_seed{seed}"
        artifact_path = config.models_dir / f"{model_kind}.pkl"
        if save_artifact:
            config.models_dir.mkdir(parents=True, exist_ok=True)
            with artifact_path.open("wb") as fh:
                pickle.dump(model, fh)

        metrics = {
            "roc_auc": round(roc, 4),
            "pr_auc": round(pr, 4),
            "log_loss": round(ll, 4),
            "ndcg_at_5": ndcg5,
            "train_rows": float(len(train_rows)),
            "test_rows": float(len(test_rows)),
        }
        lineage = {
            "source_table": "gold.training_dataset",
            "iceberg_snapshot_id": iceberg_snapshot_id,
            "dataset_mode": dataset_mode,
            "feature_columns": list(FEATURE_COLUMNS),
            "pretrained_weights_used": False,
        }
        tags = {
            "platform": "recommendation-feature-experiment-platform",
            "dataset_mode": dataset_mode,
            "pretrained_weights": "none",
            "license": "Apache-2.0",
        }
        mlflow_dir = _write_mlflow_metadata(
            config.mlflow_tracking_dir,
            run_id=run_id,
            model_name=model_kind,
            params=params,
            metrics=metrics,
            tags=tags,
            lineage=lineage,
        )
        return model, RankerTrainingReport(
            run_id=run_id,
            model_name=model_kind,
            dataset_mode=dataset_mode,
            train_rows=len(train_rows),
            test_rows=len(test_rows),
            positive_rate_test=round(float(np.mean(y_test)) if len(y_test) else 0.0, 4),
            roc_auc=round(roc, 4),
            pr_auc=round(pr, 4),
            logloss=round(ll, 4),
            ndcg_at_5=ndcg5,
            artifact_path=str(artifact_path),
            mlflow_run_dir=str(mlflow_dir),
        )


def load_ranker(config: PlatformConfig, model_kind: str) -> Any | None:
    path = config.models_dir / f"{model_kind}.pkl"
    if not path.exists():
        return None
    with path.open("rb") as fh:
        return pickle.load(fh)


def generate_and_rank_candidates(
    *,
    user_id: str,
    online_store: OnlineFeatureStore,
    config: PlatformConfig,
    experiment_id: str | None = None,
    top_k: int = 5,
    candidate_pool_size: int = 25,
) -> dict[str, Any]:
    """Live online candidate generation + variant-aware ranking using online feature store."""
    t0 = time.perf_counter()
    exp_id = experiment_id or config.default_experiment_id
    assignment = assign_user_variant(
        user_id=user_id,
        experiment_id=exp_id,
        salt=config.default_experiment_salt,
    )
    variant = assignment["variant"]
    model_kind = "treatment_gbdt" if variant == "treatment" else "baseline_logreg"
    model = load_ranker(config, model_kind)

    user_feats = online_store.get_user_features(user_id)
    is_cold_user = user_feats is None or bool(user_feats.get("is_cold_start_user", False))
    u_watch = float(user_feats["user_watch_seconds_total"]) if user_feats else 0.0
    u_plays = int(user_feats["user_play_count"]) if user_feats else 0
    u_comp_rate = float(user_feats["user_completion_rate"]) if user_feats else 0.0
    u_skip_rate = float(user_feats["user_skip_rate"]) if user_feats else 0.0
    u_rebuf_rate = float(user_feats["user_rebuffer_rate"]) if user_feats else 0.0
    u_top_genre = str(user_feats["user_top_genre"]) if user_feats else "unknown"

    # Candidate generation: active catalog items prioritized by genre affinity + popularity + cold-start exploration
    all_items = [it for it in online_store.list_entities("item", limit=500) if bool(it.get("is_active", True))]
    warm_candidates = sorted(
        all_items,
        key=lambda it: (
            1 if (u_top_genre != "unknown" and str(it["genre"]) == u_top_genre) else 0,
            float(it.get("popularity_prior", 0.0)),
        ),
        reverse=True,
    )[:candidate_pool_size]

    # Reserve up to 2 exploration slots for cold-start items
    cold_items = [it for it in all_items if bool(it.get("is_cold_start_item", False))][:2]
    seen_ids = {str(it["item_id"]) for it in warm_candidates}
    for ci in cold_items:
        if str(ci["item_id"]) not in seen_ids:
            warm_candidates.append(ci)
            seen_ids.add(str(ci["item_id"]))

    if not warm_candidates:
        return {
            "user_id": user_id,
            "experiment_assignment": assignment,
            "model_version": model_kind,
            "is_cold_start_user": is_cold_user,
            "user_top_genre": u_top_genre,
            "recommendations": [],
            "latency_ms": round((time.perf_counter() - t0) * 1000.0, 3),
        }

    feature_rows = []
    for it in warm_candidates:
        genre_match = 1 if (u_top_genre != "unknown" and str(it["genre"]) == u_top_genre) else 0
        feature_rows.append(
            [
                u_watch,
                float(u_plays),
                u_comp_rate,
                u_skip_rate,
                u_rebuf_rate,
                float(genre_match),
                float(it["popularity_prior"]),
                float(it["item_completion_rate_so_far"]),
                float(it["duration_seconds"]),
                1.0 if is_cold_user else 0.0,
                1.0 if bool(it.get("is_cold_start_item", False)) else 0.0,
            ]
        )

    x_mat = np.array(feature_rows, dtype=np.float64)
    if model is not None and hasattr(model, "predict_proba"):
        scores = model.predict_proba(x_mat)[:, 1]
    else:
        # Deterministic heuristic fallback if ranker not yet trained
        scores = 0.45 * x_mat[:, 5] + 0.35 * x_mat[:, 6] + 0.20 * x_mat[:, 7]

    scored: list[dict[str, Any]] = []
    for it, sc in zip(warm_candidates, scores, strict=False):
        genre_match = u_top_genre != "unknown" and str(it["genre"]) == u_top_genre
        scored.append(
            {
                "item_id": str(it["item_id"]),
                "title": str(it["title"]),
                "genre": str(it["genre"]),
                "score": round(float(sc), 4),
                "genre_match": genre_match,
                "popularity_prior": float(it["popularity_prior"]),
                "item_completion_rate": float(it["item_completion_rate_so_far"]),
                "is_cold_start_item": bool(it.get("is_cold_start_item", False)),
                "feature_event_ts_ms": int(it["event_ts_ms"]),
            }
        )

    scored.sort(key=lambda r: r["score"], reverse=True)
    latency_ms = round((time.perf_counter() - t0) * 1000.0, 3)
    TELEMETRY.recommendation_latency_ms.observe(latency_ms)

    return {
        "user_id": user_id,
        "experiment_assignment": assignment,
        "model_version": model_kind,
        "is_cold_start_user": is_cold_user,
        "user_top_genre": u_top_genre,
        "user_feature_event_ts_ms": int(user_feats["event_ts_ms"]) if user_feats else 0,
        "user_feature_materialized_ts_ms": int(user_feats.get("materialized_ts_ms", 0)) if user_feats else 0,
        "candidate_pool_size": len(warm_candidates),
        "recommendations": scored[:top_k],
        "latency_ms": latency_ms,
    }
