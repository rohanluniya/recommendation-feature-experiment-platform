#!/usr/bin/env python3
"""Reproducible benchmark suite for Project 04 (smoke/local/stress, serving latency, PIT leakage, CUPED scaling, Iceberg compaction)."""

from __future__ import annotations

import csv
import json
import platform
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import psutil

from recsys_platform.config import REPO_ROOT, get_config
from recsys_platform.experimentation import evaluate_experiment
from recsys_platform.features import (
    audit_pit_leakage,
    build_naive_leaking_dataset,
    build_pit_training_dataset,
    build_temporal_feature_histories,
)
from recsys_platform.ingestion import validate_and_quarantine_interactions
from recsys_platform.lakehouse import IcebergLakehouse
from recsys_platform.online_store import OnlineFeatureStore
from recsys_platform.pipeline import run_full_pipeline
from recsys_platform.ranker import generate_and_rank_candidates
from recsys_platform.sessionizer import run_event_time_sessionization
from recsys_platform.simulator import SimulationConfig, generate_simulation

RESULTS_DIR = REPO_ROOT / "results" / "benchmarks"
PLOT_PATH = REPO_ROOT / "docs" / "assets" / "benchmark_recsys_feature_experiment.png"

PROFILES = [
    {"name": "smoke", "users": 100, "items": 30, "interactions_per_user": 10, "seed": 42},
    {"name": "local", "users": 300, "items": 75, "interactions_per_user": 18, "seed": 42},
    {"name": "stress", "users": 800, "items": 150, "interactions_per_user": 22, "seed": 42},
]


def _hardware_metadata() -> dict[str, Any]:
    vm = psutil.virtual_memory()
    return {
        "os": platform.system(),
        "os_release": platform.release(),
        "machine": platform.machine(),
        "python_version": platform.python_version(),
        "cpu_cores_logical": psutil.cpu_count(logical=True),
        "ram_gib": round(vm.total / (1024**3), 2),
    }


def _benchmark_profile(profile: dict[str, Any], work_dir: Path) -> tuple[dict[str, Any], Any, Any]:
    cfg = get_config(data_dir=work_dir / profile["name"] / "data")
    cfg.mlflow_tracking_dir = work_dir / profile["name"] / "mlruns"
    cfg.ensure_dirs()

    proc = psutil.Process()
    rss_before = proc.memory_info().rss / (1024**2)

    scfg = SimulationConfig(
        seed=int(profile["seed"]),
        num_users=int(profile["users"]),
        num_items=int(profile["items"]),
        interactions_per_user=int(profile["interactions_per_user"]),
    )

    t0 = time.perf_counter()
    bundle = generate_simulation(scfg)
    t_sim = time.perf_counter() - t0

    t1 = time.perf_counter()
    ing = validate_and_quarantine_interactions(bundle.interaction_events)
    t_ing = time.perf_counter() - t1

    t2 = time.perf_counter()
    sess = run_event_time_sessionization(ing.valid_events)
    t_sess = time.perf_counter() - t2

    t3 = time.perf_counter()
    u_pit, i_pit = build_temporal_feature_histories(
        ing.valid_events, bundle.catalog_changes, created_ts_ms=int(time.time() * 1000)
    )
    pit_rows = build_pit_training_dataset(
        ing.valid_events, u_pit, i_pit, delayed_labels=bundle.delayed_labels, include_delayed_labels=True
    )
    t_pit = time.perf_counter() - t3

    pit_audit = audit_pit_leakage(pit_rows)
    naive_rows = build_naive_leaking_dataset(
        ing.valid_events, u_pit, i_pit, delayed_labels=bundle.delayed_labels, include_delayed_labels=True
    )
    naive_audit = audit_pit_leakage(naive_rows)

    # Full Iceberg + online store persistence + rankers
    t4 = time.perf_counter()
    summary = run_full_pipeline(cfg, scfg, run_delayed_backfill=True, train_models=True)
    t_full = time.perf_counter() - t4

    rss_after = proc.memory_info().rss / (1024**2)

    raw_n = len(bundle.interaction_events)
    valid_n = len(ing.valid_events)
    row = {
        "profile": profile["name"],
        "users": profile["users"],
        "items": profile["items"],
        "raw_events": raw_n,
        "valid_events": valid_n,
        "dlq_events": len(ing.dlq_events),
        "duplicates_dropped": ing.duplicates_dropped,
        "sessions": len(sess.final_sessions),
        "late_retractions": sess.late_retractions,
        "pit_training_rows": len(pit_rows),
        "pit_leakage_violations": pit_audit.total_leakage_violations,
        "naive_leakage_violations": naive_audit.total_leakage_violations,
        "parity_mismatches": summary["online_store"]["parity"]["total_mismatches"],
        "sim_seconds": round(t_sim, 3),
        "ingestion_events_per_sec": round(raw_n / max(t_ing, 1e-6), 1),
        "sessionization_events_per_sec": round(valid_n / max(t_sess, 1e-6), 1),
        "pit_join_rows_per_sec": round(len(pit_rows) / max(t_pit, 1e-6), 1),
        "full_e2e_seconds": round(t_full, 3),
        "full_e2e_events_per_sec": round(raw_n / max(t_full, 1e-6), 1),
        "peak_rss_mib": round(max(rss_before, rss_after), 1),
        "baseline_logreg_pit_auc": summary["rankers"]["baseline_logreg_pit"]["roc_auc"],
        "treatment_gbdt_pit_auc": summary["rankers"]["treatment_gbdt_pit"]["roc_auc"],
        "naive_leaking_gbdt_auc": summary["rankers"]["naive_leaking_gbdt_audit"]["roc_auc"],
        "auc_leakage_inflation": summary["rankers"]["auc_leakage_inflation"],
    }
    return row, cfg, summary


def _benchmark_serving_latency(cfg: Any) -> dict[str, Any]:
    store = OnlineFeatureStore(cfg)
    user_ids = [u["user_id"] for u in store.list_entities("user", limit=300)]

    # Warm-up pass
    for uid in user_ids[:20]:
        store.get_user_features(uid)

    # 1,000 online feature store lookups
    feat_lat: list[float] = []
    t_f0 = time.perf_counter()
    for i in range(1000):
        uid = user_ids[i % len(user_ids)]
        ts = time.perf_counter()
        _ = store.get_user_features(uid)
        feat_lat.append((time.perf_counter() - ts) * 1000.0)
    feat_total = time.perf_counter() - t_f0

    # 300 live recommendation ranking calls
    rec_lat: list[float] = []
    t_r0 = time.perf_counter()
    for i in range(300):
        uid = user_ids[i % len(user_ids)]
        ts = time.perf_counter()
        _ = generate_and_rank_candidates(user_id=uid, online_store=store, config=cfg, top_k=5)
        rec_lat.append((time.perf_counter() - ts) * 1000.0)
    rec_total = time.perf_counter() - t_r0

    return {
        "online_feature_lookup": {
            "requests": 1000,
            "p50_ms": round(float(np.percentile(feat_lat, 50)), 3),
            "p95_ms": round(float(np.percentile(feat_lat, 95)), 3),
            "p99_ms": round(float(np.percentile(feat_lat, 99)), 3),
            "mean_ms": round(float(np.mean(feat_lat)), 3),
            "qps": round(1000.0 / max(feat_total, 1e-6), 1),
        },
        "live_recommendation_ranking": {
            "requests": 300,
            "p50_ms": round(float(np.percentile(rec_lat, 50)), 3),
            "p95_ms": round(float(np.percentile(rec_lat, 95)), 3),
            "p99_ms": round(float(np.percentile(rec_lat, 99)), 3),
            "mean_ms": round(float(np.mean(rec_lat)), 3),
            "qps": round(300.0 / max(rec_total, 1e-6), 1),
        },
    }


def _benchmark_cuped_scaling() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for n_users in (160, 300, 800):
        scfg = SimulationConfig(seed=42, num_users=n_users, num_items=60, interactions_per_user=16)
        bundle = generate_simulation(scfg)
        ing = validate_and_quarantine_interactions(bundle.interaction_events)
        sess = run_event_time_sessionization(ing.valid_events)
        ev = evaluate_experiment(
            exposures=bundle.experiment_exposures,
            sessions=sess.final_sessions,
            delayed_labels=bundle.delayed_labels,
            interactions=ing.valid_events,
            experiment_id=scfg.experiment_id,
        )
        rows.append(
            {
                "sample_users": ev.total_users,
                "control_users": ev.control_users,
                "treatment_users": ev.treatment_users,
                "srm_pvalue": ev.srm_pvalue,
                "srm_detected": ev.srm_detected,
                "unadjusted_pooled_var": ev.unadjusted_pooled_var,
                "cuped_pooled_var": ev.cuped_pooled_var,
                "variance_reduction_pct": ev.cuped_variance_reduction_pct,
                "unadjusted_ci_width": ev.unadjusted_ci_width,
                "cuped_ci_width": ev.cuped_ci_width,
                "ci_width_reduction_pct": ev.cuped_ci_width_reduction_pct,
                "unadjusted_pvalue": ev.unadjusted_pvalue,
                "cuped_pvalue": ev.cuped_pvalue,
                "cuped_theta": ev.cuped_theta,
                "corr_xy": ev.cuped_corr_xy,
                "decision": ev.decision,
            }
        )
    return rows


def _benchmark_iceberg_compaction(cfg: Any) -> dict[str, Any]:
    lh = IcebergLakehouse(cfg)
    base_rows = lh.read_records("gold.user_features_pit")
    chunk_size = max(50, len(base_rows) // 12)
    # Create 12 fragmented micro-batch appends simulating streaming small-file accumulation
    lh.write_table("gold.user_features_pit", base_rows[:chunk_size], mode="overwrite")
    for idx in range(1, 12):
        chunk = base_rows[idx * chunk_size : (idx + 1) * chunk_size]
        if chunk:
            lh.write_table("gold.user_features_pit", chunk, mode="append")

    files_before = lh.data_file_count("gold.user_features_pit")
    before_times = []
    for _ in range(5):
        t0 = time.perf_counter()
        _ = lh.read_arrow("gold.user_features_pit")
        before_times.append((time.perf_counter() - t0) * 1000.0)

    comp = lh.compact_table("gold.user_features_pit")

    after_times = []
    for _ in range(5):
        t1 = time.perf_counter()
        _ = lh.read_arrow("gold.user_features_pit")
        after_times.append((time.perf_counter() - t1) * 1000.0)

    med_before = round(float(np.median(before_times)), 3)
    med_after = round(float(np.median(after_times)), 3)
    speedup = round(med_before / max(med_after, 1e-6), 2)
    return {
        "table_id": "gold.user_features_pit",
        "row_count": comp["row_count"],
        "files_before": files_before,
        "files_after": comp["files_after"],
        "file_reduction_pct": comp["file_reduction_pct"],
        "median_scan_before_ms": med_before,
        "median_scan_after_ms": med_after,
        "scan_speedup_x": speedup,
    }


def _generate_plot(
    profiles: list[dict[str, Any]],
    serving: dict[str, Any],
    cuped: list[dict[str, Any]],
    compaction: dict[str, Any],
) -> None:
    plt.rcParams.update(
        {
            "figure.facecolor": "#0b101b",
            "axes.facecolor": "#121a2b",
            "axes.edgecolor": "#253452",
            "axes.labelcolor": "#cbd5e1",
            "xtick.color": "#93a6c5",
            "ytick.color": "#93a6c5",
            "text.color": "#e6edf7",
            "grid.color": "#1e2c46",
            "font.size": 10,
        }
    )

    fig, axes = plt.subplots(2, 2, figsize=(13.5, 8.6), dpi=160)
    fig.suptitle(
        "Project 04: Real-Time Recommendation Feature Store & Experimentation Benchmarks (Apple M4 Pro, local_native)",
        fontsize=13,
        fontweight="bold",
        y=0.98,
    )

    # Panel 1: Throughput across load profiles
    ax = axes[0, 0]
    names = [p["profile"] for p in profiles]
    x = np.arange(len(names))
    w = 0.28
    ing_k = [p["ingestion_events_per_sec"] / 1000.0 for p in profiles]
    sess_k = [p["sessionization_events_per_sec"] / 1000.0 for p in profiles]
    e2e_k = [p["full_e2e_events_per_sec"] / 1000.0 for p in profiles]
    ax.bar(x - w, ing_k, width=w, label="Avro Ingest + DLQ", color="#38bdf8")
    ax.bar(x, sess_k, width=w, label="Event-Time Sessionizer", color="#a78bfa")
    ax.bar(x + w, e2e_k, width=w, label="Full E2E (Iceberg+PIT+ML)", color="#22c55e")
    ax.set_xticks(x)
    ax.set_xticklabels([f"{p['profile']}\n({p['raw_events']:,} ev)" for p in profiles])
    ax.set_ylabel("Throughput (k events/sec)")
    ax.set_title("A. Stage & End-to-End Throughput by Load Profile")
    ax.grid(axis="y", alpha=0.5)
    ax.legend(frameon=False, fontsize=8.5)

    # Panel 2: Serving latency p50 / p95 / p99
    ax = axes[0, 1]
    pcts = ["p50", "p95", "p99"]
    xp = np.arange(len(pcts))
    f_vals = [
        serving["online_feature_lookup"]["p50_ms"],
        serving["online_feature_lookup"]["p95_ms"],
        serving["online_feature_lookup"]["p99_ms"],
    ]
    r_vals = [
        serving["live_recommendation_ranking"]["p50_ms"],
        serving["live_recommendation_ranking"]["p95_ms"],
        serving["live_recommendation_ranking"]["p99_ms"],
    ]
    b1 = ax.bar(xp - 0.18, f_vals, width=0.34, label="Online Feature Lookup (n=1,000)", color="#38bdf8")
    b2 = ax.bar(xp + 0.18, r_vals, width=0.34, label="Live Ranker + Bucket (n=300)", color="#f59e0b")
    for bars in (b1, b2):
        for bar in bars:
            h = bar.get_height()
            ax.annotate(f"{h:.2f}", (bar.get_x() + bar.get_width() / 2, h), ha="center", va="bottom", fontsize=8.5, xytext=(0, 3), textcoords="offset points")
    ax.axhline(10.0, color="#ef4444", linestyle="--", linewidth=1.1, label="10ms p99 Feature SLO")
    ax.set_xticks(xp)
    ax.set_xticklabels(pcts)
    ax.set_ylabel("Latency (ms)")
    ax.set_title("B. Online Feature & Live Recommendation Serving Latency")
    ax.grid(axis="y", alpha=0.5)
    ax.legend(frameon=False, fontsize=8.5)

    # Panel 3: PIT vs Naive Join Leakage AUC Inflation (local & stress profiles)
    ax = axes[1, 0]
    p_local = next(p for p in profiles if p["profile"] == "local")
    p_stress = next(p for p in profiles if p["profile"] == "stress")
    cats = ["local (300u)", "stress (800u)"]
    xc = np.arange(len(cats))
    base_auc = [p_local["baseline_logreg_pit_auc"], p_stress["baseline_logreg_pit_auc"]]
    gbdt_auc = [p_local["treatment_gbdt_pit_auc"], p_stress["treatment_gbdt_pit_auc"]]
    leak_auc = [p_local["naive_leaking_gbdt_auc"], p_stress["naive_leaking_gbdt_auc"]]
    ax.bar(xc - 0.25, base_auc, width=0.24, label="Baseline LogReg (0 PIT leaks)", color="#38bdf8")
    ax.bar(xc, gbdt_auc, width=0.24, label="Treatment GBDT (0 PIT leaks)", color="#22c55e")
    ax.bar(xc + 0.25, leak_auc, width=0.24, label="Naive Global Join (100% LEAK)", color="#ef4444")
    ax.set_ylim(0.45, 0.85)
    ax.set_xticks(xc)
    ax.set_xticklabels(cats)
    ax.set_ylabel("Held-Out ROC-AUC")
    ax.set_title("C. Strict PIT ASOF vs Naive Join Leakage Inflation")
    ax.grid(axis="y", alpha=0.5)
    ax.legend(frameon=False, fontsize=8.5)

    # Panel 4: CUPED 95% CI Width Tightening & Iceberg Compaction Callout
    ax = axes[1, 1]
    xs = np.arange(len(cuped))
    unadj_w = [c["unadjusted_ci_width"] for c in cuped]
    cuped_w = [c["cuped_ci_width"] for c in cuped]
    ax.bar(xs - 0.18, unadj_w, width=0.34, label="Unadjusted 95% CI Width", color="#f59e0b")
    b_c = ax.bar(xs + 0.18, cuped_w, width=0.34, label="CUPED Adjusted 95% CI Width", color="#22c55e")
    for idx, bar in enumerate(b_c):
        h = bar.get_height()
        vr = cuped[idx]["variance_reduction_pct"]
        ax.annotate(f"-{vr:.1f}% var", (bar.get_x() + bar.get_width() / 2, h), ha="center", va="bottom", fontsize=8.5, color="#4ade80", xytext=(0, 3), textcoords="offset points")
    ax.set_xticks(xs)
    ax.set_xticklabels([f"N={c['sample_users']}" for c in cuped])
    ax.set_ylabel("95% CI Width (watch min/user)")
    ax.set_title(
        f"D. CUPED CI Tightening | Iceberg Compact: {compaction['files_before']}->{compaction['files_after']} files ({compaction['scan_speedup_x']}x)"
    )
    ax.grid(axis="y", alpha=0.5)
    ax.legend(frameon=False, fontsize=8.5)

    fig.tight_layout(rect=(0, 0, 1, 0.94))
    PLOT_PATH.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(PLOT_PATH, facecolor=fig.get_facecolor())
    plt.close(fig)


def main() -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    hw = _hardware_metadata()

    profile_rows: list[dict[str, Any]] = []
    serving_bench: dict[str, Any] = {}
    compaction_bench: dict[str, Any] = {}

    with tempfile.TemporaryDirectory(prefix="recsys_bench_") as tmp_str:
        work_dir = Path(tmp_str)
        for prof in PROFILES:
            row, cfg, _summary = _benchmark_profile(prof, work_dir)
            profile_rows.append(row)
            if prof["name"] == "local":
                serving_bench = _benchmark_serving_latency(cfg)
                compaction_bench = _benchmark_iceberg_compaction(cfg)

    cuped_rows = _benchmark_cuped_scaling()

    # Write CSV artifacts
    load_csv = RESULTS_DIR / "load_profiles.csv"
    with load_csv.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(profile_rows[0].keys()))
        writer.writeheader()
        writer.writerows(profile_rows)

    cuped_csv = RESULTS_DIR / "cuped_scaling.csv"
    with cuped_csv.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(cuped_rows[0].keys()))
        writer.writeheader()
        writer.writerows(cuped_rows)

    summary = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "hardware": hw,
        "runtime_mode": "local_native",
        "load_profiles": profile_rows,
        "serving_latency": serving_bench,
        "cuped_scaling": cuped_rows,
        "iceberg_compaction": compaction_bench,
    }
    summary_json = RESULTS_DIR / "benchmark_summary.json"
    summary_json.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    _generate_plot(profile_rows, serving_bench, cuped_rows, compaction_bench)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
