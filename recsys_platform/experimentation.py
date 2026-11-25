"""Experimentation engine: deterministic SHA-256 bucketing, sticky exposure checks, SRM chi-squared detector, CUPED variance reduction, and guardrails."""

from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass
from typing import Any

import numpy as np
from scipy import stats

from recsys_platform.observability import TELEMETRY
from recsys_platform.simulator import assign_bucket_deterministic


@dataclass(slots=True)
class ExperimentEvaluation:
    """Comprehensive statistical readout for an A/B experiment."""

    experiment_id: str
    total_users: int
    control_users: int
    treatment_users: int
    expected_treatment_ratio: float
    observed_treatment_ratio: float
    cross_bucket_contaminated_users: int
    srm_chi2_stat: float
    srm_pvalue: float
    srm_detected: bool
    # Watch-time metric: unadjusted vs CUPED
    unadjusted_control_mean: float
    unadjusted_treatment_mean: float
    unadjusted_lift: float
    unadjusted_ci95_low: float
    unadjusted_ci95_high: float
    unadjusted_ci_width: float
    unadjusted_pvalue: float
    unadjusted_pooled_var: float
    cuped_theta: float
    cuped_corr_xy: float
    cuped_control_mean: float
    cuped_treatment_mean: float
    cuped_lift: float
    cuped_ci95_low: float
    cuped_ci95_high: float
    cuped_ci_width: float
    cuped_pvalue: float
    cuped_pooled_var: float
    cuped_variance_reduction_pct: float
    cuped_ci_width_reduction_pct: float
    # Completion rate primary binary metric
    control_completion_rate: float
    treatment_completion_rate: float
    completion_rate_lift_pp: float
    completion_pvalue: float
    # Guardrails & interference caveats
    guardrails: dict[str, Any]
    interference_diagnostics: dict[str, Any]
    decision: str


def assign_user_variant(
    user_id: str,
    experiment_id: str,
    salt: str,
    treatment_ratio: float = 0.50,
) -> dict[str, Any]:
    """Deterministically assign a user to control or treatment."""
    variant, bucket_hash = assign_bucket_deterministic(
        user_id=user_id,
        experiment_id=experiment_id,
        salt=salt,
        treatment_ratio=treatment_ratio,
    )
    return {
        "user_id": user_id,
        "experiment_id": experiment_id,
        "variant": variant,
        "bucket_hash": bucket_hash,
        "treatment_ratio": treatment_ratio,
    }


def detect_srm(
    control_count: int,
    treatment_count: int,
    expected_treatment_ratio: float = 0.50,
    alpha: float = 0.001,
) -> tuple[float, float, bool]:
    """Run SciPy chi-squared goodness-of-fit test for Sample Ratio Mismatch."""
    total = control_count + treatment_count
    if total == 0:
        return 0.0, 1.0, False
    expected_t = total * expected_treatment_ratio
    expected_c = total * (1.0 - expected_treatment_ratio)
    chi2_stat, p_value = stats.chisquare(
        f_obs=[control_count, treatment_count],
        f_exp=[expected_c, expected_t],
    )
    return float(chi2_stat), float(p_value), bool(p_value < alpha)


def compute_cuped_adjustment(
    y: np.ndarray,
    x: np.ndarray,
) -> tuple[np.ndarray, float, float, float]:
    """Apply CUPED variance reduction: Y_cuped = Y - theta * (X - mean(X)).

    Returns (y_cuped, theta, corr_xy, variance_reduction_pct).
    """
    if len(y) < 3 or len(x) != len(y):
        return y.copy(), 0.0, 0.0, 0.0
    var_x = float(np.var(x, ddof=1))
    var_y = float(np.var(y, ddof=1))
    if var_x <= 1e-12 or var_y <= 1e-12:
        return y.copy(), 0.0, 0.0, 0.0
    cov_xy = float(np.cov(y, x, ddof=1)[0, 1])
    theta = cov_xy / var_x
    corr_xy = cov_xy / math.sqrt(var_x * var_y)
    y_cuped = y - theta * (x - float(np.mean(x)))
    var_cuped = float(np.var(y_cuped, ddof=1))
    var_red_pct = max(0.0, (1.0 - (var_cuped / var_y)) * 100.0)
    return y_cuped, theta, corr_xy, var_red_pct


def _welch_ci_and_pvalue(ctrl: np.ndarray, trt: np.ndarray) -> tuple[float, float, float, float]:
    """Compute Welch two-sample t-test lift, 95% CI, and two-sided p-value."""
    n_c, n_t = len(ctrl), len(trt)
    if n_c < 2 or n_t < 2:
        return 0.0, 0.0, 0.0, 1.0
    mean_c = float(np.mean(ctrl))
    mean_t = float(np.mean(trt))
    lift = mean_t - mean_c
    var_c = float(np.var(ctrl, ddof=1))
    var_t = float(np.var(trt, ddof=1))
    se = math.sqrt(var_c / n_c + var_t / n_t)
    if se <= 1e-12:
        return lift, lift, lift, 1.0
    t_res = stats.ttest_ind(trt, ctrl, equal_var=False)
    pvalue = float(t_res.pvalue)
    # Welch-Satterthwaite degrees of freedom
    num = (var_c / n_c + var_t / n_t) ** 2
    den = ((var_c / n_c) ** 2) / (n_c - 1) + ((var_t / n_t) ** 2) / (n_t - 1)
    df = max(1.0, num / max(den, 1e-12))
    t_crit = float(stats.t.ppf(0.975, df))
    return lift, lift - t_crit * se, lift + t_crit * se, pvalue


def evaluate_experiment(
    exposures: list[dict[str, Any]],
    sessions: list[dict[str, Any]],
    delayed_labels: list[dict[str, Any]],
    interactions: list[dict[str, Any]],
    *,
    experiment_id: str,
    expected_treatment_ratio: float = 0.50,
    srm_alpha: float = 0.001,
) -> ExperimentEvaluation:
    """Evaluate A/B experiment with sticky exposure checks, SRM, CUPED, guardrails, and interference diagnostics."""
    with TELEMETRY.span("experiment.evaluate", {"experiment_id": experiment_id, "exposures": len(exposures)}):
        # 1. Sticky user exposure deduplication + cross-bucket contamination detection
        user_variants: dict[str, set[str]] = defaultdict(set)
        user_pre_x: dict[str, float] = {}
        variant_latencies: dict[str, list[float]] = defaultdict(list)

        for exp in exposures:
            if str(exp["experiment_id"]) != experiment_id:
                continue
            uid = str(exp["user_id"])
            var = str(exp["variant"])
            user_variants[uid].add(var)
            user_pre_x.setdefault(uid, float(exp["pre_experiment_watch_minutes"]))
            variant_latencies[var].append(float(exp.get("serving_latency_ms", 0.0)))

        contaminated = sum(1 for variants in user_variants.values() if len(variants) > 1)
        clean_assignment: dict[str, str] = {
            uid: next(iter(variants))
            for uid, variants in user_variants.items()
            if len(variants) == 1
        }

        # 2. Aggregate post-exposure outcomes per user from sessions + delayed labels
        user_watch_min: dict[str, float] = defaultdict(float)
        user_completions: dict[str, int] = defaultdict(int)
        user_plays: dict[str, int] = defaultdict(int)
        user_skips: dict[str, int] = defaultdict(int)
        user_impressions: dict[str, int] = defaultdict(int)
        user_rebuffers: dict[str, int] = defaultdict(int)
        user_heartbeats: dict[str, int] = defaultdict(int)

        for sess in sessions:
            uid = str(sess["user_id"])
            if uid not in clean_assignment:
                continue
            user_watch_min[uid] += float(sess["total_watch_seconds"]) / 60.0
            user_completions[uid] += int(sess["completions"])
            user_skips[uid] += int(sess["skips"])
            user_rebuffers[uid] += int(sess["rebuffer_count"])
            user_heartbeats[uid] += int(sess["heartbeat_count"])

        # Add delayed completions & watch time into post-exposure outcomes
        for dl in delayed_labels:
            uid = str(dl["user_id"])
            if uid in clean_assignment and int(dl["completed"]) == 1:
                user_completions[uid] += 1

        # Count plays, impressions, and shared catalog items per variant for interference diagnostics
        variant_items: dict[str, set[str]] = defaultdict(set)
        for ev in interactions:
            uid = str(ev["user_id"])
            if uid not in clean_assignment:
                continue
            var = clean_assignment[uid]
            ev_type = str(ev["event_type"])
            if ev_type == "IMPRESSION":
                user_impressions[uid] += 1
                variant_items[var].add(str(ev["item_id"]))
            elif ev_type == "PLAY_START":
                user_plays[uid] += 1

        ctrl_users = sorted(uid for uid, v in clean_assignment.items() if v == "control")
        trt_users = sorted(uid for uid, v in clean_assignment.items() if v == "treatment")
        all_users = ctrl_users + trt_users

        # 3. Sample Ratio Mismatch check
        chi2_stat, srm_pval, srm_detected = detect_srm(
            len(ctrl_users), len(trt_users), expected_treatment_ratio=expected_treatment_ratio, alpha=srm_alpha
        )
        TELEMETRY.experiment_srm_pvalue.labels(experiment_id=experiment_id).set(srm_pval)

        # 4. Build outcome vectors Y and pre-experiment covariate X for CUPED
        y_all = np.array([user_watch_min[u] for u in all_users], dtype=np.float64)
        x_all = np.array([user_pre_x[u] for u in all_users], dtype=np.float64)
        n_c = len(ctrl_users)

        y_ctrl = y_all[:n_c]
        y_trt = y_all[n_c:]

        unadj_lift, unadj_lo, unadj_hi, unadj_p = _welch_ci_and_pvalue(y_ctrl, y_trt)
        unadj_var = float(np.var(y_all, ddof=1)) if len(y_all) > 1 else 0.0

        y_cuped_all, theta, corr_xy, var_red_pct = compute_cuped_adjustment(y_all, x_all)
        yc_ctrl = y_cuped_all[:n_c]
        yc_trt = y_cuped_all[n_c:]

        cuped_lift, cuped_lo, cuped_hi, cuped_p = _welch_ci_and_pvalue(yc_ctrl, yc_trt)
        cuped_var = float(np.var(y_cuped_all, ddof=1)) if len(y_cuped_all) > 1 else 0.0

        unadj_ci_w = max(0.0, unadj_hi - unadj_lo)
        cuped_ci_w = max(0.0, cuped_hi - cuped_lo)
        ci_w_red_pct = round(((unadj_ci_w - cuped_ci_w) / max(unadj_ci_w, 1e-9)) * 100.0, 2)

        TELEMETRY.experiment_cuped_variance_reduction_pct.labels(experiment_id=experiment_id).set(
            round(var_red_pct, 3)
        )

        # 5. Binary completion rate metric (completions / plays)
        ctrl_plays = sum(user_plays[u] for u in ctrl_users)
        trt_plays = sum(user_plays[u] for u in trt_users)
        ctrl_comps = min(ctrl_plays, sum(user_completions[u] for u in ctrl_users))
        trt_comps = min(trt_plays, sum(user_completions[u] for u in trt_users))
        p_c = ctrl_comps / max(1, ctrl_plays)
        p_t = trt_comps / max(1, trt_plays)
        p_pool = (ctrl_comps + trt_comps) / max(1, ctrl_plays + trt_plays)
        se_prop = math.sqrt(max(1e-12, p_pool * (1.0 - p_pool) * (1.0 / max(1, ctrl_plays) + 1.0 / max(1, trt_plays))))
        z_stat = (p_t - p_c) / se_prop
        comp_pvalue = float(2.0 * (1.0 - stats.norm.cdf(abs(z_stat))))

        # 6. Guardrail metrics
        ctrl_p95_lat = float(np.percentile(variant_latencies["control"], 95)) if variant_latencies["control"] else 0.0
        trt_p95_lat = float(np.percentile(variant_latencies["treatment"], 95)) if variant_latencies["treatment"] else 0.0
        ctrl_rebuf = sum(user_rebuffers[u] for u in ctrl_users) / max(1, sum(user_heartbeats[u] for u in ctrl_users))
        trt_rebuf = sum(user_rebuffers[u] for u in trt_users) / max(1, sum(user_heartbeats[u] for u in trt_users))
        ctrl_skip = sum(user_skips[u] for u in ctrl_users) / max(1, sum(user_impressions[u] for u in ctrl_users))
        trt_skip = sum(user_skips[u] for u in trt_users) / max(1, sum(user_impressions[u] for u in trt_users))

        guardrails = {
            "serving_latency_p95_ms": {
                "control": round(ctrl_p95_lat, 3),
                "treatment": round(trt_p95_lat, 3),
                "delta_ms": round(trt_p95_lat - ctrl_p95_lat, 3),
                "threshold_max_delta_ms": 5.0,
                "passed": (trt_p95_lat - ctrl_p95_lat) <= 5.0,
            },
            "rebuffer_rate": {
                "control": round(ctrl_rebuf, 4),
                "treatment": round(trt_rebuf, 4),
                "delta": round(trt_rebuf - ctrl_rebuf, 4),
                "threshold_max_delta": 0.02,
                "passed": (trt_rebuf - ctrl_rebuf) <= 0.02,
            },
            "skip_rate": {
                "control": round(ctrl_skip, 4),
                "treatment": round(trt_skip, 4),
                "delta": round(trt_skip - ctrl_skip, 4),
                "threshold_max_delta": 0.05,
                "passed": (trt_skip - ctrl_skip) <= 0.05,
            },
        }
        all_guardrails_ok = all(g["passed"] for g in guardrails.values())

        # 7. Interference & spillover caveat diagnostics
        shared_items = len(variant_items["control"] & variant_items["treatment"])
        union_items = len(variant_items["control"] | variant_items["treatment"])
        jaccard_overlap = round(shared_items / max(1, union_items), 4)
        interference_diagnostics = {
            "shared_catalog_item_jaccard": jaccard_overlap,
            "shared_items_count": shared_items,
            "network_spillover_risk": "moderate_catalog_feedback_loop" if jaccard_overlap > 0.7 else "low",
            "caveat": (
                "Both variants share item-level popularity/completion features in the global item feature table. "
                "Long-running treatment traffic that boosts item engagement can slightly lift control item features "
                "(catalog popularity spillover). Mitigate via holdback clusters or variant-isolated item counters in production."
            ),
        }

        if srm_detected or contaminated > 0:
            decision = "INVALID_STOP_SRM_OR_CONTAMINATION"
        elif not all_guardrails_ok:
            decision = "HOLD_GUARDRAIL_REGRESSION"
        elif cuped_p < 0.05 and cuped_lift > 0:
            decision = "SHIP_SIGNIFICANT_POSITIVE_LIFT"
        else:
            decision = "KEEP_RUNNING_INCONCLUSIVE"

        return ExperimentEvaluation(
            experiment_id=experiment_id,
            total_users=len(all_users),
            control_users=len(ctrl_users),
            treatment_users=len(trt_users),
            expected_treatment_ratio=expected_treatment_ratio,
            observed_treatment_ratio=round(len(trt_users) / max(1, len(all_users)), 4),
            cross_bucket_contaminated_users=contaminated,
            srm_chi2_stat=round(chi2_stat, 4),
            srm_pvalue=round(srm_pval, 6),
            srm_detected=srm_detected,
            unadjusted_control_mean=round(float(np.mean(y_ctrl)) if len(y_ctrl) else 0.0, 3),
            unadjusted_treatment_mean=round(float(np.mean(y_trt)) if len(y_trt) else 0.0, 3),
            unadjusted_lift=round(unadj_lift, 3),
            unadjusted_ci95_low=round(unadj_lo, 3),
            unadjusted_ci95_high=round(unadj_hi, 3),
            unadjusted_ci_width=round(unadj_ci_w, 3),
            unadjusted_pvalue=round(unadj_p, 6),
            unadjusted_pooled_var=round(unadj_var, 3),
            cuped_theta=round(theta, 4),
            cuped_corr_xy=round(corr_xy, 4),
            cuped_control_mean=round(float(np.mean(yc_ctrl)) if len(yc_ctrl) else 0.0, 3),
            cuped_treatment_mean=round(float(np.mean(yc_trt)) if len(yc_trt) else 0.0, 3),
            cuped_lift=round(cuped_lift, 3),
            cuped_ci95_low=round(cuped_lo, 3),
            cuped_ci95_high=round(cuped_hi, 3),
            cuped_ci_width=round(cuped_ci_w, 3),
            cuped_pvalue=round(cuped_p, 6),
            cuped_pooled_var=round(cuped_var, 3),
            cuped_variance_reduction_pct=round(var_red_pct, 2),
            cuped_ci_width_reduction_pct=ci_w_red_pct,
            control_completion_rate=round(p_c, 4),
            treatment_completion_rate=round(p_t, 4),
            completion_rate_lift_pp=round((p_t - p_c) * 100.0, 2),
            completion_pvalue=round(comp_pvalue, 6),
            guardrails=guardrails,
            interference_diagnostics=interference_diagnostics,
            decision=decision,
        )
