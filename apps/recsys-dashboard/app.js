(function () {
  "use strict";

  const $ = (id) => document.getElementById(id);

  function fmt(num, digits = 2) {
    if (num === null || num === undefined || Number.isNaN(Number(num))) return "--";
    return Number(num).toFixed(digits);
  }

  function pillHtml(text, tone = "neutral") {
    return `<span class="pill pill-${tone}">${text}</span>`;
  }

  function renderKv(container, pairs) {
    container.innerHTML = pairs
      .map(
        ([k, v]) => `
        <div class="kv-item">
          <div class="kv-k">${k}</div>
          <div class="kv-v" title="${String(v)}">${v}</div>
        </div>`
      )
      .join("");
  }

  function renderCupedChart(exp) {
    const minVal = Math.min(exp.unadjusted_ci95_low, exp.cuped_ci95_low, -5);
    const maxVal = Math.max(exp.unadjusted_ci95_high, exp.cuped_ci95_high, 40);
    const pad = (maxVal - minVal) * 0.12;
    const lo = minVal - pad;
    const hi = maxVal + pad;
    const xMap = (v) => 125 + ((v - lo) / (hi - lo)) * 395;
    const xZero = xMap(0);

    const svg = `
      <svg viewBox="0 0 560 150" role="img" aria-label="CUPED vs Unadjusted 95% Confidence Interval Plot">
        <line x1="${xZero.toFixed(1)}" y1="16" x2="${xZero.toFixed(1)}" y2="122" stroke="#64748b" stroke-dasharray="4 4" stroke-width="1.2"/>
        <text x="${xZero.toFixed(1)}" y="138" fill="#94a3b8" font-size="10.5" text-anchor="middle" font-family="monospace">0 (No Lift)</text>

        <text x="10" y="49" fill="#cbd5e1" font-size="11.5" font-weight="600">Unadjusted</text>
        <line x1="${xMap(exp.unadjusted_ci95_low).toFixed(1)}" y1="45" x2="${xMap(exp.unadjusted_ci95_high).toFixed(1)}" y2="45" stroke="#f59e0b" stroke-width="4" stroke-linecap="round"/>
        <circle cx="${xMap(exp.unadjusted_lift).toFixed(1)}" cy="45" r="5.5" fill="#fbbf24"/>
        <text x="${(xMap(exp.unadjusted_ci95_high) + 6).toFixed(1)}" y="49" fill="#fbbf24" font-size="10.5" font-family="monospace">p=${fmt(exp.unadjusted_pvalue, 4)}</text>

        <text x="10" y="96" fill="#cbd5e1" font-size="11.5" font-weight="600">CUPED Adj.</text>
        <line x1="${xMap(exp.cuped_ci95_low).toFixed(1)}" y1="92" x2="${xMap(exp.cuped_ci95_high).toFixed(1)}" y2="92" stroke="#22c55e" stroke-width="4.5" stroke-linecap="round"/>
        <circle cx="${xMap(exp.cuped_lift).toFixed(1)}" cy="92" r="5.5" fill="#4ade80"/>
        <text x="${(xMap(exp.cuped_ci95_high) + 6).toFixed(1)}" y="96" fill="#4ade80" font-size="10.5" font-family="monospace">p=${fmt(exp.cuped_pvalue, 5)}</text>
      </svg>`;
    $("cuped-chart").innerHTML = svg;
  }

  function renderPitChart(rankers) {
    const baseAuc = rankers?.baseline_logreg_pit?.roc_auc ?? 0.5;
    const gbdtAuc = rankers?.treatment_gbdt_pit?.roc_auc ?? 0.5;
    const naiveAuc = rankers?.naive_leaking_gbdt_audit?.roc_auc ?? 0.5;

    const bars = [
      { label: "Baseline LogReg (Strict PIT)", val: baseAuc, color: "#38bdf8" },
      { label: "Treatment GBDT (Strict PIT)", val: gbdtAuc, color: "#22c55e" },
      { label: "Naive Global Join (LEAKING)", val: naiveAuc, color: "#ef4444" },
    ];
    const rows = bars
      .map((b, idx) => {
        const y = 18 + idx * 42;
        const w = Math.max(20, Math.min(310, b.val * 320));
        return `
          <text x="8" y="${y + 16}" fill="#cbd5e1" font-size="11.5" font-weight="600">${b.label}</text>
          <rect x="205" y="${y + 4}" width="315" height="18" rx="4" fill="#182338"/>
          <rect x="205" y="${y + 4}" width="${w.toFixed(1)}" height="18" rx="4" fill="${b.color}"/>
          <text x="526" y="${y + 17}" fill="#f8fafc" font-size="11.5" font-family="monospace">${fmt(b.val, 4)}</text>
        `;
      })
      .join("");

    $("pit-chart").innerHTML = `
      <svg viewBox="0 0 570 148" role="img" aria-label="PIT vs Naive Leaking Ranker ROC-AUC Comparison">
        ${rows}
      </svg>`;
  }

  async function runRecommendationSim() {
    const userId = $("user-select").value || "user_00000";
    const topK = Number($("topk-select").value || "5");
    const apiKey = $("api-key-input").value;

    const headers = { "Content-Type": "application/json", "X-API-Key": apiKey };
    const [recRes, featRes] = await Promise.all([
      fetch("/v1/recommend", {
        method: "POST",
        headers,
        body: JSON.stringify({ user_id: userId, top_k: topK }),
      }),
      fetch(`/v1/features/online/${encodeURIComponent(userId)}`, { headers }),
    ]);

    if (!recRes.ok) {
      $("assignment-summary").innerHTML = `<div class="kv-item"><div class="kv-k">Auth / API Error</div><div class="kv-v">HTTP ${recRes.status}</div></div>`;
      return;
    }
    const rec = await recRes.json();
    const feat = featRes.ok ? await featRes.json() : null;
    const assign = rec.experiment_assignment || {};

    $("sim-latency-pill").textContent = `latency: ${fmt(rec.latency_ms, 2)} ms`;
    renderKv($("assignment-summary"), [
      ["Experiment Variant", `${assign.variant} (hash ${assign.bucket_hash})`],
      ["Active Ranker", rec.model_version],
      ["Top Genre Affinity", rec.user_top_genre],
      ["Cold-Start User", rec.is_cold_start_user ? "YES (prior fallback)" : "NO (warm history)"],
      ["Online Watch Total", feat ? `${fmt(feat.features.user_watch_seconds_total, 1)} s` : "--"],
      ["Completion Rate", feat ? `${fmt(feat.features.user_completion_rate * 100, 1)}%` : "--"],
      ["Feature Backend", feat ? feat.backend_mode : "--"],
      ["Freshness Age", feat ? `${fmt(feat.freshness_age_seconds, 1)} s` : "--"],
    ]);

    const tbody = $("rec-table").querySelector("tbody");
    tbody.innerHTML = (rec.recommendations || [])
      .map(
        (r, i) => `
        <tr>
          <td>${i + 1}</td>
          <td class="text-col">${r.title}</td>
          <td>${r.genre}</td>
          <td>${r.genre_match ? pillHtml("MATCH", "ok") : pillHtml("EXPLORE", "neutral")}</td>
          <td>${fmt(r.score, 4)}</td>
          <td>${fmt(r.popularity_prior, 3)}</td>
        </tr>`
      )
      .join("");
  }

  async function loadOverview() {
    const res = await fetch("/v1/dashboard/overview");
    if (!res.ok) return;
    const data = await res.json();
    const s = data.summary;
    const exp = s.experiment;
    const strictPit = s.pit_audit.strict_pit;
    const naivePit = s.pit_audit.naive_global_join;
    const parity = s.online_store.parity;
    const sess = s.sessionization;
    const dq = s.data_quality;

    $("env-pill").textContent = String(data.environment || "local_native").toUpperCase();
    $("run-id-pill").textContent = `run: ${s.run_id}`;
    $("freshness-pill").textContent = `freshness lag: ${fmt(parity.freshness_lag_seconds, 2)}s`;

    $("kpi-pit-violations").textContent = `${strictPit.total_leakage_violations} leaks`;
    $("kpi-pit-sub").textContent = `vs ${naivePit.total_leakage_violations.toLocaleString()} naive global leaks`;

    $("kpi-parity").textContent = parity.passed ? "100% Exact" : `${parity.total_mismatches} skew`;
    $("kpi-parity-sub").textContent = `${parity.users_checked} users / ${parity.items_checked} items checked`;

    $("kpi-cuped").textContent = `-${fmt(exp.cuped_variance_reduction_pct, 1)}% Var`;
    $("kpi-cuped-sub").textContent = `CI width ${fmt(exp.unadjusted_ci_width, 1)} -> ${fmt(exp.cuped_ci_width, 1)} min`;

    $("kpi-srm").textContent = exp.srm_detected ? "SRM ALERT" : "Healthy";
    $("kpi-srm-sub").textContent = `p=${fmt(exp.srm_pvalue, 4)} (${exp.control_users}C / ${exp.treatment_users}T)`;

    $("kpi-sessions").textContent = `${sess.final_sessions.toLocaleString()}`;
    $("kpi-sessions-sub").textContent = `${sess.late_retractions} retractions / ${sess.late_dropped_events} dropped`;

    $("kpi-dq").textContent = `${dq.passed_checks}/${dq.total_checks} PASS`;
    $("kpi-dq-sub").textContent = `${s.ingestion.dlq_events} DLQ / ${s.ingestion.duplicates_dropped} dups deduped`;

    $("exp-decision-pill").textContent = exp.decision;
    $("exp-decision-pill").className = `pill ${exp.srm_detected ? "pill-danger" : "pill-ok"}`;

    renderCupedChart(exp);
    renderKv($("cuped-metrics-grid"), [
      ["CUPED Theta", fmt(exp.cuped_theta, 4)],
      ["Corr(Y, X)", fmt(exp.cuped_corr_xy, 4)],
      ["CUPED Lift", `+${fmt(exp.cuped_lift, 2)} min`],
      ["Unadj. p-val", fmt(exp.unadjusted_pvalue, 4)],
      ["CUPED p-val", fmt(exp.cuped_pvalue, 6)],
      ["Comp. Lift", `+${fmt(exp.completion_rate_lift_pp, 2)} pp`],
    ]);

    renderPitChart(s.rankers);
    const bf = s.delayed_label_backfill || {};
    renderKv($("pit-metrics-grid"), [
      ["Strict PIT Leaks", `${strictPit.total_leakage_violations}`],
      ["Naive Leaks", `${naivePit.total_leakage_violations.toLocaleString()}`],
      ["AUC Inflation", `+${fmt(s.rankers?.auc_leakage_inflation, 4)}`],
      ["Delayed Backfill", `+${bf.delayed_labels_applied ?? 0} labels`],
      ["TS Mutated", `${bf.feature_timestamps_mutated ?? 0}`],
      ["Cold-Start Rows", `${strictPit.cold_start_user_rows}`],
    ]);

    // Guardrails table
    const gBody = $("guardrails-table").querySelector("tbody");
    gBody.innerHTML = Object.entries(exp.guardrails || {})
      .map(([name, g]) => {
        const delta = g.delta_ms !== undefined ? `${fmt(g.delta_ms, 3)} ms` : fmt(g.delta, 4);
        return `
          <tr>
            <td class="text-col">${name}</td>
            <td>${g.control}</td>
            <td>${g.treatment}</td>
            <td>${delta}</td>
            <td>${pillHtml(g.passed ? "PASS" : "FAIL", g.passed ? "ok" : "danger")}</td>
          </tr>`;
      })
      .join("");

    const inter = exp.interference_diagnostics || {};
    $("interference-note").textContent =
      `Catalog Interference Diagnostic (Jaccard overlap ${fmt(inter.shared_catalog_item_jaccard, 3)}, ${inter.shared_items_count} shared titles): ${inter.caveat || ""}`;

    // Iceberg table inventory
    $("iceberg-table").querySelector("tbody").innerHTML = (s.iceberg_inventory || [])
      .map(
        (t) => `
        <tr>
          <td>${t.table_id}</td>
          <td>${Number(t.row_count).toLocaleString()}</td>
          <td>${t.data_files}</td>
          <td>${t.snapshot_count}</td>
        </tr>`
      )
      .join("");

    // DQ checks
    $("dq-table").querySelector("tbody").innerHTML = (dq.checks || [])
      .map(
        (c) => `
        <tr>
          <td>${c.table_name}</td>
          <td>${c.check_name}</td>
          <td>${c.observed_value}</td>
          <td>${c.threshold}</td>
          <td>${pillHtml(c.status, c.status === "PASS" ? "ok" : "danger")}</td>
          <td class="text-col">${c.details}</td>
        </tr>`
      )
      .join("");

    // Sessions sample
    $("sessions-table").querySelector("tbody").innerHTML = (data.sample_sessions || [])
      .slice(0, 8)
      .map(
        (row) => `
        <tr>
          <td>${row.user_id}</td>
          <td>${fmt(row.total_watch_seconds, 1)}</td>
          <td>${row.distinct_devices} (${row.device_switch_count} sw)</td>
          <td>v${row.revision}</td>
          <td>${row.had_late_retraction ? pillHtml("RETRACTED", "warn") : pillHtml("ON-TIME", "neutral")}</td>
        </tr>`
      )
      .join("");

    // DLQ sample
    $("dlq-table").querySelector("tbody").innerHTML = (data.sample_dlq || [])
      .slice(0, 6)
      .map(
        (row) => `
        <tr>
          <td>${row.event_id}</td>
          <td>v${row.schema_version}</td>
          <td class="text-col">${String(row.reason).slice(0, 68)}</td>
        </tr>`
      )
      .join("");

    // Traces
    $("trace-strip").innerHTML = (data.recent_traces || [])
      .slice(-6)
      .map((tr) => `<span class="trace-chip">${tr.name} (${fmt(tr.duration_ms, 2)}ms)</span>`)
      .join("");

    // Populate user select & demo key once
    const select = $("user-select");
    if (select.options.length === 0 && Array.isArray(data.sample_users)) {
      const choices = [...data.sample_users];
      if (!choices.includes("user_00235")) choices.push("user_00235");
      select.innerHTML = choices
        .map((u) => `<option value="${u}">${u}${u === "user_00235" ? " (cold-start)" : ""}</option>`)
        .join("");
    }
    if (!$("api-key-input").value && data.demo_serving_api_key) {
      $("api-key-input").value = data.demo_serving_api_key;
    }

    await runRecommendationSim();
  }

  $("refresh-btn").addEventListener("click", loadOverview);
  $("run-rec-btn").addEventListener("click", runRecommendationSim);
  loadOverview();
})();
