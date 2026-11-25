# Staff / Principal Data Engineering Interview Deep Dive

This guide summarizes the architectural invariants, failure modes, and statistical mechanics implemented in this repository.

## 1. Why does a naive feature join silently destroy a recommendation ranker?
When training on historical impressions, joining the latest user/item table leaks future completions and watch-time accumulated *after* `request_ts_ms` directly into the input vector. In our benchmark on `local` (5,430 training rows), a naive global join leaked future features into 100% of rows and inflated `HistGradientBoostingClassifier` ROC-AUC by **+0.1277** (`0.6913` vs. `0.5636` strict PIT). In production, online serving only sees past features (`<= request_ts_ms`), so the model collapses online. Our platform prevents this via immutable event-time feature histories, binary-search ASOF joins, Hypothesis property tests (`test_pit_properties.py`), and a CI/runtime DQ gate asserting `pit_leakage_violations == 0`.

## 2. How do you handle delayed conversions without introducing temporal leakage?
Episode completions and subscription upgrades frequently arrive hours after an impression. Re-running a naive join when those labels arrive would pull newer user/item features into old decision rows. Instead, `apply_delayed_label_backfill()` joins delayed labels strictly by `(user_id, item_id, request_ts_ms)` onto existing PIT-joined rows, mutates **only** `label_completed`, `label_source="delayed_backfill"`, and `label_arrival_ts_ms`, asserts `feature_timestamps_mutated == 0`, and writes a new Apache Iceberg V2 snapshot so analysts can time-travel between pre-backfill and post-backfill ground truth.

## 3. Explain the exact event-time sessionization & retraction lifecycle.
Events arrive ordered by `ingest_ts_ms` (processing time), not `event_ts_ms`. The sessionizer advances a monotonic watermark (`max_event_ts - 30s`). A user session window closes when `watermark - session_end > 30m`. If a late event for that session arrives while `watermark <= session_end + 30m + 5m (allowed_lateness)`, the engine emits a retraction (`-U`) for the previously published session followed by an updated revision (`+U`, `revision += 1`, `had_late_retraction = True`). Events arriving past the cleanup horizon are dropped to a side-output counter (`late_dropped_events`) to prevent unbounded state growth, and inactive user state is evicted after 24h TTL.

## 4. How is offline-vs-online feature parity guaranteed and recovered after a cache wipe?
The online store (Valkey 9.2.0 / SQLite WAL) is strictly a **derived, rebuildable projection** of Iceberg Gold (`gold.user_features_pit`, `gold.item_features_pit`). After every materialization, `validate_parity_against_lakehouse()` compares every numeric and categorical feature vector between Iceberg Gold and the online store (`tolerance = 1e-6`). If the online store is completely wiped (`CHAOS-04`), serving gracefully falls back to cold-start priors while `POST /v1/admin/rebuild-online-store` repopulates all entities deterministically from Iceberg Gold with `0` mismatches (`max_abs_numeric_diff == 0.0`).

## 5. Why use SHA-256 salted hashing, chi-squared SRM gating, and CUPED?
- **Salted SHA-256 bucketing (`SHA-256(experiment_id:salt:user_id) mod 10000`):** Stateless, deterministic across API pods, and orthogonal across concurrent experiments when salts differ.
- **SRM Chi-Squared Gate (`alpha = 0.001`):** A subtle routing or logging bug (e.g., treatment crashes dropping exposure logs, shifting 50/50 to 57/43) invalidates every downstream p-value. `CHAOS-05` proves our detector trips (`chi2=13.448, p=0.000245`) and forces `INVALID_STOP_SRM_OR_CONTAMINATION`.
- **CUPED (`Y_cuped = Y - theta * (X - mean(X))`):** Using pre-experiment 7-day watch time `X` (strictly independent of treatment assignment) with pooled `theta = Cov(Y, X) / Var(X)` is unbiased and provably satisfies `Var(Y_cuped) = (1 - rho^2) * Var(Y) <= Var(Y)` (verified via Hypothesis property testing!). In our benchmark across `N=160..800`, CUPED reduced variance by ~76% and cut 95% CI width by ~52.5%.

## 6. What interference/spillover caveat still applies to user-randomized recommendation A/B tests?
Both control and treatment viewers share item-level popularity and completion features in `gold.item_features_pit`. Our engine measures `shared_catalog_item_jaccard` and surfaces an explicit catalog feedback-loop caveat in both the API and Control Tower UI; production mitigation uses holdback clusters or variant-isolated item counters.
