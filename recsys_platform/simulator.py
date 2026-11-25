"""Deterministic seeded simulator for Netflix-style streaming interactions, catalog CDC, and A/B experiments."""

from __future__ import annotations

import hashlib
import random
from dataclasses import dataclass, field
from typing import Any

GENRES = ("sci_fi", "thriller", "drama", "comedy", "documentary", "action", "animation", "mystery")
DEVICES = ("smart_tv", "mobile_ios", "mobile_android", "web_chrome", "tablet_ipad")
MATURITY_RATINGS = ("TV-G", "TV-PG", "TV-14", "TV-MA")
SEARCH_TERMS = (
    "space",
    "heist",
    "detective",
    "standup",
    "nature",
    "cyberpunk",
    "ocean",
    "time travel",
)


def deterministic_id(prefix: str, *parts: Any) -> str:
    """Create a stable, deterministic identifier from seedable components."""
    raw = "|".join(str(p) for p in parts)
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]
    return f"{prefix}_{digest}"


def assign_bucket_deterministic(
    user_id: str,
    experiment_id: str,
    salt: str,
    treatment_ratio: float = 0.50,
) -> tuple[str, int]:
    """Deterministic SHA-256 user bucketing into control/treatment with 10,000-slot hash space."""
    key = f"{experiment_id}:{salt}:{user_id}".encode()
    digest = hashlib.sha256(key).digest()
    bucket = int.from_bytes(digest[:8], byteorder="big") % 10_000
    threshold = int(round(treatment_ratio * 10_000))
    variant = "treatment" if bucket < threshold else "control"
    return variant, bucket


@dataclass(slots=True)
class SimulationConfig:
    """Parameters governing synthetic data generation and adversarial scenario rates."""

    seed: int = 42
    num_users: int = 240
    num_items: int = 60
    num_cold_start_users: int = 12
    num_cold_start_items: int = 6
    interactions_per_user: int = 18
    base_ts_ms: int = 1_764_000_000_000  # 2025-11-24T16:00:00Z deterministic anchor
    duplicate_heartbeat_rate: float = 0.08
    late_within_allowed_rate: float = 0.04
    late_beyond_allowed_rate: float = 0.015
    corrupt_event_rate: float = 0.02
    v2_schema_rate: float = 0.35
    delayed_label_rate: float = 0.25
    device_switch_rate: float = 0.12
    experiment_id: str = "exp_recsys_ranker_v2"
    experiment_salt: str = "recsys-platform-salt-2026"
    treatment_ratio: float = 0.50  # Set != 0.50 (e.g. 0.56) to inject SRM fault
    treatment_lift_completion: float = 0.065
    treatment_lift_watch_minutes: float = 3.2


@dataclass(slots=True)
class SimulatedBundle:
    """Complete deterministic simulation output."""

    config: SimulationConfig
    users: list[dict[str, Any]]
    catalog_changes: list[dict[str, Any]]
    interaction_events: list[dict[str, Any]]
    experiment_exposures: list[dict[str, Any]]
    delayed_labels: list[dict[str, Any]]
    summary: dict[str, Any] = field(default_factory=dict)


def generate_simulation(cfg: SimulationConfig | None = None) -> SimulatedBundle:
    """Generate deterministic viewing interactions, catalog CDC, exposures, and delayed labels."""
    cfg = cfg or SimulationConfig()
    rng = random.Random(cfg.seed)

    # 1. Generate content catalog + CDC creation/update events (including cold-start items)
    items: list[dict[str, Any]] = []
    catalog_changes: list[dict[str, Any]] = []
    for idx in range(cfg.num_items):
        item_id = f"item_{idx:04d}"
        genre = GENRES[idx % len(GENRES)]
        duration_sec = rng.choice([1500, 2400, 3100, 5400, 6600, 7200])
        release_year = rng.randint(2016, 2026)
        maturity = rng.choice(MATURITY_RATINGS)
        # Power-law popularity skew
        popularity_prior = round(min(0.98, max(0.05, rng.paretovariate(2.2) * 0.25)), 4)
        is_cold_start = idx >= (cfg.num_items - cfg.num_cold_start_items)
        if is_cold_start:
            popularity_prior = round(rng.uniform(0.08, 0.22), 4)
        create_ts = cfg.base_ts_ms - 86_400_000 * (1 if is_cold_start else 30) + idx * 1000
        item: dict[str, Any] = {
            "item_id": item_id,
            "title": f"{genre.replace('_', ' ').title()} Chronicles #{idx + 1}",
            "genre": genre,
            "duration_seconds": duration_sec,
            "release_year": release_year,
            "maturity_rating": maturity,
            "popularity_prior": popularity_prior,
            "is_active": True,
            "is_cold_start": is_cold_start,
            "created_ts_ms": create_ts,
        }
        items.append(item)
        catalog_changes.append(
            {
                "change_id": deterministic_id("cdc", cfg.seed, item_id, "c", 0),
                "op": "c",
                "item_id": item_id,
                "title": item["title"],
                "genre": genre,
                "duration_seconds": duration_sec,
                "release_year": release_year,
                "maturity_rating": maturity,
                "popularity_prior": popularity_prior,
                "is_active": True,
                "event_ts_ms": create_ts,
                "ingest_ts_ms": create_ts + 250,
                "source_system": "postgres-catalog-cdc",
            }
        )

    # Inject realistic catalog edits mid-stream (popularity recalibration, metadata edits, 1 soft deactivation)
    for edit_idx in range(min(12, cfg.num_items)):
        target = items[edit_idx * 3 % cfg.num_items]
        edit_ts = cfg.base_ts_ms + 3_600_000 * (1 + (edit_idx % 4)) + edit_idx * 17_000
        new_pop = round(min(0.99, target["popularity_prior"] * 1.15 + 0.03), 4)
        is_active = edit_idx != 11
        catalog_changes.append(
            {
                "change_id": deterministic_id("cdc", cfg.seed, target["item_id"], "u", edit_idx),
                "op": "u",
                "item_id": target["item_id"],
                "title": f"{target['title']} (Remastered)" if edit_idx % 2 == 0 else target["title"],
                "genre": target["genre"],
                "duration_seconds": target["duration_seconds"],
                "release_year": target["release_year"],
                "maturity_rating": target["maturity_rating"],
                "popularity_prior": new_pop,
                "is_active": is_active,
                "event_ts_ms": edit_ts,
                "ingest_ts_ms": edit_ts + 400,
                "source_system": "postgres-catalog-cdc",
            }
        )

    warm_items = [it for it in items if not it["is_cold_start"]]
    # Popularity weights for Zipf-like item selection
    warm_weights = [it["popularity_prior"] ** 1.4 for it in warm_items]

    # 2. Generate users with latent preferences and strong pre-experiment covariate X_i (for CUPED)
    users: list[dict[str, Any]] = []
    for u_idx in range(cfg.num_users):
        user_id = f"user_{u_idx:05d}"
        is_cold = u_idx >= (cfg.num_users - cfg.num_cold_start_users)
        fav_genre = GENRES[rng.randrange(len(GENRES))]
        secondary_genre = GENRES[(GENRES.index(fav_genre) + 1 + rng.randrange(3)) % len(GENRES)]
        # Pre-experiment baseline engagement (minutes watched in prior 7 days)
        engagement_latent = rng.gammavariate(3.5, 8.0) if not is_cold else rng.uniform(0.5, 4.0)
        pre_exp_watch_min = round(max(0.0, engagement_latent + rng.gauss(0.0, 2.5)), 3)
        primary_device = rng.choice(DEVICES)
        variant, bucket_hash = assign_bucket_deterministic(
            user_id=user_id,
            experiment_id=cfg.experiment_id,
            salt=cfg.experiment_salt,
            treatment_ratio=cfg.treatment_ratio,
        )
        users.append(
            {
                "user_id": user_id,
                "fav_genre": fav_genre,
                "secondary_genre": secondary_genre,
                "engagement_latent": engagement_latent,
                "pre_experiment_watch_minutes": pre_exp_watch_min,
                "primary_device": primary_device,
                "is_cold_start": is_cold,
                "variant": variant,
                "bucket_hash": bucket_hash,
            }
        )

    # 3. Generate multi-session interaction timeline, exposures, delayed labels, and adversarial events
    interactions: list[dict[str, Any]] = []
    exposures: list[dict[str, Any]] = []
    delayed_labels: list[dict[str, Any]] = []

    duplicate_count = 0
    late_within_count = 0
    late_beyond_count = 0
    corrupt_count = 0
    device_switch_count = 0
    v2_count = 0

    for u_idx, user in enumerate(users):
        user_id = user["user_id"]
        variant = user["variant"]
        # Distribute users across a 6-hour streaming window with multiple sessions per user
        cursor_ts = cfg.base_ts_ms + rng.randint(0, 1_800_000)
        device_type = user["primary_device"]
        device_id = deterministic_id("dev", user_id, device_type)

        n_steps = 3 if user["is_cold_start"] else cfg.interactions_per_user
        # Heavy power-user skew for first 5% of users
        if not user["is_cold_start"] and u_idx < max(2, cfg.num_users // 20):
            n_steps = int(n_steps * 1.8)

        for step in range(n_steps):
            # Start a new session every ~5 interactions (gap > 30 min = 1,800,000 ms)
            if step > 0 and step % 5 == 0:
                cursor_ts += rng.randint(2_100_000, 4_200_000)
            else:
                cursor_ts += rng.randint(25_000, 180_000)

            # Pick candidate item: treatment ranker aligns slightly better with user's favorite genre
            genre_match_prob = 0.62 if variant == "treatment" else 0.44
            if rng.random() < genre_match_prob:
                matching = [it for it in warm_items if it["genre"] in (user["fav_genre"], user["secondary_genre"])]
                item = rng.choice(matching) if matching else rng.choices(warm_items, weights=warm_weights, k=1)[0]
            else:
                item = rng.choices(warm_items, weights=warm_weights, k=1)[0]

            item_id = item["item_id"]
            is_fav_match = item["genre"] == user["fav_genre"]

            def _make_event(
                ev_type: str,
                ev_ts: int,
                seq_tag: str,
                *,
                watch_sec: float = 0.0,
                pos_sec: float = 0.0,
                rebuf: int = 0,
                query: str | None = None,
                dev_id: str = device_id,
                dev_type: str = device_type,
                ingest_delay_ms: int | None = None,
                uid: str = user_id,
                iid: str = item_id,
                st: int = step,
                var: str = variant,
            ) -> dict[str, Any]:
                nonlocal v2_count
                delay = ingest_delay_ms if ingest_delay_ms is not None else rng.randint(80, 1200)
                use_v2 = rng.random() < cfg.v2_schema_rate
                ev: dict[str, Any] = {
                    "event_id": deterministic_id("ev", cfg.seed, uid, iid, ev_type, st, seq_tag),
                    "event_type": ev_type,
                    "user_id": uid,
                    "item_id": iid,
                    "device_id": dev_id,
                    "device_type": dev_type,
                    "event_ts_ms": ev_ts,
                    "ingest_ts_ms": ev_ts + delay,
                    "watch_seconds": round(watch_sec, 2),
                    "position_seconds": round(pos_sec, 2),
                    "rebuffer_count": rebuf,
                    "search_query": query,
                    "experiment_id": cfg.experiment_id,
                    "experiment_variant": var,
                    "source_system": "playback-gateway",
                    "schema_version": 2 if use_v2 else 1,
                }
                if use_v2:
                    v2_count += 1
                    ev["device_os_version"] = rng.choice(["tvOS-19.1", "iOS-19.2", "Android-16", "Web-138"])
                    ev["playback_bitrate_kbps"] = rng.choice([2500, 4800, 8200, 15000])
                return ev

            # Occasional search before impression
            if step % 6 == 0:
                interactions.append(
                    _make_event("SEARCH", cursor_ts - 10_000, "search", query=rng.choice(SEARCH_TERMS))
                )

            # Recommendation Impression + Sticky Experiment Exposure on recommendation boundary
            imp_ev = _make_event("IMPRESSION", cursor_ts, "imp")
            interactions.append(imp_ev)

            # Log exposure once per session anchor (step == 0 or step == 5)
            if step in (0, 5):
                req_id = deterministic_id("req", cfg.seed, user_id, step)
                serving_lat = round(
                    max(1.2, rng.gauss(4.2 if variant == "treatment" else 3.8, 0.9)), 3
                )
                exposures.append(
                    {
                        "exposure_id": deterministic_id("exp", cfg.seed, cfg.experiment_id, user_id, step),
                        "experiment_id": cfg.experiment_id,
                        "variant": variant,
                        "user_id": user_id,
                        "request_id": req_id,
                        "exposure_ts_ms": cursor_ts,
                        "pre_experiment_watch_minutes": user["pre_experiment_watch_minutes"],
                        "assigned_bucket_hash": user["bucket_hash"],
                        "ranker_model_version": "ranker_gbdt_v2" if variant == "treatment" else "ranker_baseline_v1",
                        "serving_latency_ms": serving_lat,
                    }
                )

            # Determine play / skip / completion outcome correlated with latent engagement + genre match + variant
            base_play_prob = 0.42 + (0.20 if is_fav_match else 0.0) + (0.04 if variant == "treatment" else 0.0)
            base_play_prob += min(0.18, user["engagement_latent"] / 180.0)
            if rng.random() > min(0.95, base_play_prob):
                # User skipped impression
                interactions.append(_make_event("SKIP", cursor_ts + 4_000, "skip"))
                continue

            # Play start
            play_ts = cursor_ts + 5_000
            interactions.append(_make_event("PLAY_START", play_ts, "play"))

            # Optional device switch mid-session
            if rng.random() < cfg.device_switch_rate:
                new_device = rng.choice([d for d in DEVICES if d != device_type])
                device_type = new_device
                device_id = deterministic_id("dev", user_id, device_type)
                device_switch_count += 1
                interactions.append(
                    _make_event("DEVICE_SWITCH", play_ts + 12_000, "devsw", dev_id=device_id, dev_type=device_type)
                )

            # Emit heartbeats (watch time strongly correlated with pre_experiment_watch_minutes for CUPED)
            # Target watch_seconds per play driven by latent engagement + treatment lift + noise
            expected_min = (
                0.35 * user["engagement_latent"]
                + (2.2 if is_fav_match else 0.0)
                + (cfg.treatment_lift_watch_minutes / 3.0 if variant == "treatment" else 0.0)
                + rng.gauss(0.0, 2.8)
            )
            total_watch_sec = max(20.0, min(float(item["duration_seconds"]), expected_min * 60.0))
            n_hb = 2 if total_watch_sec < 300 else 3
            per_hb = total_watch_sec / n_hb
            pos = 0.0
            for hb_i in range(n_hb):
                pos += per_hb
                hb_ts = play_ts + (hb_i + 1) * 30_000
                rebuf = 1 if rng.random() < 0.04 else 0
                hb_ev = _make_event(
                    "HEARTBEAT",
                    hb_ts,
                    f"hb_{hb_i}",
                    watch_sec=per_hb,
                    pos_sec=pos,
                    rebuf=rebuf,
                )
                interactions.append(hb_ev)

                # Adversarial 1: Duplicate heartbeat storm (identical event_id replayed 2-4x)
                if rng.random() < cfg.duplicate_heartbeat_rate:
                    burst = rng.randint(2, 4)
                    for _ in range(burst):
                        dup = dict(hb_ev)
                        dup["ingest_ts_ms"] = hb_ev["ingest_ts_ms"] + rng.randint(50, 900)
                        interactions.append(dup)
                        duplicate_count += 1

            # Completion probability (driven by match + engagement + treatment lift)
            comp_prob = (
                0.24
                + (0.18 if is_fav_match else 0.0)
                + min(0.22, user["engagement_latent"] / 150.0)
                + (cfg.treatment_lift_completion if variant == "treatment" else 0.0)
            )
            completed = rng.random() < min(0.92, comp_prob)
            end_ts = play_ts + (n_hb + 1) * 30_000

            if completed:
                # Adversarial 5: Delayed completion/conversion label arriving 2-6 hours later
                if rng.random() < cfg.delayed_label_rate:
                    delay_hours = rng.uniform(2.0, 6.0)
                    label_arrival_ts = int(end_ts + delay_hours * 3_600_000)
                    delayed_labels.append(
                        {
                            "label_id": deterministic_id("lbl", cfg.seed, user_id, item_id, step),
                            "user_id": user_id,
                            "item_id": item_id,
                            "request_ts_ms": cursor_ts,
                            "label_event_ts_ms": end_ts,
                            "label_arrival_ts_ms": label_arrival_ts,
                            "completed": 1,
                            "converted_subscription": 1 if rng.random() < 0.08 else 0,
                            "delayed_watch_seconds": round(total_watch_sec, 2),
                            "experiment_id": cfg.experiment_id,
                            "experiment_variant": variant,
                        }
                    )
                else:
                    interactions.append(
                        _make_event("COMPLETE", end_ts, "comp", watch_sec=30.0, pos_sec=float(item["duration_seconds"]))
                    )
                    if rng.random() < 0.35:
                        interactions.append(_make_event("THUMBS_UP", end_ts + 3_000, "up"))
            else:
                interactions.append(_make_event("PAUSE", end_ts, "pause", pos_sec=pos))
                if rng.random() < 0.08:
                    interactions.append(_make_event("THUMBS_DOWN", end_ts + 2_000, "down"))

            # Adversarial 2a: Late event arriving just after session gap closes (within allowed lateness -> retraction)
            if rng.random() < cfg.late_within_allowed_rate:
                late_offset = rng.randint(1_890_000, 2_050_000)
                late_ev = _make_event(
                    "HEARTBEAT",
                    play_ts + 15_000,
                    "late_ok",
                    watch_sec=45.0,
                    pos_sec=pos + 45.0,
                    ingest_delay_ms=late_offset,
                )
                interactions.append(late_ev)
                late_within_count += 1

            # Adversarial 2b: Too-late event arriving beyond (gap + allowed_lateness -> side-output drop)
            if rng.random() < cfg.late_beyond_allowed_rate:
                too_late_ev = _make_event(
                    "HEARTBEAT",
                    play_ts - 2_600_000,  # >43 min behind stream clock
                    "too_late",
                    watch_sec=25.0,
                    pos_sec=25.0,
                    ingest_delay_ms=3_600_000,
                )
                interactions.append(too_late_ev)
                late_beyond_count += 1

    # Adversarial 7: Inject corrupt / schema-invalid records for DLQ quarantine
    num_corrupt = max(5, int(len(interactions) * cfg.corrupt_event_rate))
    for c_idx in range(num_corrupt):
        mode = c_idx % 4
        base_ts = cfg.base_ts_ms + 600_000 + c_idx * 5000
        if mode == 0:
            # Invalid enum event_type
            bad: dict[str, Any] = {
                "event_id": deterministic_id("bad", cfg.seed, c_idx),
                "event_type": "CORRUPT_OPCODE",
                "user_id": f"user_{c_idx:05d}",
                "item_id": "item_0001",
                "device_id": "dev_bad",
                "device_type": "smart_tv",
                "event_ts_ms": base_ts,
                "ingest_ts_ms": base_ts + 100,
                "watch_seconds": 10.0,
                "position_seconds": 10.0,
                "rebuffer_count": 0,
            }
        elif mode == 1:
            # Negative watch_seconds semantic violation
            bad = {
                "event_id": deterministic_id("bad", cfg.seed, c_idx),
                "event_type": "HEARTBEAT",
                "user_id": f"user_{c_idx:05d}",
                "item_id": "item_0002",
                "device_id": "dev_bad",
                "device_type": "web_chrome",
                "event_ts_ms": base_ts,
                "ingest_ts_ms": base_ts + 100,
                "watch_seconds": -99.5,
                "position_seconds": 0.0,
                "rebuffer_count": 0,
                "search_query": None,
                "experiment_id": cfg.experiment_id,
                "experiment_variant": "control",
                "source_system": "playback-gateway",
                "schema_version": 1,
            }
        elif mode == 2:
            # Missing required user_id field
            bad = {
                "event_id": deterministic_id("bad", cfg.seed, c_idx),
                "event_type": "PLAY_START",
                "item_id": "item_0003",
                "device_id": "dev_bad",
                "device_type": "mobile_ios",
                "event_ts_ms": base_ts,
                "ingest_ts_ms": base_ts + 100,
            }
        else:
            # Wrong type for event_ts_ms (string instead of long)
            bad = {
                "event_id": deterministic_id("bad", cfg.seed, c_idx),
                "event_type": "IMPRESSION",
                "user_id": f"user_{c_idx:05d}",
                "item_id": "item_0004",
                "device_id": "dev_bad",
                "device_type": "tablet_ipad",
                "event_ts_ms": "not-a-timestamp",
                "ingest_ts_ms": base_ts + 100,
            }
        interactions.append(bad)
        corrupt_count += 1

    # Sort arrival stream by ingest_ts_ms (processing time) so event_ts_ms arrives naturally out of order!
    interactions.sort(key=lambda r: int(r.get("ingest_ts_ms", cfg.base_ts_ms)) if isinstance(r.get("ingest_ts_ms"), int) else cfg.base_ts_ms)
    catalog_changes.sort(key=lambda r: r["event_ts_ms"])
    exposures.sort(key=lambda r: r["exposure_ts_ms"])
    delayed_labels.sort(key=lambda r: r["label_arrival_ts_ms"])

    summary = {
        "seed": cfg.seed,
        "num_users": len(users),
        "num_cold_start_users": cfg.num_cold_start_users,
        "num_items": len(items),
        "num_cold_start_items": cfg.num_cold_start_items,
        "catalog_change_events": len(catalog_changes),
        "total_raw_interactions": len(interactions),
        "experiment_exposures": len(exposures),
        "delayed_labels": len(delayed_labels),
        "injected_duplicate_heartbeats": duplicate_count,
        "injected_late_within_allowed": late_within_count,
        "injected_late_beyond_allowed": late_beyond_count,
        "injected_corrupt_events": corrupt_count,
        "injected_device_switches": device_switch_count,
        "v2_schema_events": v2_count,
        "treatment_ratio_configured": cfg.treatment_ratio,
    }
    return SimulatedBundle(
        config=cfg,
        users=users,
        catalog_changes=catalog_changes,
        interaction_events=interactions,
        experiment_exposures=exposures,
        delayed_labels=delayed_labels,
        summary=summary,
    )
