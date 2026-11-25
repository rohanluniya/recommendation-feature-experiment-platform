"""Deterministic event-time sessionization engine with watermarks, allowed lateness, retractions, and state TTL."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from recsys_platform.observability import TELEMETRY
from recsys_platform.simulator import deterministic_id


@dataclass(slots=True)
class SessionState:
    """Mutable per-session accumulator maintained inside bounded user state."""

    session_id: str
    user_id: str
    start_ts_ms: int
    end_ts_ms: int
    event_count: int = 0
    heartbeat_count: int = 0
    total_watch_seconds: float = 0.0
    rebuffer_count: int = 0
    completions: int = 0
    skips: int = 0
    searches: int = 0
    thumbs_up: int = 0
    thumbs_down: int = 0
    device_switch_count: int = 0
    devices_used: set[str] = field(default_factory=set)
    items_interacted: set[str] = field(default_factory=set)
    seen_event_ids: set[str] = field(default_factory=set)
    revision: int = 1
    is_closed: bool = False
    had_late_retraction: bool = False
    last_device_type: str = ""
    experiment_variant: str = "control"

    def to_record(self, changelog_op: str, emitted_watermark_ms: int) -> dict[str, Any]:
        duration_sec = max(0.0, (self.end_ts_ms - self.start_ts_ms) / 1000.0)
        return {
            "session_id": self.session_id,
            "user_id": self.user_id,
            "session_start_ts_ms": self.start_ts_ms,
            "session_end_ts_ms": self.end_ts_ms,
            "session_duration_seconds": round(duration_sec, 2),
            "event_count": self.event_count,
            "heartbeat_count": self.heartbeat_count,
            "total_watch_seconds": round(self.total_watch_seconds, 2),
            "rebuffer_count": self.rebuffer_count,
            "completions": self.completions,
            "skips": self.skips,
            "searches": self.searches,
            "thumbs_up": self.thumbs_up,
            "thumbs_down": self.thumbs_down,
            "device_switch_count": self.device_switch_count,
            "distinct_devices": len(self.devices_used),
            "distinct_items": len(self.items_interacted),
            "revision": self.revision,
            "changelog_op": changelog_op,
            "had_late_retraction": self.had_late_retraction,
            "emitted_watermark_ms": emitted_watermark_ms,
            "experiment_variant": self.experiment_variant,
        }


@dataclass(slots=True)
class SessionizationOutput:
    """Outputs and diagnostics produced by the event-time sessionizer."""

    final_sessions: list[dict[str, Any]]
    changelog_stream: list[dict[str, Any]]
    late_retractions: int
    late_dropped_events: int
    duplicates_suppressed: int
    ttl_evicted_users: int
    peak_active_users_in_state: int
    final_watermark_ms: int


class EventTimeSessionizer:
    """Flink-semantics event-time inactivity-gap sessionizer with bounded TTL state.

    Guarantees:
    - Event-time ordering via advancing watermark (`max_event_ts - out_of_order_bound_ms`).
    - Inactivity-gap session windowing (`gap_ms`, default 30 min) with cross-device continuity.
    - Deduplication of replayed heartbeat `event_id`s inside stateful accumulator.
    - Retraction (`-U`) + update (`+U`) emission when late events arrive within `allowed_lateness_ms`.
    - Side-output drop for events older than `watermark - allowed_lateness_ms`.
    - Bounded user-level state eviction when `watermark - user_last_ts > state_ttl_ms`.
    """

    def __init__(
        self,
        *,
        inactivity_gap_ms: int = 1_800_000,
        out_of_order_bound_ms: int = 30_000,
        allowed_lateness_ms: int = 300_000,
        state_ttl_ms: int = 86_400_000,
    ) -> None:
        self.inactivity_gap_ms = inactivity_gap_ms
        self.out_of_order_bound_ms = out_of_order_bound_ms
        self.allowed_lateness_ms = allowed_lateness_ms
        self.state_ttl_ms = state_ttl_ms

        self.max_event_ts_ms: int = 0
        self.watermark_ms: int = 0
        self._last_sweep_watermark_ms: int = 0
        self._timer_coalesce_ms: int = 10_000
        self._user_sessions: dict[str, list[SessionState]] = {}
        self._user_last_ts: dict[str, int] = {}
        self._closed_sessions_archive: dict[str, dict[str, Any]] = {}
        self._changelog: list[dict[str, Any]] = []

        self.late_retractions = 0
        self.late_dropped_events = 0
        self.duplicates_suppressed = 0
        self.ttl_evicted_users = 0
        self.peak_active_users = 0

    def _apply_event_to_session(self, sess: SessionState, ev: dict[str, Any]) -> bool:
        ev_id = str(ev["event_id"])
        if ev_id in sess.seen_event_ids:
            self.duplicates_suppressed += 1
            return False
        sess.seen_event_ids.add(ev_id)
        ev_ts = int(ev["event_ts_ms"])
        sess.start_ts_ms = min(sess.start_ts_ms, ev_ts)
        sess.end_ts_ms = max(sess.end_ts_ms, ev_ts)
        sess.event_count += 1
        sess.items_interacted.add(str(ev["item_id"]))

        dev_type = str(ev.get("device_type", "unknown"))
        if sess.last_device_type and dev_type != sess.last_device_type:
            sess.device_switch_count += 1
        sess.last_device_type = dev_type
        sess.devices_used.add(dev_type)

        if ev.get("experiment_variant"):
            sess.experiment_variant = str(ev["experiment_variant"])

        ev_type = str(ev["event_type"])
        if ev_type == "HEARTBEAT":
            sess.heartbeat_count += 1
            sess.total_watch_seconds += float(ev.get("watch_seconds", 0.0))
            sess.rebuffer_count += int(ev.get("rebuffer_count", 0))
        elif ev_type == "COMPLETE":
            sess.completions += 1
            sess.total_watch_seconds += float(ev.get("watch_seconds", 0.0))
        elif ev_type == "SKIP":
            sess.skips += 1
        elif ev_type == "SEARCH":
            sess.searches += 1
        elif ev_type == "THUMBS_UP":
            sess.thumbs_up += 1
        elif ev_type == "THUMBS_DOWN":
            sess.thumbs_down += 1
        elif ev_type == "DEVICE_SWITCH":
            sess.device_switch_count += 1
        return True

    def _maybe_close_and_evict(self) -> None:
        """Close sessions past inactivity gap and evict user state past TTL."""
        to_evict: list[str] = []
        for user_id, sessions in self._user_sessions.items():
            for sess in sessions:
                if not sess.is_closed and (self.watermark_ms - sess.end_ts_ms) > self.inactivity_gap_ms:
                    sess.is_closed = True
                    rec = sess.to_record("+I", self.watermark_ms)
                    self._changelog.append(rec)
                    self._closed_sessions_archive[sess.session_id] = rec
                    TELEMETRY.session_updates.labels(kind="window_close").inc()

            # Evict entire user state once watermark passes state TTL beyond last activity
            last_ts = self._user_last_ts.get(user_id, 0)
            if last_ts > 0 and (self.watermark_ms - last_ts) > self.state_ttl_ms:
                for sess in sessions:
                    if not sess.is_closed:
                        sess.is_closed = True
                        rec = sess.to_record("+I", self.watermark_ms)
                        self._changelog.append(rec)
                        self._closed_sessions_archive[sess.session_id] = rec
                to_evict.append(user_id)

        for user_id in to_evict:
            del self._user_sessions[user_id]
            self._user_last_ts.pop(user_id, None)
            self.ttl_evicted_users += 1
            TELEMETRY.session_updates.labels(kind="ttl_eviction").inc()

    def process_event(self, ev: dict[str, Any]) -> None:
        ev_ts = int(ev["event_ts_ms"])
        user_id = str(ev["user_id"])

        # Advance event-time watermark monotonically
        if ev_ts > self.max_event_ts_ms:
            self.max_event_ts_ms = ev_ts
            candidate_wm = max(0, self.max_event_ts_ms - self.out_of_order_bound_ms)
            if candidate_wm > self.watermark_ms:
                self.watermark_ms = candidate_wm

        # Find an existing session whose [start - gap, end + gap] covers ev_ts
        sessions = self._user_sessions.setdefault(user_id, [])
        matched: SessionState | None = None
        for sess in sessions:
            if (sess.start_ts_ms - self.inactivity_gap_ms) <= ev_ts <= (sess.end_ts_ms + self.inactivity_gap_ms):
                matched = sess
                break

        # Textbook Flink allowed lateness: window closes at (end + gap), cleanup at (end + gap + allowed_lateness)
        if matched is not None and matched.is_closed:
            cleanup_horizon = matched.end_ts_ms + self.inactivity_gap_ms + self.allowed_lateness_ms
            if self.watermark_ms > cleanup_horizon:
                self.late_dropped_events += 1
                TELEMETRY.session_updates.labels(kind="late_dropped").inc()
                return
        elif matched is None and self.watermark_ms > 0:
            # No existing session matched; drop if older than global lateness horizon
            global_cutoff = self.watermark_ms - self.inactivity_gap_ms - self.allowed_lateness_ms
            if ev_ts < global_cutoff:
                self.late_dropped_events += 1
                TELEMETRY.session_updates.labels(kind="late_dropped").inc()
                return

        self._user_last_ts[user_id] = max(self._user_last_ts.get(user_id, 0), ev_ts)
        self.peak_active_users = max(self.peak_active_users, len(self._user_sessions))

        if matched is None:
            sid = deterministic_id("sess", user_id, ev_ts // self.inactivity_gap_ms, len(sessions))
            matched = SessionState(session_id=sid, user_id=user_id, start_ts_ms=ev_ts, end_ts_ms=ev_ts)
            sessions.append(matched)
            self._apply_event_to_session(matched, ev)
        else:
            was_closed = matched.is_closed
            prior_snapshot = matched.to_record("-U", self.watermark_ms) if was_closed else None
            applied = self._apply_event_to_session(matched, ev)
            if applied and was_closed:
                # Late event within allowed lateness: emit retraction (-U) + updated session (+U)
                matched.revision += 1
                matched.had_late_retraction = True
                if prior_snapshot is not None:
                    self._changelog.append(prior_snapshot)
                updated_rec = matched.to_record("+U", self.watermark_ms)
                self._changelog.append(updated_rec)
                self._closed_sessions_archive[matched.session_id] = updated_rec
                self.late_retractions += 1
                TELEMETRY.session_updates.labels(kind="late_retraction").inc()

        # Merge adjacent sessions if a late/out-of-order bridge event joined two windows
        if len(sessions) > 1:
            sessions.sort(key=lambda s: s.start_ts_ms)
            merged: list[SessionState] = [sessions[0]]
            for nxt in sessions[1:]:
                prev = merged[-1]
                if nxt.start_ts_ms <= prev.end_ts_ms + self.inactivity_gap_ms:
                    prev.end_ts_ms = max(prev.end_ts_ms, nxt.end_ts_ms)
                    prev.event_count += nxt.event_count
                    prev.heartbeat_count += nxt.heartbeat_count
                    prev.total_watch_seconds += nxt.total_watch_seconds
                    prev.rebuffer_count += nxt.rebuffer_count
                    prev.completions += nxt.completions
                    prev.skips += nxt.skips
                    prev.searches += nxt.searches
                    prev.thumbs_up += nxt.thumbs_up
                    prev.thumbs_down += nxt.thumbs_down
                    prev.device_switch_count += nxt.device_switch_count
                    prev.devices_used |= nxt.devices_used
                    prev.items_interacted |= nxt.items_interacted
                    prev.seen_event_ids |= nxt.seen_event_ids
                    prev.revision += 1
                else:
                    merged.append(nxt)
            self._user_sessions[user_id] = merged

        if (self.watermark_ms - self._last_sweep_watermark_ms) >= self._timer_coalesce_ms:
            self._last_sweep_watermark_ms = self.watermark_ms
            self._maybe_close_and_evict()

    def finalize(self) -> SessionizationOutput:
        """Flush remaining open sessions at end-of-stream and return final deduplicated table + changelog."""
        for sessions in self._user_sessions.values():
            for sess in sessions:
                if not sess.is_closed:
                    sess.is_closed = True
                    rec = sess.to_record("+I", self.watermark_ms)
                    self._changelog.append(rec)
                    self._closed_sessions_archive[sess.session_id] = rec
                    TELEMETRY.session_updates.labels(kind="flush_close").inc()

        final_sessions = sorted(
            self._closed_sessions_archive.values(),
            key=lambda r: (r["session_start_ts_ms"], r["user_id"], r["session_id"]),
        )
        return SessionizationOutput(
            final_sessions=final_sessions,
            changelog_stream=list(self._changelog),
            late_retractions=self.late_retractions,
            late_dropped_events=self.late_dropped_events,
            duplicates_suppressed=self.duplicates_suppressed,
            ttl_evicted_users=self.ttl_evicted_users,
            peak_active_users_in_state=self.peak_active_users,
            final_watermark_ms=self.watermark_ms,
        )


def run_event_time_sessionization(
    events: list[dict[str, Any]],
    *,
    inactivity_gap_ms: int = 1_800_000,
    out_of_order_bound_ms: int = 30_000,
    allowed_lateness_ms: int = 300_000,
    state_ttl_ms: int = 86_400_000,
) -> SessionizationOutput:
    """Process a stream of events in arrival order and produce event-time sessions & retractions."""
    with TELEMETRY.span("streaming.sessionize", {"event_count": len(events)}):
        engine = EventTimeSessionizer(
            inactivity_gap_ms=inactivity_gap_ms,
            out_of_order_bound_ms=out_of_order_bound_ms,
            allowed_lateness_ms=allowed_lateness_ms,
            state_ttl_ms=state_ttl_ms,
        )
        for ev in events:
            engine.process_event(ev)
        return engine.finalize()
