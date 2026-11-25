"""Prometheus metrics and OpenTelemetry tracing instrumentation."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest

from recsys_platform.logging_utils import TRACE_ID_VAR


class TelemetryHub:
    """Holds isolated Prometheus metrics and OpenTelemetry span capture."""

    def __init__(self) -> None:
        self.registry = CollectorRegistry(auto_describe=True)
        self.events_ingested = Counter(
            "recsys_events_ingested_total",
            "Total events ingested into the platform",
            ["stream", "status"],
            registry=self.registry,
        )
        self.dlq_events = Counter(
            "recsys_dlq_events_total",
            "Total events routed to quarantine DLQ",
            ["reason"],
            registry=self.registry,
        )
        self.session_updates = Counter(
            "recsys_session_updates_total",
            "Session state emissions (new, update, late_retraction, ttl_eviction)",
            ["kind"],
            registry=self.registry,
        )
        self.feature_serving_latency_ms = Histogram(
            "recsys_online_feature_latency_ms",
            "Online feature lookup latency in milliseconds",
            buckets=(0.2, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0),
            registry=self.registry,
        )
        self.recommendation_latency_ms = Histogram(
            "recsys_recommendation_latency_ms",
            "End-to-end recommendation API latency in milliseconds",
            buckets=(0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0),
            registry=self.registry,
        )
        self.feature_freshness_lag_seconds = Gauge(
            "recsys_feature_freshness_lag_seconds",
            "Lag between latest interaction event_ts and online feature store materialization",
            registry=self.registry,
        )
        self.online_offline_parity_mismatch_ratio = Gauge(
            "recsys_online_offline_parity_mismatch_ratio",
            "Fraction of sampled users with online/offline feature skew above tolerance",
            registry=self.registry,
        )
        self.experiment_srm_pvalue = Gauge(
            "recsys_experiment_srm_pvalue",
            "Chi-squared goodness-of-fit p-value for experiment traffic allocation",
            ["experiment_id"],
            registry=self.registry,
        )
        self.experiment_cuped_variance_reduction_pct = Gauge(
            "recsys_experiment_cuped_variance_reduction_pct",
            "Percentage variance reduction achieved by CUPED covariate adjustment",
            ["experiment_id"],
            registry=self.registry,
        )
        self.pit_leakage_violations = Gauge(
            "recsys_pit_leakage_violations",
            "Number of future feature leakage violations detected in PIT training dataset",
            registry=self.registry,
        )
        self.api_requests = Counter(
            "recsys_api_requests_total",
            "API requests served grouped by route and HTTP status",
            ["route", "status_code"],
            registry=self.registry,
        )

        # OpenTelemetry in-memory exporter for deterministic local trace verification
        self.span_exporter = InMemorySpanExporter()
        resource = Resource.create({"service.name": "recsys-feature-experiment-platform"})
        self.tracer_provider = TracerProvider(resource=resource)
        self.tracer_provider.add_span_processor(SimpleSpanProcessor(self.span_exporter))
        self.tracer = self.tracer_provider.get_tracer("recsys_platform")

    @contextmanager
    def span(self, name: str, attributes: dict[str, Any] | None = None) -> Iterator[trace.Span]:
        with self.tracer.start_as_current_span(name) as active_span:
            if attributes:
                for key, val in attributes.items():
                    if val is not None:
                        active_span.set_attribute(key, val)
            ctx = active_span.get_span_context()
            trace_hex = f"{ctx.trace_id:032x}" if ctx and ctx.trace_id else ""
            token = TRACE_ID_VAR.set(trace_hex)
            try:
                yield active_span
            finally:
                TRACE_ID_VAR.reset(token)

    def render_prometheus(self) -> bytes:
        return generate_latest(self.registry)

    def recent_traces(self, limit: int = 20) -> list[dict[str, Any]]:
        spans = self.span_exporter.get_finished_spans()[-limit:]
        result: list[dict[str, Any]] = []
        for s in spans:
            duration_ms = 0.0
            if s.start_time and s.end_time:
                duration_ms = round((s.end_time - s.start_time) / 1_000_000.0, 3)
            result.append(
                {
                    "name": s.name,
                    "trace_id": f"{s.context.trace_id:032x}",
                    "span_id": f"{s.context.span_id:016x}",
                    "duration_ms": duration_ms,
                    "attributes": dict(s.attributes or {}),
                }
            )
        return result


TELEMETRY = TelemetryHub()
