"""Structured JSON logging with trace/correlation/run IDs and PII/secret redaction."""

from __future__ import annotations

import json
import logging
import re
import time
import uuid
from contextvars import ContextVar
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

CORRELATION_ID_VAR: ContextVar[str] = ContextVar("correlation_id", default="")
RUN_ID_VAR: ContextVar[str] = ContextVar("run_id", default="")
TRACE_ID_VAR: ContextVar[str] = ContextVar("trace_id", default="")

_SENSITIVE_KEY_RE = re.compile(
    r"(api[_-]?key|token|secret|password|authorization|email|ip_address|ssn|credit_card)",
    re.IGNORECASE,
)
_EMAIL_RE = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")
_BEARER_RE = re.compile(r"(Bearer\s+)[A-Za-z0-9._\-]+", re.IGNORECASE)


def redact_value(key: str, value: Any) -> Any:
    """Redact sensitive keys or inline PII/secret patterns."""
    if _SENSITIVE_KEY_RE.search(key):
        return "[REDACTED]"
    if isinstance(value, str):
        sanitized = _EMAIL_RE.sub("[REDACTED_EMAIL]", value)
        sanitized = _BEARER_RE.sub(r"\1[REDACTED_TOKEN]", sanitized)
        return sanitized
    if isinstance(value, dict):
        return {str(k): redact_value(str(k), v) for k, v in value.items()}
    if isinstance(value, list):
        return [redact_value(key, item) for item in value]
    return value


class JsonFormatter(logging.Formatter):
    """Formats log records as single-line structured JSON with tracing context."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": redact_value("message", record.getMessage()),
            "correlation_id": CORRELATION_ID_VAR.get() or None,
            "run_id": RUN_ID_VAR.get() or None,
            "trace_id": TRACE_ID_VAR.get() or None,
        }
        extra_fields = getattr(record, "extra_fields", None)
        if isinstance(extra_fields, dict):
            for k, v in extra_fields.items():
                payload[k] = redact_value(str(k), v)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def get_logger(name: str = "recsys_platform") -> logging.Logger:
    """Return a logger configured with structured JSON output."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(JsonFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


def log_event(logger: logging.Logger, level: int, message: str, **fields: Any) -> None:
    """Emit a structured JSON log record with redacted extra fields."""
    logger.log(level, message, extra={"extra_fields": fields})


def new_correlation_id() -> str:
    return f"corr-{uuid.uuid4().hex[:12]}"


def append_audit_event(
    audit_path: Path,
    *,
    actor_role: str,
    action: str,
    resource: str,
    status: str,
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Append an immutable redacted security/governance audit record to JSONL."""
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    entry: dict[str, Any] = {
        "audit_id": f"aud-{uuid.uuid4().hex[:12]}",
        "ts_ms": int(time.time() * 1000),
        "ts_iso": datetime.now(UTC).isoformat(),
        "actor_role": actor_role,
        "action": action,
        "resource": resource,
        "status": status,
        "correlation_id": CORRELATION_ID_VAR.get() or None,
        "trace_id": TRACE_ID_VAR.get() or None,
        "details": redact_value("details", details or {}),
    }
    with audit_path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry, default=str) + "\n")
    return entry
