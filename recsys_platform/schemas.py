"""Avro schema contract loader, validator, serializer, and evolution checker."""

from __future__ import annotations

import io
import json
from typing import Any, cast

import fastavro
from fastavro.validation import ValidationError, validate

from recsys_platform.config import REPO_ROOT

SCHEMAS_DIR = REPO_ROOT / "schemas" / "avro"


def load_avro_schema(filename: str) -> dict[str, Any]:
    """Load and parse an Avro schema from schemas/avro/."""
    path = SCHEMAS_DIR / filename
    raw = json.loads(path.read_text(encoding="utf-8"))
    return cast(dict[str, Any], fastavro.parse_schema(raw))


class SchemaRegistryLocal:
    """In-process Avro contract registry mirroring Apicurio Registry rules."""

    def __init__(self) -> None:
        self.schemas: dict[str, dict[str, Any]] = {
            "playback_interaction_v1": load_avro_schema("playback_interaction_v1.avsc"),
            "playback_interaction_v2": load_avro_schema("playback_interaction_v2.avsc"),
            "catalog_change_v1": load_avro_schema("catalog_change_v1.avsc"),
            "experiment_exposure_v1": load_avro_schema("experiment_exposure_v1.avsc"),
        }

    def validate_record(self, schema_name: str, record: dict[str, Any]) -> tuple[bool, str | None]:
        """Validate a dict against a named Avro schema."""
        schema = self.schemas[schema_name]
        try:
            validate(record, schema, raise_errors=True)
            return True, None
        except (ValidationError, KeyError, TypeError, ValueError) as exc:
            return False, str(exc)

    def serialize(self, schema_name: str, record: dict[str, Any]) -> bytes:
        """Encode a single record using Avro schemaless binary encoding."""
        buf = io.BytesIO()
        fastavro.schemaless_writer(buf, self.schemas[schema_name], record)
        return buf.getvalue()

    def deserialize(
        self,
        writer_schema_name: str,
        payload: bytes,
        reader_schema_name: str | None = None,
    ) -> dict[str, Any]:
        """Decode an Avro binary payload with optional schema evolution reader schema."""
        buf = io.BytesIO()
        buf.write(payload)
        buf.seek(0)
        writer_schema = self.schemas[writer_schema_name]
        reader_schema = self.schemas[reader_schema_name] if reader_schema_name else None
        return cast(dict[str, Any], fastavro.schemaless_reader(buf, writer_schema, reader_schema))

    def check_backward_compatibility(
        self,
        old_schema_name: str,
        new_schema_name: str,
        sample_old_record: dict[str, Any],
    ) -> tuple[bool, dict[str, Any]]:
        """Verify that a consumer using new_schema can read bytes written with old_schema."""
        payload = self.serialize(old_schema_name, sample_old_record)
        decoded = self.deserialize(old_schema_name, payload, reader_schema_name=new_schema_name)
        return True, decoded


SCHEMA_REGISTRY = SchemaRegistryLocal()
