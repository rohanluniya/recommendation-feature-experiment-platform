#!/usr/bin/env python3
"""Generate a CycloneDX 1.5 JSON Software Bill of Materials (SBOM) from the active Python environment."""

from __future__ import annotations

import importlib.metadata as metadata
import json
import uuid
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
OUT_PATH = REPO_ROOT / "results" / "compliance" / "sbom.cdx.json"


def main() -> None:
    components = []
    for dist in sorted(metadata.distributions(), key=lambda d: (d.metadata["Name"] or "").lower()):
        name = dist.metadata["Name"] or "unknown"
        version = dist.version or "0.0.0"
        purl = f"pkg:pypi/{name.lower().replace('_', '-')}@{version}"
        components.append(
            {
                "type": "library",
                "name": name,
                "version": version,
                "purl": purl,
            }
        )

    sbom = {
        "bomFormat": "CycloneDX",
        "specVersion": "1.5",
        "serialNumber": f"urn:uuid:{uuid.uuid5(uuid.NAMESPACE_DNS, 'recommendation-feature-experiment-platform')}",
        "version": 1,
        "metadata": {
            "timestamp": datetime.now(UTC).isoformat(),
            "component": {
                "type": "application",
                "name": "recommendation-feature-experiment-platform",
                "version": "0.1.0",
                "licenses": [{"license": {"id": "Apache-2.0"}}],
            },
        },
        "components": components,
    }
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(sbom, indent=2), encoding="utf-8")
    print(
        json.dumps(
            {
                "bomFormat": sbom["bomFormat"],
                "specVersion": sbom["specVersion"],
                "component_count": len(components),
                "output": str(OUT_PATH.relative_to(REPO_ROOT)),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
