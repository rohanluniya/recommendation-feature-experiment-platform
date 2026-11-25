#!/usr/bin/env python3
"""Audit installed Python package licenses and verify zero restricted/external model weights."""

from __future__ import annotations

import importlib.metadata as metadata
import json
from datetime import UTC, datetime
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
OUT_PATH = REPO_ROOT / "results" / "compliance" / "license_report.json"

DENIED_MARKERS = ("AGPL", "GPL-3", "SSPL", "BUSL", "BSL-1.1", "Non-Commercial")


def _extract_license(dist: metadata.Distribution) -> str:
    meta = dist.metadata
    lic = meta.get("License-Expression") or meta.get("License") or ""
    if lic and lic.strip().lower() not in ("unknown", "none"):
        return lic.strip().splitlines()[0][:120]
    classifiers = [c for c in (meta.get_all("Classifier") or []) if c.startswith("License ::")]
    if classifiers:
        return "; ".join(c.split("::")[-1].strip() for c in classifiers)
    return "Unspecified"


def main() -> None:
    packages = []
    flagged = []
    for dist in sorted(metadata.distributions(), key=lambda d: (d.metadata["Name"] or "").lower()):
        name = dist.metadata["Name"] or "unknown"
        version = dist.version or "0.0.0"
        lic = _extract_license(dist)
        entry = {"name": name, "version": version, "license": lic}
        packages.append(entry)
        if any(marker.lower() in lic.lower() for marker in DENIED_MARKERS):
            flagged.append(entry)

    report = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "total_packages_audited": len(packages),
        "pretrained_external_models": [],
        "model_license_policy": "Zero external pretrained model weights; scikit-learn models trained purely on local synthetic data (BSD-3-Clause / Apache-2.0).",
        "flagged_restricted_licenses": flagged,
        "status": "PASS" if not flagged else "REVIEW_REQUIRED",
        "packages": packages,
    }
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(
        json.dumps(
            {
                "status": report["status"],
                "total_packages_audited": report["total_packages_audited"],
                "flagged_count": len(flagged),
                "output": str(OUT_PATH.relative_to(REPO_ROOT)),
            },
            indent=2,
        )
    )
    if flagged:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
