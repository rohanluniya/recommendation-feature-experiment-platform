#!/usr/bin/env python3
"""Static repository hygiene audit: AST parse, JSON/YAML validity, pinned tag enforcement, and secret/PII checks."""

from __future__ import annotations

import ast
import json
import re
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
SKIP_DIRS = {".venv", ".git", ".local_data", ".pytest_cache", ".mypy_cache", ".ruff_cache", ".hypothesis", "__pycache__"}
UNPINNED_TAG = ":" + "latest"
SECRET_PATTERNS = (
    re.compile(r"github_" + r"pat_[A-Za-z0-9_]+"),
    re.compile(r"ghp_[A-Za-z0-9]{20,}"),
    re.compile(r"AKIA[0-9A-Z]{16}"),
)


def iter_repo_files() -> list[Path]:
    files: list[Path] = []
    for path in REPO_ROOT.rglob("*"):
        if any(part in SKIP_DIRS for part in path.parts):
            continue
        if path.is_file():
            files.append(path)
    return sorted(files)


def main() -> None:
    errors: list[str] = []
    checked = {"python": 0, "json": 0, "yaml": 0, "text_scanned": 0}

    for path in iter_repo_files():
        rel = str(path.relative_to(REPO_ROOT))
        suffix = path.suffix.lower()

        if suffix == ".py":
            try:
                ast.parse(path.read_text(encoding="utf-8"), filename=rel)
                checked["python"] += 1
            except SyntaxError as exc:
                errors.append(f"Python syntax error in {rel}: {exc}")
        elif suffix in {".json", ".avsc"}:
            try:
                json.loads(path.read_text(encoding="utf-8"))
                checked["json"] += 1
            except json.JSONDecodeError as exc:
                errors.append(f"Invalid JSON in {rel}: {exc}")
        elif suffix in {".yml", ".yaml"}:
            try:
                list(yaml.safe_load_all(path.read_text(encoding="utf-8")))
                checked["yaml"] += 1
            except yaml.YAMLError as exc:
                errors.append(f"Invalid YAML in {rel}: {exc}")

        if suffix not in {".png", ".jpg", ".jpeg", ".parquet", ".pkl", ".db", ".lock"}:
            try:
                text = path.read_text(encoding="utf-8")
            except UnicodeDecodeError:
                continue
            checked["text_scanned"] += 1
            if UNPINNED_TAG in text:
                errors.append(f"Unpinned tag forbidden in {rel}")
            for pat in SECRET_PATTERNS:
                if pat.search(text):
                    errors.append(f"Secret token pattern detected in {rel}")

    result = {"status": "PASS" if not errors else "FAIL", "checked": checked, "errors": errors}
    print(json.dumps(result, indent=2))
    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
