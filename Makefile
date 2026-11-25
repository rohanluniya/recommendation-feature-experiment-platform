.PHONY: setup run materialize feast-apply serve test test-unit test-chaos benchmark lint typecheck audit compliance clean

PORT ?= 8030
HOST ?= 127.0.0.1

setup:
	uv sync --extra dev --python 3.12

run:
	uv run python scripts/run_vertical_slice.py --seed 42

materialize:
	uv run python services/feature_materializer.py --export-feast

feast-apply: materialize
	uv run feast -c feast_repo apply

serve:
	uv run uvicorn apps.recsys_api.main:app --host $(HOST) --port $(PORT)

test:
	uv run pytest -v --junitxml=results/tests/pytest_report.xml

test-unit:
	uv run pytest -v -m "unit or contract"

test-chaos:
	uv run pytest -v tests/chaos/test_chaos_drills.py

benchmark:
	uv run python benchmarks/run_benchmarks.py

lint:
	uv run ruff check .

typecheck:
	uv run mypy recsys_platform apps services

audit:
	uv run python scripts/static_audit.py

compliance:
	uv run python scripts/check_licenses.py
	uv run python scripts/generate_sbom.py

clean:
	rm -rf .local_data .pytest_cache .mypy_cache .ruff_cache .hypothesis
