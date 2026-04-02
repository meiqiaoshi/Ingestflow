# Convenience targets (Unix/macOS). Run from repo root with venv activated if you use one.

PYTHON ?= python3

.PHONY: help install-dev lint test check run-sample sample-parquet

help:
	@echo "make install-dev    pip install -r requirements-dev.txt"
	@echo "make lint           ruff check main.py src tests scripts"
	@echo "make test           pytest"
	@echo "make check          lint + test"
	@echo "make sample-parquet generate data/sample_orders.parquet"
	@echo "make run-sample     python main.py run --config configs/sample.yaml"

install-dev:
	$(PYTHON) -m pip install -r requirements-dev.txt

lint:
	$(PYTHON) -m ruff check main.py src tests scripts

test:
	$(PYTHON) -m pytest

check: lint test

sample-parquet:
	$(PYTHON) scripts/generate_sample_parquet.py

run-sample:
	$(PYTHON) main.py run --config configs/sample.yaml
