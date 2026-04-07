# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] — 2026-03-27

### Added

- YAML-driven pipelines: **CSV**, **Parquet**, **HTTP** (pagination, retries, OAuth2 / Bearer / Basic, HMAC), **PostgreSQL** (`query` or `table`/`schema`) into **DuckDB**.
- **Transform** and **validation** (required columns, null/type checks); **incremental** watermark loads and upsert.
- **CLI**: `run`, `runs list` (filters, `table`/`json`/`csv`, `--output`).
- Run metadata in **`ingestion_runs`**; optional **stderr JSON** summary line (`ingestflow_run`).
- **Integration tests** (HTTP mocks, Postgres in CI), **Ruff** + **pytest** with **coverage** in CI (Python 3.11–3.13).
- **Makefile** helpers; optional **pre-commit**; **Dependabot** for pip and GitHub Actions.
- Optional **Streamlit** dashboard for run history (`requirements-dashboard.txt`).
- Documentation: **`docs/config_spec.md`** (including run history & observability), **`docs/roadmap.md`**.

[0.1.0]: https://github.com/meiqiaoshi/Ingestflow/releases/tag/v0.1.0
