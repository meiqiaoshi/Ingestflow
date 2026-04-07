# IngestFlow

[![CI](https://github.com/meiqiaoshi/Ingestflow/actions/workflows/ci.yml/badge.svg)](https://github.com/meiqiaoshi/Ingestflow/actions/workflows/ci.yml)

A lightweight, config-driven data ingestion framework for onboarding diverse data sources into analytical systems.

---

## 🚀 Overview

IngestFlow is designed to simulate a production-style data ingestion layer in a modern data platform.  
It enables users to define ingestion pipelines via configuration files, supporting multiple data sources, schema validation, and incremental loading.

Instead of writing ad-hoc scripts for each dataset, IngestFlow provides a reusable and extensible framework for standardized data ingestion.

### Status (v0.1 milestone)

The **v0.1** line is a **usable baseline**: YAML-driven pipelines into **DuckDB**, **incremental** loads, **run history** in `ingestion_runs`, **CLI** (`run`, `runs list`), integration tests, **CI** (Python 3.11–3.13 with optional Postgres checks), optional **Streamlit** run browser, and **Dependabot**.

- **Config, columns, and observability:** [`docs/config_spec.md`](docs/config_spec.md) — see **section 9** for `ingestion_runs`, `source_path`, and the stderr JSON summary.
- **Planned vs delivered:** [`docs/roadmap.md`](docs/roadmap.md).
- **Release notes:** [`CHANGELOG.md`](CHANGELOG.md) (current: **0.1.0**).

**Known limitations (v0.1):** CLI is **single-process** (no distributed scheduler); default warehouse is **local DuckDB**; supported sources are **CSV, Parquet, HTTP, PostgreSQL** as documented—broader platform integrations stay out of scope for this milestone.

---

## Quick start

Requires **Python 3.10+** and dependencies in `requirements.txt` (`pandas`, `pyyaml`, `duckdb`).

```bash
cd Ingestflow
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

**Optional — install the repo as a package** (same Python **3.10+** as above; metadata in **`pyproject.toml`**):

```bash
pip install -e .
ingestflow run --config configs/sample.yaml   # same as: python main.py run --config ...
```

Run the sample pipeline:

```bash
python main.py run --config configs/sample.yaml
```

The same works as **`python main.py --config configs/sample.yaml`** (legacy; `run` is inserted automatically).

List recent runs stored in the warehouse DuckDB (table `ingestion_runs`):

```bash
python main.py runs list
python main.py runs list --db warehouse.duckdb --limit 10 --status success
python main.py runs list --config-contains sample.yaml
python main.py runs list --since 2024-01-01 --until 2025-12-31
python main.py runs list --format json
python main.py runs list --format csv
python main.py runs list --format csv -o /tmp/runs.csv
```

**Run history (Streamlit, optional — Phase 7):** browse `ingestion_runs` in a browser with the same filters as `runs list`.

```bash
pip install -r requirements-dashboard.txt
streamlit run scripts/dashboard_runs.py
# or: make dashboard   (after installing requirements-dashboard.txt)
```

Set the DuckDB path in the sidebar (default `warehouse.duckdb`).

Generate the sample Parquet file before running the Parquet pipeline:

```bash
python scripts/generate_sample_parquet.py
```

Then run the Parquet pipeline:

```bash
python main.py run --config configs/sample_parquet.yaml
```

Optional logging:

```bash
python main.py run --config configs/sample.yaml --verbose
python main.py run --config configs/sample.yaml --quiet
```

Dry run (no DuckDB **writes**: no load, no checkpoint update, no `ingestion_runs` insert; incremental mode may still **read** the existing checkpoint to preview row counts):

```bash
python main.py run --config configs/sample.yaml --dry-run
```

Logs go to **stderr** at `INFO` by default.

After each **`run`** completes, **one extra line of JSON** is also written to stderr (`event: ingestflow_run`, with `run_id`, `config_path`, `status`, `duration_seconds`, `rows_loaded`, etc.) for log pipelines; it is separate from the human-readable log format above. Use **`--no-json-summary`** on `run` to disable that line.

Optional **`.env`** in the project directory sets environment variables before config load (`python-dotenv`). For HTTP sources, use **`${VAR_NAME}`** in `source.headers` or `source.body` string values to inject secrets without putting them in YAML (see `docs/config_spec.md`).

### Tests

```bash
pip install -r requirements-dev.txt
ruff check main.py src tests scripts
pytest
```

On macOS/Linux you can use **`make check`** (or **`make lint`**, **`make test`**) from the repo root; see **`Makefile`**. **`make test-cov`** runs **pytest** with **coverage** for **`src/`** and **`main.py`** (same as CI). **`make sample-parquet`** then **`make run-sample`** matches the Quick start Parquet flow; **`make run-sample`** alone runs the CSV sample config.

Optional: install **`pre-commit`** (`requirements-dev.txt`), run **`pre-commit install`**, then hooks match CI lint; **`make precommit`** runs all hooks once. To exercise the **Postgres** integration test locally, set **`INGESTFLOW_TEST_PG_DSN`** to a reachable DSN (CI sets this automatically with a service container).

CI runs **`ruff check`** (Pyflakes-style `F` rules) and **pytest** (Postgres **extract** plus **`run_pipeline`** from a **`query`** or **`table`/`schema`** source into DuckDB) on **Python 3.11, 3.12, and 3.13**.

Unit tests mock DuckDB so the default `warehouse.duckdb` is untouched. **`tests/test_integration_pipeline.py`** runs CSV → temp DuckDB `replace`; **`tests/test_integration_incremental.py`** runs two upsert loads with **incremental watermark** (same CSV path, growing file) in CI as well.

On GitHub, pushes and pull requests to `main` run the same suite via **GitHub Actions** (`.github/workflows/ci.yml`). **Dependabot** (`.github/dependabot.yml`) proposes weekly updates for **pip** and **GitHub Actions**.

By default DuckDB writes to **`warehouse.duckdb`** in the project root (override with `target.db_path` in YAML). That file may contain:

- **Business tables** (e.g. `raw_orders` from the sample config)
- **`ingestion_runs`** — one row per run (run id, status, timestamps, row counts, errors, `load_mode`, `incremental_enabled`, `db_path`, resolved `config_path`)
- **`ingestion_state`** — incremental checkpoints when `load.incremental.enabled` is true

---

## 🎯 Key Features

- 🔌 **Multi-Source Ingestion**
  - CSV files
  - Parquet files (`pyarrow`)
  - HTTP JSON (`source.type: http`, GET/POST, pagination, retries; bearer/basic/**OAuth2**/**HMAC-SHA256** via env)
  - PostgreSQL read (`source.type: postgres`, `query` or **`table`/`schema`**, optional **`max_rows`** / **`statement_timeout_ms`**, `psycopg2-binary`)
  - REST APIs (OAuth / HMAC — planned)
  - Other databases (planned)

- ⚙️ **Config-Driven Pipelines**
  - Define ingestion logic using YAML/JSON configs
  - No need to rewrite code for each dataset

- 🧱 **Schema Validation & Transformation**
  - Column renaming
  - Type casting
  - Basic validation rules

- 🔄 **Incremental Loading**
  - Append / upsert modes
  - Timestamp watermark checkpoints (`ingestion_state`)

- 🧾 **Run Metadata & Audit Logging**
  - Track pipeline execution history
  - Record row counts, duration, status

- 📊 **Execution Reports**
  - CLI summaries (initial)
  - Dashboard (future)

---

## 🧠 Project Motivation

In real-world data engineering, one of the biggest challenges is **reliable and repeatable data ingestion**.

Teams often rely on:
- one-off scripts
- inconsistent data formats
- fragile pipelines

IngestFlow aims to address this by providing:

> A standardized, reusable ingestion layer that improves reliability, traceability, and scalability.

---

## 🏗️ Architecture (High-Level)

```
        +-------------------+
        |   Data Sources    |
        +--------+----------+
                 |
                 v
        +-------------------+
        |   IngestFlow      |
        |-------------------|
        | - Extract         |
        | - Validate        |
        | - Transform       |
        | - Load            |
        | - Log Metadata    |
        +-------------------+
                 |
                 v
        +-------------------+
        |   Target Storage  |
        |  (DuckDB / etc.)  |
        +-------------------+
```

---

## 📁 Project Structure (Planned)

```
ingestflow/
│
├── core/
│   ├── extractor/
│   ├── transformer/
│   ├── loader/
│   ├── validator/
│   └── metadata/
│
├── connectors/
│   ├── csv/
│   ├── api/
│   └── database/
│
├── configs/
│   └── sample.yaml
│
├── runs/
│   └── logs/
│
├── main.py
└── README.md
```

---

## ⚙️ Example Config

```yaml
source:
  type: csv
  path: data/orders.csv

target:
  type: duckdb
  table: raw_orders

transform:
  rename_columns:
    orderid: order_id
  cast_types:
    amount: float
    created_at: datetime

load:
  mode: append
```

---

## ▶️ Usage

```bash
python main.py run --config configs/sample.yaml
```

See `docs/config_spec.md` for the full YAML schema. **Section 9** there describes **`ingestion_runs`** columns, **`source_path`** rules, **`runs list` / Streamlit** output, and the **stderr JSON summary** line (`ingestflow_run`).

---

## Roadmap

Planned phases, **implementation status**, and **current focus** are documented in [`docs/roadmap.md`](docs/roadmap.md). The README stays user-facing; the roadmap file is the source of truth for what is shipped versus planned.

---

## License

Released under the [MIT License](LICENSE).

---

## 🧾 Author

Meiqiao Shi  
MS Data Science @ Rutgers University

---

## 📌 Note

This project is built as part of a data engineering portfolio, focusing on system design, modular architecture, and production-like workflows.
