# IngestFlow

A lightweight, config-driven data ingestion framework for onboarding diverse data sources into analytical systems.

---

## 🚀 Overview

IngestFlow is designed to simulate a production-style data ingestion layer in a modern data platform.  
It enables users to define ingestion pipelines via configuration files, supporting multiple data sources, schema validation, and incremental loading.

Instead of writing ad-hoc scripts for each dataset, IngestFlow provides a reusable and extensible framework for standardized data ingestion.

---

## Quick start

Requires **Python 3.10+** and dependencies in `requirements.txt` (`pandas`, `pyyaml`, `duckdb`).

```bash
cd Ingestflow
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
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

Optional **`.env`** in the project directory sets environment variables before config load (`python-dotenv`). For HTTP sources, use **`${VAR_NAME}`** in `source.headers` or `source.body` string values to inject secrets without putting them in YAML (see `docs/config_spec.md`).

### Tests

```bash
pip install -r requirements-dev.txt
pytest
```

Unit tests mock DuckDB so the default `warehouse.duckdb` is untouched. **`tests/test_integration_pipeline.py`** runs CSV → temp DuckDB `replace`; **`tests/test_integration_incremental.py`** runs two upsert loads with **incremental watermark** (same CSV path, growing file) in CI as well.

On GitHub, pushes and pull requests to `main` run the same suite via **GitHub Actions** (`.github/workflows/ci.yml`).

By default DuckDB writes to **`warehouse.duckdb`** in the project root (override with `target.db_path` in YAML). That file may contain:

- **Business tables** (e.g. `raw_orders` from the sample config)
- **`ingestion_runs`** — one row per run (run id, status, timestamps, row counts, errors, `load_mode`, `incremental_enabled`, `db_path`, resolved `config_path`)
- **`ingestion_state`** — incremental checkpoints when `load.incremental.enabled` is true

---

## 🎯 Key Features

- 🔌 **Multi-Source Ingestion**
  - CSV files
  - Parquet files (`pyarrow`)
  - HTTP JSON (`source.type: http`, GET/POST, pagination, retries; bearer/basic via env var **names**)
  - PostgreSQL read (`source.type: postgres`, `dsn` + `query`, `psycopg2-binary`)
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

See `docs/config_spec.md` for the full YAML schema.

---

## Roadmap

Planned phases, **implementation status**, and **current focus** are documented in [`docs/roadmap.md`](docs/roadmap.md). The README stays user-facing; the roadmap file is the source of truth for what is shipped versus planned.

---

## 🧾 Author

Meiqiao Shi  
MS Data Science @ Rutgers University

---

## 📌 Note

This project is built as part of a data engineering portfolio, focusing on system design, modular architecture, and production-like workflows.
