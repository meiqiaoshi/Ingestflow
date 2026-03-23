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
python main.py --config configs/sample.yaml
```

Optional logging:

```bash
python main.py --config configs/sample.yaml --verbose
python main.py --config configs/sample.yaml --quiet
```

Logs go to **stderr** at `INFO` by default.

By default DuckDB writes to **`warehouse.duckdb`** in the project root (override with `target.db_path` in YAML). That file may contain:

- **Business tables** (e.g. `raw_orders` from the sample config)
- **`ingestion_runs`** — one row per run (run id, status, timestamps, row counts, errors)
- **`ingestion_state`** — incremental checkpoints when `load.incremental.enabled` is true

---

## 🎯 Key Features

- 🔌 **Multi-Source Ingestion**
  - CSV files
  - REST APIs (planned)
  - Databases (planned)

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
python main.py --config configs/sample.yaml
```

See `docs/config_spec.md` for the full YAML schema.

---

## 🛣️ Roadmap

### Phase 1
- CSV ingestion
- Basic transformation
- Load into DuckDB
- Run metadata logging

### Phase 2
- API connector
- Schema validation
- Error handling

### Phase 3
- Incremental loading
- State management
- Retry mechanism

### Phase 4
- CLI tools
- Dashboard (Streamlit)
- Docker support

---

## 🧾 Author

Meiqiao Shi  
MS Data Science @ Rutgers University

---

## 📌 Note

This project is built as part of a data engineering portfolio, focusing on system design, modular architecture, and production-like workflows.
