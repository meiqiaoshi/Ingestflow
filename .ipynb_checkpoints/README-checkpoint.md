# IngestFlow

A lightweight, config-driven data ingestion framework for onboarding diverse data sources into analytical systems.

---

## 🚀 Overview

IngestFlow is designed to simulate a production-style data ingestion layer in a modern data platform.  
It enables users to define ingestion pipelines via configuration files, supporting multiple data sources, schema validation, and incremental loading.

Instead of writing ad-hoc scripts for each dataset, IngestFlow provides a reusable and extensible framework for standardized data ingestion.

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

- 🔄 **Incremental Loading (Planned)**
  - Append / upsert modes
  - State tracking

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

## ▶️ Usage (Planned)

```bash
python main.py run --config configs/orders.yaml
```

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
