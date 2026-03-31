# Roadmap — IngestFlow

## Overview

This roadmap outlines the planned development phases for IngestFlow.

The goal is to build the project incrementally, starting from a minimal but working ingestion pipeline and gradually expanding it into a more reusable, production-style ingestion framework.

---

## Implementation status

Snapshot of how the phases map to the codebase today:

| Phase | Theme | Status |
|-------|--------|--------|
| **1** | CSV → DuckDB pipeline, YAML config, transform, CLI | **Delivered** |
| **2** | Run metadata (`ingestion_runs`), logging, execution summary | **Delivered** |
| **3** | Validation (`required_columns`, `null_checks`, `type_checks`) | **Delivered** |
| **4** | Append / upsert, checkpoints, incremental, composite `primary_key` | **Delivered** |
| **5** | Parquet + HTTP (**bearer/basic env auth**, JSON, pagination) + **PostgreSQL** read, **dispatcher** | **Partial** — OAuth2 flows / HMAC / streaming, more DBs **planned** |
| **6** | Tests (`pytest`), CI, `.env` / `${VAR}`, **integration**: CSV replace + **incremental two-run** (checkpoint + upsert) | **Partial** — HTTP + real DB combo optional |
| **7** | **`runs list`** (filters, **`--format`**, **`--output` / `-o`** file), dashboards TBD | **Partial** — Streamlit **planned** |

---

## Phase 1 — Minimal Working Ingestion Pipeline

### Objective

Build the first end-to-end ingestion flow for a local CSV source into DuckDB.

### Scope

- Read pipeline config from YAML
- Extract data from a CSV file
- Apply simple transformations
  - column renaming
  - type casting
- Load data into DuckDB
- Print a basic execution summary

### Deliverables

- Project skeleton and package structure
- YAML config loader
- CSV extractor
- Basic transformer
- DuckDB loader
- CLI entry point
- Sample config and sample dataset

### Success Criteria

- A user can run one command and ingest a CSV file into DuckDB successfully
- Core logic is separated into reusable modules
- The pipeline is understandable and easy to extend

---

## Phase 2 — Metadata Tracking and Audit Logging

### Objective

Add execution tracking to make ingestion runs observable and auditable.

### Scope

- Create run IDs
- Record run start/end timestamps
- Track row counts
- Track execution status
- Store metadata in a lightweight local database
- Write structured logs for each run

### Deliverables

- Run metadata model
- Metadata store layer
- Audit logging utilities
- Execution summary with status and duration

### Success Criteria

- Every pipeline execution generates a metadata record
- Failures are captured with useful status information
- Previous runs can be reviewed later

---

## Phase 3 — Validation Layer

### Objective

Introduce validation rules before loading data into the target.

### Scope

- Required column checks
- Missing column detection
- Type validation (`validation.type_checks` in config)
- Optional null checks (`validation.null_checks` in config)
- Config-defined validation rules

### Deliverables

- Validation module
- Validation result object
- Config support for validation rules
- Clear error messages for validation failures

### Success Criteria

- Invalid data is detected before loading
- Validation behavior is configurable
- Validation failures are easy to debug

---

## Phase 4 — Incremental Loading

### Objective

Support non-full-refresh ingestion patterns.

### Scope

- Append mode
- Upsert mode
- Basic checkpoint/state tracking
- Timestamp-based incremental ingestion
- Primary key-based deduplication (single or composite `primary_key`)

### Deliverables

- Load mode abstraction
- State tracking model
- Checkpoint persistence
- Incremental ingestion logic

### Success Criteria

- The system can ingest only new or changed records
- Repeated runs do not create uncontrolled duplicates
- Incremental state is persisted and reusable

---

## Phase 5 — Additional Connectors

### Objective

Expand beyond CSV ingestion.

### Scope

- Parquet file source (`source.type: parquet`)
- HTTP JSON source (`source.type: http`, GET/POST, retries, offset pagination)
- REST API connector (OAuth, HMAC, streaming — planned)
- Database connector (planned)
- Shared connector interface (planned)
- Source-specific configuration support

### Deliverables

- API extractor
- Database extractor
- Connector abstraction layer
- Example configs for additional source types

### Success Criteria

- The project supports multiple source types
- New connectors fit naturally into the existing design
- Configuration remains consistent across connectors

---

## Phase 6 — Developer Experience Improvements

### Objective

Make the project easier to run, test, and extend.

### Scope

- Better CLI commands
- Improved error messages
- Unit tests
- Integration tests
- `.env` support
- Better sample data and examples

### Deliverables

- CLI improvements
- Test suite
- Local development setup instructions
- Example workflows

### Success Criteria

- New contributors can run the project easily
- Core components have test coverage
- Common failure cases are easier to diagnose

---

## Phase 7 — Monitoring and Extension Layer

### Objective

Prepare IngestFlow for richer operational visibility and future integrations.

### Scope

- Execution history viewer
- Simple dashboard (optional)
- Better run summaries
- Integration hooks for external observability systems
- Compatibility with downstream monitoring tools such as SentinelDQ

### Deliverables

- Run history query interface
- Optional Streamlit dashboard
- Monitoring-oriented output formats
- Integration notes for observability workflows

### Success Criteria

- Users can inspect ingestion history easily
- Operational visibility improves
- The project connects naturally with broader data platform tooling

---

## Recommended Development Order

The original sequence still reflects dependencies (each phase builds on the last). Phases **1–4** are in place; ongoing work follows **5 → 6 → 7** for connectors, developer experience, and operations.

1. Phase 1 — Minimal Working Ingestion Pipeline *(done)*
2. Phase 2 — Metadata Tracking and Audit Logging *(done)*
3. Phase 3 — Validation Layer *(done)*
4. Phase 4 — Incremental Loading *(done)*
5. Phase 5 — Additional Connectors *(in progress — finish planned connectors / auth patterns)*
6. Phase 6 — Developer Experience Improvements *(in progress — expand integration coverage if needed)*
7. Phase 7 — Monitoring and Extension Layer *(in progress — CLI run history; optional dashboard)*

---

## Current focus

Near-term priorities:

1. **Phase 5** — extend HTTP (OAuth2 / HMAC / streaming pagination where needed) or add a first **database read** connector if a concrete use case appears.
2. **Phase 6** — optional **more integration tests** (HTTP mock + real DB, or incremental second run); further CLI/DX polish as needed.
3. **Phase 7** — optional **dashboard** or external **observability hooks** (run history already exportable as JSON/CSV).

The CSV → DuckDB baseline and config-driven core are no longer the blocking goal; they are established.