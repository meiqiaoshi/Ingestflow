# Roadmap — IngestFlow

## Overview

This roadmap outlines the planned development phases for IngestFlow.

The goal is to build the project incrementally, starting from a minimal but working ingestion pipeline and gradually expanding it into a more reusable, production-style ingestion framework.

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
- Type validation
- Optional null checks
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
- Primary key-based deduplication

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

- REST API connector
- Database connector
- Shared connector interface
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

The recommended implementation order is:

1. Phase 1 — Minimal Working Ingestion Pipeline
2. Phase 2 — Metadata Tracking and Audit Logging
3. Phase 3 — Validation Layer
4. Phase 4 — Incremental Loading
5. Phase 5 — Additional Connectors
6. Phase 6 — Developer Experience Improvements
7. Phase 7 — Monitoring and Extension Layer

---

## Current Focus

The immediate priority is:

> Build a clean, minimal, end-to-end CSV → DuckDB ingestion pipeline with config-driven execution.

This will serve as the foundation for all later features.