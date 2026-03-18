# Architecture — IngestFlow

## 1. Purpose

This document describes the high-level architecture of IngestFlow.

The goal is to define a modular ingestion system that is easy to understand, extend, and maintain.  
Rather than building one-off scripts, IngestFlow is structured as a reusable framework for onboarding data into analytical systems.

---

## 2. High-Level Architecture

IngestFlow sits between raw data sources and target analytical storage.

+-------------------+
|   Data Sources    |
|-------------------|
| CSV               |
| REST API          |
| Database          |
+---------+---------+
          |
          v
+-------------------+
|    IngestFlow     |
|-------------------|
| Config Loader     |
| Extractor         |
| Validator         |
| Transformer       |
| Loader            |
| Metadata Tracker  |
+---------+---------+
          |
          v
+-------------------+
|  Target Storage   |
|-------------------|
| DuckDB            |
| SQLite            |
| Other targets...  |
+-------------------+

---

## 3. Core Design Principles

### 3.1 Modular Components

Each major ingestion responsibility is isolated into its own component:

- Config Loader
- Extractor
- Validator
- Transformer
- Loader
- Metadata Tracker

---

### 3.2 Config-Driven Behavior

Pipelines should be controlled through configuration files rather than hardcoded logic.

---

### 3.3 Extensibility

The system should support future additions such as:

- new connectors
- new loaders
- richer validation
- incremental loading

---

## 4. Execution Flow

1. Read config  
2. Initialize run metadata  
3. Extract data  
4. Validate data  
5. Transform data  
6. Load data  
7. Record metadata  
8. Output summary  

---

## 5. Module Responsibilities

### Config Loader
Loads and validates configuration.

### Extractor
Reads data from source (CSV initially).

### Validator
Checks schema and rules.

### Transformer
Applies column renaming and type casting.

### Loader
Writes data to DuckDB.

### Metadata Tracker
Tracks run execution (time, rows, status).

---

## 6. Proposed Project Structure

ingestflow/
│
├── core/
├── extractor/
├── validator/
├── transformer/
├── loader/
├── metadata/
├── configs/
├── data/
├── tests/
├── main.py
└── README.md

---

## 7. Runner Orchestration

A central runner coordinates all modules:

- load config
- run extractor
- validate
- transform
- load
- track metadata

---

## 8. Failure Handling Strategy

- fail fast on critical errors  
- clear error messages  
- record failure in metadata  

---

## 9. Initial Tech Stack

- Python  
- pandas  
- PyYAML  
- DuckDB  

---

## 10. Summary

IngestFlow is a modular ingestion framework with clear separation of responsibilities, designed for extensibility and production-style workflows.
