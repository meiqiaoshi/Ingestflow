# Config Specification — IngestFlow

## 1. Overview

This document defines the configuration structure for IngestFlow.

All ingestion pipelines are defined via YAML configuration files.  
The configuration fully describes how data is extracted, transformed, validated, and loaded.

---

## 2. Top-Level Structure

A config file consists of the following sections:

- source
- target
- transform (optional)
- validation (optional)
- load

---

## 3. Source

Defines where the data comes from.

### Example (CSV)

```yaml
source:
  type: csv
  path: data/orders.csv
```

### Fields

- type: source type (csv, api, database)
- path: file path (for csv)

---

## 4. Target

Defines where data will be loaded.

### Example

```yaml
target:
  type: duckdb
  table: raw_orders
```

### Fields

- type: target system (duckdb)
- table: destination table name

---

## 5. Transform (Optional)

Defines transformations applied before loading.

### Example

```yaml
transform:
  rename_columns:
    orderid: order_id
  cast_types:
    amount: float
    created_at: datetime
```

### Supported Operations

- rename_columns: mapping of old → new column names
- cast_types: column type casting

---

## 6. Validation (Optional)

Defines rules to validate data before loading.

### Example

```yaml
validation:
  required_columns:
    - order_id
    - amount
```

### Supported Rules

- required_columns: list of required fields
- (future) null_checks
- (future) type_checks

---

## 7. Load

Defines how data is written to the target.

### Example

```yaml
load:
  mode: append
  incremental:
    enabled: true
    watermark_column: created_at
```

### Supported Modes

- append: insert all rows
- replace: replace target table
- upsert (planned)

### Incremental (Timestamp Watermark)

Supported fields under `load.incremental`:

- enabled: true/false
- watermark_column: column used for checkpoint filtering

Behavior:

- system reads previous checkpoint from `ingestion_state`
- keeps only rows where `watermark_column > checkpoint`
- writes new checkpoint after successful load

---

## 8. Full Example

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

validation:
  required_columns:
    - order_id
    - amount

load:
  mode: append
  incremental:
    enabled: true
    watermark_column: created_at
```

---

## 9. Design Principles

- Config should be simple and readable
- Avoid over-engineering early
- Extend gradually as features grow
- Maintain backward compatibility

---

## 10. Future Extensions

- API source configuration
- Database connection parameters
- Incremental loading configs
- Advanced validation rules
