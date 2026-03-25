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

### Example (Parquet)

```yaml
source:
  type: parquet
  path: data/orders.parquet
```

Parquet ingestion requires **`pyarrow`** (listed in `requirements.txt`).

### Example (HTTP JSON)

```yaml
source:
  type: http
  url: https://api.example.com/items
  method: GET
  headers:
    Accept: application/json
  # If the response is {"data": [ {...}, ... ]} instead of a bare array:
  # records_key: data
```

Only **GET** is supported in the current HTTP connector. The response must be a JSON **array of objects**, or a JSON object with a list under `records_key`.

### Fields

- type: source type (`csv`, `parquet`, `http`; more connectors planned)
- path: file path (for `csv` and `parquet`)
- url: HTTPS URL (for `http`)
- method: `GET` (only option for now)
- headers: optional mapping of request headers
- records_key: optional key when the JSON root is an object wrapping the array

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
  null_checks:
    - order_id
    - amount
  type_checks:
    order_id: int
    amount: float
```

### Supported Rules

- required_columns: list of required fields
- null_checks: list of columns that must not contain null, NaN, or NaT
- type_checks: mapping `column: expected_kind` where `expected_kind` is one of `int`, `float`, `bool`, `str`, `datetime` (runs **after** `transform`, so it validates dtypes post-cast)

---

## 7. Load

Defines how data is written to the target.

### Example

```yaml
load:
  mode: upsert
  incremental:
    enabled: true
    watermark_column: created_at
    primary_key: order_id
```

Composite primary key example:

```yaml
load:
  mode: upsert
  primary_key: [order_id, region]
```

### Supported Modes

- append: insert all rows
- replace: replace target table
- upsert: delete rows matching primary key(s) (single or composite), then insert new rows

### Validation Constraints

- `load.mode` must be one of: `replace`, `append`, `upsert`
- `load.mode: upsert` requires `primary_key` (under `load` or `load.incremental`)
- `load.mode: replace` cannot be combined with `load.incremental.enabled: true`
- `target.table`, `primary_key`, and `watermark_column` must be valid SQL-style identifiers:
  letters/numbers/underscore only, and cannot start with a number

### Incremental (Timestamp Watermark)

Supported fields under `load.incremental`:

- enabled: true/false
- watermark_column: column used for checkpoint filtering
- primary_key: primary key for `load.mode: upsert` — a single column name (string) or a list of column names for a composite key (can also be set under `load.primary_key`)

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
    created_at: datetime

validation:
  required_columns:
    - order_id
    - amount
  null_checks:
    - order_id
    - amount
  type_checks:
    order_id: int
    amount: float
    created_at: datetime

load:
  mode: upsert
  incremental:
    enabled: true
    watermark_column: created_at
    primary_key: order_id
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
