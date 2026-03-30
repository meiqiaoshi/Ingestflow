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

### Example (HTTP POST + single object)

```yaml
source:
  type: http
  url: https://api.example.com/items
  method: POST
  body:
    name: example
  allow_single_object: true
```

Use **`allow_single_object: true`** when the API returns one JSON object (not an array).

### Example (HTTP offset pagination)

```yaml
source:
  type: http
  url: https://api.example.com/items
  method: GET
  pagination:
    enabled: true
    strategy: offset_query
    page_size: 20
    max_requests: 50
    limit_param: _limit
    offset_param: _start
    start_offset: 0
```

Query parameters are merged into `url`; pages are concatenated until a short page or `max_requests`.

### Example (HTTP page pagination)

```yaml
source:
  type: http
  url: https://api.example.com/items
  method: GET
  pagination:
    enabled: true
    strategy: page_query
    page_size: 20
    max_pages: 50
    page_param: page
    page_size_param: per_page
    start_page: 1
```

For `page_query`, requests are made page by page until an empty/short page or `max_pages`.

### Example (HTTP retries)

```yaml
source:
  type: http
  url: https://api.example.com/items
  retry:
    count: 3
    backoff_seconds: 1.0
```

Retries apply to transient `urllib` failures (not JSON parse errors).

### Environment variables and `.env`

On startup, **`main.py` loads a `.env` file** from the current working directory (if present) via `python-dotenv`. Variables set in the shell environment take precedence over `.env`.

For **`source.type: http`**, string values in **`headers`** and **`body`** may use placeholders. The same ``${VAR_NAME}`` syntax applies to **`source.type: postgres`** fields **`dsn`** and **`query`**.

- **`${VAR_NAME}`** — replaced with the value of environment variable `VAR_NAME` at run time.

Example:

```yaml
source:
  type: http
  url: https://api.example.com/items
  headers:
    Authorization: Bearer ${API_TOKEN}
```

If a placeholder references a variable that is not set, the pipeline fails with a clear error.

Do not commit real secrets: keep `.env` out of version control (already listed in `.gitignore`).

### Example (PostgreSQL)

Read-only **single query** into a DataFrame (requires **`psycopg2-binary`** in `requirements.txt`).

```yaml
source:
  type: postgres
  dsn: ${POSTGRES_DSN}
  query: |
    SELECT id, amount, created_at
    FROM public.orders
    WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
```

- **`dsn`**: libpq connection string (placeholders ``${VAR}`` are expanded like HTTP headers).
- **`query`**: trusted SQL (operator-controlled); may also use ``${VAR}`` in the string.

### Fields

- type: source type (`csv`, `parquet`, `http`, `postgres`)
- path: file path (for `csv` and `parquet`)
- url: HTTPS URL (for `http`)
- method: `GET` or `POST`
- body: JSON object for `POST` (optional; defaults to `{}`; string values may include ``${VAR}``)
- headers: optional mapping of request headers
- records_key: optional key when the JSON root is an object wrapping the array
- allow_single_object: when `true`, a single JSON object is loaded as one row
- timeout_seconds: optional request timeout (default `120`)
- pagination: optional; one of:
  - `offset_query`: `enabled`, `strategy`, `page_size`, `max_requests`, optional `limit_param`, `offset_param`, `start_offset`
  - `page_query`: `enabled`, `strategy`, `page_size`, `max_pages`, optional `page_param`, `page_size_param`, `start_page`
- retry: optional; `count` (≥1), `backoff_seconds`
- dsn: PostgreSQL connection string (for `postgres`; ``${VAR}`` allowed)
- query: SQL string for `postgres` (``${VAR}`` allowed)

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
