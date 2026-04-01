import re

import yaml

from core.http_auth import validate_http_auth_config
from core.http_hmac import validate_hmac_config


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _is_valid_identifier(name: str) -> bool:
    return bool(_IDENTIFIER_RE.match(name))


def _validate_identifier(value: str, field_name: str) -> None:
    if not isinstance(value, str) or not _is_valid_identifier(value):
        raise ValueError(
            f"Invalid {field_name}: '{value}'. Use letters/numbers/underscore and do not start with a number."
        )


def _normalize_primary_key(primary_key):
    if primary_key is None:
        return []
    if isinstance(primary_key, str):
        return [primary_key]
    if isinstance(primary_key, list):
        return [str(c) for c in primary_key]
    raise ValueError("load.primary_key must be a string or list of strings")


def validate_runtime_config(config: dict) -> None:
    source = config.get("source", {})
    target = config.get("target", {})
    load = config.get("load", {}) or {}
    incremental = load.get("incremental", {}) or {}

    src_type = source.get("type")
    if src_type not in ("csv", "parquet", "http", "postgres"):
        raise ValueError("source.type must be 'csv', 'parquet', 'http', or 'postgres'")

    if src_type in ("csv", "parquet"):
        if not isinstance(source.get("path"), str) or not source.get("path"):
            raise ValueError("source.path must be a non-empty string")
    elif src_type == "http":
        if not isinstance(source.get("url"), str) or not source.get("url"):
            raise ValueError("source.url must be a non-empty string")
        method = source.get("method", "GET")
        if not isinstance(method, str) or method.upper() not in ("GET", "POST"):
            raise ValueError("source.method must be GET or POST for HTTP sources")
        if method.upper() == "GET" and source.get("body"):
            raise ValueError("source.body is only valid when source.method is POST")
        if source.get("body") is not None and not isinstance(source.get("body"), dict):
            raise ValueError("source.body must be a mapping when provided")
        hdrs = source.get("headers")
        if hdrs is not None and not isinstance(hdrs, dict):
            raise ValueError("source.headers must be a mapping when provided")
        rk = source.get("records_key")
        if rk is not None and not isinstance(rk, str):
            raise ValueError("source.records_key must be a string when provided")
        if source.get("allow_single_object") is not None and not isinstance(
            source.get("allow_single_object"), bool
        ):
            raise ValueError("source.allow_single_object must be a boolean when provided")
        pag = source.get("pagination")
        if pag is not None:
            if not isinstance(pag, dict):
                raise ValueError("source.pagination must be a mapping when provided")
            if pag.get("enabled"):
                strat = pag.get("strategy")
                if strat not in ("offset_query", "page_query"):
                    raise ValueError(
                        "pagination.strategy must be 'offset_query' or 'page_query' when enabled"
                    )
                for key in ("page_size",):
                    if key not in pag:
                        raise ValueError(f"pagination.{key} is required when pagination.enabled=true")
                if strat == "offset_query":
                    if "max_requests" not in pag:
                        raise ValueError(
                            "pagination.max_requests is required for pagination.strategy='offset_query'"
                        )
                if strat == "page_query":
                    if "max_pages" not in pag:
                        raise ValueError(
                            "pagination.max_pages is required for pagination.strategy='page_query'"
                        )
        rtry = source.get("retry")
        if rtry is not None:
            if not isinstance(rtry, dict):
                raise ValueError("source.retry must be a mapping when provided")
            if "count" in rtry and int(rtry["count"]) < 1:
                raise ValueError("source.retry.count must be >= 1")

        validate_http_auth_config(source)
        validate_hmac_config(source)

    elif src_type == "postgres":
        dsn = source.get("dsn")
        if not isinstance(dsn, str) or not str(dsn).strip():
            raise ValueError("source.dsn must be a non-empty string for postgres sources")
        q_raw = source.get("query")
        tbl = source.get("table")
        has_query = isinstance(q_raw, str) and bool(q_raw.strip())
        has_table = isinstance(tbl, str) and bool(tbl.strip())
        if has_query and has_table:
            raise ValueError("source.query and source.table cannot both be set for postgres")
        if not has_query and not has_table:
            raise ValueError(
                "postgres source requires either source.query or source.table (with optional source.schema)"
            )
        if has_query:
            pass
        else:
            _validate_identifier(tbl.strip(), "source.table")
            sc = source.get("schema", "public")
            if not isinstance(sc, str) or not sc.strip():
                raise ValueError("source.schema must be a non-empty string (omit for default public)")
            _validate_identifier(sc.strip(), "source.schema")

        sto = source.get("statement_timeout_ms")
        if sto is not None:
            if not isinstance(sto, int) or sto < 1:
                raise ValueError(
                    "source.statement_timeout_ms must be a positive integer (milliseconds) when provided"
                )
        mx = source.get("max_rows")
        if mx is not None:
            if not isinstance(mx, int) or mx < 1:
                raise ValueError("source.max_rows must be a positive integer when provided")

    if target.get("type") != "duckdb":
        raise ValueError("Only target.type='duckdb' is supported")
    _validate_identifier(target.get("table"), "target.table")

    mode = load.get("mode", "replace")
    if mode not in {"replace", "append", "upsert"}:
        raise ValueError("load.mode must be one of: replace, append, upsert")

    incremental_enabled = bool(incremental.get("enabled", False))
    watermark_column = incremental.get("watermark_column")
    if incremental_enabled:
        if not watermark_column:
            raise ValueError(
                "load.incremental.watermark_column is required when incremental.enabled=true"
            )
        _validate_identifier(watermark_column, "load.incremental.watermark_column")

    # This combination is usually not intended: replace recreates full table.
    if mode == "replace" and incremental_enabled:
        raise ValueError(
            "Unsupported config: load.mode='replace' cannot be combined with incremental.enabled=true"
        )

    primary_key = load.get("primary_key", incremental.get("primary_key"))
    pk_cols = _normalize_primary_key(primary_key)
    if mode == "upsert" and not pk_cols:
        raise ValueError("Upsert mode requires load.primary_key (or load.incremental.primary_key)")
    for col in pk_cols:
        _validate_identifier(col, "primary_key column")


def load_config(path: str) -> dict:
    with open(path, "r") as f:
        config = yaml.safe_load(f)

    # minimal validation
    if "source" not in config:
        raise ValueError("Missing 'source' in config")
    if "target" not in config:
        raise ValueError("Missing 'target' in config")

    validate_runtime_config(config)

    return config