import re

import yaml


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

    if source.get("type") != "csv":
        raise ValueError("Only source.type='csv' is supported")
    if not isinstance(source.get("path"), str) or not source.get("path"):
        raise ValueError("source.path must be a non-empty string")

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