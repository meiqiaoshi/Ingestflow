"""Source extraction dispatch for supported connectors."""

from __future__ import annotations

import hashlib
from typing import Any, Dict, Optional

import pandas as pd

from core.env_resolve import resolve_env_in_obj, resolve_env_placeholders
from extractor.csv_extractor import extract_csv
from extractor.http_extractor import extract_http
from extractor.parquet_extractor import extract_parquet
from extractor.postgres_extractor import extract_postgres


def _postgres_fingerprint(dsn: str, query: str) -> str:
    raw = f"{dsn}|{query}"
    h = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    return f"postgres:{h[:16]}"


def extract_source(source: Dict[str, Any]) -> pd.DataFrame:
    src_type = source["type"]

    if src_type == "csv":
        return extract_csv(source["path"])
    if src_type == "parquet":
        return extract_parquet(source["path"])
    if src_type == "http":
        headers: Optional[Dict[str, Any]] = source.get("headers")
        if headers is not None:
            headers = resolve_env_in_obj(dict(headers))
        body: Optional[Dict[str, Any]] = source.get("body")
        if body is not None:
            body = resolve_env_in_obj(dict(body))
        return extract_http(
            source["url"],
            method=source.get("method", "GET"),
            headers=headers,
            records_key=source.get("records_key"),
            body=body,
            allow_single_object=bool(source.get("allow_single_object", False)),
            timeout_s=float(source.get("timeout_seconds", 120.0)),
            pagination=source.get("pagination"),
            retry=source.get("retry"),
        )
    if src_type == "postgres":
        dsn = resolve_env_placeholders(str(source["dsn"]).strip())
        query = resolve_env_placeholders(str(source["query"]).strip())
        return extract_postgres(dsn, query)

    raise NotImplementedError(f"Unsupported source.type: {src_type}")


def source_fingerprint(source: Dict[str, Any]) -> str:
    src_type = source["type"]
    if src_type in ("csv", "parquet"):
        return source["path"]
    if src_type == "http":
        return source["url"]
    if src_type == "postgres":
        dsn = source.get("dsn")
        q = source.get("query")
        if not isinstance(dsn, str) or not isinstance(q, str):
            raise ValueError("postgres source requires string dsn and query for pipeline key")
        return _postgres_fingerprint(dsn, q)
    raise ValueError(f"Unsupported source.type for pipeline key: {src_type}")
