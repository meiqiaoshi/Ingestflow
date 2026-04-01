"""Source extraction dispatch for supported connectors."""

from __future__ import annotations

import hashlib
from typing import Any, Dict, Optional

import pandas as pd

from core.env_resolve import resolve_env_in_obj, resolve_env_placeholders
from core.http_auth import merge_http_env_headers
from core.http_hmac import apply_hmac_sha256_headers
from extractor.csv_extractor import extract_csv
from extractor.http_extractor import extract_http
from extractor.parquet_extractor import extract_parquet
from extractor.postgres_extractor import extract_postgres, postgres_select_star_sql


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
        headers = merge_http_env_headers(source, headers)
        body: Optional[Dict[str, Any]] = source.get("body")
        if body is not None:
            body = resolve_env_in_obj(dict(body))
        headers = apply_hmac_sha256_headers(
            source,
            headers,
            body,
            source["url"],
            str(source.get("method", "GET")),
        )
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
        q_raw = source.get("query")
        sto = source.get("statement_timeout_ms")
        sto_i = int(sto) if isinstance(sto, int) else None
        mr = source.get("max_rows")
        mr_i = int(mr) if isinstance(mr, int) else None
        if isinstance(q_raw, str) and q_raw.strip():
            query = resolve_env_placeholders(q_raw.strip())
            return extract_postgres(
                dsn,
                query,
                statement_timeout_ms=sto_i,
                max_rows=mr_i,
            )
        schema = str(source.get("schema", "public")).strip() or "public"
        table = str(source["table"]).strip()
        query = postgres_select_star_sql(schema, table, max_rows=mr_i)
        return extract_postgres(
            dsn,
            query,
            statement_timeout_ms=sto_i,
            max_rows=None,
        )

    raise NotImplementedError(f"Unsupported source.type: {src_type}")


def source_fingerprint(source: Dict[str, Any]) -> str:
    src_type = source["type"]
    if src_type in ("csv", "parquet"):
        return source["path"]
    if src_type == "http":
        return source["url"]
    if src_type == "postgres":
        dsn = source.get("dsn")
        if not isinstance(dsn, str):
            raise ValueError("postgres source requires string dsn for pipeline key")
        q_raw = source.get("query")
        if isinstance(q_raw, str) and q_raw.strip():
            q = q_raw
        else:
            schema = str(source.get("schema", "public")).strip() or "public"
            table = str(source.get("table", "")).strip()
            q = postgres_select_star_sql(schema, table)
        return _postgres_fingerprint(dsn, q)
    raise ValueError(f"Unsupported source.type for pipeline key: {src_type}")
