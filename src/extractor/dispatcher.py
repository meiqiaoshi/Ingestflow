"""Source extraction dispatch for supported connectors."""

from __future__ import annotations

from typing import Any, Dict, Optional

import pandas as pd

from core.env_resolve import resolve_env_in_obj
from extractor.csv_extractor import extract_csv
from extractor.http_extractor import extract_http
from extractor.parquet_extractor import extract_parquet


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

    raise NotImplementedError(f"Unsupported source.type: {src_type}")


def source_fingerprint(source: Dict[str, Any]) -> str:
    src_type = source["type"]
    if src_type in ("csv", "parquet"):
        return source["path"]
    if src_type == "http":
        return source["url"]
    raise ValueError(f"Unsupported source.type for pipeline key: {src_type}")
