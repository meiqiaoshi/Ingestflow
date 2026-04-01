"""HTTP(S) JSON extractor (stdlib only). Supports GET/POST, pagination, retries."""

from __future__ import annotations

import json
import time
from typing import Any, Callable, Dict, List, Optional
import urllib.error
import urllib.request
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import pandas as pd


def _merge_query(url: str, params: Dict[str, Any]) -> str:
    """Merge query parameters into URL (overwrites duplicate keys)."""
    parts = urlparse(url)
    existing = dict(parse_qsl(parts.query))
    for k, v in params.items():
        existing[str(k)] = str(v)
    new_query = urlencode(sorted(existing.items()))
    return urlunparse(parts._replace(query=new_query))


def _json_to_dataframe(
    payload: Any,
    *,
    records_key: Optional[str],
    allow_single_object: bool,
) -> pd.DataFrame:
    if records_key:
        if not isinstance(payload, dict):
            raise ValueError("records_key requires a JSON object response")
        if records_key not in payload:
            raise ValueError(f"records_key {records_key!r} not found in JSON response")
        payload = payload[records_key]

    if isinstance(payload, list):
        if not payload:
            return pd.DataFrame()
        return pd.DataFrame(payload)
    if isinstance(payload, dict) and allow_single_object:
        return pd.DataFrame([payload])
    raise ValueError(
        "HTTP JSON must be a list of records, or a single object with allow_single_object=true, "
        "or use records_key for a nested list/object"
    )


def _fetch_raw(
    url: str,
    *,
    method: str,
    headers: Optional[Dict[str, str]],
    body: Optional[Dict[str, Any]],
    timeout_s: float,
) -> str:
    m = method.upper()
    data_bytes: Optional[bytes] = None
    if m == "POST":
        if body is None:
            body = {}
        if not isinstance(body, dict):
            raise ValueError("source.body must be a mapping for POST")
        data_bytes = json.dumps(body).encode("utf-8")
    elif m != "GET":
        raise ValueError(f"HTTP source supports GET or POST, got {method!r}")
    if m == "GET" and body:
        raise ValueError("source.body is only valid for POST")

    req = urllib.request.Request(url, data=data_bytes, method=m)
    hdrs = dict(headers) if headers else {}
    if m == "POST":
        hdrs.setdefault("Content-Type", "application/json")
    for key, val in hdrs.items():
        req.add_header(str(key), str(val))

    with urllib.request.urlopen(req, timeout=timeout_s) as resp:
        return resp.read().decode("utf-8")


def _with_retry(
    fn: Callable[[], Any],
    *,
    count: int,
    backoff_seconds: float,
) -> Any:
    for attempt in range(max(1, count)):
        try:
            return fn()
        except (urllib.error.URLError, urllib.error.HTTPError):
            if attempt < count - 1:
                time.sleep(max(0.0, backoff_seconds))
            else:
                raise
    raise RuntimeError("_with_retry: unreachable")


def _fetch_dataframe_once(
    url: str,
    *,
    method: str,
    headers: Optional[Dict[str, str]],
    body: Optional[Dict[str, Any]],
    records_key: Optional[str],
    allow_single_object: bool,
    timeout_s: float,
    retry_count: int,
    retry_backoff_s: float,
) -> pd.DataFrame:
    def inner() -> pd.DataFrame:
        raw = _fetch_raw(
            url,
            method=method,
            headers=headers,
            body=body,
            timeout_s=timeout_s,
        )
        payload: Any = json.loads(raw)
        return _json_to_dataframe(
            payload,
            records_key=records_key,
            allow_single_object=allow_single_object,
        )

    try:
        if retry_count > 1:
            return _with_retry(
                inner,
                count=retry_count,
                backoff_seconds=retry_backoff_s,
            )
        return inner()
    except urllib.error.HTTPError as e:
        raise ValueError(f"HTTP request failed: {e.code} {e.reason}") from e
    except urllib.error.URLError as e:
        raise ValueError(f"HTTP request failed: {e.reason}") from e


def extract_http(
    url: str,
    *,
    method: str = "GET",
    headers: Optional[Dict[str, str]] = None,
    records_key: Optional[str] = None,
    body: Optional[Dict[str, Any]] = None,
    allow_single_object: bool = False,
    timeout_s: float = 120.0,
    pagination: Optional[Dict[str, Any]] = None,
    retry: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    """
    Fetch JSON and build a DataFrame.

    - Default: one HTTP request; response is a JSON array of objects, or ``records_key`` path.
    - POST: optional JSON ``body``; use ``allow_single_object`` when the API returns one object.
    - ``pagination.strategy: offset_query`` merges limit/offset query params and loops until
      a short page or ``max_requests`` is reached.
    - ``pagination.strategy: page_query`` merges page/page_size query params and loops until
      a short page or ``max_pages`` is reached.
    - ``retry``: ``count`` (default 1) and ``backoff_seconds`` (default 1.0) for transient failures.
    """
    m = (method or "GET").upper()
    rc = 1
    backoff = 1.0
    if retry:
        rc = int(retry.get("count", 1))
        backoff = float(retry.get("backoff_seconds", 1.0))

    if not pagination or not pagination.get("enabled"):
        return _fetch_dataframe_once(
            url,
            method=m,
            headers=headers,
            body=body,
            records_key=records_key,
            allow_single_object=allow_single_object,
            timeout_s=timeout_s,
            retry_count=rc,
            retry_backoff_s=backoff,
        )

    strat = pagination.get("strategy")
    page_size = int(pagination["page_size"])
    frames: List[pd.DataFrame] = []

    if strat == "offset_query":
        limit_param = str(pagination.get("limit_param", "_limit"))
        offset_param = str(pagination.get("offset_param", "_start"))
        max_requests = int(pagination.get("max_requests", 50))
        start_offset = int(pagination.get("start_offset", 0))

        offset = start_offset
        for _ in range(max_requests):
            page_url = _merge_query(
                url,
                {limit_param: page_size, offset_param: offset},
            )
            chunk = _fetch_dataframe_once(
                page_url,
                method=m,
                headers=headers,
                body=body,
                records_key=records_key,
                allow_single_object=allow_single_object,
                timeout_s=timeout_s,
                retry_count=rc,
                retry_backoff_s=backoff,
            )
            if chunk.empty:
                break
            frames.append(chunk)
            if len(chunk) < page_size:
                break
            offset += page_size
    elif strat == "page_query":
        page_param = str(pagination.get("page_param", "page"))
        page_size_param = str(pagination.get("page_size_param", "page_size"))
        max_pages = int(pagination.get("max_pages", 50))
        start_page = int(pagination.get("start_page", 1))

        page = start_page
        for _ in range(max_pages):
            page_url = _merge_query(
                url,
                {page_param: page, page_size_param: page_size},
            )
            chunk = _fetch_dataframe_once(
                page_url,
                method=m,
                headers=headers,
                body=body,
                records_key=records_key,
                allow_single_object=allow_single_object,
                timeout_s=timeout_s,
                retry_count=rc,
                retry_backoff_s=backoff,
            )
            if chunk.empty:
                break
            frames.append(chunk)
            if len(chunk) < page_size:
                break
            page += 1
    else:
        raise ValueError("pagination.strategy must be 'offset_query' or 'page_query'")

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)
