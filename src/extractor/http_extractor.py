"""HTTP(S) JSON array extractor (stdlib only; GET requests)."""

from __future__ import annotations

import json
from typing import Any, Dict, Optional
import urllib.error
import urllib.request

import pandas as pd


def extract_http(
    url: str,
    *,
    method: str = "GET",
    headers: Optional[Dict[str, str]] = None,
    records_key: Optional[str] = None,
    timeout_s: float = 120.0,
) -> pd.DataFrame:
    """
    Fetch JSON and build a DataFrame from a list of objects.

    - Response must be a JSON array, or an object where ``records_key`` points to the array.
    - Only GET is supported in this minimal Phase 5 implementation.
    """
    m = (method or "GET").upper()
    if m != "GET":
        raise ValueError(f"HTTP source supports only GET for now, got {method!r}")

    req = urllib.request.Request(url, method="GET")
    if headers:
        for key, val in headers.items():
            req.add_header(str(key), str(val))

    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            raw = resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        raise ValueError(f"HTTP request failed: {e.code} {e.reason}") from e
    except urllib.error.URLError as e:
        raise ValueError(f"HTTP request failed: {e.reason}") from e

    payload: Any = json.loads(raw)

    if records_key:
        if not isinstance(payload, dict):
            raise ValueError("records_key requires a JSON object response")
        if records_key not in payload:
            raise ValueError(f"records_key {records_key!r} not found in JSON response")
        payload = payload[records_key]

    if not isinstance(payload, list):
        raise ValueError(
            "HTTP JSON must decode to a list of records, or use records_key for a nested list"
        )
    if not payload:
        return pd.DataFrame()
    return pd.DataFrame(payload)
