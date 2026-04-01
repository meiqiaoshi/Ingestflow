"""Optional HMAC-SHA256 signature header for HTTP sources (secret from env)."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
from typing import Any, Dict, Optional

import re

_ENV_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_env_var_name(name: str, field: str) -> None:
    if not isinstance(name, str) or not _ENV_NAME_RE.match(name):
        raise ValueError(
            f"Invalid {field}: use letters, numbers, underscore; "
            "must not start with a digit."
        )


def validate_hmac_config(source: Dict[str, Any]) -> None:
    """Validate optional ``hmac_sha256_*`` fields for ``source.type: http``."""
    sec = source.get("hmac_sha256_secret_env")
    hdr = source.get("hmac_sha256_header")
    if sec is not None:
        if not isinstance(sec, str):
            raise ValueError("hmac_sha256_secret_env must be a string when provided")
        _validate_env_var_name(sec, "source.hmac_sha256_secret_env")
    if hdr is not None:
        if sec is None:
            raise ValueError(
                "hmac_sha256_header requires hmac_sha256_secret_env to be set"
            )
        if not isinstance(hdr, str) or not hdr.strip():
            raise ValueError("hmac_sha256_header must be a non-empty string when provided")


def apply_hmac_sha256_headers(
    source: Dict[str, Any],
    headers: Optional[Dict[str, str]],
    body: Optional[Dict[str, Any]],
    url: str,
    method: str,
) -> Optional[Dict[str, str]]:
    """
    If ``hmac_sha256_secret_env`` is set, add a hex digest header (default ``X-Signature``).

    - **GET**: signs the request URL string (UTF-8).
    - **POST**: signs a canonical JSON body (``sort_keys=True``, compact separators).
    """
    env = source.get("hmac_sha256_secret_env")
    if not env:
        return headers
    secret = os.environ.get(env)
    if secret is None:
        raise ValueError(
            f"Environment variable {env!r} is not set (hmac_sha256_secret_env)"
        )
    hdr_name = str(source.get("hmac_sha256_header") or "X-Signature").strip()
    if not hdr_name:
        hdr_name = "X-Signature"
    m = (method or "GET").upper()
    if m == "POST":
        payload = json.dumps(
            body or {}, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
    else:
        payload = url.encode("utf-8")
    sig = hmac.new(secret.encode("utf-8"), payload, hashlib.sha256).hexdigest()
    out: Dict[str, str] = dict(headers) if headers else {}
    out[hdr_name] = sig
    return out
