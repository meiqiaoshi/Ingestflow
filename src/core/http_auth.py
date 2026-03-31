"""Optional HTTP auth from environment variable names (no secrets in YAML)."""

from __future__ import annotations

import base64
import os
import re
from typing import Any, Dict, Optional

_ENV_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_http_auth_config(source: Dict[str, Any]) -> None:
    """Validate optional bearer/basic env fields for ``source.type: http``."""
    hdrs = source.get("headers")
    hdr_keys_lower = (
        {str(k).lower() for k in hdrs.keys()} if isinstance(hdrs, dict) else set()
    )
    bear = source.get("bearer_token_env")
    bu = source.get("basic_auth_user_env")
    bp = source.get("basic_auth_password_env")

    for key in ("bearer_token_env", "basic_auth_user_env", "basic_auth_password_env"):
        val = source.get(key)
        if val is not None:
            if not isinstance(val, str):
                raise ValueError(f"source.{key} must be a string when provided")
            _validate_env_var_name(val, f"source.{key}")

    if bear and (bu or bp):
        raise ValueError(
            "source cannot combine bearer_token_env with basic_auth_user_env / basic_auth_password_env"
        )
    if (bu is None) != (bp is None):
        raise ValueError(
            "source.basic_auth_user_env and basic_auth_password_env must be set together"
        )
    if "authorization" in hdr_keys_lower and (bear or bu or bp):
        raise ValueError(
            "source.headers Authorization cannot be combined with bearer_token_env or basic_auth_*"
        )


def _validate_env_var_name(name: str, field: str) -> None:
    if not isinstance(name, str) or not _ENV_NAME_RE.match(name):
        raise ValueError(
            f"Invalid {field}: use letters, numbers, underscore; "
            "must not start with a digit."
        )


def merge_http_env_headers(
    source: Dict[str, Any],
    headers: Optional[Dict[str, Any]],
) -> Optional[Dict[str, str]]:
    """
    Merge YAML ``headers`` with optional ``Authorization`` from env-backed fields:

    - ``bearer_token_env``: env var name holding the raw bearer token.
    - ``basic_auth_user_env`` + ``basic_auth_password_env``: env var names for user/password.

    If ``headers`` already defines ``Authorization`` (any casing), env-based auth
    must not be configured (validated in ``validate_runtime_config``).
    """
    bearer_env = source.get("bearer_token_env")
    basic_user_env = source.get("basic_auth_user_env")
    basic_pass_env = source.get("basic_auth_password_env")

    out: Dict[str, str] = {}
    if headers:
        for k, v in headers.items():
            out[str(k)] = str(v)

    lower = {k.lower() for k in out}

    if bearer_env:
        token = os.environ.get(bearer_env)
        if token is None:
            raise ValueError(
                f"Environment variable {bearer_env!r} is not set (bearer_token_env)"
            )
        out["Authorization"] = f"Bearer {token}"

    if basic_user_env or basic_pass_env:
        if not basic_user_env or not basic_pass_env:
            raise ValueError(
                "basic_auth_user_env and basic_auth_password_env must both be set"
            )
        user = os.environ.get(basic_user_env)
        password = os.environ.get(basic_pass_env)
        if user is None:
            raise ValueError(
                f"Environment variable {basic_user_env!r} is not set (basic_auth_user_env)"
            )
        if password is None:
            raise ValueError(
                f"Environment variable {basic_pass_env!r} is not set (basic_auth_password_env)"
            )
        raw = f"{user}:{password}".encode("utf-8")
        b64 = base64.b64encode(raw).decode("ascii")
        out["Authorization"] = f"Basic {b64}"

    return out if out else None
