"""Optional HTTP auth from environment variable names (no secrets in YAML)."""

from __future__ import annotations

import base64
import os
import re
from typing import Any, Dict, Optional

from core.oauth2_client import fetch_client_credentials_token

_ENV_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_http_auth_config(source: Dict[str, Any]) -> None:
    """Validate optional bearer/basic/OAuth2 env fields for ``source.type: http``."""
    hdrs = source.get("headers")
    hdr_keys_lower = (
        {str(k).lower() for k in hdrs.keys()} if isinstance(hdrs, dict) else set()
    )
    bear = source.get("bearer_token_env")
    bu = source.get("basic_auth_user_env")
    bp = source.get("basic_auth_password_env")

    oauth_url = source.get("oauth2_token_url")
    oauth_cid = source.get("oauth2_client_id_env")
    oauth_sec = source.get("oauth2_client_secret_env")
    oauth_set = [x is not None for x in (oauth_url, oauth_cid, oauth_sec)]
    if any(oauth_set) and not all(oauth_set):
        raise ValueError(
            "oauth2_token_url, oauth2_client_id_env, and oauth2_client_secret_env must be set together"
        )

    for key in ("bearer_token_env", "basic_auth_user_env", "basic_auth_password_env"):
        val = source.get(key)
        if val is not None:
            if not isinstance(val, str):
                raise ValueError(f"source.{key} must be a string when provided")
            _validate_env_var_name(val, f"source.{key}")

    if oauth_url:
        if not isinstance(oauth_url, str) or not oauth_url.strip():
            raise ValueError("oauth2_token_url must be a non-empty string when provided")
        _validate_env_var_name(oauth_cid, "source.oauth2_client_id_env")
        _validate_env_var_name(oauth_sec, "source.oauth2_client_secret_env")
        sc = source.get("oauth2_scope")
        if sc is not None and not isinstance(sc, str):
            raise ValueError("oauth2_scope must be a string when provided")
        to = source.get("oauth2_timeout_seconds")
        if to is not None and not isinstance(to, (int, float)):
            raise ValueError("oauth2_timeout_seconds must be a number when provided")
        if bear or bu or bp:
            raise ValueError(
                "OAuth2 cannot be combined with bearer_token_env or basic_auth_*"
            )

    if bear and (bu or bp):
        raise ValueError(
            "source cannot combine bearer_token_env with basic_auth_user_env / basic_auth_password_env"
        )
    if (bu is None) != (bp is None):
        raise ValueError(
            "source.basic_auth_user_env and basic_auth_password_env must be set together"
        )
    has_oauth = bool(oauth_url)
    if "authorization" in hdr_keys_lower and (bear or bu or bp or has_oauth):
        raise ValueError(
            "source.headers Authorization cannot be combined with bearer_token_env, basic_auth_*, or oauth2_*"
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

    - ``oauth2_*``: client_credentials token (sets ``Authorization: Bearer``).
    - ``bearer_token_env``: env var name holding the raw bearer token.
    - ``basic_auth_user_env`` + ``basic_auth_password_env``: env var names for user/password.

    If ``headers`` already defines ``Authorization`` (any casing), env-based auth
    must not be configured (validated in ``validate_runtime_config``).
    """
    bearer_env = source.get("bearer_token_env")
    basic_user_env = source.get("basic_auth_user_env")
    basic_pass_env = source.get("basic_auth_password_env")
    oauth_url = source.get("oauth2_token_url")

    out: Dict[str, str] = {}
    if headers:
        for k, v in headers.items():
            out[str(k)] = str(v)

    if oauth_url:
        cid_env = source["oauth2_client_id_env"]
        sec_env = source["oauth2_client_secret_env"]
        client_id = os.environ.get(cid_env)
        client_secret = os.environ.get(sec_env)
        if client_id is None:
            raise ValueError(
                f"Environment variable {cid_env!r} is not set (oauth2_client_id_env)"
            )
        if client_secret is None:
            raise ValueError(
                f"Environment variable {sec_env!r} is not set (oauth2_client_secret_env)"
            )
        scope = source.get("oauth2_scope")
        timeout = float(source.get("oauth2_timeout_seconds", 60.0))
        token = fetch_client_credentials_token(
            oauth_url.strip(),
            client_id,
            client_secret,
            scope=scope if isinstance(scope, str) else None,
            timeout_s=timeout,
        )
        out["Authorization"] = f"Bearer {token}"
    elif bearer_env:
        token = os.environ.get(bearer_env)
        if token is None:
            raise ValueError(
                f"Environment variable {bearer_env!r} is not set (bearer_token_env)"
            )
        out["Authorization"] = f"Bearer {token}"
    elif basic_user_env or basic_pass_env:
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
