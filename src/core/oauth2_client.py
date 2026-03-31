"""OAuth2 client_credentials token fetch (stdlib HTTP)."""

from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Optional


def fetch_client_credentials_token(
    token_url: str,
    client_id: str,
    client_secret: str,
    *,
    scope: Optional[str] = None,
    timeout_s: float = 60.0,
) -> str:
    """
    POST ``application/x-www-form-urlencoded`` to ``token_url`` and return ``access_token``.

    Raises:
        ValueError: on HTTP error, missing ``access_token``, or invalid JSON.
    """
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }
    if scope:
        data["scope"] = scope
    body = urllib.parse.urlencode(data).encode("utf-8")
    req = urllib.request.Request(token_url, data=body, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            raw = resp.read().decode("utf-8")
    except urllib.error.HTTPError as e:
        raise ValueError(f"OAuth2 token request failed: HTTP {e.code}") from e
    except urllib.error.URLError as e:
        raise ValueError(f"OAuth2 token request failed: {e.reason}") from e

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError("OAuth2 token response is not valid JSON") from e
    if "access_token" not in payload:
        raise ValueError("OAuth2 token response missing access_token")
    return str(payload["access_token"])
