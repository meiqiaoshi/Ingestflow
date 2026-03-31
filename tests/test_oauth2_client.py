"""OAuth2 client_credentials token fetch."""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest

from core.oauth2_client import fetch_client_credentials_token


def test_fetch_client_credentials_token_parses_json() -> None:
    payload = {"access_token": "tok-xyz", "token_type": "Bearer"}
    raw = json.dumps(payload).encode("utf-8")

    class FakeResp:
        def read(self):
            return raw

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    with patch("core.oauth2_client.urllib.request.urlopen", return_value=FakeResp()):
        tok = fetch_client_credentials_token(
            "https://oauth.example.com/token",
            "id",
            "secret",
            scope="read",
        )
    assert tok == "tok-xyz"


def test_fetch_client_credentials_token_missing_access_token() -> None:
    raw = json.dumps({"token_type": "Bearer"}).encode("utf-8")

    class FakeResp:
        def read(self):
            return raw

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    with patch("core.oauth2_client.urllib.request.urlopen", return_value=FakeResp()):
        with pytest.raises(ValueError, match="access_token"):
            fetch_client_credentials_token("https://x/token", "a", "b")
