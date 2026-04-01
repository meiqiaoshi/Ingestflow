"""HMAC-SHA256 headers for HTTP sources."""

from __future__ import annotations

import hashlib

import pytest
import hmac
import json

from core.http_hmac import apply_hmac_sha256_headers, validate_hmac_config


def test_apply_hmac_get_signs_url(monkeypatch) -> None:
    monkeypatch.setenv("HMAC_SEC", "secret")
    h = apply_hmac_sha256_headers(
        {"hmac_sha256_secret_env": "HMAC_SEC"},
        None,
        None,
        "https://api.example.com/x?a=1",
        "GET",
    )
    assert h is not None
    expected = hmac.new(
        b"secret",
        "https://api.example.com/x?a=1".encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    assert h["X-Signature"] == expected


def test_apply_hmac_post_signs_canonical_json(monkeypatch) -> None:
    monkeypatch.setenv("HMAC_SEC", "secret")
    body = {"z": 1, "a": 2}
    h = apply_hmac_sha256_headers(
        {
            "hmac_sha256_secret_env": "HMAC_SEC",
            "hmac_sha256_header": "Signature",
        },
        {"Accept": "application/json"},
        body,
        "https://api.example.com/x",
        "POST",
    )
    assert h is not None
    canon = json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")
    expected = hmac.new(b"secret", canon, hashlib.sha256).hexdigest()
    assert h["Signature"] == expected
    assert h["Accept"] == "application/json"


def test_validate_hmac_bad_header() -> None:
    with pytest.raises(ValueError, match="hmac_sha256_header"):
        validate_hmac_config({"hmac_sha256_secret_env": "X", "hmac_sha256_header": ""})


def test_validate_hmac_header_without_secret() -> None:
    with pytest.raises(ValueError, match="hmac_sha256_secret_env"):
        validate_hmac_config({"hmac_sha256_header": "Signature"})
