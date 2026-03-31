"""HTTP env-based auth (bearer / basic)."""

from __future__ import annotations

import base64

import pytest

from core.http_auth import merge_http_env_headers, validate_http_auth_config


def test_merge_bearer_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MY_TOKEN", "secret")
    h = merge_http_env_headers(
        {"bearer_token_env": "MY_TOKEN"},
        None,
    )
    assert h is not None
    assert h["Authorization"] == "Bearer secret"


def test_merge_basic_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("U", "alice")
    monkeypatch.setenv("P", "x")
    h = merge_http_env_headers(
        {
            "basic_auth_user_env": "U",
            "basic_auth_password_env": "P",
        },
        {"Accept": "application/json"},
    )
    assert h is not None
    assert h["Accept"] == "application/json"
    expected = base64.b64encode(b"alice:x").decode("ascii")
    assert h["Authorization"] == f"Basic {expected}"


def test_merge_bearer_missing_env_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("MISSING_TOKEN_X", raising=False)
    with pytest.raises(ValueError, match="MISSING_TOKEN_X"):
        merge_http_env_headers({"bearer_token_env": "MISSING_TOKEN_X"}, None)


def test_validate_conflicts_with_headers_authorization() -> None:
    with pytest.raises(ValueError, match="Authorization cannot be combined"):
        validate_http_auth_config(
            {
                "headers": {"Authorization": "Bearer x"},
                "bearer_token_env": "T",
            }
        )


def test_validate_bearer_and_basic_rejected() -> None:
    with pytest.raises(ValueError, match="cannot combine"):
        validate_http_auth_config(
            {
                "bearer_token_env": "A",
                "basic_auth_user_env": "U",
                "basic_auth_password_env": "P",
            }
        )


def test_validate_basic_incomplete() -> None:
    with pytest.raises(ValueError, match="together"):
        validate_http_auth_config({"basic_auth_user_env": "U"})
