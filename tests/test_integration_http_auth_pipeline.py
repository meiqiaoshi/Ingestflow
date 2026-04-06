"""HTTP auth (bearer, basic, OAuth2, HMAC GET/POST) via ``run_pipeline`` + mocks."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from pathlib import Path
from unittest.mock import patch

import duckdb
import pytest
import yaml

import main


class _FakeResp:
    def __init__(self, body: str) -> None:
        self._body = body.encode("utf-8")

    def read(self) -> bytes:
        return self._body

    def __enter__(self) -> _FakeResp:
        return self

    def __exit__(self, *args: object) -> bool:
        return False


def _authorization_header(req: object) -> str | None:
    from urllib.request import Request

    assert isinstance(req, Request)
    for k, v in req.header_items():
        if k.lower() == "authorization":
            return v
    return None


def test_run_pipeline_http_bearer_env_to_duckdb(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``bearer_token_env`` → Authorization on the outbound request → DuckDB."""
    monkeypatch.setenv("INGESTFLOW_TEST_HTTP_BEARER", "tok_e2e_9")

    raw = json.dumps([{"k": 1}])

    def _urlopen(req: object, timeout=None) -> _FakeResp:
        assert _authorization_header(req) == "Bearer tok_e2e_9"
        return _FakeResp(raw)

    db_file = tmp_path / "wh_bearer.duckdb"
    config_path = tmp_path / "http_bearer.yaml"
    config = {
        "source": {
            "type": "http",
            "url": "https://example.com/api/protected",
            "bearer_token_env": "INGESTFLOW_TEST_HTTP_BEARER",
        },
        "target": {
            "type": "duckdb",
            "table": "http_e2e_bearer",
            "db_path": str(db_file),
        },
        "load": {"mode": "replace"},
    }
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")

    with patch(
        "extractor.http_extractor.urllib.request.urlopen",
        side_effect=_urlopen,
    ) as mocked:
        main.run_pipeline(str(config_path), dry_run=False)

    assert mocked.call_count == 1
    con = duckdb.connect(str(db_file))
    try:
        n = con.execute("SELECT COUNT(*) FROM http_e2e_bearer").fetchone()[0]
        assert int(n) == 1
    finally:
        con.close()


def test_run_pipeline_http_basic_auth_env_to_duckdb(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``basic_auth_*_env`` → Basic Authorization on the outbound request → DuckDB."""
    monkeypatch.setenv("INGESTFLOW_TEST_BASIC_U", "alice")
    monkeypatch.setenv("INGESTFLOW_TEST_BASIC_P", "e2e")

    raw = json.dumps([{"n": 99}])
    expected_b64 = base64.b64encode(b"alice:e2e").decode("ascii")

    def _urlopen(req: object, timeout=None) -> _FakeResp:
        assert _authorization_header(req) == f"Basic {expected_b64}"
        return _FakeResp(raw)

    db_file = tmp_path / "wh_basic.duckdb"
    config_path = tmp_path / "http_basic.yaml"
    config = {
        "source": {
            "type": "http",
            "url": "https://example.com/api/basic",
            "basic_auth_user_env": "INGESTFLOW_TEST_BASIC_U",
            "basic_auth_password_env": "INGESTFLOW_TEST_BASIC_P",
        },
        "target": {
            "type": "duckdb",
            "table": "http_e2e_basic",
            "db_path": str(db_file),
        },
        "load": {"mode": "replace"},
    }
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")

    with patch(
        "extractor.http_extractor.urllib.request.urlopen",
        side_effect=_urlopen,
    ) as mocked:
        main.run_pipeline(str(config_path), dry_run=False)

    assert mocked.call_count == 1
    con = duckdb.connect(str(db_file))
    try:
        n = con.execute("SELECT n FROM http_e2e_basic LIMIT 1").fetchone()[0]
        assert int(n) == 99
    finally:
        con.close()


def test_run_pipeline_http_oauth2_client_credentials_to_duckdb(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """OAuth2 env client id/secret → mocked token fetch → Bearer on data request → DuckDB."""
    monkeypatch.setenv("INGESTFLOW_TEST_OAUTH_CID", "cid_e2e")
    monkeypatch.setenv("INGESTFLOW_TEST_OAUTH_CSEC", "csec_e2e")

    token_url = "https://id.example.com/oauth/token"
    raw = json.dumps([{"item": "from_api"}])

    def _urlopen(req: object, timeout=None) -> _FakeResp:
        assert _authorization_header(req) == "Bearer oauth-access-e2e"
        return _FakeResp(raw)

    db_file = tmp_path / "wh_oauth.duckdb"
    config_path = tmp_path / "http_oauth.yaml"
    config = {
        "source": {
            "type": "http",
            "url": "https://example.com/api/oauth-data",
            "oauth2_token_url": token_url,
            "oauth2_client_id_env": "INGESTFLOW_TEST_OAUTH_CID",
            "oauth2_client_secret_env": "INGESTFLOW_TEST_OAUTH_CSEC",
        },
        "target": {
            "type": "duckdb",
            "table": "http_e2e_oauth",
            "db_path": str(db_file),
        },
        "load": {"mode": "replace"},
    }
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")

    with patch(
        "core.http_auth.fetch_client_credentials_token",
        return_value="oauth-access-e2e",
    ) as fetch_mock:
        with patch(
            "extractor.http_extractor.urllib.request.urlopen",
            side_effect=_urlopen,
        ) as url_mock:
            main.run_pipeline(str(config_path), dry_run=False)

    fetch_mock.assert_called_once()
    assert fetch_mock.call_args[0][0] == token_url
    assert fetch_mock.call_args[0][1] == "cid_e2e"
    assert fetch_mock.call_args[0][2] == "csec_e2e"

    assert url_mock.call_count == 1

    con = duckdb.connect(str(db_file))
    try:
        cell = con.execute(
            "SELECT item FROM http_e2e_oauth LIMIT 1"
        ).fetchone()[0]
        assert str(cell) == "from_api"
    finally:
        con.close()


def test_run_pipeline_http_hmac_get_to_duckdb(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """GET HMAC signs URL; dispatcher applies secret from env before extract."""
    monkeypatch.setenv("INGESTFLOW_TEST_HTTP_HMAC", "hmac_secret_e2e")
    url = "https://example.com/api/signed"
    expected_sig = hmac.new(
        b"hmac_secret_e2e",
        url.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    raw = json.dumps([{"x": 42}])

    def _urlopen(req: object, timeout=None) -> _FakeResp:
        from urllib.request import Request

        assert isinstance(req, Request)
        sig = None
        for k, v in req.header_items():
            if k.lower() == "x-ingestflow-sig":
                sig = v
                break
        assert sig == expected_sig
        return _FakeResp(raw)

    db_file = tmp_path / "wh_hmac.duckdb"
    config_path = tmp_path / "http_hmac.yaml"
    config = {
        "source": {
            "type": "http",
            "url": url,
            "hmac_sha256_secret_env": "INGESTFLOW_TEST_HTTP_HMAC",
            "hmac_sha256_header": "X-Ingestflow-Sig",
        },
        "target": {
            "type": "duckdb",
            "table": "http_e2e_hmac",
            "db_path": str(db_file),
        },
        "load": {"mode": "replace"},
    }
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")

    with patch(
        "extractor.http_extractor.urllib.request.urlopen",
        side_effect=_urlopen,
    ) as mocked:
        main.run_pipeline(str(config_path), dry_run=False)

    assert mocked.call_count == 1
    con = duckdb.connect(str(db_file))
    try:
        v = con.execute("SELECT x FROM http_e2e_hmac LIMIT 1").fetchone()[0]
        assert int(v) == 42
    finally:
        con.close()


def test_run_pipeline_http_hmac_post_body_to_duckdb(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """POST HMAC signs canonical JSON body (sort_keys); full pipeline into DuckDB."""
    monkeypatch.setenv("INGESTFLOW_TEST_HTTP_HMAC_POST", "post_hmac_secret")
    url = "https://example.com/api/hmac-post"
    body = {"z": 1, "a": 2}
    canon = json.dumps(body, sort_keys=True, separators=(",", ":")).encode("utf-8")
    expected_sig = hmac.new(
        b"post_hmac_secret",
        canon,
        hashlib.sha256,
    ).hexdigest()

    raw = json.dumps([{"ok": True}])

    def _urlopen(req: object, timeout=None) -> _FakeResp:
        from urllib.request import Request

        assert isinstance(req, Request)
        assert req.get_method() == "POST"
        assert req.data is not None
        sig = None
        for k, v in req.header_items():
            if k.lower() == "x-signature-post":
                sig = v
                break
        assert sig == expected_sig
        return _FakeResp(raw)

    db_file = tmp_path / "wh_hmac_post.duckdb"
    config_path = tmp_path / "http_hmac_post.yaml"
    config = {
        "source": {
            "type": "http",
            "url": url,
            "method": "POST",
            "body": body,
            "hmac_sha256_secret_env": "INGESTFLOW_TEST_HTTP_HMAC_POST",
            "hmac_sha256_header": "X-Signature-Post",
        },
        "target": {
            "type": "duckdb",
            "table": "http_e2e_hmac_post",
            "db_path": str(db_file),
        },
        "load": {"mode": "replace"},
    }
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")

    with patch(
        "extractor.http_extractor.urllib.request.urlopen",
        side_effect=_urlopen,
    ) as mocked:
        main.run_pipeline(str(config_path), dry_run=False)

    assert mocked.call_count == 1
    con = duckdb.connect(str(db_file))
    try:
        ok = con.execute("SELECT ok FROM http_e2e_hmac_post LIMIT 1").fetchone()[0]
        assert ok is True or str(ok).lower() == "true"
    finally:
        con.close()
