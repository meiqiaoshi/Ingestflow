"""HTTP auth (bearer env, HMAC) through ``run_pipeline`` with mocked ``urlopen``."""

from __future__ import annotations

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
