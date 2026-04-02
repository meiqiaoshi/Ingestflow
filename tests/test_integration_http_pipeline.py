"""HTTP source through ``main.run_pipeline`` with urllib mocked; real temp DuckDB."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import duckdb
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


def test_run_pipeline_http_get_json_array_to_duckdb(tmp_path: Path) -> None:
    """Config-driven HTTP GET → extract → load; metadata in ingestion_runs."""
    payload = [{"order_id": 1, "amount": 10.5}, {"order_id": 2, "amount": 20.0}]
    raw = json.dumps(payload)

    db_file = tmp_path / "wh.duckdb"
    config_path = tmp_path / "http_get.yaml"
    config = {
        "source": {"type": "http", "url": "https://example.com/api/orders"},
        "target": {
            "type": "duckdb",
            "table": "http_e2e_orders",
            "db_path": str(db_file),
        },
        "load": {"mode": "replace"},
    }
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")

    with patch(
        "extractor.http_extractor.urllib.request.urlopen",
        return_value=_FakeResp(raw),
    ):
        main.run_pipeline(str(config_path), dry_run=False)

    con = duckdb.connect(str(db_file))
    try:
        n = con.execute("SELECT COUNT(*) FROM http_e2e_orders").fetchone()[0]
        assert int(n) == 2
        runs = con.execute(
            "SELECT status, rows_loaded, target_table, source_path FROM ingestion_runs"
        ).fetchall()
        assert len(runs) == 1
        assert runs[0][0] == "success"
        assert runs[0][1] == 2
        assert runs[0][2] == "http_e2e_orders"
        assert runs[0][3] == "https://example.com/api/orders"
    finally:
        con.close()


def test_run_pipeline_http_records_key_nested_to_duckdb(tmp_path: Path) -> None:
    """Nested JSON via records_key; still end-to-end through run_pipeline."""
    payload = {"items": [{"sku": "x", "qty": 3}]}
    raw = json.dumps(payload)

    db_file = tmp_path / "wh2.duckdb"
    config_path = tmp_path / "http_nested.yaml"
    config = {
        "source": {
            "type": "http",
            "url": "https://example.com/api/stock",
            "records_key": "items",
        },
        "target": {
            "type": "duckdb",
            "table": "http_e2e_stock",
            "db_path": str(db_file),
        },
        "load": {"mode": "replace"},
    }
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")

    with patch(
        "extractor.http_extractor.urllib.request.urlopen",
        return_value=_FakeResp(raw),
    ):
        main.run_pipeline(str(config_path), dry_run=False)

    con = duckdb.connect(str(db_file))
    try:
        n = con.execute("SELECT COUNT(*) FROM http_e2e_stock").fetchone()[0]
        assert int(n) == 1
        row = con.execute(
            "SELECT sku, qty FROM http_e2e_stock LIMIT 1"
        ).fetchone()
        assert row[0] == "x"
        assert int(row[1]) == 3
    finally:
        con.close()


def test_run_pipeline_http_pagination_offset_merges_pages_to_duckdb(
    tmp_path: Path,
) -> None:
    """offset_query pagination: multiple urlopen calls → merged rows in DuckDB."""
    pages = [
        json.dumps([{"n": 1}]),
        json.dumps([{"n": 2}]),
        json.dumps([]),
    ]

    db_file = tmp_path / "wh_page.duckdb"
    config_path = tmp_path / "http_paginated.yaml"
    config = {
        "source": {
            "type": "http",
            "url": "https://example.com/items",
            "pagination": {
                "enabled": True,
                "strategy": "offset_query",
                "page_size": 1,
                "max_requests": 10,
                "limit_param": "_limit",
                "offset_param": "_start",
                "start_offset": 0,
            },
        },
        "target": {
            "type": "duckdb",
            "table": "http_e2e_paged",
            "db_path": str(db_file),
        },
        "load": {"mode": "replace"},
    }
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")

    with patch(
        "extractor.http_extractor.urllib.request.urlopen",
        side_effect=[_FakeResp(p) for p in pages],
    ) as mocked:
        main.run_pipeline(str(config_path), dry_run=False)

    assert mocked.call_count == 3

    con = duckdb.connect(str(db_file))
    try:
        n = con.execute("SELECT COUNT(*) FROM http_e2e_paged").fetchone()[0]
        assert int(n) == 2
        ns = con.execute("SELECT n FROM http_e2e_paged ORDER BY n").fetchall()
        assert [r[0] for r in ns] == [1, 2]
        runs = con.execute("SELECT rows_loaded FROM ingestion_runs").fetchall()
        assert len(runs) == 1
        assert runs[0][0] == 2
    finally:
        con.close()


def test_run_pipeline_http_pagination_page_query_merges_pages_to_duckdb(
    tmp_path: Path,
) -> None:
    """page_query pagination: two requests, short final page → merged rows in DuckDB."""
    pages = [
        json.dumps([{"n": 10}, {"n": 11}]),
        json.dumps([{"n": 12}]),
    ]

    db_file = tmp_path / "wh_page_query.duckdb"
    config_path = tmp_path / "http_page_query.yaml"
    config = {
        "source": {
            "type": "http",
            "url": "https://example.com/items",
            "pagination": {
                "enabled": True,
                "strategy": "page_query",
                "page_size": 2,
                "max_pages": 10,
                "page_param": "page",
                "page_size_param": "per_page",
                "start_page": 1,
            },
        },
        "target": {
            "type": "duckdb",
            "table": "http_e2e_page_query",
            "db_path": str(db_file),
        },
        "load": {"mode": "replace"},
    }
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")

    with patch(
        "extractor.http_extractor.urllib.request.urlopen",
        side_effect=[_FakeResp(p) for p in pages],
    ) as mocked:
        main.run_pipeline(str(config_path), dry_run=False)

    assert mocked.call_count == 2

    con = duckdb.connect(str(db_file))
    try:
        n = con.execute("SELECT COUNT(*) FROM http_e2e_page_query").fetchone()[0]
        assert int(n) == 3
        ns = con.execute("SELECT n FROM http_e2e_page_query ORDER BY n").fetchall()
        assert [r[0] for r in ns] == [10, 11, 12]
        runs = con.execute("SELECT rows_loaded FROM ingestion_runs").fetchall()
        assert len(runs) == 1
        assert runs[0][0] == 3
    finally:
        con.close()


def test_run_pipeline_http_post_json_body_to_duckdb(tmp_path: Path) -> None:
    """POST with JSON body through config → dispatcher → extract → DuckDB."""
    from urllib.request import Request

    response_rows = [{"row_id": 7, "label": "ok"}]
    raw = json.dumps(response_rows)

    def _urlopen(req: object, timeout=None) -> _FakeResp:
        assert isinstance(req, Request)
        assert req.get_method() == "POST"
        assert req.data is not None
        assert b"needle" in req.data
        return _FakeResp(raw)

    db_file = tmp_path / "wh_post.duckdb"
    config_path = tmp_path / "http_post.yaml"
    config = {
        "source": {
            "type": "http",
            "url": "https://example.com/api/search",
            "method": "POST",
            "body": {"q": "needle", "limit": 10},
        },
        "target": {
            "type": "duckdb",
            "table": "http_e2e_post",
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
        row = con.execute(
            "SELECT row_id, label FROM http_e2e_post LIMIT 1"
        ).fetchone()
        assert int(row[0]) == 7
        assert row[1] == "ok"
    finally:
        con.close()
