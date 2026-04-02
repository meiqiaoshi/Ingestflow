"""HTTP source through ``main.run_pipeline`` with urllib mocked; real temp DuckDB."""

from __future__ import annotations

import json
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
