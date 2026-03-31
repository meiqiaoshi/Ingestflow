"""Tests for ``main._write_runs_df`` output shapes."""

from __future__ import annotations

import argparse
import io
import json
from pathlib import Path

import pandas as pd
import pytest

import main as main_mod


def test_write_runs_df_json_roundtrip() -> None:
    df = pd.DataFrame(
        [
            {
                "run_id": "abc",
                "status": "success",
                "rows_loaded": 3,
            }
        ]
    )
    buf = io.StringIO()
    main_mod._write_runs_df(df, "json", buf)
    rows = json.loads(buf.getvalue())
    assert len(rows) == 1
    assert rows[0]["run_id"] == "abc"
    assert rows[0]["status"] == "success"


def test_write_runs_df_csv_header_and_row() -> None:
    df = pd.DataFrame([{"run_id": "x", "status": "failed"}])
    buf = io.StringIO()
    main_mod._write_runs_df(df, "csv", buf)
    lines = buf.getvalue().strip().split("\n")
    assert "run_id" in lines[0]
    assert "x" in lines[1]


def test_write_runs_df_table() -> None:
    df = pd.DataFrame([{"a": 1}])
    buf = io.StringIO()
    main_mod._write_runs_df(df, "table", buf)
    assert "1" in buf.getvalue()


def test_runs_list_writes_to_output_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``--output`` writes formatted content to a file."""
    db = tmp_path / "w.duckdb"
    db.touch()
    out = tmp_path / "runs.json"
    df = pd.DataFrame([{"run_id": "r1", "status": "success"}])
    monkeypatch.setattr(main_mod, "list_ingestion_runs", lambda *a, **k: df)

    args = argparse.Namespace(
        db=str(db),
        limit=20,
        status=None,
        config_contains=None,
        since=None,
        until=None,
        format="json",
        output=str(out),
    )
    main_mod._cmd_runs_list(args)
    text = out.read_text(encoding="utf-8")
    assert "r1" in text
    assert text.strip().startswith("[")
