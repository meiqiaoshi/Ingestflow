"""Tests for ``main._print_runs_df`` output shapes."""

from __future__ import annotations

import io
import json
import sys
from unittest.mock import patch

import pandas as pd

import main as main_mod


def test_print_runs_df_json_roundtrip() -> None:
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
    with patch.object(sys, "stdout", buf):
        main_mod._print_runs_df(df, "json")
    rows = json.loads(buf.getvalue())
    assert len(rows) == 1
    assert rows[0]["run_id"] == "abc"
    assert rows[0]["status"] == "success"


def test_print_runs_df_csv_header_and_row() -> None:
    df = pd.DataFrame([{"run_id": "x", "status": "failed"}])
    buf = io.StringIO()
    with patch.object(sys, "stdout", buf):
        main_mod._print_runs_df(df, "csv")
    lines = buf.getvalue().strip().split("\n")
    assert "run_id" in lines[0]
    assert "x" in lines[1]


def test_print_runs_df_table() -> None:
    df = pd.DataFrame([{"a": 1}])
    buf = io.StringIO()
    with patch.object(sys, "stdout", buf):
        main_mod._print_runs_df(df, "table")
    assert "1" in buf.getvalue()
