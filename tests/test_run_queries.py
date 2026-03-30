"""Tests for metadata.run_queries."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import duckdb

from metadata.run_queries import list_ingestion_runs, parse_iso_datetime
from metadata.run_tracker import ensure_runs_table


def _insert_run(
    con: duckdb.DuckDBPyConnection,
    *,
    run_id: str,
    status: str,
    config_path: str,
    finished_at: Optional[datetime] = None,
) -> None:
    fin = finished_at or datetime(2024, 1, 2, tzinfo=timezone.utc)
    con.execute(
        """
        INSERT INTO ingestion_runs (
            run_id, started_at, finished_at, status,
            source_path, target_table, rows_loaded, error_message,
            load_mode, incremental_enabled, db_path, config_path
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            datetime(2024, 1, 1, tzinfo=timezone.utc),
            fin,
            status,
            "/data/x.csv",
            "t1",
            5,
            None,
            "replace",
            False,
            "w.duckdb",
            config_path,
        ],
    )


def test_parse_iso_datetime_date_and_z() -> None:
    assert parse_iso_datetime("2024-06-15").year == 2024
    assert parse_iso_datetime("2024-06-15T12:00:00Z").hour == 12


def test_list_ingestion_runs_order_and_filter(tmp_path: Path) -> None:
    db = tmp_path / "w.duckdb"
    con = duckdb.connect(str(db))
    try:
        ensure_runs_table(con)
        _insert_run(
            con,
            run_id="11111111-1111-1111-1111-111111111111",
            status="success",
            config_path="/proj/configs/a.yaml",
        )
        _insert_run(
            con,
            run_id="22222222-2222-2222-2222-222222222222",
            status="failed",
            config_path="/proj/configs/b.yaml",
        )
    finally:
        con.close()

    df = list_ingestion_runs(str(db), limit=10, status="failed")
    assert len(df) == 1
    assert df.iloc[0]["status"] == "failed"
    assert "b.yaml" in str(df.iloc[0]["config_path"])

    df2 = list_ingestion_runs(str(db), limit=10, config_path_contains="configs/a")
    assert len(df2) == 1
    assert "a.yaml" in str(df2.iloc[0]["config_path"])


def test_list_ingestion_runs_since_until(tmp_path: Path) -> None:
    db = tmp_path / "w.duckdb"
    con = duckdb.connect(str(db))
    try:
        ensure_runs_table(con)
        _insert_run(
            con,
            run_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            status="success",
            config_path="/a.yaml",
            finished_at=datetime(2024, 3, 1, 10, 0, 0, tzinfo=timezone.utc),
        )
        _insert_run(
            con,
            run_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            status="success",
            config_path="/b.yaml",
            finished_at=datetime(2024, 6, 1, 10, 0, 0, tzinfo=timezone.utc),
        )
    finally:
        con.close()

    mid = datetime(2024, 4, 1, tzinfo=timezone.utc)
    df = list_ingestion_runs(
        str(db),
        limit=10,
        since=mid,
    )
    assert len(df) == 1
    assert "bbbbbbbb" in str(df.iloc[0]["run_id"])

    df2 = list_ingestion_runs(
        str(db),
        limit=10,
        until=datetime(2024, 4, 1, tzinfo=timezone.utc),
    )
    assert len(df2) == 1
    assert "aaaaaaaa" in str(df2.iloc[0]["run_id"])
