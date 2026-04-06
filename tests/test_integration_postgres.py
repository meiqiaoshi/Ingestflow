"""PostgreSQL integration when ``INGESTFLOW_TEST_PG_DSN`` is set (CI provides Postgres)."""

from __future__ import annotations

import os
from pathlib import Path

import duckdb
import psycopg2
import pytest
import yaml

import main
from extractor.postgres_extractor import extract_postgres

DSN = os.environ.get("INGESTFLOW_TEST_PG_DSN", "").strip()

PG_SKIP = pytest.mark.skipif(
    not DSN,
    reason="INGESTFLOW_TEST_PG_DSN not set (optional local Postgres)",
)


@PG_SKIP
def test_extract_postgres_real_select_literal() -> None:
    df = extract_postgres(DSN, "SELECT 1 AS n")
    assert len(df) == 1
    assert int(df.iloc[0]["n"]) == 1


_E2E_TABLE = "ingestflow_e2e_run"
_E2E_SCHEMA = "ingestflow_e2e_schema"
_E2E_TBL = "tbl_from_table"


@PG_SKIP
def test_run_pipeline_postgres_query_to_duckdb(tmp_path: Path) -> None:
    """Postgres ``query`` → ``run_pipeline`` → DuckDB ``replace`` + ``ingestion_runs``."""
    conn = psycopg2.connect(DSN)
    try:
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {_E2E_TABLE}")
        cur.execute(
            f"CREATE TABLE {_E2E_TABLE} (id INTEGER PRIMARY KEY, label TEXT)"
        )
        cur.execute(
            f"INSERT INTO {_E2E_TABLE} (id, label) VALUES (1, 'a'), (2, 'b')"
        )
    finally:
        conn.close()

    db_file = tmp_path / "pg_pipeline.duckdb"
    config_path = tmp_path / "postgres_pipeline.yaml"
    config = {
        "source": {
            "type": "postgres",
            "dsn": DSN,
            "query": f"SELECT id, label FROM {_E2E_TABLE} ORDER BY id",
        },
        "target": {
            "type": "duckdb",
            "table": "pg_e2e_rows",
            "db_path": str(db_file),
        },
        "load": {"mode": "replace"},
    }
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")

    try:
        main.run_pipeline(str(config_path), dry_run=False)
    finally:
        c2 = psycopg2.connect(DSN)
        try:
            c2.autocommit = True
            c2.cursor().execute(f"DROP TABLE IF EXISTS {_E2E_TABLE}")
        finally:
            c2.close()

    con = duckdb.connect(str(db_file))
    try:
        rows = con.execute(
            "SELECT id, label FROM pg_e2e_rows ORDER BY id"
        ).fetchall()
        assert len(rows) == 2
        assert int(rows[0][0]) == 1 and str(rows[0][1]) == "a"
        assert int(rows[1][0]) == 2 and str(rows[1][1]) == "b"
        runs = con.execute(
            "SELECT status, rows_loaded, source_path FROM ingestion_runs"
        ).fetchall()
        assert len(runs) == 1
        assert runs[0][0] == "success"
        assert runs[0][1] == 2
        assert str(runs[0][2]).startswith("postgres:")
        assert _E2E_TABLE in str(runs[0][2])
    finally:
        con.close()


@PG_SKIP
def test_run_pipeline_postgres_table_and_schema_to_duckdb(tmp_path: Path) -> None:
    """Postgres ``table`` + ``schema`` (no ``query``) → ``postgres_select_star_sql`` → DuckDB."""
    conn = psycopg2.connect(DSN)
    try:
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{_E2E_SCHEMA}"')
        cur.execute(
            f'DROP TABLE IF EXISTS "{_E2E_SCHEMA}"."{_E2E_TBL}"'
        )
        cur.execute(
            f'CREATE TABLE "{_E2E_SCHEMA}"."{_E2E_TBL}" '
            f"(id INTEGER PRIMARY KEY, v TEXT)"
        )
        cur.execute(
            f'INSERT INTO "{_E2E_SCHEMA}"."{_E2E_TBL}" (id, v) '
            f"VALUES (10, 'p'), (20, 'q')"
        )
    finally:
        conn.close()

    db_file = tmp_path / "pg_by_table.duckdb"
    config_path = tmp_path / "postgres_table.yaml"
    config = {
        "source": {
            "type": "postgres",
            "dsn": DSN,
            "schema": _E2E_SCHEMA,
            "table": _E2E_TBL,
        },
        "target": {
            "type": "duckdb",
            "table": "pg_e2e_from_pg_table",
            "db_path": str(db_file),
        },
        "load": {"mode": "replace"},
    }
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")

    try:
        main.run_pipeline(str(config_path), dry_run=False)
    finally:
        c2 = psycopg2.connect(DSN)
        try:
            c2.autocommit = True
            cur2 = c2.cursor()
            cur2.execute(
                f'DROP TABLE IF EXISTS "{_E2E_SCHEMA}"."{_E2E_TBL}"'
            )
            cur2.execute(
                f'DROP SCHEMA IF EXISTS "{_E2E_SCHEMA}"'
            )
        finally:
            c2.close()

    con = duckdb.connect(str(db_file))
    try:
        rows = con.execute(
            "SELECT id, v FROM pg_e2e_from_pg_table ORDER BY id"
        ).fetchall()
        assert len(rows) == 2
        assert int(rows[0][0]) == 10 and str(rows[0][1]) == "p"
        assert int(rows[1][0]) == 20 and str(rows[1][1]) == "q"
        runs = con.execute(
            "SELECT status, rows_loaded, source_path FROM ingestion_runs"
        ).fetchall()
        assert len(runs) == 1
        assert runs[0][0] == "success"
        assert runs[0][1] == 2
        assert str(runs[0][2]) == (
            f"postgres:{_E2E_SCHEMA}.{_E2E_TBL}"
        )
    finally:
        con.close()
