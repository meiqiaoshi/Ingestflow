"""PostgreSQL extractor (mocked DB I/O)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd

from extractor.postgres_extractor import extract_postgres, postgres_select_star_sql


def test_postgres_select_star_sql() -> None:
    assert postgres_select_star_sql("public", "orders") == (
        'SELECT * FROM "public"."orders"'
    )
    assert postgres_select_star_sql("public", "orders", max_rows=100) == (
        'SELECT * FROM "public"."orders" LIMIT 100'
    )


def test_extract_postgres_calls_read_sql() -> None:
    expected = pd.DataFrame({"id": [1], "x": ["a"]})
    conn = MagicMock()

    with patch("extractor.postgres_extractor.psycopg2.connect", return_value=conn):
        with patch(
            "extractor.postgres_extractor.pd.read_sql_query", return_value=expected
        ) as m:
            out = extract_postgres("postgresql://u:p@localhost/db", "SELECT 1")

    m.assert_called_once()
    assert m.call_args[0][0] == "SELECT 1"
    assert m.call_args[0][1] is conn
    pd.testing.assert_frame_equal(out, expected)
    conn.close.assert_called_once()


def test_extract_postgres_max_rows_wraps_query() -> None:
    expected = pd.DataFrame({"id": [1]})
    conn = MagicMock()
    with patch("extractor.postgres_extractor.psycopg2.connect", return_value=conn):
        with patch(
            "extractor.postgres_extractor.pd.read_sql_query", return_value=expected
        ) as m:
            extract_postgres(
                "postgresql://localhost/db",
                "SELECT 1",
                max_rows=10,
            )
    sql = m.call_args[0][0]
    assert "LIMIT 10" in sql
    assert "_ingestflow_sub" in sql
