"""``main._source_label`` for run metadata (postgres query vs table)."""

from __future__ import annotations

import main


def test_source_label_postgres_query() -> None:
    assert (
        main._source_label(
            {"type": "postgres", "dsn": "postgresql://x", "query": "SELECT 1"}
        )
        == "postgres:SELECT 1"
    )


def test_source_label_postgres_schema_table() -> None:
    assert (
        main._source_label(
            {
                "type": "postgres",
                "dsn": "postgresql://x",
                "schema": "ingestflow_e2e_schema",
                "table": "tbl_from_table",
            }
        )
        == "postgres:ingestflow_e2e_schema.tbl_from_table"
    )


def test_source_label_postgres_table_defaults_schema_public() -> None:
    assert (
        main._source_label(
            {"type": "postgres", "dsn": "postgresql://x", "table": "orders"}
        )
        == "postgres:public.orders"
    )


def test_source_label_postgres_long_query_truncates() -> None:
    q = "x" * 200
    out = main._source_label(
        {"type": "postgres", "dsn": "postgresql://x", "query": q}
    )
    assert out == "postgres:" + "x" * 120 + "..."
