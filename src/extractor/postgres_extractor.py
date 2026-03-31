"""PostgreSQL read-only extractor (single query → DataFrame)."""

from __future__ import annotations

import re

import pandas as pd
import psycopg2

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _assert_pg_identifier(name: str, field: str) -> None:
    if not isinstance(name, str) or not _IDENTIFIER_RE.match(name):
        raise ValueError(
            f"Invalid {field}: '{name}'. Use letters/numbers/underscore; "
            "do not start with a number."
        )


def postgres_select_star_sql(schema: str, table: str) -> str:
    """Build ``SELECT * FROM "schema"."table"`` with quoted identifiers."""
    _assert_pg_identifier(schema, "source.schema")
    _assert_pg_identifier(table, "source.table")
    return f'SELECT * FROM "{schema}"."{table}"'


def extract_postgres(dsn: str, query: str) -> pd.DataFrame:
    """
    Run ``query`` against PostgreSQL using ``dsn`` (libpq connection string).

    The caller must supply a **trusted** SQL string (operator-controlled config).
    """
    conn = psycopg2.connect(dsn)
    try:
        return pd.read_sql_query(query, conn)
    finally:
        conn.close()
