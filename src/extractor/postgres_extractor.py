"""PostgreSQL read-only extractor (single query → DataFrame)."""

from __future__ import annotations

import re
from typing import Optional

import pandas as pd
import psycopg2

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _assert_pg_identifier(name: str, field: str) -> None:
    if not isinstance(name, str) or not _IDENTIFIER_RE.match(name):
        raise ValueError(
            f"Invalid {field}: '{name}'. Use letters/numbers/underscore; "
            "do not start with a number."
        )


def postgres_select_star_sql(
    schema: str, table: str, *, max_rows: Optional[int] = None
) -> str:
    """Build ``SELECT * FROM "schema"."table"`` with quoted identifiers."""
    _assert_pg_identifier(schema, "source.schema")
    _assert_pg_identifier(table, "source.table")
    base = f'SELECT * FROM "{schema}"."{table}"'
    if max_rows is not None:
        return f"{base} LIMIT {int(max_rows)}"
    return base


def extract_postgres(
    dsn: str,
    query: str,
    *,
    statement_timeout_ms: Optional[int] = None,
    max_rows: Optional[int] = None,
) -> pd.DataFrame:
    """
    Run ``query`` against PostgreSQL using ``dsn`` (libpq connection string).

    The caller must supply a **trusted** SQL string (operator-controlled config).

    ``statement_timeout_ms`` sets PostgreSQL ``statement_timeout`` (milliseconds).

    If ``max_rows`` is set, the SQL is wrapped as a subquery and ``LIMIT`` is applied
    (for custom ``query`` mode; table shortcut may already include ``LIMIT``).
    """
    inner = query.rstrip().rstrip(";")
    if max_rows is not None:
        final_sql = (
            f"SELECT * FROM ({inner}) AS _ingestflow_sub LIMIT {int(max_rows)}"
        )
    else:
        final_sql = inner

    conn = psycopg2.connect(dsn)
    try:
        cur = conn.cursor()
        if statement_timeout_ms is not None:
            cur.execute("SET statement_timeout = %s", [int(statement_timeout_ms)])
        cur.close()
        return pd.read_sql_query(final_sql, conn)
    finally:
        conn.close()
