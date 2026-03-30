"""PostgreSQL read-only extractor (single query → DataFrame)."""

from __future__ import annotations

import pandas as pd
import psycopg2


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
