from __future__ import annotations

import duckdb
import pandas as pd
from typing import List, Optional, Sequence, Union


def _table_exists(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    result = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [table],
    ).fetchone()
    return bool(result and result[0] > 0)


def _create_empty_table_like_df(
    con: duckdb.DuckDBPyConnection, df: pd.DataFrame, table: str
) -> None:
    # DuckDB infers schema from the dataframe; this creates an empty table.
    con.execute(f"CREATE TABLE {table} AS SELECT * FROM df WHERE 1=0")


def _normalize_primary_key(
    primary_key: Optional[Union[str, Sequence[str]]],
) -> List[str]:
    if primary_key is None:
        return []
    if isinstance(primary_key, str):
        return [primary_key]
    if isinstance(primary_key, (list, tuple)):
        return [str(c) for c in primary_key]
    raise ValueError(
        f"primary_key must be a string or list of column names, got {type(primary_key)}"
    )


def _upsert_delete_sql(table: str, pk_cols: List[str]) -> str:
    """
    Delete target rows whose composite primary key matches any row in the incoming `df`.

    Uses EXISTS for compatibility with single- and multi-column keys in DuckDB.
    """
    join_conds = " AND ".join([f"t.{c} = d.{c}" for c in pk_cols])
    return (
        f"DELETE FROM {table} AS t WHERE EXISTS (SELECT 1 FROM df AS d WHERE {join_conds})"
    )


def load_to_duckdb(
    df: pd.DataFrame,
    table: str,
    mode: str = "replace",
    db_path: str = "warehouse.duckdb",
    primary_key: Optional[Union[str, Sequence[str]]] = None,
) -> int:
    con = duckdb.connect(db_path)

    try:
        if mode == "replace":
            con.execute(f"DROP TABLE IF EXISTS {table}")
            con.execute(f"CREATE TABLE {table} AS SELECT * FROM df")
            return len(df)

        if mode == "append":
            if not _table_exists(con, table):
                if len(df) == 0:
                    _create_empty_table_like_df(con, df, table)
                else:
                    con.execute(f"CREATE TABLE {table} AS SELECT * FROM df")
            if len(df) == 0:
                return 0
            con.execute(f"INSERT INTO {table} SELECT * FROM df")
            return len(df)

        if mode == "upsert":
            pk_cols = _normalize_primary_key(primary_key)
            if not pk_cols:
                raise ValueError("Upsert mode requires 'primary_key' (string or list of columns)")
            for col in pk_cols:
                if col not in df.columns:
                    raise ValueError(
                        f"Upsert mode failed: primary_key column '{col}' not found in dataframe"
                    )

            if not _table_exists(con, table):
                if len(df) == 0:
                    _create_empty_table_like_df(con, df, table)
                else:
                    con.execute(f"CREATE TABLE {table} AS SELECT * FROM df")
            if len(df) == 0:
                return 0

            # Minimal upsert strategy:
            # 1) delete existing rows whose primary keys are present in incoming df
            # 2) insert the incoming df
            con.execute(_upsert_delete_sql(table, pk_cols))
            con.execute(f"INSERT INTO {table} SELECT * FROM df")
            return len(df)

        else:
            raise ValueError(f"Unsupported load mode: {mode}")
    finally:
        con.close()