import duckdb
import pandas as pd
from typing import Optional


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


def load_to_duckdb(
    df: pd.DataFrame,
    table: str,
    mode: str = "replace",
    db_path: str = "warehouse.duckdb",
    primary_key: Optional[str] = None,
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
            if not primary_key:
                raise ValueError("Upsert mode requires 'primary_key'")
            if primary_key not in df.columns:
                raise ValueError(
                    f"Upsert mode failed: primary_key '{primary_key}' not found in dataframe"
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
            con.execute(
                f"DELETE FROM {table} WHERE {primary_key} IN (SELECT {primary_key} FROM df)"
            )
            con.execute(f"INSERT INTO {table} SELECT * FROM df")
            return len(df)

        else:
            raise ValueError(f"Unsupported load mode: {mode}")
    finally:
        con.close()