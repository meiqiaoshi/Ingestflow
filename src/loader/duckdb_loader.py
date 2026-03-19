import duckdb
import pandas as pd


def _table_exists(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    result = con.execute(
        "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
        [table],
    ).fetchone()
    return bool(result and result[0] > 0)


def load_to_duckdb(
    df: pd.DataFrame, table: str, mode: str = "replace", db_path: str = "warehouse.duckdb"
) -> int:
    con = duckdb.connect(db_path)

    try:
        if mode == "replace":
            con.execute(f"DROP TABLE IF EXISTS {table}")
            con.execute(f"CREATE TABLE {table} AS SELECT * FROM df")
        elif mode == "append":
            if not _table_exists(con, table):
                con.execute(f"CREATE TABLE {table} AS SELECT * FROM df")
            else:
                con.execute(f"INSERT INTO {table} SELECT * FROM df")
        else:
            raise ValueError(f"Unsupported load mode: {mode}")
        return len(df)
    finally:
        con.close()