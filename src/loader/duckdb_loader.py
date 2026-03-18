import duckdb
import pandas as pd


def load_to_duckdb(df: pd.DataFrame, table: str, mode: str = "replace"):
    con = duckdb.connect("warehouse.duckdb")

    if mode == "replace":
        con.execute(f"DROP TABLE IF EXISTS {table}")
        con.execute(f"CREATE TABLE {table} AS SELECT * FROM df")
    elif mode == "append":
        con.execute(f"INSERT INTO {table} SELECT * FROM df")
    else:
        raise ValueError(f"Unsupported load mode: {mode}")

    con.close()