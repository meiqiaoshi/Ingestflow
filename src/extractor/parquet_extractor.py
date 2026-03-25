import pandas as pd


def extract_parquet(path: str) -> pd.DataFrame:
    """Load a Parquet file into a DataFrame (requires pyarrow or fastparquet)."""
    return pd.read_parquet(path)
