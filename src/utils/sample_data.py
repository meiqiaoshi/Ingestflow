from __future__ import annotations

from pathlib import Path

import pandas as pd


def generate_parquet_from_csv(
    *,
    csv_path: str,
    parquet_path: str,
    overwrite: bool = False,
) -> Path:
    parquet_file = Path(parquet_path)
    if parquet_file.exists() and not overwrite:
        return parquet_file

    csv_file = Path(csv_path)
    if not csv_file.exists():
        raise FileNotFoundError(f"CSV not found: {csv_file}")

    parquet_file.parent.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(csv_file)
    df.to_parquet(parquet_file, index=False)
    return parquet_file

