from __future__ import annotations

from datetime import datetime
from typing import Optional

import duckdb
import pandas as pd


def ensure_incremental_state_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS ingestion_state (
            pipeline_key VARCHAR PRIMARY KEY,
            watermark_column VARCHAR,
            last_checkpoint VARCHAR,
            updated_at TIMESTAMP
        )
        """
    )


def get_last_checkpoint(db_path: str, pipeline_key: str) -> Optional[str]:
    con = duckdb.connect(db_path)
    try:
        ensure_incremental_state_table(con)
        row = con.execute(
            "SELECT last_checkpoint FROM ingestion_state WHERE pipeline_key = ?",
            [pipeline_key],
        ).fetchone()
        return row[0] if row else None
    finally:
        con.close()


def upsert_checkpoint(
    db_path: str, pipeline_key: str, watermark_column: str, checkpoint_value: str
) -> None:
    con = duckdb.connect(db_path)
    try:
        ensure_incremental_state_table(con)
        con.execute(
            """
            INSERT INTO ingestion_state (pipeline_key, watermark_column, last_checkpoint, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (pipeline_key) DO UPDATE
            SET watermark_column = EXCLUDED.watermark_column,
                last_checkpoint = EXCLUDED.last_checkpoint,
                updated_at = EXCLUDED.updated_at
            """,
            [pipeline_key, watermark_column, checkpoint_value, datetime.utcnow()],
        )
    finally:
        con.close()


def filter_incremental_by_watermark(
    df: pd.DataFrame, watermark_column: str, last_checkpoint: Optional[str]
) -> pd.DataFrame:
    if watermark_column not in df.columns:
        raise ValueError(
            f"Incremental load failed: watermark column '{watermark_column}' not found in dataframe"
        )

    if last_checkpoint is None:
        return df

    watermark_series = pd.to_datetime(df[watermark_column], errors="coerce")
    checkpoint = pd.to_datetime(last_checkpoint)
    return df[watermark_series > checkpoint].copy()


def max_checkpoint_value(df: pd.DataFrame, watermark_column: str) -> Optional[str]:
    if df.empty:
        return None
    max_value = pd.to_datetime(df[watermark_column], errors="coerce").max()
    if pd.isna(max_value):
        return None
    return max_value.isoformat()

