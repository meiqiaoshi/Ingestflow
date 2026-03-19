from __future__ import annotations

from typing import Any, Dict, Iterable, List

import pandas as pd


def _missing_required_columns(df: pd.DataFrame, required_columns: Iterable[str]) -> List[str]:
    return [c for c in required_columns if c not in df.columns]


def validate_data(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    """
    Minimal validation layer (Phase 3 start).

    Currently supported:
    - validation.required_columns: list of required fields
    """
    validation = config.get("validation", {}) or {}

    required_columns = validation.get("required_columns", []) or []
    if required_columns:
        missing = _missing_required_columns(df, required_columns)
        if missing:
            raise ValueError(f"Validation failed: missing required columns: {missing}")

    return df

