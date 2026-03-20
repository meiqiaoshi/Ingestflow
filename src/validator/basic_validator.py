from __future__ import annotations

from typing import Any, Dict, Iterable, List, Tuple

import pandas as pd


def _missing_required_columns(df: pd.DataFrame, required_columns: Iterable[str]) -> List[str]:
    return [c for c in required_columns if c not in df.columns]


def _null_violations(df: pd.DataFrame, columns: Iterable[str]) -> List[Tuple[str, int]]:
    """Return (column_name, null_count) for columns that have at least one null."""
    bad: List[Tuple[str, int]] = []
    for col in columns:
        if col not in df.columns:
            raise ValueError(
                f"Validation failed: null_checks column '{col}' not present in dataframe"
            )
        null_mask = df[col].isna()
        n = int(null_mask.sum())
        if n > 0:
            bad.append((col, n))
    return bad


def validate_data(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    """
    Minimal validation layer (Phase 3 start).

    Currently supported:
    - validation.required_columns: list of required fields
    - validation.null_checks: list of columns that must not contain null/NaN/NaT
    """
    validation = config.get("validation", {}) or {}

    required_columns = validation.get("required_columns", []) or []
    if required_columns:
        missing = _missing_required_columns(df, required_columns)
        if missing:
            raise ValueError(f"Validation failed: missing required columns: {missing}")

    null_checks = validation.get("null_checks", []) or []
    if null_checks:
        violations = _null_violations(df, null_checks)
        if violations:
            parts = [f"{col} ({n} nulls)" for col, n in violations]
            raise ValueError(
                f"Validation failed: null values found in columns: {', '.join(parts)}"
            )

    return df

