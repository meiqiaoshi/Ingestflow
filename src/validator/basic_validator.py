from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import pandas.api.types as ptypes


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


def _type_check_column(df: pd.DataFrame, col: str, expected: str) -> Optional[str]:
    """
    Return an error message if column dtype does not match expected kind, else None.

    Expected values (aligned with transform.cast_types): int, float, bool, str, datetime, date.
    """
    if col not in df.columns:
        return f"type_checks column '{col}' not present in dataframe"

    expected = str(expected).lower().strip()
    s = df[col]

    if expected in ("datetime", "date"):
        if ptypes.is_datetime64_any_dtype(s):
            return None
        return f"column '{col}' expected datetime, got dtype {s.dtype}"

    if expected == "int":
        if ptypes.is_integer_dtype(s):
            return None
        return f"column '{col}' expected int, got dtype {s.dtype}"

    if expected == "float":
        if ptypes.is_float_dtype(s) or ptypes.is_integer_dtype(s):
            return None
        return f"column '{col}' expected numeric (float), got dtype {s.dtype}"

    if expected == "bool":
        if ptypes.is_bool_dtype(s):
            return None
        return f"column '{col}' expected bool, got dtype {s.dtype}"

    if expected == "str":
        if ptypes.is_string_dtype(s):
            return None
        if s.dtype == object:
            return None
        return f"column '{col}' expected str, got dtype {s.dtype}"

    return f"unsupported type_checks kind '{expected}' for column '{col}'"


def validate_data(df: pd.DataFrame, config: Dict[str, Any]) -> pd.DataFrame:
    """
    Minimal validation layer (Phase 3 start).

    Currently supported:
    - validation.required_columns: list of required fields
    - validation.null_checks: list of columns that must not contain null/NaN/NaT
    - validation.type_checks: mapping column name -> expected dtype kind (int, float, bool, str, datetime)
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

    type_checks = validation.get("type_checks") or {}
    if type_checks:
        if not isinstance(type_checks, dict):
            raise ValueError("validation.type_checks must be a mapping of column -> expected type")
        errors: List[str] = []
        for col, expected in type_checks.items():
            msg = _type_check_column(df, str(col), expected)
            if msg:
                errors.append(msg)
        if errors:
            raise ValueError("Validation failed: " + "; ".join(errors))

    return df

