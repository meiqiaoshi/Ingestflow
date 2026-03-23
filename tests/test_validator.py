import pandas as pd
import pytest

from validator.basic_validator import validate_data


def test_required_columns_missing() -> None:
    df = pd.DataFrame({"a": [1]})
    with pytest.raises(ValueError, match="missing required columns"):
        validate_data(df, {"validation": {"required_columns": ["b"]}})


def test_null_checks_detects_null() -> None:
    df = pd.DataFrame({"a": [1, None]})
    with pytest.raises(ValueError, match="null values found"):
        validate_data(df, {"validation": {"null_checks": ["a"]}})


def test_type_checks_int_ok() -> None:
    df = pd.DataFrame({"a": [1, 2]})
    validate_data(df, {"validation": {"type_checks": {"a": "int"}}})
