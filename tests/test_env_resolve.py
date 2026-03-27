import os

import pytest

from core.env_resolve import resolve_env_in_obj, resolve_env_placeholders


def test_resolve_env_placeholders_substitutes() -> None:
    os.environ["INGESTFLOW_TEST_TOKEN"] = "secret-value"
    try:
        assert (
            resolve_env_placeholders("Bearer ${INGESTFLOW_TEST_TOKEN}")
            == "Bearer secret-value"
        )
    finally:
        os.environ.pop("INGESTFLOW_TEST_TOKEN", None)


def test_resolve_env_placeholders_missing_raises() -> None:
    os.environ.pop("INGESTFLOW_TEST_MISSING_XYZ", None)
    with pytest.raises(ValueError, match="INGESTFLOW_TEST_MISSING_XYZ"):
        resolve_env_placeholders("${INGESTFLOW_TEST_MISSING_XYZ}")


def test_resolve_env_in_obj_nested() -> None:
    os.environ["INGESTFLOW_TEST_A"] = "a"
    os.environ["INGESTFLOW_TEST_B"] = "b"
    try:
        out = resolve_env_in_obj(
            {"h": {"Authorization": "Bearer ${INGESTFLOW_TEST_A}"}, "x": 1}
        )
        assert out["h"]["Authorization"] == "Bearer a"
        assert out["x"] == 1
        out2 = resolve_env_in_obj({"items": ["${INGESTFLOW_TEST_B}"]})
        assert out2["items"] == ["b"]
    finally:
        os.environ.pop("INGESTFLOW_TEST_A", None)
        os.environ.pop("INGESTFLOW_TEST_B", None)
