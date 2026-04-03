"""PostgreSQL integration when ``INGESTFLOW_TEST_PG_DSN`` is set (CI provides Postgres)."""

from __future__ import annotations

import os

import pytest

from extractor.postgres_extractor import extract_postgres

DSN = os.environ.get("INGESTFLOW_TEST_PG_DSN", "").strip()


@pytest.mark.skipif(
    not DSN,
    reason="INGESTFLOW_TEST_PG_DSN not set (optional local Postgres)",
)
def test_extract_postgres_real_select_literal() -> None:
    df = extract_postgres(DSN, "SELECT 1 AS n")
    assert len(df) == 1
    assert int(df.iloc[0]["n"]) == 1
