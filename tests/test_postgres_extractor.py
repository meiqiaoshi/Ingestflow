"""PostgreSQL extractor (mocked DB I/O)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pandas as pd

from extractor.postgres_extractor import extract_postgres


def test_extract_postgres_calls_read_sql() -> None:
    expected = pd.DataFrame({"id": [1], "x": ["a"]})
    conn = MagicMock()

    with patch("extractor.postgres_extractor.psycopg2.connect", return_value=conn):
        with patch(
            "extractor.postgres_extractor.pd.read_sql_query", return_value=expected
        ) as m:
            out = extract_postgres("postgresql://u:p@localhost/db", "SELECT 1")

    m.assert_called_once()
    assert m.call_args[0][0] == "SELECT 1"
    assert m.call_args[0][1] is conn
    pd.testing.assert_frame_equal(out, expected)
    conn.close.assert_called_once()
