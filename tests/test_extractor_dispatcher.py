from unittest.mock import MagicMock

import pandas as pd
import pytest

from extractor import dispatcher


def test_extract_source_dispatches_csv(monkeypatch: pytest.MonkeyPatch) -> None:
    expected = pd.DataFrame({"a": [1]})
    mock = MagicMock(return_value=expected)
    monkeypatch.setattr(dispatcher, "extract_csv", mock)
    out = dispatcher.extract_source({"type": "csv", "path": "data/x.csv"})
    mock.assert_called_once_with("data/x.csv")
    pd.testing.assert_frame_equal(out, expected)


def test_extract_source_dispatches_http(monkeypatch: pytest.MonkeyPatch) -> None:
    expected = pd.DataFrame({"id": [1]})
    mock = MagicMock(return_value=expected)
    monkeypatch.setattr(dispatcher, "extract_http", mock)
    out = dispatcher.extract_source(
        {
            "type": "http",
            "url": "https://example.com/api",
            "method": "GET",
            "records_key": "data",
        }
    )
    mock.assert_called_once()
    pd.testing.assert_frame_equal(out, expected)


def test_extract_source_http_resolves_env_in_headers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("INGESTFLOW_DISPATCHER_HDR", "token-123")
    expected = pd.DataFrame({"id": [1]})
    mock = MagicMock(return_value=expected)
    monkeypatch.setattr(dispatcher, "extract_http", mock)
    dispatcher.extract_source(
        {
            "type": "http",
            "url": "https://example.com/api",
            "headers": {"Authorization": "Bearer ${INGESTFLOW_DISPATCHER_HDR}"},
        }
    )
    kwargs = mock.call_args.kwargs
    assert kwargs["headers"] == {"Authorization": "Bearer token-123"}


def test_extract_source_http_bearer_token_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("API_TOKEN", "tok")
    expected = pd.DataFrame({"id": [1]})
    mock = MagicMock(return_value=expected)
    monkeypatch.setattr(dispatcher, "extract_http", mock)
    dispatcher.extract_source(
        {
            "type": "http",
            "url": "https://example.com/api",
            "bearer_token_env": "API_TOKEN",
        }
    )
    assert mock.call_args.kwargs["headers"]["Authorization"] == "Bearer tok"


def test_extract_source_unknown_type() -> None:
    with pytest.raises(NotImplementedError, match="Unsupported source.type"):
        dispatcher.extract_source({"type": "db"})


def test_source_fingerprint() -> None:
    assert dispatcher.source_fingerprint({"type": "csv", "path": "a.csv"}) == "a.csv"
    assert (
        dispatcher.source_fingerprint({"type": "http", "url": "https://x/y"})
        == "https://x/y"
    )
    fp = dispatcher.source_fingerprint(
        {
            "type": "postgres",
            "dsn": "postgresql://localhost/db",
            "query": "SELECT 1",
        }
    )
    assert fp.startswith("postgres:")
    assert len(fp) == len("postgres:") + 16


def test_extract_source_dispatches_postgres(monkeypatch: pytest.MonkeyPatch) -> None:
    expected = pd.DataFrame({"id": [1]})
    mock = MagicMock(return_value=expected)
    monkeypatch.setattr(dispatcher, "extract_postgres", mock)
    out = dispatcher.extract_source(
        {
            "type": "postgres",
            "dsn": "postgresql://localhost/db",
            "query": "SELECT 1",
        }
    )
    mock.assert_called_once()
    pd.testing.assert_frame_equal(out, expected)


def test_extract_source_postgres_table_builds_select_star(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = pd.DataFrame()
    mock = MagicMock(return_value=expected)
    monkeypatch.setattr(dispatcher, "extract_postgres", mock)
    dispatcher.extract_source(
        {
            "type": "postgres",
            "dsn": "postgresql://localhost/db",
            "table": "orders",
            "schema": "public",
        }
    )
    assert mock.call_args[0][1] == 'SELECT * FROM "public"."orders"'
