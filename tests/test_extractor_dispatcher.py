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


def test_extract_source_unknown_type() -> None:
    with pytest.raises(NotImplementedError, match="Unsupported source.type"):
        dispatcher.extract_source({"type": "db"})


def test_source_fingerprint() -> None:
    assert dispatcher.source_fingerprint({"type": "csv", "path": "a.csv"}) == "a.csv"
    assert (
        dispatcher.source_fingerprint({"type": "http", "url": "https://x/y"})
        == "https://x/y"
    )
