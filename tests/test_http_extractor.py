import json
from unittest.mock import patch

import pandas as pd

from extractor.http_extractor import extract_http


def test_extract_http_json_array() -> None:
    payload = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
    raw = json.dumps(payload).encode("utf-8")

    class FakeResp:
        def read(self):
            return raw

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    with patch("extractor.http_extractor.urllib.request.urlopen", return_value=FakeResp()):
        df = extract_http("https://example.com/x")
    assert len(df) == 2
    assert list(df.columns) == ["id", "name"]


def test_extract_http_records_key() -> None:
    payload = {"data": [{"x": 1}]}
    raw = json.dumps(payload).encode("utf-8")

    class FakeResp:
        def read(self):
            return raw

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    with patch("extractor.http_extractor.urllib.request.urlopen", return_value=FakeResp()):
        df = extract_http("https://example.com/x", records_key="data")
    assert len(df) == 1
    assert df.iloc[0]["x"] == 1
