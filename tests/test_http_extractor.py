import json
from unittest.mock import patch

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


def test_extract_http_post_single_object() -> None:
    payload = {"id": 101, "title": "t"}
    raw = json.dumps(payload).encode("utf-8")

    class FakeResp:
        def read(self):
            return raw

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    with patch("extractor.http_extractor.urllib.request.urlopen", return_value=FakeResp()):
        df = extract_http(
            "https://example.com/x",
            method="POST",
            body={"a": 1},
            allow_single_object=True,
        )
    assert len(df) == 1
    assert df.iloc[0]["id"] == 101


def test_extract_http_pagination_offset_merges_pages() -> None:
    pages = [
        json.dumps([{"n": 1}]),
        json.dumps([{"n": 2}]),
        json.dumps([]),
    ]

    class FakeResp:
        def __init__(self, body: str):
            self._body = body.encode("utf-8")

        def read(self):
            return self._body

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    with patch(
        "extractor.http_extractor.urllib.request.urlopen",
        side_effect=[FakeResp(p) for p in pages],
    ):
        df = extract_http(
            "https://example.com/items",
            pagination={
                "enabled": True,
                "strategy": "offset_query",
                "page_size": 1,
                "max_requests": 10,
                "limit_param": "_limit",
                "offset_param": "_start",
                "start_offset": 0,
            },
        )
    assert len(df) == 2
    assert list(df["n"]) == [1, 2]
