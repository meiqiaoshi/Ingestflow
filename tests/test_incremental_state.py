import pandas as pd

from loader.incremental_state import filter_incremental_by_watermark


def test_filter_all_rows_when_no_checkpoint() -> None:
    df = pd.DataFrame({"t": ["2024-01-01", "2024-01-02"], "x": [1, 2]})
    df["t"] = pd.to_datetime(df["t"])
    out = filter_incremental_by_watermark(df, "t", None)
    assert len(out) == 2


def test_filter_after_checkpoint() -> None:
    df = pd.DataFrame({"t": ["2024-01-01", "2024-01-05"], "x": [1, 2]})
    df["t"] = pd.to_datetime(df["t"])
    out = filter_incremental_by_watermark(df, "t", "2024-01-03")
    assert len(out) == 1
    assert int(out.iloc[0]["x"]) == 2
