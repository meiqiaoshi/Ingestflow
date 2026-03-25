import pandas as pd

from extractor.parquet_extractor import extract_parquet


def test_extract_parquet_reads_roundtrip(tmp_path) -> None:
    df_in = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    path = tmp_path / "t.parquet"
    df_in.to_parquet(path, index=False)
    df_out = extract_parquet(str(path))
    pd.testing.assert_frame_equal(df_out, df_in)
