from pathlib import Path

import pandas as pd

from utils.sample_data import generate_parquet_from_csv


def test_generate_parquet_from_csv(tmp_path: Path) -> None:
    df = pd.DataFrame({"orderid": [1, 2], "amount": [1.5, 2.0]})
    csv_path = tmp_path / "sample.csv"
    parquet_path = tmp_path / "sample.parquet"

    df.to_csv(csv_path, index=False)

    out = generate_parquet_from_csv(
        csv_path=str(csv_path), parquet_path=str(parquet_path), overwrite=True
    )
    assert Path(out).exists()

    df_out = pd.read_parquet(out)
    pd.testing.assert_frame_equal(df_out, df)

