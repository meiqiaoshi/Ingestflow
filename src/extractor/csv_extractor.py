import pandas as pd


def extract_csv(path: str) -> pd.DataFrame:
    df = pd.read_csv(path)
    return df