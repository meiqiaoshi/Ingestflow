import pandas as pd


def apply_transformations(df: pd.DataFrame, config: dict) -> pd.DataFrame:
    transform = config.get("transform", {})

    # rename columns
    rename_map = transform.get("rename_columns", {})
    if rename_map:
        df = df.rename(columns=rename_map)

    # cast types
    cast_map = transform.get("cast_types", {})
    for col, dtype in cast_map.items():
        if col in df.columns:
            if dtype == "datetime":
                df[col] = pd.to_datetime(df[col])
            else:
                df[col] = df[col].astype(dtype)

    return df