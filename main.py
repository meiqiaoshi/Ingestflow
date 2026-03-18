import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from core.config import load_config
from extractor.csv_extractor import extract_csv
from transformer.basic_transformer import apply_transformations
from loader.duckdb_loader import load_to_duckdb


def run_pipeline(config_path: str):
    config = load_config(config_path)

    # extract
    source = config["source"]
    if source["type"] != "csv":
        raise NotImplementedError("Only CSV source is supported in Phase 1")

    df = extract_csv(source["path"])

    # transform
    df = apply_transformations(df, config)

    # load
    target = config["target"]
    load_mode = config.get("load", {}).get("mode", "replace")

    load_to_duckdb(df, table=target["table"], mode=load_mode)

    # summary
    print("\n=== Ingestion Completed ===")
    print(f"Source: {source['path']}")
    print(f"Target table: {target['table']}")
    print(f"Rows loaded: {len(df)}")
    print(f"Columns: {list(df.columns)}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    run_pipeline(args.config)