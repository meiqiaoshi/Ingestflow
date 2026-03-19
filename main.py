import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from core.config import load_config
from extractor.csv_extractor import extract_csv
from transformer.basic_transformer import apply_transformations
from loader.duckdb_loader import load_to_duckdb
from metadata.run_tracker import create_run_id, record_run, utc_now


def run_pipeline(config_path: str):
    config = load_config(config_path)

    run_id = create_run_id()
    started_at = utc_now()

    # extract
    source = config["source"]
    if source["type"] != "csv":
        raise NotImplementedError("Only CSV source is supported in Phase 1")

    target = config["target"]
    load_mode = config.get("load", {}).get("mode", "replace")

    df = None
    status = "success"
    rows_loaded = None
    error_message = None

    try:
        df = extract_csv(source["path"])

        # transform
        df = apply_transformations(df, config)

        # load
        load_to_duckdb(df, table=target["table"], mode=load_mode)
        rows_loaded = len(df)
    except Exception as e:
        status = "failed"
        error_message = str(e)
        raise
    finally:
        finished_at = utc_now()
        try:
            record_run(
                db_path="warehouse.duckdb",
                run_id=run_id,
                started_at=started_at,
                finished_at=finished_at,
                status=status,
                source_path=source["path"],
                target_table=target["table"],
                rows_loaded=rows_loaded,
                error_message=error_message,
            )
        except Exception:
            # Don't hide the original pipeline error if metadata recording fails.
            pass

    # summary
    duration_s = (utc_now() - started_at).total_seconds()
    print("\n=== Ingestion Completed ===")
    print(f"Run ID: {run_id}")
    print(f"Status: {status}")
    print(f"Duration (s): {duration_s:.3f}")
    print(f"Source: {source['path']}")
    print(f"Target table: {target['table']}")
    if df is not None:
        print(f"Rows loaded: {rows_loaded}")
        print(f"Columns: {list(df.columns)}\n")
    else:
        print(f"Rows loaded: {rows_loaded}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    run_pipeline(args.config)