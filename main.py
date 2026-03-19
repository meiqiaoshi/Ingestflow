import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from core.config import load_config
from extractor.csv_extractor import extract_csv
from transformer.basic_transformer import apply_transformations
from loader.duckdb_loader import load_to_duckdb
from loader.incremental_state import (
    filter_incremental_by_watermark,
    get_last_checkpoint,
    max_checkpoint_value,
    upsert_checkpoint,
)
from metadata.run_tracker import create_run_id, record_run, utc_now
from validator.basic_validator import validate_data


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
    db_path = config.get("target", {}).get("db_path", "warehouse.duckdb")
    incremental_cfg = config.get("load", {}).get("incremental", {}) or {}
    incremental_enabled = bool(incremental_cfg.get("enabled", False))
    watermark_column = incremental_cfg.get("watermark_column")
    pipeline_key = None
    last_checkpoint = None
    new_checkpoint = None

    df = None
    status = "success"
    rows_loaded = None
    error_message = None
    exc_info = None

    try:
        df = extract_csv(source["path"])

        # transform
        df = apply_transformations(df, config)

        # validate (Phase 3 start)
        df = validate_data(df, config)

        if incremental_enabled:
            if not watermark_column:
                raise ValueError(
                    "Incremental load requires 'load.incremental.watermark_column'"
                )

            pipeline_key = f"{source['path']}::{target['table']}::{watermark_column}"
            last_checkpoint = get_last_checkpoint(db_path=db_path, pipeline_key=pipeline_key)
            df = filter_incremental_by_watermark(
                df=df, watermark_column=watermark_column, last_checkpoint=last_checkpoint
            )

        # load
        rows_loaded = load_to_duckdb(
            df, table=target["table"], mode=load_mode, db_path=db_path
        )

        if incremental_enabled and pipeline_key:
            new_checkpoint = max_checkpoint_value(df, watermark_column)
            if new_checkpoint is not None:
                upsert_checkpoint(
                    db_path=db_path,
                    pipeline_key=pipeline_key,
                    watermark_column=watermark_column,
                    checkpoint_value=new_checkpoint,
                )
    except Exception as e:
        status = "failed"
        error_message = str(e)
        # If we have a dataframe at this point, capture the row count for reporting.
        if df is not None:
            rows_loaded = len(df)
        exc_info = sys.exc_info()
    finally:
        finished_at = utc_now()
        try:
            record_run(
                db_path=db_path,
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
    print(f"Load mode: {load_mode}")
    if df is not None:
        print(f"Rows loaded: {rows_loaded}")
        print(f"Columns: {list(df.columns)}\n")
    else:
        print(f"Rows loaded: {rows_loaded}\n")
    if incremental_enabled:
        print("Incremental: enabled")
        print(f"Watermark column: {watermark_column}")
        print(f"Previous checkpoint: {last_checkpoint}")
        print(f"New checkpoint: {new_checkpoint}\n")

    if status == "failed" and error_message:
        print(f"Error: {error_message[:500]}")

    if status == "failed" and exc_info is not None:
        # Print summary first, then re-raise to ensure CI / callers see a failure.
        raise exc_info[1].with_traceback(exc_info[2])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    run_pipeline(args.config)