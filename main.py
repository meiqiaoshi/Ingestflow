import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from core.config import load_config
from core.logging_config import configure_logging
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

logger = logging.getLogger(__name__)


def run_pipeline(config_path: str, *, dry_run: bool = False) -> None:
    logger.info("Loading config: %s", config_path)
    if dry_run:
        logger.info("Dry run: no writes to DuckDB (load, checkpoints, run metadata)")
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
    primary_key = config.get("load", {}).get("primary_key") or incremental_cfg.get(
        "primary_key"
    )
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

        if dry_run:
            rows_loaded = len(df)
            if incremental_enabled and pipeline_key:
                new_checkpoint = max_checkpoint_value(df, watermark_column)
        else:
            # load
            rows_loaded = load_to_duckdb(
                df,
                table=target["table"],
                mode=load_mode,
                db_path=db_path,
                primary_key=primary_key,
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
        if not dry_run:
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
                    load_mode=load_mode,
                    incremental_enabled=incremental_enabled,
                )
            except Exception:
                # Don't hide the original pipeline error if metadata recording fails.
                pass

    # summary
    duration_s = (utc_now() - started_at).total_seconds()
    logger.info("=== Ingestion Completed ===")
    if dry_run:
        logger.info("Dry run: true (no database writes)")
    logger.info("Run ID: %s", run_id)
    logger.info("Status: %s", status)
    logger.info("Duration (s): %.3f", duration_s)
    logger.info("Source: %s", source["path"])
    logger.info("Target table: %s", target["table"])
    logger.info("Load mode: %s", load_mode)
    if df is not None:
        logger.info("Rows loaded: %s", rows_loaded)
        logger.info("Columns: %s", list(df.columns))
    else:
        logger.info("Rows loaded: %s", rows_loaded)
    if incremental_enabled:
        logger.info("Incremental: enabled")
        logger.info("Watermark column: %s", watermark_column)
        logger.info("Previous checkpoint: %s", last_checkpoint)
        logger.info("New checkpoint: %s", new_checkpoint)

    if status == "failed" and exc_info is not None:
        logger.error("Pipeline failed: %s", error_message[:500] if error_message else "")
        raise exc_info[1].with_traceback(exc_info[2])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="IngestFlow CSV → DuckDB pipeline")
    parser.add_argument("--config", required=True, help="Path to YAML pipeline config")
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Debug logging"
    )
    parser.add_argument(
        "--quiet", "-q", action="store_true", help="Warnings and errors only"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and transform only; do not load, update checkpoints, or write run metadata",
    )
    args = parser.parse_args()

    configure_logging(verbose=args.verbose, quiet=args.quiet)
    run_pipeline(args.config, dry_run=args.dry_run)