import argparse
import csv
import logging
import sys
from pathlib import Path
from typing import TextIO

from dotenv import load_dotenv

# Repo layout: packages live under ``src/``. When installed, they are on sys.path already.
_src = Path(__file__).resolve().parent / "src"
if _src.is_dir():
    sys.path.insert(0, str(_src))

from core.config import load_config
from core.logging_config import configure_logging
from core.run_summary_json import emit_run_summary_json
from extractor.dispatcher import extract_source, source_fingerprint
from transformer.basic_transformer import apply_transformations
from loader.duckdb_loader import load_to_duckdb
from loader.incremental_state import (
    filter_incremental_by_watermark,
    get_last_checkpoint,
    max_checkpoint_value,
    upsert_checkpoint,
)
from metadata.run_queries import list_ingestion_runs, parse_iso_datetime
from metadata.run_tracker import create_run_id, record_run, utc_now
from validator.basic_validator import validate_data

logger = logging.getLogger(__name__)

_RUNS_LIST_CSV_COLUMNS = [
    "run_id",
    "started_at",
    "finished_at",
    "status",
    "source_path",
    "target_table",
    "rows_loaded",
    "error_message",
    "load_mode",
    "incremental_enabled",
    "db_path",
    "config_path",
]


def _source_label(source: dict) -> str:
    """Human-readable source description for logs and run metadata."""
    stype = source.get("type")
    if stype in ("csv", "parquet"):
        return str(source.get("path") or "")
    if stype == "http":
        return str(source.get("url") or "")
    if stype == "postgres":
        q = str(source.get("query") or "").strip()
        if q:
            suffix = q[:120] + ("..." if len(q) > 120 else "")
        else:
            schema = str(source.get("schema", "public")).strip() or "public"
            tbl = str(source.get("table") or "").strip()
            if tbl:
                hint = f"{schema}.{tbl}"
                suffix = hint[:120] + ("..." if len(hint) > 120 else "")
            else:
                suffix = ""
        return "postgres:" + suffix
    return str(source.get("path") or source.get("url") or "")


def run_pipeline(
    config_path: str,
    *,
    dry_run: bool = False,
    json_summary: bool = True,
) -> None:
    resolved_config_path = str(Path(config_path).resolve())
    logger.info("Loading config: %s", config_path)
    if dry_run:
        logger.info("Dry run: no writes to DuckDB (load, checkpoints, run metadata)")
    config = load_config(config_path)

    run_id = create_run_id()
    started_at = utc_now()

    # extract
    source = config["source"]
    src_type = source["type"]
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
        df = extract_source(source)

        # transform
        df = apply_transformations(df, config)

        # validate (Phase 3 start)
        df = validate_data(df, config)

        if incremental_enabled:
            if not watermark_column:
                raise ValueError(
                    "Incremental load requires 'load.incremental.watermark_column'"
                )

            pipeline_key = (
                f"{src_type}:{source_fingerprint(source)}::"
                f"{target['table']}::{watermark_column}"
            )
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
                    source_path=_source_label(source),
                    target_table=target["table"],
                    rows_loaded=rows_loaded,
                    error_message=error_message,
                    load_mode=load_mode,
                    incremental_enabled=incremental_enabled,
                    config_path=resolved_config_path,
                )
            except Exception:
                # Don't hide the original pipeline error if metadata recording fails.
                pass

    # summary
    finished_at = utc_now()
    duration_s = (finished_at - started_at).total_seconds()
    logger.info("=== Ingestion Completed ===")
    if dry_run:
        logger.info("Dry run: true (no database writes)")
    logger.info("Run ID: %s", run_id)
    logger.info("Status: %s", status)
    logger.info("Duration (s): %.3f", duration_s)
    logger.info("Source: %s", _source_label(source))
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

    if json_summary:
        emit_run_summary_json(
            run_id=run_id,
            config_path=resolved_config_path,
            status=status,
            started_at=started_at,
            finished_at=finished_at,
            duration_seconds=duration_s,
            rows_loaded=rows_loaded,
            target_table=target["table"],
            load_mode=load_mode,
            incremental_enabled=incremental_enabled,
            dry_run=dry_run,
            source_type=src_type,
            db_path=db_path,
            error_message=error_message,
        )

    if status == "failed" and exc_info is not None:
        logger.error("Pipeline failed: %s", error_message[:500] if error_message else "")
        raise exc_info[1].with_traceback(exc_info[2])


def _ensure_run_subcommand(argv: list[str]) -> list[str]:
    """Legacy: ``python main.py --config ...`` → ``python main.py run --config ...``."""
    if len(argv) < 2:
        return argv
    if argv[1] in ("-h", "--help", "runs", "run"):
        return argv
    if "--config" in argv[1:]:
        return [argv[0], "run"] + argv[1:]
    return argv


def _write_runs_df(df, fmt: str, stream: TextIO) -> None:
    """Write non-empty run rows to ``stream`` (``table`` | ``json`` | ``csv``)."""
    if fmt == "json":
        stream.write(
            df.to_json(
                orient="records",
                date_format="iso",
                default_handler=str,
            )
            + "\n"
        )
        return
    if fmt == "csv":
        df.to_csv(stream, index=False, lineterminator="\n")
        return
    stream.write(df.to_string(index=False) + "\n")


def _cmd_runs_list(args: argparse.Namespace) -> None:
    db_path = args.db
    if not Path(db_path).resolve().exists():
        print(f"No database file at {db_path!r}", file=sys.stderr)
        sys.exit(1)
    since = parse_iso_datetime(args.since) if args.since else None
    until = parse_iso_datetime(args.until) if args.until else None
    if since is not None and until is not None and since > until:
        print("--since must be on or before --until", file=sys.stderr)
        sys.exit(2)
    df = list_ingestion_runs(
        db_path,
        limit=args.limit,
        status=args.status,
        config_path_contains=args.config_contains,
        since=since,
        until=until,
    )
    out_fmt = args.format
    out_path = args.output

    def _emit(stream: TextIO, *, to_file: bool) -> None:
        if df.empty:
            if out_fmt == "json":
                stream.write("[]\n")
            elif out_fmt == "csv":
                w = csv.writer(stream, lineterminator="\n")
                w.writerow(_RUNS_LIST_CSV_COLUMNS)
            else:
                if not to_file:
                    print("(no rows)", file=sys.stderr)
            return
        _write_runs_df(df, out_fmt, stream)

    if out_path:
        with open(out_path, "w", encoding="utf-8") as f:
            _emit(f, to_file=True)
    else:
        _emit(sys.stdout, to_file=False)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="IngestFlow — config-driven ingestion into DuckDB",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_run = sub.add_parser("run", help="Run ingestion pipeline")
    p_run.add_argument(
        "--config", required=True, help="Path to YAML pipeline config"
    )
    p_run.add_argument(
        "--verbose", "-v", action="store_true", help="Debug logging"
    )
    p_run.add_argument(
        "--quiet", "-q", action="store_true", help="Warnings and errors only"
    )
    p_run.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and transform only; do not load, update checkpoints, or write run metadata",
    )
    p_run.add_argument(
        "--no-json-summary",
        action="store_true",
        help="Do not print the structured JSON summary line to stderr on completion",
    )

    p_runs = sub.add_parser("runs", help="Inspect ingestion run history")
    runs_sub = p_runs.add_subparsers(dest="runs_cmd", required=True)
    p_list = runs_sub.add_parser("list", help="List recent runs from ingestion_runs")
    p_list.add_argument(
        "--db",
        default="warehouse.duckdb",
        help="DuckDB file path (default: warehouse.duckdb)",
    )
    p_list.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Max rows to print (default: 20, max: 500)",
    )
    p_list.add_argument(
        "--status",
        help="Filter by status (e.g. success, failed)",
    )
    p_list.add_argument(
        "--config-contains",
        dest="config_contains",
        metavar="TEXT",
        help="Filter rows whose config_path contains TEXT (case-insensitive)",
    )
    p_list.add_argument(
        "--since",
        metavar="WHEN",
        help="Include runs with finished_at >= WHEN (ISO 8601 date or datetime)",
    )
    p_list.add_argument(
        "--until",
        metavar="WHEN",
        help="Include runs with finished_at <= WHEN (ISO 8601 date or datetime)",
    )
    p_list.add_argument(
        "--format",
        choices=("table", "json", "csv"),
        default="table",
        help="Output format (default: table)",
    )
    p_list.add_argument(
        "--output",
        "-o",
        metavar="PATH",
        help="Write the same output to this file instead of stdout",
    )
    return parser


def main() -> None:
    load_dotenv()
    argv = _ensure_run_subcommand(sys.argv)
    parser = _build_parser()
    args = parser.parse_args(argv[1:])

    if args.command == "run":
        configure_logging(verbose=args.verbose, quiet=args.quiet)
        run_pipeline(
            args.config,
            dry_run=args.dry_run,
            json_summary=not args.no_json_summary,
        )
    elif args.command == "runs":
        if args.runs_cmd == "list":
            _cmd_runs_list(args)
        else:
            parser.error("Unknown runs subcommand")
    else:
        parser.error("Unknown command")


if __name__ == "__main__":
    main()