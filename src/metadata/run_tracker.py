import duckdb
from datetime import datetime, timezone
from typing import Optional
import uuid


def create_run_id() -> str:
    return str(uuid.uuid4())


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def ensure_runs_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS ingestion_runs (
            run_id VARCHAR PRIMARY KEY,
            started_at TIMESTAMP,
            finished_at TIMESTAMP,
            status VARCHAR,
            source_path VARCHAR,
            target_table VARCHAR,
            rows_loaded BIGINT,
            error_message VARCHAR,
            load_mode VARCHAR,
            incremental_enabled BOOLEAN,
            db_path VARCHAR,
            config_path VARCHAR
        )
        """
    )
    # Older databases may have been created without the columns below.
    for stmt in (
        "ALTER TABLE ingestion_runs ADD COLUMN IF NOT EXISTS load_mode VARCHAR",
        "ALTER TABLE ingestion_runs ADD COLUMN IF NOT EXISTS incremental_enabled BOOLEAN",
        "ALTER TABLE ingestion_runs ADD COLUMN IF NOT EXISTS db_path VARCHAR",
        "ALTER TABLE ingestion_runs ADD COLUMN IF NOT EXISTS config_path VARCHAR",
    ):
        con.execute(stmt)


def record_run(
    *,
    db_path: str,
    run_id: str,
    started_at: datetime,
    finished_at: datetime,
    status: str,
    source_path: str,
    target_table: str,
    rows_loaded: Optional[int],
    error_message: Optional[str],
    load_mode: str,
    incremental_enabled: bool,
    config_path: str,
) -> None:
    # Keep error strings from growing indefinitely.
    if error_message is not None and len(error_message) > 2000:
        error_message = error_message[:2000] + "...(truncated)"

    con = duckdb.connect(db_path)
    try:
        ensure_runs_table(con)
        con.execute(
            """
            INSERT INTO ingestion_runs (
                run_id, started_at, finished_at, status,
                source_path, target_table, rows_loaded, error_message,
                load_mode, incremental_enabled, db_path, config_path
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                run_id,
                started_at,
                finished_at,
                status,
                source_path,
                target_table,
                rows_loaded,
                error_message,
                load_mode,
                incremental_enabled,
                db_path,
                config_path,
            ],
        )
    finally:
        con.close()

