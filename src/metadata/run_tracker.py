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
            error_message VARCHAR
        )
        """
    )


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
                source_path, target_table, rows_loaded, error_message
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
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
            ],
        )
    finally:
        con.close()

