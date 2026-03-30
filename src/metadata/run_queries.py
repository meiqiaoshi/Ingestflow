"""Read-only queries against ``ingestion_runs``."""

from __future__ import annotations

from datetime import date, datetime
from typing import List, Optional

import duckdb
import pandas as pd

from metadata.run_tracker import ensure_runs_table


def parse_iso_datetime(value: str) -> datetime:
    """
    Parse an ISO 8601 **date** or **datetime** string (``Z`` suffix allowed).

    Bare dates (``YYYY-MM-DD``) are interpreted as midnight local time.
    """
    v = value.strip()
    if v.endswith("Z"):
        v = v[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(v)
    except ValueError:
        d = date.fromisoformat(v)
        return datetime.combine(d, datetime.min.time())


def list_ingestion_runs(
    db_path: str,
    *,
    limit: int = 20,
    status: Optional[str] = None,
    config_path_contains: Optional[str] = None,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
) -> pd.DataFrame:
    """
    Return recent rows from ``ingestion_runs``, newest ``finished_at`` first.

    ``config_path_contains`` matches if the resolved config path contains the
    substring (case-insensitive).

    ``since`` / ``until`` restrict ``finished_at`` (inclusive bounds).
    """
    limit = max(1, min(int(limit), 500))

    con = duckdb.connect(db_path)
    try:
        ensure_runs_table(con)
        clauses: List[str] = []
        params: List[object] = []

        if status is not None:
            clauses.append("status = ?")
            params.append(status)

        if config_path_contains is not None:
            clauses.append("lower(config_path) LIKE ?")
            params.append(f"%{config_path_contains.lower()}%")

        if since is not None:
            clauses.append("finished_at >= ?")
            params.append(since)
        if until is not None:
            clauses.append("finished_at <= ?")
            params.append(until)

        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        sql = f"""
            SELECT
                run_id,
                started_at,
                finished_at,
                status,
                source_path,
                target_table,
                rows_loaded,
                load_mode,
                incremental_enabled,
                config_path
            FROM ingestion_runs
            {where}
            ORDER BY finished_at DESC NULLS LAST, started_at DESC NULLS LAST
            LIMIT ?
        """
        params.append(limit)
        return con.execute(sql, params).df()
    finally:
        con.close()
