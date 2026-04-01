"""Single-line JSON summary for log aggregation (stderr)."""

from __future__ import annotations

import json
import sys
from datetime import datetime
from typing import Any, Dict, Optional


def emit_run_summary_json(
    *,
    run_id: str,
    config_path: str,
    status: str,
    started_at: datetime,
    finished_at: datetime,
    duration_seconds: float,
    rows_loaded: Optional[int],
    target_table: str,
    load_mode: str,
    incremental_enabled: bool,
    dry_run: bool,
    source_type: str,
    db_path: str,
    error_message: Optional[str] = None,
) -> None:
    """
    Print one JSON object to **stderr** (no logging formatter), for Loki/ELK/etc.

    Field ``event`` is always ``ingestflow_run`` so filters can route these lines.
    """
    payload: Dict[str, Any] = {
        "event": "ingestflow_run",
        "run_id": run_id,
        "config_path": config_path,
        "status": status,
        "started_at": started_at.isoformat(),
        "finished_at": finished_at.isoformat(),
        "duration_seconds": round(duration_seconds, 6),
        "rows_loaded": rows_loaded,
        "target_table": target_table,
        "load_mode": load_mode,
        "incremental_enabled": incremental_enabled,
        "dry_run": dry_run,
        "source_type": source_type,
        "db_path": db_path,
    }
    if error_message:
        payload["error"] = error_message[:2000]
    print(json.dumps(payload, ensure_ascii=False), file=sys.stderr, flush=True)
