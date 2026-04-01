"""Structured JSON run summary line."""

from __future__ import annotations

import json
from datetime import datetime, timezone

from core.run_summary_json import emit_run_summary_json


def test_emit_run_summary_json_line(capsys) -> None:
    started = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    finished = datetime(2024, 1, 1, 12, 0, 1, tzinfo=timezone.utc)
    emit_run_summary_json(
        run_id="rid",
        config_path="/cfg/p.yaml",
        status="success",
        started_at=started,
        finished_at=finished,
        duration_seconds=1.0,
        rows_loaded=3,
        target_table="t1",
        load_mode="replace",
        incremental_enabled=False,
        dry_run=False,
        source_type="csv",
        db_path="w.duckdb",
    )
    err = capsys.readouterr().err.strip()
    row = json.loads(err)
    assert row["event"] == "ingestflow_run"
    assert row["run_id"] == "rid"
    assert row["status"] == "success"
    assert row["rows_loaded"] == 3
    assert "error" not in row


def test_emit_includes_error_on_failure(capsys) -> None:
    started = datetime(2024, 1, 1, tzinfo=timezone.utc)
    emit_run_summary_json(
        run_id="r",
        config_path="/c.yaml",
        status="failed",
        started_at=started,
        finished_at=started,
        duration_seconds=0.1,
        rows_loaded=None,
        target_table="t",
        load_mode="append",
        incremental_enabled=False,
        dry_run=False,
        source_type="csv",
        db_path="w.duckdb",
        error_message="boom",
    )
    row = json.loads(capsys.readouterr().err.strip())
    assert row["status"] == "failed"
    assert row["error"] == "boom"
