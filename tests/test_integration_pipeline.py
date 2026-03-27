"""End-to-end tests: real DuckDB file, no mocks for load/metadata (CSV → warehouse)."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest
import yaml

import main


@pytest.fixture
def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def test_full_pipeline_csv_to_duckdb_replace(
    tmp_path: Path, repo_root: Path
) -> None:
    """Loads sample CSV into a temp DuckDB file; asserts table row count and ingestion_runs."""
    csv_path = repo_root / "data" / "sample_orders.csv"
    assert csv_path.is_file(), f"fixture data missing: {csv_path}"

    db_file = tmp_path / "test_wh.duckdb"
    config_path = tmp_path / "pipeline.yaml"
    config = {
        "source": {"type": "csv", "path": str(csv_path.resolve())},
        "target": {
            "type": "duckdb",
            "table": "e2e_raw_orders",
            "db_path": str(db_file),
        },
        "transform": {
            "rename_columns": {"orderid": "order_id"},
            "cast_types": {"amount": "float", "created_at": "datetime"},
        },
        "load": {"mode": "replace"},
    }
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")

    main.run_pipeline(str(config_path), dry_run=False)

    con = duckdb.connect(str(db_file))
    try:
        n = con.execute("SELECT COUNT(*) FROM e2e_raw_orders").fetchone()[0]
        assert int(n) == 3

        runs = con.execute(
            "SELECT status, rows_loaded, target_table FROM ingestion_runs"
        ).fetchall()
        assert len(runs) == 1
        assert runs[0][0] == "success"
        assert runs[0][1] == 3
        assert runs[0][2] == "e2e_raw_orders"
    finally:
        con.close()
