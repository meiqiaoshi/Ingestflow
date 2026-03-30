"""End-to-end incremental: two pipeline runs share one CSV path; checkpoint filters new rows."""

from __future__ import annotations

from pathlib import Path

import duckdb
import yaml

import main


def _write_csv(path: Path, content: str) -> None:
    path.write_text(content.strip() + "\n", encoding="utf-8")


def test_incremental_second_run_loads_only_new_rows(tmp_path: Path) -> None:
    csv_path = tmp_path / "orders.csv"
    db_file = tmp_path / "inc.duckdb"
    config_path = tmp_path / "incremental.yaml"

    _write_csv(
        csv_path,
        """
orderid,amount,created_at
1,10.0,2024-01-01
2,20.0,2024-01-02
""",
    )

    config = {
        "source": {"type": "csv", "path": str(csv_path.resolve())},
        "target": {
            "type": "duckdb",
            "table": "inc_orders",
            "db_path": str(db_file),
        },
        "transform": {
            "rename_columns": {"orderid": "order_id"},
            "cast_types": {"amount": "float", "created_at": "datetime"},
        },
        "load": {
            "mode": "upsert",
            "primary_key": "order_id",
            "incremental": {
                "enabled": True,
                "watermark_column": "created_at",
            },
        },
    }
    config_path.write_text(yaml.safe_dump(config), encoding="utf-8")

    main.run_pipeline(str(config_path), dry_run=False)

    con = duckdb.connect(str(db_file))
    try:
        n1 = con.execute("SELECT COUNT(*) FROM inc_orders").fetchone()[0]
        assert int(n1) == 2
        ck1 = con.execute(
            "SELECT last_checkpoint FROM ingestion_state"
        ).fetchone()
        assert ck1 is not None
        assert ck1[0] is not None
    finally:
        con.close()

    _write_csv(
        csv_path,
        """
orderid,amount,created_at
1,10.0,2024-01-01
2,20.0,2024-01-02
3,30.0,2024-01-03
""",
    )

    main.run_pipeline(str(config_path), dry_run=False)

    con = duckdb.connect(str(db_file))
    try:
        n2 = con.execute("SELECT COUNT(*) FROM inc_orders").fetchone()[0]
        assert int(n2) == 3

        runs = con.execute(
            "SELECT rows_loaded FROM ingestion_runs ORDER BY finished_at"
        ).fetchall()
        assert len(runs) == 2
        assert runs[0][0] == 2
        assert runs[1][0] == 1

        ck2 = con.execute(
            "SELECT last_checkpoint FROM ingestion_state"
        ).fetchone()
        assert ck2 is not None
        assert "2024-01-03" in str(ck2[0])
    finally:
        con.close()
