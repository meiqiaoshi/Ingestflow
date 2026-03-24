"""Integration-style tests for ``main.run_pipeline`` with DuckDB writes mocked."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

import main


@pytest.fixture
def sample_config_path() -> str:
    return str(Path(__file__).resolve().parents[1] / "configs" / "sample.yaml")


def test_dry_run_skips_load_record_run_and_checkpoint_write(
    monkeypatch: pytest.MonkeyPatch, sample_config_path: str
) -> None:
    mock_load = MagicMock()
    mock_record = MagicMock()
    mock_upsert = MagicMock()
    monkeypatch.setattr(main, "load_to_duckdb", mock_load)
    monkeypatch.setattr(main, "record_run", mock_record)
    monkeypatch.setattr(main, "upsert_checkpoint", mock_upsert)
    monkeypatch.setattr(main, "get_last_checkpoint", lambda **kwargs: None)

    main.run_pipeline(sample_config_path, dry_run=True)

    mock_load.assert_not_called()
    mock_record.assert_not_called()
    mock_upsert.assert_not_called()


def test_normal_run_calls_load_and_record_run(
    monkeypatch: pytest.MonkeyPatch, sample_config_path: str
) -> None:
    mock_load = MagicMock(return_value=3)
    mock_record = MagicMock()
    mock_upsert = MagicMock()
    monkeypatch.setattr(main, "load_to_duckdb", mock_load)
    monkeypatch.setattr(main, "record_run", mock_record)
    monkeypatch.setattr(main, "upsert_checkpoint", mock_upsert)
    monkeypatch.setattr(main, "get_last_checkpoint", lambda **kwargs: None)

    main.run_pipeline(sample_config_path, dry_run=False)

    mock_load.assert_called_once()
    mock_record.assert_called_once()
    kwargs = mock_record.call_args.kwargs
    assert kwargs["status"] == "success"
    assert kwargs["load_mode"] == "upsert"
    assert kwargs["incremental_enabled"] is True
    assert kwargs["db_path"] == "warehouse.duckdb"
    mock_upsert.assert_called()
