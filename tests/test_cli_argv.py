"""CLI argv normalization for legacy ``--config`` invocations."""

import main as main_module


def test_ensure_run_subcommand_inserts_run_before_config() -> None:
    argv = ["main.py", "--config", "x.yaml"]
    assert main_module._ensure_run_subcommand(argv) == [
        "main.py",
        "run",
        "--config",
        "x.yaml",
    ]


def test_ensure_run_subcommand_preserves_explicit_run() -> None:
    argv = ["main.py", "run", "--config", "x.yaml"]
    assert main_module._ensure_run_subcommand(argv) == argv


def test_ensure_run_subcommand_preserves_runs_list() -> None:
    argv = ["main.py", "runs", "list", "--db", "w.duckdb"]
    assert main_module._ensure_run_subcommand(argv) == argv
