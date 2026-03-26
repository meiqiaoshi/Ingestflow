from __future__ import annotations

import argparse
from pathlib import Path

import sys


def _ensure_src_on_path() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    src_dir = repo_root / "src"
    if str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))


def main() -> None:
    _ensure_src_on_path()
    from utils.sample_data import generate_parquet_from_csv

    parser = argparse.ArgumentParser(description="Generate sample Parquet from sample CSV")
    parser.add_argument(
        "--csv",
        default="data/sample_orders.csv",
        help="Input CSV path (default: data/sample_orders.csv)",
    )
    parser.add_argument(
        "--parquet",
        default="data/sample_orders.parquet",
        help="Output Parquet path (default: data/sample_orders.parquet)",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite output file if it exists",
    )
    args = parser.parse_args()

    out = generate_parquet_from_csv(
        csv_path=args.csv,
        parquet_path=args.parquet,
        overwrite=args.overwrite,
    )
    print(f"Generated: {out}")


if __name__ == "__main__":
    main()

