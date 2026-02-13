#!/usr/bin/env python3
"""Download 5 years of core sales data from all sucursales using pos_core.

Uses 90-day chunks to avoid Wansoft API size limits.
Exports consolidated fact_sales_item_line to CSV when done.
Requires: secrets.env at project root with WS_BASE, WS_USER, WS_PASS.
Run from project root: python scripts/download_5yr_sales.py
Use --test for a 7-day run to verify without full download.
Use --export-only to export existing clean data to CSV (no download).
"""

import argparse
import os
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

# Add project root and src so pos_frontend and pos_core are importable
_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))
if str(_root / "src") not in sys.path:
    sys.path.insert(0, str(_root / "src"))

from pos_frontend.config.paths import get_project_root, load_secrets_env

load_secrets_env()

from pos_core import DataPaths
from pos_core.sales import core as sales_core

# WS_BASE only required for download (not --export-only)
if not os.environ.get("WS_BASE"):
    print("Note: WS_BASE not set. Use --export-only to export existing data only.")


CHUNK_DAYS = 90  # Smaller chunks to avoid Wansoft API size limits


def _load_from_clean_csvs(paths: DataPaths, start_str: str, end_str: str) -> pd.DataFrame:
    """Load fact_sales_item_line from clean CSVs, filtering by date range."""
    csv_files = list(paths.clean_sales.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files in {paths.clean_sales}")
    dfs = [pd.read_csv(f, encoding="utf-8") for f in csv_files]
    df = pd.concat(dfs, ignore_index=True)
    if "operating_date" in df.columns:
        df["operating_date"] = pd.to_datetime(df["operating_date"]).dt.date
        start_d = date.fromisoformat(start_str)
        end_d = date.fromisoformat(end_str)
        df = df[(df["operating_date"] >= start_d) & (df["operating_date"] <= end_d)]
    return df


def main() -> int:
    parser = argparse.ArgumentParser(description="Download 5yr sales and export to CSV")
    parser.add_argument("--test", action="store_true", help="Run 7-day test only")
    parser.add_argument(
        "--export-only",
        action="store_true",
        help="Export existing clean data to CSV (no download)",
    )
    args = parser.parse_args()

    root = get_project_root()
    paths = DataPaths.from_root(root / "data", root / "sucursales.json")

    end = date.today()
    start = end - timedelta(days=7 if args.test else 5 * 365)
    start_str = start.isoformat()
    end_str = end.isoformat()

    if args.export_only:
        if not os.environ.get("WS_BASE"):
            print("Running --export-only without WS_BASE (no download).")
        print(f"Loading existing clean sales for {start_str} to {end_str}...")
        df = _load_from_clean_csvs(paths, start_str, end_str)
        print(f"Loaded {len(df)} rows from clean CSVs.")
    else:
        if not os.environ.get("WS_BASE"):
            print("Error: WS_BASE not set. Ensure secrets.env exists with WS_BASE, WS_USER, WS_PASS.")
            sys.exit(1)
        print(f"Downloading core sales for {start_str} to {end_str} (all sucursales)")
        print(f"Using {CHUNK_DAYS}-day chunks")
        cur = start
        chunk_num = 0
        while cur <= end:
            chunk_end = min(cur + timedelta(days=CHUNK_DAYS - 1), end)
            chunk_num += 1
            print(f"Chunk {chunk_num}: {cur.isoformat()} to {chunk_end.isoformat()}")
            sales_core.fetch(paths, cur.isoformat(), chunk_end.isoformat(), mode="force")
            cur = chunk_end + timedelta(days=1)
        df = sales_core.load(paths, start_str, end_str)
        print(f"Done. {len(df)} rows in fact_sales_item_line.")

    out_path = root / "data" / f"fact_sales_5yr_{start_str}_{end_str}.csv"
    df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"Saved: {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
