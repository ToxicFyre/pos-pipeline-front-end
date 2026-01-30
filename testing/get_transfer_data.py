from __future__ import annotations

from datetime import date, timedelta

from pos_core import DataPaths
from pos_core.transfers import marts, core
from pathlib import Path

def run_transfers_core(
    start_date: str,
    end_date: str,
    data_root: str | Path = "data",
    branches_file: str | Path = "sucursales.json",
) -> Path:
    """
    Runs core.fetch_transfers and returns the path to the CSV that it writes:

        data/c_processed/transfers/core_transfers_<start>_<end>.csv
    """
    data_root = Path(data_root)
    paths = DataPaths.from_root(data_root, Path(branches_file))

    df = core.fetch(paths, start_date, end_date, mode="force")
    print(df.head())

    # core.fetch() saves individual branch files to b_clean/transfers/batch/
    batch_dir = data_root / "b_clean" / "transfers" / "batch"

    if not batch_dir.exists():
        raise FileNotFoundError(f"Expected batch directory not found: {batch_dir}")

    return batch_dir

def run_transfers_mart(
    start_date: str,
    end_date: str,
    data_root: str | Path = "data",
    branches_file: str | Path = "sucursales.json",
) -> Path:
    """
    Runs marts.fetch_transfers and returns the path to the CSV that it writes:

        data/c_processed/transfers/mart_transfers_pivot_<start>_<end>.csv
    """
    data_root = Path(data_root)
    paths = DataPaths.from_root(data_root, Path(branches_file))

    df = marts.fetch_pivot(paths, start_date, end_date, mode="force")
    print(df.head())

    csv_path = (
        data_root
        / "c_processed"
        / "transfers"
        / f"mart_transfers_pivot_{start_date}_{end_date}.csv"
    )

    if not csv_path.exists():
        raise FileNotFoundError(f"Expected CSV not found: {csv_path}")

    return csv_path


if __name__ == "__main__":

    start_date_after = date(2025, 12, 8)
    end_date_after = date(2026, 1, 25)
    start_str_after = start_date_after.isoformat()
    end_str_after = end_date_after.isoformat()
    csv_path_after_mart = run_transfers_mart(start_str_after, end_str_after)
    batch_dir_after = run_transfers_core(start_str_after, end_str_after)
    print(f"Mart CSV generated: {csv_path_after_mart}")
    print(f"Core batch directory: {batch_dir_after}")

    start_date_before = date(2025, 10, 20)
    end_date_before = date(2025, 12, 7)
    start_str_before = start_date_before.isoformat()
    end_str_before = end_date_before.isoformat()
    csv_path_before_mart = run_transfers_mart(start_str_before, end_str_before)
    batch_dir_before = run_transfers_core(start_str_before, end_str_before)
    print(f"Mart CSV generated: {csv_path_before_mart}")
    print(f"Core batch directory: {batch_dir_before}")
