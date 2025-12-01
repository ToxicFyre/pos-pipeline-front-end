# reporting/weekly_sales_report.py

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

from pos_core import DataPaths
from pos_core.sales import marts


def get_last_full_week() -> tuple[date, date]:
    """
    Returns (start, end) for the last full Monday–Sunday week.
    Adjust if your 'week' is defined differently.
    """
    today = date.today()
    weekday = today.weekday()  # Monday=0
    last_sunday = today - timedelta(days=weekday + 1)
    last_monday = last_sunday - timedelta(days=6)
    return last_monday, last_sunday


def run_sales_group_mart(
    start_date: str,
    end_date: str,
    data_root: str | Path = "data",
    branches_file: str | Path = "sucursales.json",
) -> Path:
    """
    Runs marts.fetch_group and returns the path to the CSV that it writes:

        data/c_processed/sales/mart_sales_by_group_<start>_<end>.csv
    """
    data_root = Path(data_root)
    paths = DataPaths.from_root(data_root, Path(branches_file))

    df = marts.fetch_group(paths, start_date, end_date)
    print(df.head())

    csv_path = (
        data_root
        / "c_processed"
        / "sales"
        / f"mart_sales_by_group_{start_date}_{end_date}.csv"
    )

    if not csv_path.exists():
        raise FileNotFoundError(f"Expected CSV not found: {csv_path}")

    return csv_path


if __name__ == "__main__":
    # Example standalone usage: last full week
    start, end = get_last_full_week()
    start_str = start.isoformat()
    end_str = end.isoformat()

    csv_path = run_sales_group_mart(start_str, end_str)
    print(f"CSV generated: {csv_path}")
