# pos_frontend.reporting.monthly_pv_sales

from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from pos_core import DataPaths
from pos_core.order_times import raw as order_times_raw


# Spanish month names mapping
SPANISH_MONTHS = {
    1: "Enero",
    2: "Febrero",
    3: "Marzo",
    4: "Abril",
    5: "Mayo",
    6: "Junio",
    7: "Julio",
    8: "Agosto",
    9: "Septiembre",
    10: "Octubre",
    11: "Noviembre",
    12: "Diciembre",
}


def get_last_month_range() -> tuple[date, date]:
    """
    Returns (start, end) for the last full calendar month.
    If today is January 21, 2026, returns December 1-31, 2025.
    """
    today = date.today()
    first_of_this_month = today.replace(day=1)
    last_day_prev_month = first_of_this_month - timedelta(days=1)
    first_day_prev_month = last_day_prev_month.replace(day=1)
    return first_day_prev_month, last_day_prev_month


def fetch_order_times_excel_path(
    sucursal: str,
    start_date: str,
    end_date: str,
    data_root: str | Path = "data",
    branches_file: str | Path = "sucursales.json",
) -> Path:
    """
    Fetches order_times raw data and returns the path to the downloaded Excel file.
    Uses the branches parameter to filter before downloading.
    """
    data_root_p = Path(data_root)
    paths = DataPaths.from_root(data_root_p, Path(branches_file))

    order_times_raw.fetch(paths, start_date, end_date, branches=[sucursal])

    sucursal_filename = sucursal.replace(" ", "-")
    expected_filename = f"OrderTimes_{sucursal_filename}_{start_date}_{end_date}.xlsx"
    excel_path = data_root_p / "a_raw" / "order_times" / "batch" / expected_filename

    if not excel_path.exists():
        alt_filename = f"OrderTimes_{sucursal}_{start_date}_{end_date}.xlsx"
        alt_path = data_root_p / "a_raw" / "order_times" / "batch" / alt_filename

        if alt_path.exists():
            excel_path = alt_path
        else:
            batch_dir = data_root_p / "a_raw" / "order_times" / "batch"
            if batch_dir.exists():
                available_files = [f.name for f in batch_dir.glob(f"OrderTimes_*_{start_date}_{end_date}.xlsx")]
                if available_files:
                    raise FileNotFoundError(
                        f"Expected file '{expected_filename}' not found. "
                        f"Available files for this date range: {available_files}"
                    )
            raise FileNotFoundError(
                f"Failed to find downloaded order_times file at {excel_path}. "
                f"The fetch() call may have failed or the file naming convention differs."
            )

    return excel_path


def copy_and_rename_excel(
    source_excel_path: Path,
    sucursal: str,
    month_date: date,
    output_dir: str | Path = ".",
) -> Path:
    import shutil

    month_num = month_date.month
    year = month_date.year
    month_name_spanish = SPANISH_MONTHS[month_num]

    filename = f"Panem {sucursal} - Ventas {month_name_spanish} {year}.xlsx"
    output_path = Path(output_dir) / filename

    output_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_excel_path, output_path)

    print(f"Report copied and renamed to {output_path}")
    return output_path


def generate_monthly_report(
    sucursal: str,
    data_root: str | Path = "data",
    branches_file: str | Path = "sucursales.json",
    output_dir: str | Path | None = None,
) -> Path:
    """
    Generate monthly order_times sales report for a given sucursal.
    """
    start_date, end_date = get_last_month_range()
    start_str = start_date.isoformat()
    end_str = end_date.isoformat()

    print(f"Fetching order_times data for sucursal '{sucursal}'")
    print(f"Date range: {start_str} to {end_str}")

    excel_path = fetch_order_times_excel_path(
        sucursal=sucursal,
        start_date=start_str,
        end_date=end_str,
        data_root=data_root,
        branches_file=branches_file,
    )

    if not excel_path.exists():
        raise ValueError(f"Downloaded Excel file does not exist: {excel_path}")

    if excel_path.stat().st_size == 0:
        raise ValueError(f"Downloaded Excel file is empty: {excel_path}")

    if output_dir is None:
        data_root_p = Path(data_root)
        output_dir = data_root_p / "a_raw" / "order_times" / "temp"

    output_path = copy_and_rename_excel(
        source_excel_path=excel_path,
        sucursal=sucursal,
        month_date=start_date,
        output_dir=output_dir,
    )

    return output_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate monthly order_times sales report for a given sucursal"
    )
    parser.add_argument(
        "sucursal",
        type=str,
        help="Sucursal (branch) name as in sucursales.json",
    )
    parser.add_argument(
        "--data-root",
        type=str,
        default="data",
        help="Data root directory (default: data)",
    )
    parser.add_argument(
        "--branches-file",
        type=str,
        default="sucursales.json",
        help="Branches file path (default: sucursales.json)",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=None,
        help="Output directory for the Excel report (default: data/a_raw/order_times/temp)",
    )

    args = parser.parse_args(argv)

    try:
        output_path = generate_monthly_report(
            sucursal=args.sucursal,
            data_root=args.data_root,
            branches_file=args.branches_file,
            output_dir=args.output_dir,
        )

        print(f"Success! Report saved to: {output_path}")
        print(f"OUTPUT_PATH={output_path}")
        return 0

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
