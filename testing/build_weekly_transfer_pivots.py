"""
Build pivot marts from corrected weekly transfer CSVs.

Uses pos_core.etl.marts.transfers.build_table to aggregate each
transfers_YYYY-MM-DD_YYYY-MM-DD.csv into mart_transfers_pivot format
(branch × category). Reads from our corrected weekly CSVs (with PRECIOS/AG_PRECIOS
prices applied), not from the raw core layer.

Requires: run_weekly_transfers first to produce transfers_*.csv in weekly/
"""

from __future__ import annotations

import argparse
import logging
import re
from pathlib import Path

import pandas as pd
from pos_core.etl.marts.transfers import build_table

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def parse_week_from_filename(name: str) -> tuple[str, str] | None:
    """Extract (start_date, end_date) from transfers_YYYY-MM-DD_YYYY-MM-DD.csv"""
    m = re.match(r"transfers_(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})\.csv", name)
    if m:
        return m.group(1), m.group(2)
    return None


def build_weekly_pivots(
    weekly_dir: Path,
    output_dir: Path | None = None,
    include_cedis: bool = False,
) -> list[Path]:
    """Build mart_transfers_pivot for each corrected weekly transfer CSV.

    Args:
        weekly_dir: Directory containing transfers_*.csv
        output_dir: Where to write mart_transfers_pivot_*.csv (default: same as weekly_dir)
        include_cedis: If True, include CEDIS in pivot (default: exclude to match gold)

    Returns:
        List of output paths written.
    """
    output_dir = output_dir or weekly_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    transfer_files = sorted(weekly_dir.glob("transfers_*.csv"))
    written: list[Path] = []

    for csv_path in transfer_files:
        week = parse_week_from_filename(csv_path.name)
        if not week:
            logger.warning("Skipping non-week file: %s", csv_path.name)
            continue

        start_date, end_date = week
        logger.info("Building pivot for %s to %s", start_date, end_date)

        try:
            result_df, unmapped = build_table(str(csv_path), include_cedis=include_cedis)

            if len(unmapped) > 0:
                lost = pd.to_numeric(unmapped["Costo"], errors="coerce").fillna(0).sum()
                logger.warning("  %d unmapped rows (total $%.2f)", len(unmapped), lost)

            out_path = output_dir / f"mart_transfers_pivot_{start_date}_{end_date}.csv"
            result_df.to_csv(out_path, index=True, encoding="utf-8-sig")
            logger.info("  Saved %s", out_path)
            written.append(out_path)

        except Exception as e:
            logger.error("  Failed: %s", e)
            raise

    return written


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build pivot marts from corrected weekly transfer CSVs"
    )
    parser.add_argument(
        "--weekly-dir",
        default="data/c_processed/transfers/weekly",
        help="Directory with transfers_*.csv",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Output directory (default: same as weekly-dir)",
    )
    parser.add_argument(
        "--include-cedis",
        action="store_true",
        help="Include CEDIS in pivot (default: exclude)",
    )
    args = parser.parse_args()

    weekly_dir = Path(args.weekly_dir)
    if not weekly_dir.exists():
        logger.error("Weekly directory not found: %s", weekly_dir)
        return 1

    output_dir = Path(args.output_dir) if args.output_dir else None
    written = build_weekly_pivots(
        weekly_dir, output_dir=output_dir, include_cedis=args.include_cedis
    )
    logger.info("Built %d pivot marts", len(written))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
