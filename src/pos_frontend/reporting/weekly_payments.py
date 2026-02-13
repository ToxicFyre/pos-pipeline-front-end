# pos_frontend.reporting.weekly_payments

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import tempfile
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Literal

import pandas as pd
from pandas.api.types import is_datetime64_any_dtype

from pos_core import DataPaths
from pos_core.payments import marts as payments_marts
from pos_core.forecasting import ForecastConfig, run_payments_forecast as _run_payments_forecast
from pos_core.forecasting.models import NaiveLastWeekModel


logger = logging.getLogger(__name__)

DedupeStrategy = Literal["raise", "sum", "first"]


def get_last_full_week() -> tuple[date, date]:
    """
    Returns last Monday..last Sunday, where "last Sunday" is the most recent Sunday strictly before today.
    - If today is Monday, last Sunday is yesterday.
    - If today is Sunday, last Sunday is 7 days ago.
    """
    today = date.today()
    weekday = today.weekday()  # Monday=0 .. Sunday=6
    last_sunday = today - timedelta(days=weekday + 1)
    last_monday = last_sunday - timedelta(days=6)
    return last_monday, last_sunday


def resolve_branches_file(data_root: Path, branches_file: str | Path) -> Path:
    bf = Path(branches_file)
    return bf if bf.is_absolute() else (data_root / bf)


def atomic_write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=str(path.parent))
    os.close(fd)
    tmp = Path(tmp_path)
    try:
        df.to_csv(tmp, index=False)
        tmp.replace(path)
    finally:
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass


def atomic_write_text(text: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=str(path.parent))
    os.close(fd)
    tmp = Path(tmp_path)
    try:
        tmp.write_text(text, encoding="utf-8")
        tmp.replace(path)
    finally:
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass


def coerce_fecha_to_datetime(df: pd.DataFrame, col: str = "fecha") -> pd.DataFrame:
    if col not in df.columns:
        raise ValueError(f"Missing '{col}' column")

    if not is_datetime64_any_dtype(df[col]):
        df[col] = pd.to_datetime(df[col], errors="raise")

    # Normalize to midnight for stable grouping/merging
    df[col] = df[col].dt.normalize()
    return df


def validate_and_prepare_payments(
    df: pd.DataFrame,
    asof_date: date | None,
    dedupe: DedupeStrategy = "raise",
    strict_coverage: bool = False,
) -> pd.DataFrame:
    required_base = ["sucursal", "fecha", "ingreso_efectivo", "ingreso_credito", "ingreso_debito"]
    missing = [c for c in required_base if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}. Available: {list(df.columns)}")

    df = df.copy()
    df = coerce_fecha_to_datetime(df, "fecha")

    # Payment cols: numeric + NA-safe
    pay_cols = ["ingreso_efectivo", "ingreso_credito", "ingreso_debito"]
    for c in pay_cols:
        df[c] = pd.to_numeric(df[c], errors="raise").fillna(0)

    if "ingreso_total" not in df.columns:
        df["ingreso_total"] = df["ingreso_efectivo"] + df["ingreso_credito"] + df["ingreso_debito"]
        logger.info("Computed ingreso_total from efectivo/credito/debito.")
    else:
        df["ingreso_total"] = pd.to_numeric(df["ingreso_total"], errors="raise").fillna(0)

    # Basic sanity checks (warn, do not hard-fail)
    if (df[pay_cols + ["ingreso_total"]] < 0).any(axis=None):
        logger.warning("Found negative payment values. If refunds exist, this may be expected.")

    # Duplicates handling on (sucursal, fecha)
    key_cols = ["sucursal", "fecha"]
    dup_mask = df.duplicated(subset=key_cols, keep=False)
    if dup_mask.any():
        dup_count = int(dup_mask.sum())
        msg = f"Found {dup_count} rows with duplicated keys {key_cols}."
        if dedupe == "raise":
            raise ValueError(msg + " Use --dedupe=sum or --dedupe=first if this is expected.")
        elif dedupe == "first":
            logger.warning(msg + " Keeping first occurrence per key.")
            df = df.sort_values(key_cols).drop_duplicates(subset=key_cols, keep="first")
        elif dedupe == "sum":
            logger.warning(msg + " Aggregating by sum per key.")
            # Keep non-numeric columns minimal; sum numeric columns
            numeric_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c])]
            keep_cols = list(dict.fromkeys(key_cols + numeric_cols))  # stable unique
            df = (
                df[keep_cols]
                .groupby(key_cols, as_index=False, sort=True)[numeric_cols]
                .sum()
            )

    # As-of alignment
    if asof_date is not None:
        max_date = df["fecha"].max().date()
        if max_date != asof_date:
            raise ValueError(f"Data ends at {max_date.isoformat()} but as-of date is {asof_date.isoformat()}.")

    # Coverage check for NaiveLastWeekModel: last 7 days per branch
    if asof_date is not None:
        window_start = pd.Timestamp(asof_date) - pd.Timedelta(days=6)
        window_end = pd.Timestamp(asof_date)
        last_week = df[(df["fecha"] >= window_start) & (df["fecha"] <= window_end)]
        counts = last_week.groupby("sucursal")["fecha"].nunique()
        bad = counts[counts < 7]
        if not bad.empty:
            msg = (
                "Coverage gap in last 7 days for some branches: "
                + ", ".join(f"{k}={int(v)}/7" for k, v in bad.items())
            )
            if strict_coverage:
                raise ValueError(msg)
            logger.warning(msg)

    df = df.sort_values(["sucursal", "fecha"]).reset_index(drop=True)
    return df


def fetch_payments_daily_mart(
    start_date: str,
    end_date: str,
    data_root: str | Path,
    branches_file: str | Path,
) -> pd.DataFrame:
    data_root_p = Path(data_root)
    branches_path = resolve_branches_file(data_root_p, branches_file)
    paths = DataPaths.from_root(data_root_p, branches_path)

    df = payments_marts.fetch_daily(paths, start_date, end_date)
    logger.info("Fetched payments daily mart: %s rows, %s cols", len(df), len(df.columns))
    return df


@dataclass(frozen=True)
class RunMeta:
    run_tag: str
    created_at_utc: str
    asof_date: str
    history_start: str
    history_end: str
    horizon_days: int
    model_name: str
    rows_in: int
    cols_in: int
    date_min: str
    date_max: str
    branches: int
    output_dir: str
    forecast_csv: str
    deposit_schedule_csv: str
    legacy_forecast_csv: str
    legacy_deposit_schedule_csv: str
    args: dict[str, Any]


def run_payments_forecast(
    payments_df: pd.DataFrame,
    horizon_days: int,
    output_dir: str | Path,
    run_tag: str,
) -> tuple[Path, Path, Path, Path]:
    """
    Returns:
      (forecast_csv, deposit_csv, legacy_forecast_csv, legacy_deposit_csv)
    """
    output_dir_p = Path(output_dir)
    processed_dir = output_dir_p / "c_processed" / "payments"
    run_dir = processed_dir / "runs" / run_tag
    run_dir.mkdir(parents=True, exist_ok=True)

    config = ForecastConfig(horizon_days=horizon_days, model=NaiveLastWeekModel())
    logger.info("Forecast config: horizon_days=%s, model=%s", horizon_days, "NaiveLastWeekModel")

    result = _run_payments_forecast(payments_df, config)

    forecast_csv = run_dir / "forecast.csv"
    deposit_csv = run_dir / "deposit_schedule.csv"

    atomic_write_csv(result.forecast, forecast_csv)
    atomic_write_csv(result.deposit_schedule, deposit_csv)

    # Backward-compatible filenames (same as your original script)
    legacy_forecast_csv = processed_dir / f"forecast_{run_tag}.csv"
    legacy_deposit_csv = processed_dir / f"deposit_schedule_{run_tag}.csv"
    atomic_write_csv(result.forecast, legacy_forecast_csv)
    atomic_write_csv(result.deposit_schedule, legacy_deposit_csv)

    logger.info("Saved forecast to: %s", forecast_csv)
    logger.info("Saved deposit schedule to: %s", deposit_csv)
    logger.info("Saved legacy forecast to: %s", legacy_forecast_csv)
    logger.info("Saved legacy deposit schedule to: %s", legacy_deposit_csv)

    return forecast_csv, deposit_csv, legacy_forecast_csv, legacy_deposit_csv


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Generate payments forecast + deposit schedule.")
    p.add_argument(
        "--target-date",
        type=str,
        default=None,
        help="As-of date (YYYY-MM-DD). Forecast starts the next day. If omitted, uses last Sunday.",
    )
    p.add_argument("--history-days", type=int, default=365, help="Days of history to fetch (default: 365).")
    p.add_argument("--horizon-days", type=int, default=14, help="Forecast horizon in days (default: 14).")
    p.add_argument("--data-root", type=str, default="data", help="Data root directory (default: data).")
    p.add_argument(
        "--branches-file",
        type=str,
        default="sucursales.json",
        help="Branches file path. Relative paths resolve under --data-root.",
    )
    p.add_argument("--output-dir", type=str, default="data", help="Output root directory (default: data).")
    p.add_argument(
        "--dedupe",
        type=str,
        choices=["raise", "sum", "first"],
        default="raise",
        help="Duplicate handling for (sucursal, fecha).",
    )
    p.add_argument(
        "--strict-coverage",
        action="store_true",
        help="Fail if any branch has <7 days of data in the last week (needed for NaiveLastWeekModel).",
    )
    p.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity.",
    )
    p.add_argument(
        "--run-tag",
        type=str,
        default=None,
        help="Tag for outputs (default: target-date).",
    )
    return p


def parse_target_date(target_date_str: str | None) -> date:
    if target_date_str:
        try:
            return date.fromisoformat(target_date_str)
        except ValueError as e:
            raise ValueError(f"Invalid --target-date '{target_date_str}'. Use YYYY-MM-DD.") from e

    _, last_sunday = get_last_full_week()
    return last_sunday


def main(argv: list[str]) -> int:
    args = build_arg_parser().parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    target_date = parse_target_date(args.target_date)
    run_tag = args.run_tag or target_date.isoformat()

    history_start = target_date - timedelta(days=int(args.history_days))
    start_str = history_start.isoformat()
    end_str = target_date.isoformat()

    logger.info("Run tag: %s", run_tag)
    logger.info("As-of date: %s", target_date.isoformat())
    logger.info("History range: %s .. %s (%s days)", start_str, end_str, args.history_days)
    logger.info("Forecast starts: %s", (target_date + timedelta(days=1)).isoformat())

    raw_df = fetch_payments_daily_mart(
        start_date=start_str,
        end_date=end_str,
        data_root=args.data_root,
        branches_file=args.branches_file,
    )

    payments_df = validate_and_prepare_payments(
        raw_df,
        asof_date=target_date,
        dedupe=args.dedupe,  # type: ignore[arg-type]
        strict_coverage=bool(args.strict_coverage),
    )

    forecast_csv, deposit_csv, legacy_forecast_csv, legacy_deposit_csv = run_payments_forecast(
        payments_df=payments_df,
        horizon_days=int(args.horizon_days),
        output_dir=args.output_dir,
        run_tag=run_tag,
    )

    meta = RunMeta(
        run_tag=run_tag,
        created_at_utc=datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        asof_date=target_date.isoformat(),
        history_start=start_str,
        history_end=end_str,
        horizon_days=int(args.horizon_days),
        model_name="NaiveLastWeekModel",
        rows_in=int(len(payments_df)),
        cols_in=int(len(payments_df.columns)),
        date_min=payments_df["fecha"].min().date().isoformat(),
        date_max=payments_df["fecha"].max().date().isoformat(),
        branches=int(payments_df["sucursal"].nunique()),
        output_dir=str(Path(args.output_dir).resolve()),
        forecast_csv=str(forecast_csv.resolve()),
        deposit_schedule_csv=str(deposit_csv.resolve()),
        legacy_forecast_csv=str(legacy_forecast_csv.resolve()),
        legacy_deposit_schedule_csv=str(legacy_deposit_csv.resolve()),
        args=vars(args),
    )

    # Write manifest next to run outputs
    manifest_path = Path(args.output_dir) / "c_processed" / "payments" / "runs" / run_tag / "manifest.json"
    atomic_write_text(json.dumps(asdict(meta), indent=2, sort_keys=True), manifest_path)
    logger.info("Wrote manifest: %s", manifest_path)

    # Final summary (single line style, easy for schedulers)
    logger.info(
        "DONE | forecast=%s | deposit=%s",
        legacy_forecast_csv,
        legacy_deposit_csv,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
