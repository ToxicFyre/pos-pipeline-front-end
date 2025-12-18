# reporting/weekly_payments_forecast.py

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from pos_core import DataPaths
from pos_core.payments import marts as payments_marts
from pos_core.forecasting import ForecastConfig, run_payments_forecast as _run_payments_forecast
from pos_core.forecasting.models import NaiveLastWeekModel


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


def run_payments_daily_mart(
    start_date: str,
    end_date: str,
    data_root: str | Path = "data",
    branches_file: str | Path = "sucursales.json",
) -> pd.DataFrame:
    """
    Fetches historical payments data using the daily mart.

    Args:
        start_date: Start of historical range (ISO format string)
        end_date: End of historical range (ISO format string)
        data_root: Root directory for data files
        branches_file: Path to sucursales.json configuration file

    Returns:
        DataFrame with payments data
    """
    data_root = Path(data_root)
    paths = DataPaths.from_root(data_root, Path(branches_file))

    df = payments_marts.fetch_daily(paths, start_date, end_date)
    print(f"Fetched payments data: {len(df)} rows")
    print(df.head())

    # Calculate ingreso_total if missing (required by forecasting function)
    # ingreso_total = ingreso_efectivo + ingreso_credito + ingreso_debito
    if "ingreso_total" not in df.columns:
        payment_cols = ["ingreso_efectivo", "ingreso_credito", "ingreso_debito"]
        missing_cols = [col for col in payment_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(
                f"Missing required payment columns: {missing_cols}. "
                f"Available columns: {list(df.columns)}"
            )
        df["ingreso_total"] = (
            df["ingreso_efectivo"] + df["ingreso_credito"] + df["ingreso_debito"]
        )
        print(f"Calculated ingreso_total column")

    # Verify required columns for forecasting
    required_cols = ["sucursal", "fecha", "ingreso_efectivo", "ingreso_credito", "ingreso_debito", "ingreso_total"]
    missing_required = [col for col in required_cols if col not in df.columns]
    if missing_required:
        raise ValueError(
            f"Missing required columns for forecasting: {missing_required}. "
            f"Available columns: {list(df.columns)}"
        )
    
    # Ensure fecha is datetime type if it's a string
    if df["fecha"].dtype == "object":
        print("Converting fecha column to datetime...")
        df["fecha"] = pd.to_datetime(df["fecha"])
    
    print(f"Data validation complete. Date range: {df['fecha'].min()} to {df['fecha'].max()}")

    return df


def run_payments_forecast(  # noqa: F811
    payments_df: pd.DataFrame,
    horizon_days: int = 91,
    output_dir: str | Path = "data",
    forecast_date: str | None = None,
) -> tuple[Path, Path]:
    """
    Runs forecasting on payments data and saves results to CSV files.

    Args:
        payments_df: DataFrame from run_payments_daily_mart()
        horizon_days: Forecast horizon in days (default: 91 days / 13 weeks)
        output_dir: Directory where to save CSV files
        forecast_date: Date string for naming output files (defaults to today)

    Returns:
        Tuple of (forecast_csv_path, deposit_schedule_csv_path)
    """
    output_dir = Path(output_dir)
    if forecast_date is None:
        forecast_date = date.today().isoformat()

    # Run forecast
    print(f"\nStarting forecast with {len(payments_df)} rows...")
    print(f"DataFrame columns: {list(payments_df.columns)}")
    print(f"DataFrame shape: {payments_df.shape}")
    print(f"Date range in data: {payments_df['fecha'].min()} to {payments_df['fecha'].max()}")
    
    config = ForecastConfig(horizon_days=horizon_days, model=NaiveLastWeekModel())
    print(f"Forecast config: horizon_days={horizon_days}, model=NaiveLastWeekModel")
    print("Calling _run_payments_forecast()...")
    
    result = _run_payments_forecast(payments_df, config)  # type: ignore[attr-defined]
    
    print("Forecast calculation completed!")

    # Prepare output directory
    forecast_dir = output_dir / "c_processed" / "payments"
    forecast_dir.mkdir(parents=True, exist_ok=True)

    # Save forecast results
    forecast_csv_path = forecast_dir / f"forecast_{forecast_date}.csv"
    result.forecast.to_csv(forecast_csv_path, index=False)
    print(f"Forecast saved to: {forecast_csv_path}")
    print("Forecast preview:")
    print(result.forecast.head())

    # Save deposit schedule
    deposit_schedule_csv_path = forecast_dir / f"deposit_schedule_{forecast_date}.csv"
    result.deposit_schedule.to_csv(deposit_schedule_csv_path, index=False)
    print(f"Deposit schedule saved to: {deposit_schedule_csv_path}")
    print("Deposit schedule preview:")
    print(result.deposit_schedule.head())

    return forecast_csv_path, deposit_schedule_csv_path


if __name__ == "__main__":
    # Calculate last full week dates
    last_monday, last_sunday = get_last_full_week()
    end_str = last_sunday.isoformat()

    # Calculate historical range: 12 months back from last Monday
    historical_start = last_monday - timedelta(days=365)
    start_str = historical_start.isoformat()

    print(f"Fetching payments data from {start_str} to {end_str}")

    # Step 1: Fetch payments data
    payments_df = run_payments_daily_mart(start_str, end_str)
    print(f"\nData preparation complete. DataFrame info:")
    print(f"  Rows: {len(payments_df)}")
    print(f"  Columns: {list(payments_df.columns)}")

    # Step 2: Run forecast
    print("\n" + "="*60)
    print("STEP 2: Running forecast...")
    print("="*60)
    forecast_path, deposit_path = run_payments_forecast(
        payments_df,
        horizon_days=14,
        forecast_date=end_str,
    )

    print("\nForecast complete!")
    print(f"Forecast CSV: {forecast_path}")
    print(f"Deposit schedule CSV: {deposit_path}")

