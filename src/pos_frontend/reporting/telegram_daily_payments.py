from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import requests

from pos_frontend.reporting.weekly_payments import (
    fetch_payments_daily_mart,
    validate_and_prepare_payments,
    run_payments_forecast,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TelegramConfig:
    bot_token: str
    chat_id: str
    api_base: str = "https://api.telegram.org"


def load_telegram_config() -> TelegramConfig:
    missing = [k for k in ("TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID") if not os.environ.get(k)]
    if missing:
        raise RuntimeError("Missing env vars: " + ", ".join(missing))
    return TelegramConfig(
        bot_token=os.environ["TELEGRAM_BOT_TOKEN"],
        chat_id=os.environ["TELEGRAM_CHAT_ID"],
    )


def send_document_via_telegram(
    cfg: TelegramConfig,
    attachment_path: Path,
    caption: Optional[str] = None,
    timeout_s: int = 30,
    retries: int = 2,
    backoff_s: float = 1.5,
) -> None:
    if not attachment_path.exists():
        raise FileNotFoundError(f"Attachment not found: {attachment_path}")

    url = f"{cfg.api_base}/bot{cfg.bot_token}/sendDocument"

    last_err: Exception | None = None
    for attempt in range(retries + 1):
        try:
            with attachment_path.open("rb") as f:
                files = {"document": (attachment_path.name, f)}
                data = {"chat_id": cfg.chat_id}
                if caption:
                    data["caption"] = caption

                resp = requests.post(url, data=data, files=files, timeout=timeout_s)
                if not resp.ok:
                    body = resp.text
                    if len(body) > 1500:
                        body = body[:1500] + "..."
                    raise RuntimeError(f"Telegram HTTP {resp.status_code}: {body}")

                j = resp.json()
                if not j.get("ok", False):
                    raise RuntimeError(f"Telegram returned ok=false: {j}")

                logger.info("Sent: %s (%s bytes)", attachment_path.name, attachment_path.stat().st_size)
                return

        except (requests.RequestException, RuntimeError, OSError) as e:
            last_err = e
            if attempt >= retries:
                break
            sleep_s = backoff_s * (2**attempt)
            logger.warning("Send failed (attempt %s/%s): %s. Retrying in %.1fs...",
                           attempt + 1, retries + 1, e, sleep_s)
            time.sleep(sleep_s)

    assert last_err is not None
    raise last_err


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Daily 8am job: forecast sales/payments for today+next N days and send CSVs via Telegram."
    )
    p.add_argument("--run-date", type=str, default=None, help="Run date YYYY-MM-DD (default: today).")
    p.add_argument("--history-days", type=int, default=365, help="Days of history to fetch (default: 365).")
    p.add_argument("--horizon-days", type=int, default=14, help="Forecast horizon in days (default: 14).")

    p.add_argument("--data-root", type=str, default="data", help="Data root directory (default: data).")
    p.add_argument("--branches-file", type=str, default="sucursales.json", help="Branches file path.")
    p.add_argument("--output-dir", type=str, default="data", help="Output root directory (default: data).")

    p.add_argument("--dedupe", choices=["raise", "sum", "first"], default="raise",
                   help="Duplicate handling for (sucursal, fecha).")
    p.add_argument("--strict-coverage", action="store_true",
                   help="Fail if any branch has <7 days in last-week window (needed for NaiveLastWeekModel).")

    p.add_argument("--timeout-s", type=int, default=30, help="Telegram request timeout seconds.")
    p.add_argument("--retries", type=int, default=2, help="Telegram send retries.")
    p.add_argument("--log-level", type=str, default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    p.add_argument("--dry-run", action="store_true", help="Generate CSVs but do not send Telegram messages.")
    return p


def parse_run_date(s: str | None) -> date:
    if not s:
        return date.today()
    try:
        return date.fromisoformat(s)
    except ValueError as e:
        raise ValueError(f"Invalid --run-date '{s}'. Use YYYY-MM-DD.") from e


def main(argv: list[str]) -> int:
    args = build_arg_parser().parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    run_date = parse_run_date(args.run_date)

    # IMPORTANT:
    # We want a forecast that starts on run_date (today) for +horizon_days,
    # so our as-of date should be yesterday.
    asof_date = run_date - timedelta(days=1)

    history_start = asof_date - timedelta(days=int(args.history_days))
    start_str = history_start.isoformat()
    end_str = asof_date.isoformat()

    forecast_start = run_date.isoformat()
    horizon_days = int(args.horizon_days)

    logger.info("Run date: %s", run_date.isoformat())
    logger.info("As-of date (history ends): %s", asof_date.isoformat())
    logger.info("History range: %s .. %s", start_str, end_str)
    logger.info("Forecast range: %s .. %s",
                forecast_start,
                (run_date + timedelta(days=horizon_days - 1)).isoformat())

    raw_df = fetch_payments_daily_mart(
        start_date=start_str,
        end_date=end_str,
        data_root=args.data_root,
        branches_file=args.branches_file,
    )

    payments_df = validate_and_prepare_payments(
        raw_df,
        asof_date=asof_date,
        dedupe=args.dedupe,  # type: ignore[arg-type]
        strict_coverage=bool(args.strict_coverage),
    )

    # Use run_tag = run_date so the output name matches the day you received it
    run_tag = run_date.isoformat()

    # This should produce:
    # - forecast for run_date..run_date+horizon-1 (because history ends asof_date)
    # - deposit schedule that includes "today" deposits based on known past sales + future deposits
    _forecast_csv, _deposit_csv, legacy_forecast_csv, legacy_deposit_csv = run_payments_forecast(
        payments_df=payments_df,
        horizon_days=horizon_days,
        output_dir=args.output_dir,
        run_tag=run_tag,
    )

    logger.info("Forecast CSV: %s", legacy_forecast_csv)
    logger.info("Deposit CSV: %s", legacy_deposit_csv)

    if args.dry_run:
        logger.info("Dry run enabled. Skipping Telegram sends.")
        return 0

    tg = load_telegram_config()

    # Captions: keep short so Telegram shows them fully
    caption1 = (
        f"Payments forecast (today+{horizon_days-1})\n"
        f"Today: {forecast_start} | As-of: {asof_date.isoformat()}"
    )
    send_document_via_telegram(
        cfg=tg,
        attachment_path=legacy_forecast_csv,
        caption=caption1,
        timeout_s=int(args.timeout_s),
        retries=int(args.retries),
    )

    caption2 = (
        f"Deposit schedule (today+{horizon_days-1})\n"
        f"Today: {forecast_start} | As-of: {asof_date.isoformat()}"
    )
    send_document_via_telegram(
        cfg=tg,
        attachment_path=legacy_deposit_csv,
        caption=caption2,
        timeout_s=int(args.timeout_s),
        retries=int(args.retries),
    )

    logger.info("DONE: sent forecast + deposit schedule to Telegram.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
