"""CLI for weekly sales report + Telegram."""

from __future__ import annotations

from pos_frontend.reporting.telegram_weekly_sales import run_sales_and_send


if __name__ == "__main__":
    run_sales_and_send()
