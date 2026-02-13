"""CLI for daily payments forecast + Telegram."""

from __future__ import annotations

import sys

from pos_frontend.reporting.telegram_daily_payments import main


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
