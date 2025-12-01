# reporting/telegram_weekly_sales_report.py

from __future__ import annotations

from pathlib import Path
from datetime import date
import os

import requests

from reporting.weekly_sales_report import get_last_full_week, run_sales_group_mart


def send_csv_via_telegram(
    attachment_path: Path,
    caption: str | None = None,
) -> None:
    """
    Sends the CSV as a document to a Telegram chat.

    Uses env vars:
    - TELEGRAM_BOT_TOKEN
    - TELEGRAM_CHAT_ID
    """
    bot_token = os.environ["TELEGRAM_BOT_TOKEN"]
    chat_id = os.environ["TELEGRAM_CHAT_ID"]

    url = f"https://api.telegram.org/bot{bot_token}/sendDocument"

    with open(attachment_path, "rb") as f:
        files = {"document": (attachment_path.name, f)}
        data = {"chat_id": chat_id}
        if caption:
            data["caption"] = caption

        resp = requests.post(url, data=data, files=files)
        resp.raise_for_status()
        print("Telegram response:", resp.json())


if __name__ == "__main__":
    # 1) Decide which week to send
    start, end = get_last_full_week()
    start_str = start.isoformat()
    end_str = end.isoformat()

    # 2) Generate CSV via the other script
    csv_path = run_sales_group_mart(start_str, end_str)
    print(f"CSV ready at: {csv_path}")

    # 3) Send it via Telegram
    caption = f"Sales by group {start_str} to {end_str}"
    send_csv_via_telegram(
        attachment_path=Path(csv_path),
        caption=caption,
    )

    print("CSV sent via Telegram.")
