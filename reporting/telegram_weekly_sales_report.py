"""Shim: delegates to pos_frontend. Preserves exact .cmd behavior."""
import sys
from pathlib import Path

_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))
from shim_bootstrap import add_src_to_syspath
add_src_to_syspath()
from pos_frontend.config.paths import load_secrets_env

load_secrets_env()
from pos_frontend.reporting.telegram_weekly_sales import run_sales_and_send

if __name__ == "__main__":
    run_sales_and_send()
