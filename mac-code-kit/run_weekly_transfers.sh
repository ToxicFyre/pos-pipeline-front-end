#!/bin/bash
# Run 10-week transfer analysis with PRECIOS/AG_PRECIOS price correction.
# Requires: source ../secrets.env first, or set WS_USER, WS_PASS, WS_BASE.

set -e
cd "$(dirname "$0")/.."

echo "[$(date)] Starting weekly transfers with prices in: $(pwd)"

# Load secrets if present
if [ -f secrets.env ]; then
  set -a
  source secrets.env
  set +a
fi

python testing/get_weekly_transfers_with_prices.py --exclude-cedis-dest "$@"

echo "[$(date)] Finished"
