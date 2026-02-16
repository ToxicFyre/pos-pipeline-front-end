#!/bin/bash
# Run 10-week transfer analysis with PRECIOS/AG_PRECIOS price correction.
# Automatically loads secrets from utils/secrets.env or secrets.env in project root.

set -e
cd "$(dirname "$0")/../.."

echo "[$(date)] Starting weekly transfers with prices in: $(pwd)"

# Load secrets automatically (try utils/ first, then project root)
if [ -f utils/secrets.env ]; then
  set -a
  source utils/secrets.env
  set +a
elif [ -f secrets.env ]; then
  set -a
  source secrets.env
  set +a
fi

python3 testing/get_weekly_transfers_with_prices.py --exclude-cedis-dest "$@"

echo "[$(date)] Finished"
