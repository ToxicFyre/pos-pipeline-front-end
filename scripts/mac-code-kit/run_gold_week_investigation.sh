#!/bin/bash
# Investigation only: Fetch Feb 2-8, apply prices, filter orders like golden, compare to NUMEROS.
# Production pipeline remains unchanged (no order exclusions in run_weekly_transfers.sh).

set -e
cd "$(dirname "$0")/../.."

echo "[$(date)] Starting gold-week investigation in: $(pwd)"

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

python3 testing/gold_week_investigation.py "$@"

echo "[$(date)] Finished"
