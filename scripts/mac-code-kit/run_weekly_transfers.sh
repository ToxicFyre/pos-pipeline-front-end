#!/bin/bash
# Run 12-week transfer analysis with PRECIOS/AG_PRECIOS price correction.
# 1. Updates PRECIOS with PRECIO UNITARIO (UNIDAD logic) via run_update_precios_unit_prices.sh
# 2. Fetches last 12 weeks, applies PRECIOS/AG_PRECIOS, excludes CEDIS destination
# Automatically loads secrets from utils/secrets.env or secrets.env in project root.

set -e
cd "$(dirname "$0")/../.."

echo "[$(date)] Starting 12-week weekly transfers with prices in: $(pwd)"

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

# 1. Ensure PRECIOS has PRECIO UNITARIO column (UNIDAD: LT/KG=unit, PZ=PRECIO DRIVE/PRESENTACION)
./scripts/mac-code-kit/run_update_precios_unit_prices.sh

# 2. Run 12-week transfer analysis (--weeks 12, --exclude-cedis-dest; pass through --precios-path, --end, --ag-precios-path, etc.)
python3 testing/get_weekly_transfers_with_prices.py --exclude-cedis-dest --weeks 12 "$@"

echo "[$(date)] Finished"
