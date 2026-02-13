#!/bin/bash
# Pre-weekly check: refresh PRECIOS/AG_PRECIOS from golden reference.
# Run before get_weekly_transfers_with_prices if gold file was updated.

set -e
cd "$(dirname "$0")/../.."

echo "[$(date)] Pre-weekly check: refreshing PRECIOS/AG_PRECIOS from gold"

python testing/compare_unit_prices_full.py
rc=$?

if [ $rc -ne 0 ]; then
  echo "compare_unit_prices_full.py exited with $rc"
  exit $rc
fi

echo ""
echo "Review data/c_processed/transfers/weekly/unit_price_investigation_report.md"
echo "and transfer_products_not_in_precios.csv"
echo ""
echo "If needed: cp PRECIOS_UPDATED.xlsx PRECIOS.xlsx && cp AG_PRECIOS_UPDATED.xlsx AG_PRECIOS.xlsx"
echo ""
