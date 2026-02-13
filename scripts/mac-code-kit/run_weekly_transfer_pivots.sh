#!/bin/bash
# Build pivot marts from corrected weekly transfer CSVs.
# Run after run_weekly_transfers.sh.

set -e
cd "$(dirname "$0")/../.."

echo "[$(date)] Building pivot marts from corrected weekly transfers"

python testing/build_weekly_transfer_pivots.py

echo "[$(date)] Finished"
