#!/bin/bash
# Add PRECIO UNITARIO: when UNIDAD is LT/KG use PRECIO DRIVE; when UNIDAD is PZ use PRECIO DRIVE / PRESENTACION.
# Run this before the weekly transfer pipeline if PRECIOS uses PRESENTACION quantities.

set -e
cd "$(dirname "$0")/../.."

echo "[$(date)] Updating PRECIOS with PRECIO UNITARIO column"

python3 testing/update_precios_with_unit_prices.py "$@"

echo "[$(date)] Finished"
