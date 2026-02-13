# pos-pipeline-front-end
This is the front-end aspect of the pos pipeline, for weekly use.

**Setup on a new machine:** See [docs/setup.md](docs/setup.md).

## Weekly transfers with price correction

Fetches transfer data for each week (Mon-Sun) from Dec 1 to Feb 7, applies correct unit prices from `PRECIOS.xlsx`, and outputs consolidated CSVs plus a cost-difference report.

**Mac/Linux:** `./scripts/mac-code-kit/run_weekly_transfers.sh` (see `scripts/mac-code-kit/README.md`)

**Windows:** `run_weekly_transfers.cmd`

Or with options (requires `WS_USER`, `WS_PASS`, `WS_BASE` env vars):

```cmd
python testing/get_weekly_transfers_with_prices.py --data-root data --precios-path PRECIOS.xlsx --start 2025-12-01 --end 2026-02-07
```

Output: `data/c_processed/transfers/weekly/transfers_*.csv`, `price_correction_report.csv`, `weekly_cost_comparison.csv`, `weekly_breakdown.csv`

**Pivot marts (branch × category):** After running weekly transfers, build pivot tables from the corrected data:

```cmd
run_weekly_transfer_pivots.cmd
```

Or: `python testing/build_weekly_transfer_pivots.py`

Output: `data/c_processed/transfers/weekly/mart_transfers_pivot_YYYY-MM-DD_YYYY-MM-DD.csv` (10 files). Uses [pos-pipeline-core-etl](https://github.com/ToxicFyre/pos-pipeline-core-etl) `build_table` to aggregate by branch (K, N, C, Q, PV, HZ, CC) and category (NO-PROC, REFRICONGE, TOSTADOR, COMIDA SALADA, REPO, PAN DULCE Y SALADA). CEDIS excluded by default.

**Pre-run checklist (see [docs/ten_week_analysis_corrections.md](docs/ten_week_analysis_corrections.md)):**
1. Run `python testing/compare_unit_prices_full.py` to refresh PRECIOS and AG_PRECIOS from gold
2. Use PRECIOS_UPDATED.xlsx → PRECIOS.xlsx and AG_PRECIOS_UPDATED.xlsx → AG_PRECIOS.xlsx if updated
3. Review `transfer_products_not_in_precios.csv` and add missing products if gold has them

**Optional:** Use `--last-week-feb-8` to extend the last week to Feb 2-8 (matches gold date range). See [docs/ten_week_analysis_corrections.md](docs/ten_week_analysis_corrections.md).

**Reconciling totals (e.g. Feb 2-7):**
- Gold detail (AG + PT-R): ~312k for Feb 2-7. Gold_NUMEROS: ~283k (different aggregation; not directly comparable).
- `run_weekly_transfers.cmd` excludes CEDIS by default for gold comparison. Use `--include-cedis-dest` to include CEDIS.
- `weekly_breakdown.csv` shows per-week: Total_After, To_CEDIS, To_Branches_Only, APT_Only, AG_Only, Gold_Reference, Gold_NUMEROS (for configured weeks). 
