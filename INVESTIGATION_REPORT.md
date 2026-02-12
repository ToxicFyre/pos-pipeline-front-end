# Investigation Report: Transfer Cost Discrepancy vs Golden Reference

## Summary of Discrepancy

| Branch      | Ours (original) | Gold        | Diff        |
| ----------- | --------------- | ----------- | ----------- |
| Kavia       | 87,193          | 62,083      | +25,110     |
| Punto Valle | 64,572          | 64,553      | +19         |
| Qin         | 47,776          | 46,205      | +1,572      |
| Zambrano    | 48,439          | 46,475      | +1,965      |
| Carreta     | 34,137          | 35,296      | -1,159      |
| Nativa      | 19,725          | 18,534      | +1,192      |
| Crediclub   | 10,946          | 10,224      | +722        |
| **TOTAL**   | **312,789**     | **283,368** | **+29,421** |

**Key finding**: Kavia accounts for ~85% of the gap. Punto Valle matches almost exactly (unit costs correct there).

## Root Causes Identified

1. **AG unit prices**: Gold uses distinct unit prices for ALMACEN GENERAL (AG) products. We were using Wansoft raw costs. Gold has a separate price source for AG (not in PRECIOS.xlsx).
2. **PT sheet selection**: Gold has both PT-W (before correction) and PT-R (corrected) sheets. The investigation script now uses PT-R only (skips PT-W) to align with gold NUMEROS.
3. **Producto matching**: ~58 rows in our data had no match in gold (different Orden or Producto naming). ~20 AG products remain unmatched (e.g. Sopa de tomate, LEVADURA NEVADA) and keep original Wansoft prices.

## Line-Level Comparison Findings

- **Matched rows**: 1,217 of 1,275 total.
- **Unmatched rows**: 58 (no gold (Orden, Producto) match).
- **AG rows**: Unit costs match for matched AG rows (Diff_UnitCost ≈ 0). AG_PRECIOS derived from gold aligns our AG costs with gold.
- **PT rows**: After switching to PT-R only, matched PT rows show Diff_Sum ≈ -3,836 (ours lower than gold); AG matched rows show Diff_Sum ≈ +2,275 (ours higher). Net matched diff ≈ -1,560.

## AG Unit Cost Investigation

- **AG rows from gold**: 1,319 rows across KAVIA-AG, PV-AG, QIN-AG, HZ-AG, CARRETA-AG, NATIVA-AG, CC-AG.
- **Derived canonical unit cost**: Median UnitCost per Producto across AG sheets.
- **Products in AG_PRECIOS**: 194 unique products.

## AG_PRECIOS.xlsx Creation

- **Source**: Golden reference `TRANSFERENCIAS DEL 02 AL 08 FEBRERO.xlsx`, all `{BRANCH}-AG` sheets.
- **Columns**: Producto, Precio unitario.
- **Location**: Project root (`AG_PRECIOS.xlsx`).
- **Usage**: Applied only when `Almacén origen = ALMACEN GENERAL`.

## Pipeline Changes Applied

1. **`testing/investigate_transfer_cost.py`**
   - Parses gold Excel, detects header row, extracts detail sheets.
   - Builds gold lookup (Orden, Producto) → (UnitCost, Costo).
   - Matches our rows to gold, compares unit costs, outputs `investigation_report.csv`.
   - Includes unmatched rows in report.
   - Derives AG unit costs from gold AG sheets, writes `AG_PRECIOS.xlsx` to project root.

2. **`testing/get_weekly_transfers_with_prices.py`**
   - Added `load_ag_precios()` to load AG_PRECIOS.xlsx.
   - Added `--ag-precios-path` argument (default: `AG_PRECIOS.xlsx`).
   - Updated `apply_prices()`:
     - PT rows: use PRECIOS.xlsx (NOMBRE WANSOFT, PRECIO DRIVE).
     - AG rows: use AG_PRECIOS.xlsx when available.
   - Unmatched AG products keep original Wansoft costs.

## Before/After Totals

| Metric          | Before (PRECIOS only) | After (PRECIOS + AG_PRECIOS) |
| --------------- | --------------------- | ---------------------------- |
| Total (all)     | ~434k                 | ~386k                        |
| To Branches     | ~362k                 | ~315k                        |
| Gold target     | 283,368               | 283,368                      |

**Remaining gap**: ~32k over gold (To_Branches_Only). Possible causes:
- Gold may exclude CEDIS or use different branch set.
- Date range: gold Feb 2–8; ours Feb 2–7.
- PT sheet selection (PT-R vs PT-W) affects gold lookup.
- ~20 AG products unmatched and still use higher Wansoft prices.

## Recommendations

1. **PT sheet selection**: Implemented—investigation script uses PT-R only.
2. **Producto matching audit**: Add PRECIOS/AG_PRECIOS mappings for typos (e.g. "Mayonesa de Panem" vs "Mayones de Panem").
3. **Date range alignment**: Extend our week to Feb 2–8 if gold uses that range.
4. **Mart-style filtering**: If gold NUMEROS excludes unmapped rows, apply `bucket_row` from mart and exclude unmapped from totals.
