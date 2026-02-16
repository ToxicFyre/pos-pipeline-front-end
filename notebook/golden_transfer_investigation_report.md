# Golden Transfer Investigation Report

**Date:** February 13, 2026  
**Scope:** Feb 2–8, 2026 (golden dataset week)  
**Golden Reference:** TRANSFERENCIAS DEL 02 AL 08 FEBRERO.xlsx  
**Verification:** Full plan re-run after PRECIO UNITARIO correction (Feb 13, 2026).

---

## Executive Summary

The investigation compared our pipeline results against the golden dataset under identical conditions (order exclusions, PRECIOS/AG_PRECIOS pricing, CEDIS excluded). Key findings **after PRECIO UNITARIO correction**:

1. **Kavia AG+PT vs NUMEROS:** Our total is **13,639.76 lower** than gold (48,443.12 vs 62,082.88).
2. **PT-R vs PRECIOS:** 375 of 603 PT-R rows match PRECIOS exactly; 228 rows differ.
3. **AG rows:** Our AG unit costs match gold almost perfectly (Diff_Sum ≈ 0).
4. **PT rows:** Small residual PT diff (~9.90 total); many PT-R prices in gold differ from PRECIOS.

### Before vs After PRECIO UNITARIO Fix

| Metric | Before (PRECIO DRIVE as unit) | After (PRECIO UNITARIO) |
|--------|-------------------------------|--------------------------|
| Ours Kavia AG+PT total | 57,910.14 | 48,443.12 |
| Gold NUMEROS Kavia | 62,082.88 | 62,082.88 |
| Difference | -4,172.74 | **-13,639.76** |
| PT-R == PRECIOS (exact) | 365 | 375 |
| PT-R ≠ PRECIOS | 238 | 228 |

**Observation:** After the PRECIO UNITARIO correction, our Kavia total *dropped* by ~9.5k (57,910 → 48,443), and the gap to gold *increased*. This indicates that PRECIO UNITARIO (PRECIO DRIVE / PRESENTACION) changes the cost in both directions depending on product mix: for products with PRESENTACION > 1 we now use a smaller unit price (reducing cost); for PRESENTACION < 1 we use a larger unit price (increasing cost). The net effect was a decrease, so products with PRESENTACION > 1 dominate in this dataset. Gold’s PT-R values do not uniformly follow our PRECIOS logic; 228 rows still differ.

---

## 1. Gold-Week Investigation (Feb 2–8)

### Methodology

- Downloaded transfers for **Feb 2–8** from Wansoft
- Applied **PRECIOS.xlsx** (PT) and **AG_PRECIOS.xlsx** (AG)
- Applied **order exclusions** (AG: 3 orders; PT: 2 orders)
- Excluded **CEDIS** destinations
- Compared our Kavia AG+PT total to NUMEROS Kavia

### Results

| Metric | Value |
|--------|-------|
| Rows after apply prices | 1,370 |
| Rows after order exclusions | 1,278 |
| Rows after excluding CEDIS | 1,220 |
| **Ours Kavia AG+PT total** | **48,443.12** (151 rows) |
| **Gold NUMEROS Kavia total** | **62,082.88** |
| **Difference** | **-13,639.76** (ours lower) |

### Per-Branch Totals (Ours, gold-week, CEDIS excluded)

| Sucursal | AG+PT Total |
|----------|-------------|
| Panem - Credi Club | 11,448.92 |
| Panem - Hospital Zambrano N | 51,391.99 |
| Panem - Hotel Kavia N | 48,443.12 |
| Panem - La Carreta N | 35,593.25 |
| Panem - Plaza Nativa | 20,786.33 |
| Panem - Plaza QIN N | 47,980.65 |
| Panem - Punto Valle | 64,204.27 |

---

## 2. PT-W vs PT-R Comparison (Golden Data)

### Purpose

Check whether PT-R (corrected) prices in the golden file match PRECIOS.xlsx and how they differ from PT-W (raw Wansoft).

### Results

| Metric | Count |
|--------|-------|
| PT-W rows (golden) | 617 |
| PT-R rows (golden) | 603 |
| Matched (Branch, Orden, Producto) | 603 |
| **PT-R == PRECIOS (exact match)** | **365** |
| Correction applied (PT-R ≠ PT-W) | 467 |
| **PT-R ≠ PRECIOS** (when PRECIOS available) | **238** |

### Interpretation

- **467** rows had a correction applied (PT-R ≠ PT-W), so Wansoft raw prices were adjusted.
- Of those, **375** match PRECIOS.xlsx exactly (with PRECIO UNITARIO).
- **228** rows have PT-R UnitCost different from PRECIOS, indicating:
  - Gold uses a different price source than our PRECIOS.xlsx for some products, or
  - Rounding / formula differences, or
  - Gold applies manual overrides not reflected in PRECIOS.

### Notable Examples (PT-R differs from PRECIOS)

| Product | PT-W (Wansoft) | PT-R (Gold) | PRECIOS (UnitCost) |
|---------|----------------|-------------|---------------------|
| SALSA MORITA * | 30.37 | 105.48 | 35.16 |
| SALSA POMODORO 2024 * | 110.24 | 153.15 | 51.05 |
| Salsa Roja * | 42.97 | 105.81 | 35.27 |
| SALSA VERDE * | 37.51 | 115.50 | 38.50 |
| CREMA MUFFIN ZANAHORIA | 111.31 | 111.26 | 222.52 |

Salsas: Gold uses ~3× our PRECIOS (likely gold uses price-per-“presentation” where we use PRECIO UNITARIO). CREMA MUFFIN ZANAHORIA: Gold ≈ half our PRECIO UNITARIO.

Orders **9982-11588-2607562** and **9982-11588-2607690** (PT-excluded) show PT-R = PT-W (no correction); these are bleed-through orders not corrected in gold.

---

## 3. Full Investigation (investigate_transfer_cost)

### Methodology

- Uses **transfers_2026-02-02_2026-02-07.csv** (Feb 2–7)
- Applies order exclusions to “ours”
- Compares line-by-line with gold (AG + PT-R)
- Produces AG_PRECIOS.xlsx from gold

### Results

| Metric | Value |
|--------|-------|
| Gold rows (all) | 1,298 |
| Gold AG rows | 695 |
| Ours after order exclusions | 1,183 |
| **Matched** | **1,111** |
| **Unmatched** | **72** |
| Matched AG | 642 |
| Matched PT | 469 |

### Cost Difference by Origin (matched rows)

| Almacén origen | Diff_Sum | Count |
|----------------|----------|-------|
| ALMACEN GENERAL | ~0 (4.6e-13) | 642 |
| ALMACEN PRODUCTO TERMINADO | -9.90 | 469 |

- **AG:** Ours matches gold almost exactly.
- **PT:** Small residual diff (~9.90) across 469 matched PT rows.

---

## 4. Root Cause Analysis: Kavia Gap

### Why is ours 13,639.76 lower than NUMEROS Kavia?

Possible contributors:

1. **Date range:** Gold NUMEROS may include Feb 8; ours used Feb 2–8 fetch. Unlikely to explain the full gap.

2. **Row set:** Our Kavia total uses 151 rows; gold detail sheets may include more or different rows.

3. **NUMEROS aggregation:** NUMEROS may use a different aggregation or process than summing AG+PT detail sheets.

4. **Producto matching:** 20 unmatched products kept Wansoft prices (Mayonesa de Panem *, LEVADURA NEVADA *, etc.); some might have different gold prices.

5. **PT-R vs PRECIOS:** 238 PT-R rows differ from PRECIOS; if gold uses those PT-R prices and we use PRECIOS, our PT total could differ.

---

## 5. Recommendations

1. **Inspect NUMEROS sheet layout** to confirm how Kavia total is derived.
2. **Align PRECIOS.xlsx** with PT-R values where PT-R ≠ PRECIOS and PT-R is deemed correct. Notable PT-R vs PRECIOS mismatches: salsas (SALSA MORITA, SALSA POMODORO 2024, Salsa Roja, SALSA VERDE) where gold uses ~3× our PRECIOS; CREMA MUFFIN ZANAHORIA where gold uses half our PRECIO UNITARIO.
3. **Clarify PRESENTACION semantics:** PRECIO UNITARIO = PRECIO DRIVE / PRESENTACION assumes transfer Cantidad is in base units (e.g. liters). If Cantidad is in presentation units (e.g. bottles), PRECIO DRIVE may be correct as-is. The mixed effect (our total dropped after PRECIO UNITARIO) suggests product-specific logic.
4. **Add PRECIOS/AG_PRECIOS entries** for unmatched products (e.g. Mayonesa de Panem *, LEVADURA NEVADA *) if they exist in gold.
5. **Document order exclusions** as investigation-only; keep production pipeline unchanged (no order filtering).

---

## 6. Output Files Referenced

| File | Location |
|------|----------|
| Kavia comparison | `data/c_processed/transfers/weekly/kavia_numeros_comparison.csv` |
| Gold-week by branch | `data/c_processed/transfers/weekly/gold_week_by_branch.csv` |
| PT-W vs PT-R comparison | `data/c_processed/transfers/weekly/pt_w_vs_pt_r_comparison.csv` |
| PT-W vs PT-R report | `data/c_processed/transfers/weekly/pt_w_pt_r_report.md` |
| Investigation report | `data/c_processed/transfers/weekly/investigation_report.csv` |

---

## 7. Full Plan Run (Feb 13, 2026)

The entire plan from `/Users/mac/.cursor/plans/golden_transfer_investigation_985fae6c.plan.md` was executed after the PRECIO UNITARIO correction.

### Scripts Executed

1. **`./scripts/mac-code-kit/run_gold_week_investigation.sh`** – Fetches Feb 2–8 transfers, applies PRECIOS/AG_PRECIOS, order exclusions, excludes CEDIS, compares Kavia AG+PT to NUMEROS.
2. **`python3 testing/compare_pt_w_pt_r.py`** – Parses PT-W and PT-R sheets from golden Excel, compares to PRECIOS.xlsx.
3. **`python3 testing/investigate_transfer_cost.py`** – Line-by-line comparison of ours vs gold, produces `investigation_report.csv` and `AG_PRECIOS.xlsx`.

### Observations from Full Run

- **Gold-week fetch:** 1,370 rows after apply prices → 1,278 after order exclusions → 1,220 after CEDIS exclusion.
- **Unmatched productos** (kept Wansoft prices): Mayonesa de Panem *, LEVADURA NEVADA *, ACEITE DE OLIVA EL ANDALUZ VIRGEN SUAVE *, and 17 others.
- **investigate_transfer_cost:** 1,111 matched, 72 unmatched. AG Diff_Sum ≈ 0; PT Diff_Sum ≈ -9.90.
- **PT-R vs PRECIOS:** 10 additional rows now match (375 vs 365) with PRECIO UNITARIO; 228 still differ. Gold uses non-PRECIOS sources for salsas (higher) and CREMA MUFFIN ZANAHORIA (lower than our PRECIO UNITARIO).

---

## Appendix: Scripts Run

```bash
./scripts/mac-code-kit/run_gold_week_investigation.sh
python3 testing/compare_pt_w_pt_r.py
python3 testing/investigate_transfer_cost.py
```
