# Golden Transfer Investigation Report

**Date:** February 16, 2026  
**Scope:** Feb 2–8, 2026 (golden dataset week)  
**Golden Reference:** TRANSFERENCIAS DEL 02 AL 08 FEBRERO.xlsx  
**Verification:** Full plan re-run after UNIDAD-based PRECIO UNITARIO logic (Feb 16, 2026).

---

## Executive Summary

The investigation compared our pipeline results against the golden dataset under identical conditions (order exclusions, PRECIOS/AG_PRECIOS pricing, CEDIS excluded). Key findings **after UNIDAD-based logic**:

1. **Kavia AG+PT vs NUMEROS:** Our total is **3,591.82 lower** than gold (58,491.06 vs 62,082.88).
2. **PT-R vs PRECIOS:** 424 of 603 PT-R rows match PRECIOS exactly; 179 rows differ.
3. **Genuine product mismatches:** **0** — where gold applied correction, our PRECIOS now matches.
4. **AG rows:** Our AG unit costs match gold almost perfectly (Diff_Sum ≈ 0).

### UNIDAD Semantics (Current)

PRECIO DRIVE semantics depend on UNIDAD:
- **UNIDAD in (LT, KG):** PRECIO DRIVE is unit price → PRECIO UNITARIO = PRECIO DRIVE.
- **UNIDAD = PZ:** PRECIO DRIVE is presentation price → PRECIO UNITARIO = PRECIO DRIVE / PRESENTACION.

### Evolution of Results

| Metric | PRECIO DRIVE as unit | PRESENTACION conditional | UNIDAD (LT/KG/PZ) |
|--------|----------------------|--------------------------|-------------------|
| Ours Kavia AG+PT total | 57,910.14 | 58,728.46 | **58,491.06** |
| Gold NUMEROS Kavia | 62,082.88 | 62,082.88 | 62,082.88 |
| Difference | -4,172.74 | -3,354.42 | **-3,591.82** |
| PT-R == PRECIOS | 365 | 413 | **424** |
| PT-R ≠ PRECIOS | 238 | 190 | **179** |
| Genuine product mismatches | — | 4 | **0** |

**Observation:** With UNIDAD logic, all products where gold applied correction now match our PRECIOS. The 179 PT-R ≠ PRECIOS rows are from orders where gold did not apply correction (PT-excluded or other bleed-through).

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
| **Ours Kavia AG+PT total** | **58,728.46** (151 rows) |
| **Gold NUMEROS Kavia total** | **62,082.88** |
| **Difference** | **-3,354.42** (ours lower) |

### Per-Branch Totals (Ours, gold-week, CEDIS excluded)

| Sucursal | AG+PT Total |
|----------|-------------|
| Panem - Credi Club | 11,910.92 |
| Panem - Hospital Zambrano N | 55,887.77 |
| Panem - Hotel Kavia N | 58,728.46 |
| Panem - La Carreta N | 39,112.79 |
| Panem - Plaza Nativa | 23,428.37 |
| Panem - Plaza QIN N | 55,115.93 |
| Panem - Punto Valle | 75,197.53 |

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
| **PT-R == PRECIOS (exact match)** | **424** |
| Correction applied (PT-R ≠ PT-W) | 467 |
| **PT-R ≠ PRECIOS** (when PRECIOS available) | **179** |

### Interpretation

- **467** rows had a correction applied (PT-R ≠ PT-W), so Wansoft raw prices were adjusted.
- Of those, **424** match PRECIOS.xlsx exactly (with UNIDAD-based logic).
- **179** rows have PT-R UnitCost different from PRECIOS. These are from orders where **gold did not apply correction** (PT-R = PT-W): 38 rows from PT-excluded orders, 141 rows from 35 other bleed-through orders. In these rows gold kept Wansoft raw prices; our PRECIOS has the correct unit prices.

### Genuine Product Mismatches: None

With UNIDAD-based PRECIO UNITARIO logic, **there are 0 genuine product mismatches**. Where gold applied correction (PT-R ≠ PT-W), our PRECIOS now matches PT-R exactly.

### Products That Now Match (UNIDAD logic)

Salsas (UNIDAD=LT or PZ with correct PRESENTACION), CREMA MUFFIN ZANAHORIA, Crema de Danes, Crumble, Ensalada de pollo, and all other corrected products now align with gold when gold applied correction.

### PT-R ≠ PRECIOS Rows (gold did not correct)

The 179 differing rows are from orders where gold kept PT-W (no correction). Our PRECIOS has correct unit prices; gold did not apply PRECIOS correction to those orders.

| Source | Rows | Orders | Description |
|--------|------|--------|-------------|
| PT-excluded | 38 | 2 | Orders 9982-11588-2607562 and 9982-11588-2607690 (bleed-through from prior weeks; excluded in golden dataset) |
| Other bleed-through | 141 | 35 | Orders where gold did not apply PRECIOS correction; PT-R = PT-W (Wansoft raw) |

**Bleed-through detail:** All 46 products in the 141 other bleed-through rows are products that *are* correctly corrected (PT-R = PRECIOS) in other orders. For example, SALSA MORITA * has PT-R = 105.48 (corrected) in KAVIA orders, but in order 9982-7673-2607572 (Punto Valle) gold kept PT-W = 30.37 (no correction). So these are the same products; gold simply did not apply the PRECIOS correction to these specific orders. In our pipeline we apply PRECIOS to all PT rows, so we use 105.48; gold’s PT-R uses 30.37 for that order.


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

### Why is ours 3,591.82 lower than NUMEROS Kavia?

Possible contributors:

1. **Date range:** Gold NUMEROS may include Feb 8; ours used Feb 2–8 fetch. Unlikely to explain the full gap.

2. **Row set:** Our Kavia total uses 151 rows; gold detail sheets may include more or different rows.

3. **NUMEROS aggregation:** NUMEROS may use a different aggregation or process than summing AG+PT detail sheets.

4. **Producto matching:** 20 unmatched products kept Wansoft prices (Mayonesa de Panem *, LEVADURA NEVADA *, etc.); some might have different gold prices.

5. **PT-R vs PRECIOS:** 190 PT-R rows still differ from PRECIOS; if gold uses those PT-R prices and we use PRECIOS, our PT total could differ.

---

## 5. Recommendations

1. **Inspect NUMEROS sheet layout** to confirm how Kavia total is derived.
2. **UNIDAD semantics (implemented):** UNIDAD in (LT, KG) → PRECIO DRIVE is unit price; UNIDAD = PZ → PRECIO UNITARIO = PRECIO DRIVE / PRESENTACION. No genuine product mismatches remain.
3. **Add PRECIOS/AG_PRECIOS entries** for unmatched products (e.g. Mayonesa de Panem *, LEVADURA NEVADA *) if they exist in gold.
4. **Document order exclusions** as investigation-only; keep production pipeline unchanged (no order filtering).

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

## 7. Full Plan Run (Feb 16, 2026)

The entire plan was executed after the UNIDAD-based PRECIO UNITARIO logic.

### Scripts Executed

1. **`./scripts/mac-code-kit/run_update_precios_unit_prices.sh`** – Regenerates PRECIO UNITARIO using UNIDAD (LT/KG = unit price; PZ = PRECIO DRIVE / PRESENTACION).
2. **`./scripts/mac-code-kit/run_gold_week_investigation.sh`** – Fetches Feb 2–8 transfers, applies PRECIOS/AG_PRECIOS, order exclusions, excludes CEDIS, compares Kavia AG+PT to NUMEROS.
3. **`python3 testing/compare_pt_w_pt_r.py`** – Parses PT-W and PT-R sheets from golden Excel, compares to PRECIOS.xlsx.
4. **`python3 testing/investigate_transfer_cost.py`** – Line-by-line comparison of ours vs gold, produces `investigation_report.csv` and `AG_PRECIOS.xlsx`.

### Observations from Full Run

| Metric | Value |
|--------|-------|
| Ours Kavia AG+PT | 58,491.06 |
| Gold NUMEROS Kavia | 62,082.88 |
| Difference | -3,591.82 |
| PT-R == PRECIOS (exact) | 424 |
| PT-R ≠ PRECIOS | 179 |
| Genuine product mismatches | 0 |
| Gold-week rows (after prices) | 1,370 → 1,278 (exclusions) → 1,220 (no CEDIS) |
| Unmatched productos | 20 (Mayonesa de Panem *, LEVADURA NEVADA *, etc.) |
| investigate_transfer_cost: matched | 1,111 |
| investigate_transfer_cost: unmatched | 72 |
| AG Diff_Sum (matched) | ≈ 0 |
| PT Diff_Sum (matched) | ≈ -9.90 |

With UNIDAD logic, all products where gold applied correction now match our PRECIOS. The 179 PT-R ≠ PRECIOS rows are from orders gold did not correct.

---

## Appendix: Scripts Run

```bash
./scripts/mac-code-kit/run_update_precios_unit_prices.sh
./scripts/mac-code-kit/run_gold_week_investigation.sh
python3 testing/compare_pt_w_pt_r.py
python3 testing/investigate_transfer_cost.py
```
