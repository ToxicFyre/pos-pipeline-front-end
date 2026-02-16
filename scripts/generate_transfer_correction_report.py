#!/usr/bin/env python3
"""Generate HTML report: totals before vs after correction, and item price differences.

Uses output from the 12-week transfer pipeline:
- weekly_cost_comparison.csv
- weekly_breakdown.csv
- price_correction_report.csv
- price_changes_*.csv (per week)

Run from project root. Output: data/c_processed/transfers/weekly/transfer_correction_report.html
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pandas as pd

_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

WEEKLY_DIR = _root / "data" / "c_processed" / "transfers" / "weekly"
OUTPUT_PATH = WEEKLY_DIR / "transfer_correction_report.html"


def parse_week_from_filename(name: str) -> tuple[str, str] | None:
    m = re.match(r"price_changes_(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})\.csv", name)
    return (m.group(1), m.group(2)) if m else None


def load_all_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, list[tuple[str, str, pd.DataFrame]]]:
    """Load weekly comparison, breakdown, branch report, and all price_changes."""
    comp = pd.read_csv(WEEKLY_DIR / "weekly_cost_comparison.csv")
    breakdown = pd.read_csv(WEEKLY_DIR / "weekly_breakdown.csv")
    branch = pd.read_csv(WEEKLY_DIR / "price_correction_report.csv")

    price_changes_list: list[tuple[str, str, pd.DataFrame]] = []
    for p in sorted(WEEKLY_DIR.glob("price_changes_*.csv")):
        week = parse_week_from_filename(p.name)
        if week:
            df = pd.read_csv(p)
            if not df.empty:
                price_changes_list.append((week[0], week[1], df))

    return comp, breakdown, branch, price_changes_list


def aggregate_item_price_differences(
    price_changes_list: list[tuple[str, str, pd.DataFrame]],
) -> pd.DataFrame:
    """Aggregate price changes by Producto + Almacen_origen across all weeks."""
    rows = []
    for start, end, df in price_changes_list:
        df = df.copy()
        df["Week"] = f"{start} to {end}"
        rows.append(df)

    if not rows:
        return pd.DataFrame()

    combined = pd.concat(rows, ignore_index=True)
    combined["Costo_unitario_before"] = pd.to_numeric(combined["Costo_unitario_before"], errors="coerce")
    combined["Costo_unitario_after"] = pd.to_numeric(combined["Costo_unitario_after"], errors="coerce")
    combined["Costo_before"] = pd.to_numeric(combined["Costo_before"], errors="coerce")
    combined["Costo_after"] = pd.to_numeric(combined["Costo_after"], errors="coerce")
    combined["Cantidad"] = pd.to_numeric(combined["Cantidad"], errors="coerce")

    agg = (
        combined.groupby(["Producto", "Almacen_origen"], as_index=False)
        .agg(
            Total_Cantidad=("Cantidad", "sum"),
            Costo_before_sum=("Costo_before", "sum"),
            Costo_after_sum=("Costo_after", "sum"),
            Unit_before_avg=("Costo_unitario_before", "mean"),
            Unit_after=("Costo_unitario_after", "first"),
            Num_lines=("Cantidad", "count"),
        )
        .rename(columns={"Unit_after": "Unit_after_corrected"})
    )
    agg["Cost_Difference"] = agg["Costo_after_sum"] - agg["Costo_before_sum"]
    agg["Unit_Difference"] = agg["Unit_after_corrected"] - agg["Unit_before_avg"]
    agg["Pct_Change_Unit"] = (
        (agg["Unit_Difference"] / agg["Unit_before_avg"].replace(0, float("nan"))) * 100
    )
    agg = agg.sort_values("Cost_Difference", key=abs, ascending=False)
    return agg


def format_currency(x: float) -> str:
    return f"${x:,.2f}" if pd.notna(x) else "—"


def format_pct(x: float) -> str:
    return f"{x:+.1f}%" if pd.notna(x) else "—"


def _breakdown_rows(breakdown: pd.DataFrame) -> str:
    rows = []
    for _, row in breakdown.iterrows():
        to_cedis = row.get("To_CEDIS", "")
        to_branches = row.get("To_Branches_Only", "")
        apt = row.get("APT_Only", "")
        ag = row.get("AG_Only", "")
        rows.append(
            f"<tr><td>{row['Week']}</td><td class='num'>{format_currency(row.get('Total_After'))}</td>"
            f"<td class='num'>{format_currency(to_cedis) if pd.notna(to_cedis) else '—'}</td>"
            f"<td class='num'>{format_currency(to_branches) if pd.notna(to_branches) else '—'}</td>"
            f"<td class='num'>{format_currency(apt) if pd.notna(apt) else '—'}</td>"
            f"<td class='num'>{format_currency(ag) if pd.notna(ag) else '—'}</td></tr>"
        )
    return "".join(rows)


def build_html(
    comp: pd.DataFrame,
    breakdown: pd.DataFrame,
    branch: pd.DataFrame,
    item_agg: pd.DataFrame,
    price_changes_list: list[tuple[str, str, pd.DataFrame]],
) -> str:
    total_before = comp["Total_Before"].sum()
    total_after = comp["Total_After"].sum()
    total_diff = total_after - total_before
    total_pct = (total_diff / total_before * 100) if total_before else 0

    weeks_html = []
    for _, row in comp.iterrows():
        weeks_html.append(
            f"""
        <tr>
            <td>{row['Week']}</td>
            <td class="num">{format_currency(row['Total_Before'])}</td>
            <td class="num">{format_currency(row['Total_After'])}</td>
            <td class="num {'neg' if row['Difference'] < 0 else 'pos'}">{format_currency(row['Difference'])}</td>
            <td class="num {'neg' if row['Pct_Change'] < 0 else 'pos'}">{format_pct(row['Pct_Change'])}</td>
        </tr>"""
        )

    branch_html = []
    for _, row in branch.iterrows():
        branch_html.append(
            f"""
        <tr>
            <td>{row['Sucursal destino']}</td>
            <td class="num">{format_currency(row['Total_Before'])}</td>
            <td class="num">{format_currency(row['Total_After'])}</td>
            <td class="num {'neg' if row['Difference'] < 0 else 'pos'}">{format_currency(row['Difference'])}</td>
            <td class="num {'neg' if row['Pct_Change'] < 0 else 'pos'}">{format_pct(row['Pct_Change'])}</td>
        </tr>"""
        )

    top_items = item_agg.head(80)
    items_html = []
    for _, row in top_items.iterrows():
        items_html.append(
            f"""
        <tr>
            <td>{row['Producto']}</td>
            <td>{row['Almacen_origen']}</td>
            <td class="num">{row['Total_Cantidad']:,.0f}</td>
            <td class="num">{format_currency(row['Unit_before_avg'])}</td>
            <td class="num">{format_currency(row['Unit_after_corrected'])}</td>
            <td class="num">{format_currency(row['Unit_Difference'])}</td>
            <td class="num {'neg' if row['Pct_Change_Unit'] < 0 else 'pos'}">{format_pct(row['Pct_Change_Unit'])}</td>
            <td class="num">{format_currency(row['Costo_before_sum'])}</td>
            <td class="num">{format_currency(row['Costo_after_sum'])}</td>
            <td class="num {'neg' if row['Cost_Difference'] < 0 else 'pos'}">{format_currency(row['Cost_Difference'])}</td>
        </tr>"""
        )

    week_summary = []
    for start, end, df in price_changes_list:
        n = len(df)
        cost_before = df["Costo_before"].sum()
        cost_after = df["Costo_after"].sum()
        diff = cost_after - cost_before
        week_summary.append(
            f"<tr><td>{start} to {end}</td><td class='num'>{n:,}</td>"
            f"<td class='num'>{format_currency(cost_before)}</td><td class='num'>{format_currency(cost_after)}</td>"
            f"<td class='num {'neg' if diff < 0 else 'pos'}'>{format_currency(diff)}</td></tr>"
        )

    return f"""<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Transfer Price Correction Report — 12 Weeks</title>
    <style>
        :root {{ font-family: 'Segoe UI', system-ui, sans-serif; font-size: 14px; }}
        body {{ max-width: 1200px; margin: 0 auto; padding: 2rem; background: #f8f9fa; }}
        h1 {{ color: #1a1a2e; border-bottom: 2px solid #16213e; padding-bottom: 0.5rem; }}
        h2 {{ color: #16213e; margin-top: 2rem; }}
        h3 {{ color: #0f3460; margin-top: 1.5rem; }}
        table {{ width: 100%; border-collapse: collapse; background: white; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }}
        th, td {{ padding: 0.5rem 0.75rem; text-align: left; border-bottom: 1px solid #e9ecef; }}
        th {{ background: #16213e; color: white; font-weight: 600; }}
        .num {{ text-align: right; font-variant-numeric: tabular-nums; }}
        .neg {{ color: #c0392b; }}
        .pos {{ color: #27ae60; }}
        .summary {{ background: #e8f4f8; padding: 1rem; border-radius: 6px; margin: 1rem 0; }}
        .summary strong {{ font-size: 1.1rem; }}
        p {{ line-height: 1.6; color: #333; }}
    </style>
</head>
<body>
    <h1>Transfer Price Correction Report</h1>
    <p>Report generated from the 12-week transfer pipeline. Transfers are downloaded from Wansoft, corrected with PRECIOS.xlsx (ALMACEN PRODUCTO TERMINADO) and AG_PRECIOS.xlsx (ALMACEN GENERAL), and pivoted by branch × category.</p>

    <h2>1. Totals Before vs After Correction</h2>
    <div class="summary">
        <strong>Overall (branches only, CEDIS excluded):</strong><br>
        Before: {format_currency(total_before)} → After: {format_currency(total_after)}<br>
        Difference: <span class="{'neg' if total_diff < 0 else 'pos'}">{format_currency(total_diff)} ({format_pct(total_pct)})</span>
    </div>

    <h3>By Week (Mon–Sun)</h3>
    <table>
        <thead>
            <tr>
                <th>Week</th>
                <th class="num">Total Before</th>
                <th class="num">Total After</th>
                <th class="num">Difference</th>
                <th class="num">% Change</th>
            </tr>
        </thead>
        <tbody>
            {''.join(weeks_html)}
        </tbody>
    </table>

    <h3>Weekly Breakdown (To Branches Only)</h3>
    <p>Per-week totals after correction (To_Branches_Only excludes CEDIS). APT_Only = ALMACEN PRODUCTO TERMINADO; AG_Only = ALMACEN GENERAL.</p>
    <table>
        <thead>
            <tr>
                <th>Week</th>
                <th class="num">Total After</th>
                <th class="num">To CEDIS</th>
                <th class="num">To Branches</th>
                <th class="num">APT Only</th>
                <th class="num">AG Only</th>
            </tr>
        </thead>
        <tbody>
            {_breakdown_rows(breakdown)}
        </tbody>
    </table>

    <h3>By Destination Branch (All Weeks Combined)</h3>
    <table>
        <thead>
            <tr>
                <th>Sucursal destino</th>
                <th class="num">Total Before</th>
                <th class="num">Total After</th>
                <th class="num">Difference</th>
                <th class="num">% Change</th>
            </tr>
        </thead>
        <tbody>
            {''.join(branch_html)}
        </tbody>
    </table>

    <h2>2. Item Price Differences</h2>
    <p>This section lists products whose unit price was changed during correction. Wansoft transfer data often has incorrect or inconsistent unit costs; PRECIOS.xlsx and AG_PRECIOS.xlsx provide the authoritative prices. Each row shows the product, origin warehouse, total quantity transferred, average unit price before correction, corrected unit price, and the resulting cost impact.</p>

    <h3>Price Changes by Week (Lines Affected)</h3>
    <table>
        <thead>
            <tr>
                <th>Week</th>
                <th class="num"># Lines</th>
                <th class="num">Cost Before</th>
                <th class="num">Cost After</th>
                <th class="num">Difference</th>
            </tr>
        </thead>
        <tbody>
            {''.join(week_summary)}
        </tbody>
    </table>

    <h3>Top 80 Products by Cost Impact (All Weeks)</h3>
    <p>Products ranked by absolute cost difference. Negative = correction reduced cost; Positive = correction increased cost.</p>
    <table>
        <thead>
            <tr>
                <th>Producto</th>
                <th>Almacen origen</th>
                <th class="num">Cantidad</th>
                <th class="num">Unit Before</th>
                <th class="num">Unit After</th>
                <th class="num">Unit Diff</th>
                <th class="num">% Unit</th>
                <th class="num">Cost Before</th>
                <th class="num">Cost After</th>
                <th class="num">Cost Diff</th>
            </tr>
        </thead>
        <tbody>
            {''.join(items_html)}
        </tbody>
    </table>

    <p><em>Report generated from data in data/c_processed/transfers/weekly/</em></p>
</body>
</html>
"""


def main() -> int:
    if not WEEKLY_DIR.exists():
        print(f"Error: {WEEKLY_DIR} not found")
        return 1

    comp_path = WEEKLY_DIR / "weekly_cost_comparison.csv"
    if not comp_path.exists():
        print(f"Error: {comp_path} not found. Run the 12-week transfer pipeline first.")
        return 1

    comp, breakdown, branch, price_changes_list = load_all_data()
    item_agg = aggregate_item_price_differences(price_changes_list)

    html = build_html(comp, breakdown, branch, item_agg, price_changes_list)
    OUTPUT_PATH.write_text(html, encoding="utf-8")
    print(f"Saved: {OUTPUT_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
