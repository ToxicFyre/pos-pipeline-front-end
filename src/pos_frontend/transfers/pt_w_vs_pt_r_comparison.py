"""Compare *-PT-W (raw Wansoft) vs *-PT-R (corrected per PRECIOS) in golden Excel.

Reports whether PT-R prices match PRECIOS.xlsx and how they differ from PT-W.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import pandas as pd

from pos_frontend.config.paths import get_project_root
from pos_frontend.transfers.gold_investigation import (
    SHEET_TO_SUCURSAL,
    GOLD_Fecha_START,
    GOLD_Fecha_END,
    parse_sheet,
)
from pos_frontend.transfers.weekly_with_prices import load_precios as load_precios_base

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def parse_pt_sheets(path: Path, sheet_suffix: str) -> pd.DataFrame:
    """Parse all *-PT-W or *-PT-R sheets from golden Excel."""
    xl = pd.ExcelFile(path)
    rows = []
    for sheet in xl.sheet_names:
        if sheet_suffix not in sheet or sheet == "NUMEROS":
            continue
        parts = sheet.split("-")
        if len(parts) < 2:
            continue
        branch = parts[0]
        sucursal = SHEET_TO_SUCURSAL.get(branch, f"Panem - {branch}")
        df = pd.read_excel(path, sheet_name=sheet, header=None)
        parsed = parse_sheet(df, sheet, sucursal)
        if parsed.empty:
            continue
        parsed = parsed[
            parsed["Almacen_origen"].str.contains("ALMACEN PRODUCTO TERMINADO", na=False)
        ]
        if parsed.empty:
            continue
        parsed["Sheet"] = sheet
        parsed["Branch"] = branch
        rows.append(parsed)
    if not rows:
        return pd.DataFrame()
    out = pd.concat(rows, ignore_index=True)
    out = out[(out["Fecha"] >= GOLD_Fecha_START) & (out["Fecha"] <= GOLD_Fecha_END)]
    return out


def load_precios(path: Path) -> pd.DataFrame:
    """Load PRECIOS.xlsx (Producto, Precio unitario). Uses PRECIO UNITARIO when present."""
    if not path.exists():
        return pd.DataFrame()
    df = load_precios_base(path)
    df["_norm"] = df["Producto"].astype(str).str.strip().str.lower()
    return df[["Producto", "Precio unitario", "_norm"]].drop_duplicates(subset=["_norm"], keep="first")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Compare PT-W vs PT-R in golden Excel")
    parser.add_argument("--gold", default="TRANSFERENCIAS DEL 02 AL 08 FEBRERO.xlsx")
    parser.add_argument("--precios", default="PRECIOS.xlsx")
    parser.add_argument("--output-dir", default="data/c_processed/transfers/weekly")
    args = parser.parse_args(argv or [])

    project_root = get_project_root()
    gold_path = project_root / args.gold
    precios_path = project_root / args.precios
    output_dir = project_root / args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    if not gold_path.exists():
        logger.error("Gold file not found: %s", gold_path)
        return 1

    # Parse PT-W and PT-R
    pt_w = parse_pt_sheets(gold_path, "-PT-W")
    pt_r = parse_pt_sheets(gold_path, "-PT-R")
    logger.info("PT-W rows: %d, PT-R rows: %d", len(pt_w), len(pt_r))

    if pt_w.empty or pt_r.empty:
        logger.warning("No PT-W or PT-R data")
        return 0

    # Merge on Branch, Orden, Producto
    pt_w_key = pt_w[["Branch", "Orden", "Producto", "UnitCost"]].copy()
    pt_w_key = pt_w_key.rename(columns={"UnitCost": "UnitCost_PT_W"})
    pt_r_key = pt_r[["Branch", "Orden", "Producto", "UnitCost"]].copy()
    pt_r_key = pt_r_key.rename(columns={"UnitCost": "UnitCost_PT_R"})
    merged = pt_r_key.merge(
        pt_w_key,
        on=["Branch", "Orden", "Producto"],
        how="inner",
    )
    logger.info("Matched (Branch, Orden, Producto) rows: %d", len(merged))

    # Load PRECIOS
    precios = load_precios(precios_path)
    merged["_norm"] = merged["Producto"].astype(str).str.strip().str.lower()
    merged = merged.merge(
        precios[["_norm", "Precio unitario"]],
        on="_norm",
        how="left",
        suffixes=("", "_precios"),
    )
    merged = merged.rename(columns={"Precio unitario": "PRECIOS_UnitCost"})
    merged = merged.drop(columns=["_norm"], errors="ignore")

    # Compare
    merged["PT_R_eq_PT_W"] = (merged["UnitCost_PT_R"] - merged["UnitCost_PT_W"]).abs() < 0.001
    merged["PT_R_eq_PRECIOS"] = (
        merged["PRECIOS_UnitCost"].notna()
        & (merged["UnitCost_PT_R"] - merged["PRECIOS_UnitCost"]).abs() < 0.001
    )
    merged["Correction_Applied"] = ~merged["PT_R_eq_PT_W"]
    merged["PT_R_diff_PRECIOS"] = merged.apply(
        lambda r: (r["UnitCost_PT_R"] - r["PRECIOS_UnitCost"]) if pd.notna(r["PRECIOS_UnitCost"]) else float("nan"),
        axis=1,
    )

    # Summary
    exact_match_precios = merged["PT_R_eq_PRECIOS"].sum()
    correction_applied = merged["Correction_Applied"].sum()
    diff_precios = merged[~merged["PT_R_eq_PRECIOS"] & merged["PRECIOS_UnitCost"].notna()]
    logger.info("PT-R == PRECIOS (exact): %d", exact_match_precios)
    logger.info("Correction applied (PT-R != PT-W): %d", correction_applied)
    logger.info("PT-R != PRECIOS (when PRECIOS available): %d", len(diff_precios))

    # Write CSV
    csv_path = output_dir / "pt_w_vs_pt_r_comparison.csv"
    merged.to_csv(csv_path, index=False)
    logger.info("Saved %s", csv_path)

    # Write markdown report
    md_lines = [
        "# PT-W vs PT-R Comparison Report",
        "",
        "## Summary",
        "",
        f"- Total matched rows (Branch, Orden, Producto): {len(merged)}",
        f"- PT-R UnitCost == PRECIOS (exact match): {exact_match_precios}",
        f"- Correction applied (PT-R != PT-W): {correction_applied}",
        f"- PT-R != PRECIOS (when PRECIOS available): {len(diff_precios)}",
        "",
        "## Rows where PT-R differs from PRECIOS",
        "",
    ]
    if not diff_precios.empty:
        md_lines.append(diff_precios[["Branch", "Orden", "Producto", "UnitCost_PT_W", "UnitCost_PT_R", "PRECIOS_UnitCost"]].head(50).to_string(index=False))
    else:
        md_lines.append("(None)")
    md_path = output_dir / "pt_w_pt_r_report.md"
    with open(md_path, "w", encoding="utf-8") as f:
        f.write("\n".join(md_lines))
    logger.info("Saved %s", md_path)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
