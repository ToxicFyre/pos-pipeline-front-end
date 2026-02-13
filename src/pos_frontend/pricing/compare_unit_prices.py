"""
Full unit price investigation: compare our PRECIOS/AG_PRECIOS versus golden reference.
Outputs detailed comparison report and new PRECIOS/AG_PRECIOS files with corrected prices.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

from pos_frontend.config.paths import get_project_root
from pos_frontend.transfers.gold_investigation import parse_gold_excel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Sanity thresholds for gold prices
GOLD_MIN_REASONABLE = 0.1  # Exclude gold < 0.1 (e.g. 0.005)
GOLD_MAX_REASONABLE = 100_000  # Exclude gold > 100k
GOLD_CV_MAX = 0.5  # Max coefficient of variation across gold rows for same product
RATIO_FLAG_REVIEW = 3.0  # Flag if gold/ours or ours/gold > this (suspicious)


def normalize_producto(s: pd.Series) -> pd.Series:
    """Normalize Producto for matching (lowercase, strip)."""
    return s.astype(str).str.strip().str.lower()


def build_gold_canonical_prices(gold: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Build canonical gold unit prices per product, split by origin.
    Returns (gold_pt, gold_ag) with columns Producto, Precio_unitario, Count, Std, CV.
    """
    ag = gold[gold["Almacen_origen"].str.contains("ALMACEN GENERAL")]
    pt = gold[gold["Almacen_origen"].str.contains("ALMACEN PRODUCTO TERMINADO")]
    if pt.empty and ag.empty:
        return pd.DataFrame(), pd.DataFrame()

    def _agg(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return pd.DataFrame()
        agg = df.groupby("Producto", as_index=False).agg(
            Precio_unitario=("UnitCost", "median"),
            Count=("UnitCost", "count"),
            Std=("UnitCost", "std"),
        )
        agg["CV"] = agg["Std"] / agg["Precio_unitario"].replace(0, float("nan"))
        return agg

    gold_pt = _agg(pt)
    gold_ag = _agg(ag)
    return gold_pt, gold_ag


def load_precios(path: Path) -> pd.DataFrame:
    """Load PRECIOS.xlsx (PT)."""
    df = pd.read_excel(path, sheet_name=0)
    if "NOMBRE WANSOFT" in df.columns and "PRECIO DRIVE" in df.columns:
        df = df.rename(columns={"NOMBRE WANSOFT": "Producto", "PRECIO DRIVE": "Precio unitario"})
    df["Producto"] = df["Producto"].astype(str).str.strip()
    df["Precio unitario"] = pd.to_numeric(df["Precio unitario"], errors="coerce")
    return df[["Producto", "Precio unitario"]].drop_duplicates(subset=["Producto"], keep="first")


def load_ag_precios(path: Path) -> pd.DataFrame:
    """Load AG_PRECIOS.xlsx."""
    df = pd.read_excel(path, sheet_name=0)
    if "Producto" not in df.columns or "Precio unitario" not in df.columns:
        return pd.DataFrame()
    df["Producto"] = df["Producto"].astype(str).str.strip()
    df["Precio unitario"] = pd.to_numeric(df["Precio unitario"], errors="coerce")
    return df[["Producto", "Precio unitario"]].drop_duplicates(subset=["Producto"], keep="first")


def is_gold_reasonable(row: pd.Series, min_p: float = GOLD_MIN_REASONABLE) -> bool:
    """Check if gold price is reasonable (not obvious error)."""
    p = row.get("Precio_unitario", row.get("Gold_Precio", float("nan")))
    if pd.isna(p) or p < min_p or p > GOLD_MAX_REASONABLE:
        return False
    cv = row.get("CV", 0)
    if pd.notna(cv) and cv > GOLD_CV_MAX:
        return False  # High variance = unreliable
    return True


def merge_and_compare(
    ours: pd.DataFrame,
    gold: pd.DataFrame,
    origin: str,
) -> pd.DataFrame:
    """Merge our prices with gold, compute diff. Gold columns: Producto, Precio_unitario, Count, Std, CV."""
    ours = ours.copy()
    ours["_norm"] = normalize_producto(ours["Producto"])
    gold = gold.copy()
    gold["_norm"] = normalize_producto(gold["Producto"])
    merged = ours.merge(
        gold[["_norm", "Producto", "Precio_unitario", "Count", "Std", "CV"]],
        on="_norm",
        how="outer",
        suffixes=("", "_gold"),
    )
    # Ours has "Precio unitario" or "Precio_unitario"; gold has "Precio_unitario"
    ours_col = "Precio unitario" if "Precio unitario" in merged.columns else "Precio_unitario"
    merged["Ours_Precio"] = merged[ours_col] if ours_col in merged.columns else merged.get("Precio_unitario")
    merged["Gold_Precio"] = merged["Precio_unitario_gold"] if "Precio_unitario_gold" in merged.columns else merged["Precio_unitario"]
    if "Producto_gold" in merged.columns:
        merged["Producto"] = merged["Producto"].fillna(merged["Producto_gold"])
    merged["Producto"] = merged["Producto"].fillna(merged["_norm"])
    merged["Origin"] = origin
    merged["Diff"] = merged["Ours_Precio"] - merged["Gold_Precio"]
    merged["Pct_Diff"] = (merged["Diff"] / merged["Gold_Precio"].replace(0, float("nan"))) * 100
    merged["Gold_Reasonable"] = merged.apply(is_gold_reasonable, axis=1)
    # Flag suspicious ratio (e.g. Tocino sub 95 vs 380)
    ratio = merged["Gold_Precio"] / merged["Ours_Precio"].replace(0, float("nan"))
    merged["Flag_Review"] = (ratio > RATIO_FLAG_REVIEW) | (ratio < 1 / RATIO_FLAG_REVIEW)
    merged["Use_Gold"] = (
        merged["Gold_Reasonable"]
        & merged["Gold_Precio"].notna()
        & (merged["Diff"].abs() > 0.01)  # Only update if meaningfully different
        & ~merged["Flag_Review"]  # Exclude suspicious ratio
    )
    return merged.drop(columns=["_norm"], errors="ignore")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Full unit price investigation and PRECIOS update")
    parser.add_argument("--gold", default="TRANSFERENCIAS DEL 02 AL 08 FEBRERO.xlsx")
    parser.add_argument("--precios", default="PRECIOS.xlsx")
    parser.add_argument("--ag-precios", default="AG_PRECIOS.xlsx")
    parser.add_argument("--output-dir", default="data/c_processed/transfers/weekly")
    parser.add_argument("--output-precios", default="PRECIOS_UPDATED.xlsx")
    parser.add_argument("--output-ag-precios", default="AG_PRECIOS_UPDATED.xlsx")
    parser.add_argument("--transfers", default="data/c_processed/transfers/weekly/transfers_2026-02-02_2026-02-07.csv")
    args = parser.parse_args(argv)

    project_root = get_project_root()
    gold_path = project_root / args.gold
    precios_path = project_root / args.precios
    ag_path = project_root / args.ag_precios
    output_dir = project_root / args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    if not gold_path.exists():
        logger.error("Gold file not found: %s", gold_path)
        return 1

    logger.info("Parsing gold Excel: %s", gold_path)
    all_gold, _ = parse_gold_excel(gold_path)
    gold_pt, gold_ag = build_gold_canonical_prices(all_gold)
    logger.info("Gold PT products: %d, AG products: %d", len(gold_pt), len(gold_ag))

    # Load ours
    precios = load_precios(precios_path) if precios_path.exists() else pd.DataFrame()
    ag_precios = load_ag_precios(ag_path) if ag_path.exists() else pd.DataFrame()
    logger.info("Our PRECIOS: %d, AG_PRECIOS: %d", len(precios), len(ag_precios))

    # Compare PT
    cmp_pt = merge_and_compare(precios, gold_pt, "PT") if not precios.empty else pd.DataFrame()
    cmp_ag = merge_and_compare(ag_precios, gold_ag, "AG") if not ag_precios.empty else pd.DataFrame()

    # Build full comparison report
    report_rows = []
    if not cmp_pt.empty:
        for _, r in cmp_pt.iterrows():
            report_rows.append({
                "Producto": r["Producto"],
                "Origin": "PT",
                "Ours_Precio": r["Ours_Precio"],
                "Gold_Precio": r["Gold_Precio"],
                "Diff": r["Diff"],
                "Pct_Diff": r["Pct_Diff"],
                "Gold_Count": r.get("Count", 0),
                "Gold_CV": r.get("CV"),
                "Gold_Reasonable": r["Gold_Reasonable"],
                "Use_Gold": r["Use_Gold"],
                "Flag_Review": r.get("Flag_Review", False),
            })
    if not cmp_ag.empty:
        for _, r in cmp_ag.iterrows():
            report_rows.append({
                "Producto": r["Producto"],
                "Origin": "AG",
                "Ours_Precio": r["Ours_Precio"],
                "Gold_Precio": r["Gold_Precio"],
                "Diff": r["Diff"],
                "Pct_Diff": r["Pct_Diff"],
                "Gold_Count": r.get("Count", 0),
                "Gold_CV": r.get("CV"),
                "Gold_Reasonable": r["Gold_Reasonable"],
                "Use_Gold": r["Use_Gold"],
                "Flag_Review": r.get("Flag_Review", False),
            })

    report = pd.DataFrame(report_rows)
    report_path = output_dir / "unit_price_comparison_full.csv"
    report.to_csv(report_path, index=False)
    logger.info("Saved comparison report: %s", report_path)

    # Transfer-data comparison: products we use (Wansoft) not in PRECIOS
    transfers_path = project_root / args.transfers
    if transfers_path.exists():
        trans = pd.read_csv(transfers_path)
        trans["Producto"] = trans["Producto"].astype(str).str.strip()
        trans["_norm"] = normalize_producto(trans["Producto"])
        # Unit cost from transfers
        uc_col = "Costo unitario" if "Costo unitario" in trans.columns else None
        trans["UnitCost"] = trans[uc_col] if uc_col else trans["Costo"] / trans["Cantidad"].replace(0, float("nan"))
        trans_agg = trans.groupby("_norm").agg(
            Producto=("Producto", "first"),
            Transfer_UnitCost=("UnitCost", "median"),
            Count=("Orden", "count"),
        ).reset_index()
        precios_norm = set(normalize_producto(precios["Producto"])) if not precios.empty else set()
        ag_norm = set(normalize_producto(ag_precios["Producto"])) if not ag_precios.empty else set()
        unmatched_trans = trans_agg[~trans_agg["_norm"].isin(precios_norm | ag_norm)]
        if not unmatched_trans.empty:
            unmatched_path = output_dir / "transfer_products_not_in_precios.csv"
            unmatched_trans.to_csv(unmatched_path, index=False)
            logger.info("Transfer products not in PRECIOS/AG_PRECIOS: %d (saved to %s)", len(unmatched_trans), unmatched_path)

    # Summary stats
    use_gold_pt = cmp_pt[cmp_pt["Use_Gold"] == True] if not cmp_pt.empty else pd.DataFrame()
    use_gold_ag = cmp_ag[cmp_ag["Use_Gold"] == True] if not cmp_ag.empty else pd.DataFrame()
    excluded = report[(report["Gold_Reasonable"] == False) & (report["Gold_Precio"].notna())]
    gold_only_pt = cmp_pt[(cmp_pt["Ours_Precio"].isna()) & (cmp_pt["Gold_Precio"].notna())] if not cmp_pt.empty else pd.DataFrame()
    gold_only_ag = cmp_ag[(cmp_ag["Ours_Precio"].isna()) & (cmp_ag["Gold_Precio"].notna())] if not cmp_ag.empty else pd.DataFrame()

    logger.info("PT products to update (use gold): %d", len(use_gold_pt))
    logger.info("AG products to update (use gold): %d", len(use_gold_ag))
    logger.info("Gold prices excluded (unreasonable): %d", len(excluded))
    logger.info("PT products in gold only (to add): %d", len(gold_only_pt))
    logger.info("AG products in gold only (to add): %d", len(gold_only_ag))

    # Build new PRECIOS
    def _build_new(ours: pd.DataFrame, cmp: pd.DataFrame, add_gold_only: bool) -> pd.DataFrame:
        pc_col = "Precio unitario" if "Precio unitario" in ours.columns else "Precio_unitario"
        result = ours.copy()
        if pc_col != "Precio_unitario":
            result = result.rename(columns={pc_col: "Precio_unitario"})
        # Update where Use_Gold
        updates = cmp[cmp["Use_Gold"] == True]
        for _, r in updates.iterrows():
            prod = str(r["Producto"]).strip()
            gold_val = r["Gold_Precio"]
            mask = result["Producto"].astype(str).str.strip().str.lower() == prod.lower()
            if mask.any():
                result.loc[mask, "Precio_unitario"] = round(gold_val, 2)
        # Add gold-only products
        if add_gold_only:
            gold_only = cmp[(cmp["Ours_Precio"].isna()) & (cmp["Gold_Precio"].notna())]
            gold_only = gold_only[gold_only["Gold_Reasonable"]]
            existing = set(result["Producto"].astype(str).str.strip().str.lower())
            for _, r in gold_only.iterrows():
                prod = str(r["Producto"]).strip()
                if prod.lower() not in existing:
                    result = pd.concat([
                        result,
                        pd.DataFrame([{"Producto": prod, "Precio_unitario": round(r["Gold_Precio"], 2)}]),
                    ], ignore_index=True)
                    existing.add(prod.lower())
        return result[["Producto", "Precio_unitario"]].drop_duplicates(subset=["Producto"], keep="first")

    new_precios = _build_new(precios, cmp_pt, add_gold_only=True) if not precios.empty else gold_pt[["Producto", "Precio_unitario"]].copy()
    new_ag = _build_new(ag_precios, cmp_ag, add_gold_only=True) if not ag_precios.empty else gold_ag[["Producto", "Precio_unitario"]].copy()

    # Add transfer products not in PRECIOS/AG_PRECIOS that gold has
    if transfers_path.exists():
        trans = pd.read_csv(transfers_path)
        orig_col = next((c for c in trans.columns if "Almac" in c and "origen" in c.lower()), None)
        trans["_norm"] = normalize_producto(trans["Producto"].astype(str).str.strip())
        trans["_is_ag"] = trans[orig_col].astype(str).str.contains("ALMACEN GENERAL") if orig_col else False
        trans_agg = trans.groupby("_norm").agg(
            Producto=("Producto", "first"),
            _is_ag=("_is_ag", "any"),
        ).reset_index()
        precios_norm = set(normalize_producto(new_precios["Producto"]))
        ag_norm = set(normalize_producto(new_ag["Producto"]))
        trans_missing = trans_agg[~trans_agg["_norm"].isin(precios_norm | ag_norm)]
        gold_pt_norm = dict(zip(normalize_producto(gold_pt["Producto"]), gold_pt["Precio_unitario"]))
        gold_ag_norm = dict(zip(normalize_producto(gold_ag["Producto"]), gold_ag["Precio_unitario"]))
        added = 0
        for _, r in trans_missing.iterrows():
            n = r["_norm"]
            prod = r["Producto"]
            is_ag = r["_is_ag"]
            gpt = gold_pt_norm.get(n)
            gag = gold_ag_norm.get(n)
            def _ok(p): return p is not None and GOLD_MIN_REASONABLE <= p <= GOLD_MAX_REASONABLE
            if not is_ag and _ok(gpt):
                new_precios = pd.concat([new_precios, pd.DataFrame([{"Producto": prod, "Precio_unitario": round(gpt, 2)}])], ignore_index=True)
                precios_norm.add(n)
                added += 1
            elif is_ag and _ok(gag):
                new_ag = pd.concat([new_ag, pd.DataFrame([{"Producto": prod, "Precio_unitario": round(gag, 2)}])], ignore_index=True)
                ag_norm.add(n)
                added += 1
            elif not is_ag and _ok(gag):
                new_precios = pd.concat([new_precios, pd.DataFrame([{"Producto": prod, "Precio_unitario": round(gag, 2)}])], ignore_index=True)
                precios_norm.add(n)
                added += 1
            elif is_ag and _ok(gpt):
                new_ag = pd.concat([new_ag, pd.DataFrame([{"Producto": prod, "Precio_unitario": round(gpt, 2)}])], ignore_index=True)
                ag_norm.add(n)
                added += 1
        if added > 0:
            logger.info("Added %d transfer products from gold", added)
            new_precios = new_precios.drop_duplicates(subset=["Producto"], keep="first")
            new_ag = new_ag.drop_duplicates(subset=["Producto"], keep="first")

    # PRECIOS uses NOMBRE WANSOFT, PRECIO DRIVE
    new_precios_out = new_precios.rename(columns={"Producto": "NOMBRE WANSOFT", "Precio_unitario": "PRECIO DRIVE"})
    out_precios_path = project_root / args.output_precios
    new_precios_out.to_excel(out_precios_path, index=False)
    logger.info("Saved new PRECIOS: %s (%d products)", out_precios_path, len(new_precios_out))

    new_ag_out = new_ag.rename(columns={"Precio_unitario": "Precio unitario"}) if "Precio_unitario" in new_ag.columns else new_ag
    out_ag_path = project_root / args.output_ag_precios
    new_ag_out.to_excel(out_ag_path, index=False)
    logger.info("Saved new AG_PRECIOS: %s (%d products)", out_ag_path, len(new_ag_out))

    # Write detailed markdown report
    md_path = output_dir / "unit_price_investigation_report.md"
    with open(md_path, "w", encoding="utf-8") as f:
        f.write("# Unit Price Investigation Report\n\n")
        f.write("## Summary\n\n")
        f.write(f"- PT products in PRECIOS: {len(precios)}\n")
        f.write(f"- AG products in AG_PRECIOS: {len(ag_precios)}\n")
        f.write(f"- Gold PT products: {len(gold_pt)}\n")
        f.write(f"- Gold AG products: {len(gold_ag)}\n")
        f.write(f"- PT products updated (use gold): {len(use_gold_pt)}\n")
        f.write(f"- AG products updated (use gold): {len(use_gold_ag)}\n")
        f.write(f"- Gold prices excluded (unreasonable): {len(excluded)}\n")
        f.write(f"- PT products added (gold only): {len(gold_only_pt)}\n")
        f.write(f"- AG products added (gold only): {len(gold_only_ag)}\n\n")
        f.write("## PT Products Updated\n\n")
        if not use_gold_pt.empty:
            f.write(use_gold_pt[["Producto", "Ours_Precio", "Gold_Precio", "Diff"]].to_string(index=False))
        f.write("\n\n## AG Products Updated\n\n")
        if not use_gold_ag.empty:
            f.write(use_gold_ag[["Producto", "Ours_Precio", "Gold_Precio", "Diff"]].to_string(index=False))
        f.write("\n\n## Excluded (Gold Unreasonable)\n\n")
        if not excluded.empty:
            f.write(excluded[["Producto", "Origin", "Gold_Precio", "Gold_Reasonable"]].to_string(index=False))
        f.write("\n\n## Flag Review (suspicious ratio > 3x)\n\n")
        flagged = report[(report["Use_Gold"] == True) & report["Flag_Review"]] if "Flag_Review" in report.columns else pd.DataFrame()
        if not flagged.empty:
            f.write(flagged[["Producto", "Origin", "Ours_Precio", "Gold_Precio", "Diff"]].to_string(index=False))
    logger.info("Saved markdown report: %s", md_path)

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
