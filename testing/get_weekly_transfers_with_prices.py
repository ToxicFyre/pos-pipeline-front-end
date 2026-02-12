"""
Fetch transfer data for weekly periods (Mon-Sun), consolidate per week,
apply correct unit prices from PRECIOS.xlsx, and output a cost-difference report.

PRECIOS.xlsx uses: NOMBRE WANSOFT (matches Producto), PRECIO DRIVE (unit price).
Transfer CSVs: Orden, Almacén origen, Sucursal destino, Almacén destino, Fecha,
Estatus, Cantidad, Departamento, Clave, Producto, Presentación, Costo, IEPS,
IVA, Costo unitario.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

# Ensure project root is on path for config import
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd

from pos_core import DataPaths
from pos_core.transfers import core

from testing.config_weekly_transfers import GOLD_REFERENCE_BY_WEEK

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Producto typo aliases: variant -> canonical (lowercase for matching)
PRODUCTO_ALIASES: dict[str, str] = {
    "mayones de panem *": "mayonesa de panem *",
    "sopa de tomate*": "sopa de tomate *",
}

# Week ranges Mon-Sun from Dec 1, 2025 to Feb 7, 2026
WEEK_RANGES: list[tuple[date, date]] = [
    (date(2025, 12, 1), date(2025, 12, 7)),
    (date(2025, 12, 8), date(2025, 12, 14)),
    (date(2025, 12, 15), date(2025, 12, 21)),
    (date(2025, 12, 22), date(2025, 12, 28)),
    (date(2025, 12, 29), date(2026, 1, 4)),
    (date(2026, 1, 5), date(2026, 1, 11)),
    (date(2026, 1, 12), date(2026, 1, 18)),
    (date(2026, 1, 19), date(2026, 1, 25)),
    (date(2026, 1, 26), date(2026, 2, 1)),
    (date(2026, 2, 2), date(2026, 2, 7)),  # Optional: use (2026, 2, 2), (2026, 2, 8) with --last-week-feb-8 for gold alignment
]


def get_week_boundaries(d: date) -> tuple[date, date]:
    """Return (Monday, Sunday) for the week containing d."""
    weekday = d.weekday()  # 0=Mon, 6=Sun
    monday = d - timedelta(days=weekday)
    sunday = monday + timedelta(days=6)
    return monday, sunday


def load_precios(precios_path: str | Path) -> pd.DataFrame:
    """Load PRECIOS.xlsx and return DataFrame with Producto, Precio unitario.
    Uses NOMBRE WANSOFT -> Producto, PRECIO DRIVE -> Precio unitario."""
    path = Path(precios_path)
    df = pd.read_excel(path, sheet_name=0)
    # Map PRECIOS columns to expected names
    if "NOMBRE WANSOFT" in df.columns and "PRECIO DRIVE" in df.columns:
        df = df.rename(columns={"NOMBRE WANSOFT": "Producto", "PRECIO DRIVE": "Precio unitario"})
    df["Producto"] = df["Producto"].astype(str).str.strip()
    df["Precio unitario"] = pd.to_numeric(df["Precio unitario"], errors="coerce")
    # Drop duplicates, keep first
    df = df.drop_duplicates(subset=["Producto"], keep="first")
    return df[["Producto", "Precio unitario"]]


def load_ag_precios(ag_precios_path: str | Path | None) -> pd.DataFrame | None:
    """Load AG_PRECIOS.xlsx (Producto, Precio unitario) for ALMACEN GENERAL rows.
    Returns None if file not found."""
    if not ag_precios_path:
        return None
    path = Path(ag_precios_path)
    if not path.exists():
        return None
    df = pd.read_excel(path, sheet_name=0)
    if "Producto" not in df.columns or "Precio unitario" not in df.columns:
        return None
    df["Producto"] = df["Producto"].astype(str).str.strip()
    df["Precio unitario"] = pd.to_numeric(df["Precio unitario"], errors="coerce")
    df = df.drop_duplicates(subset=["Producto"], keep="first")
    return df[["Producto", "Precio unitario"]]


def collect_branch_csv_paths(
    batch_dir: Path,
    start_date: str,
    end_date: str,
) -> list[Path]:
    """Collect all branch CSV paths for the given week range."""
    pattern = f"TransfersIssued_*_{start_date}_{end_date}.csv"
    paths = list(batch_dir.rglob(pattern))
    return paths


def read_and_concat_transfers(csv_paths: list[Path]) -> pd.DataFrame:
    """Read and concatenate all branch CSVs into one DataFrame."""
    if not csv_paths:
        return pd.DataFrame()
    dfs = [pd.read_csv(p) for p in csv_paths]
    return pd.concat(dfs, ignore_index=True)


def normalize_producto_for_match(s: pd.Series) -> pd.Series:
    """Normalize Producto for matching: strip, lowercase, canonical asterisk."""
    out = s.astype(str).str.strip().str.lower()
    # Canonical asterisk: "producto*" and "producto *" both -> "producto *"
    out = out.str.replace(r"\s*\*$", " *", regex=True)
    return out


def apply_prices(
    df: pd.DataFrame,
    precios: pd.DataFrame,
    ag_precios: pd.DataFrame | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Left-join transfers with PRECIOS/AG_PRECIOS on Producto, update Costo unitario and Costo.
    - ALMACEN GENERAL: use AG_PRECIOS if available, else keep original
    - ALMACEN PRODUCTO TERMINADO: use PRECIOS
    Returns (updated_df, report_df with cost_diff columns).
    """
    if df.empty:
        return df, pd.DataFrame()

    df = df.copy()
    orig_col = next((c for c in df.columns if "Almac" in c and "origen" in c.lower()), None)
    if not orig_col:
        orig_col = "Almacen_origen"

    df["_Producto_norm"] = normalize_producto_for_match(df["Producto"])
    # Apply aliases when primary match would fail (e.g. Mayones -> Mayonesa)
    df["_Producto_lookup"] = df["_Producto_norm"].map(
        lambda x: PRODUCTO_ALIASES.get(x, x)
    )
    df["_Almacen_origen"] = df[orig_col].astype(str).str.strip().str.upper()

    # PT: merge with PRECIOS
    precios_norm = precios.copy()
    precios_norm["_Producto_norm"] = normalize_producto_for_match(precios_norm["Producto"])
    merged = df.merge(
        precios_norm[["_Producto_norm", "Precio unitario"]],
        left_on="_Producto_lookup",
        right_on="_Producto_norm",
        how="left",
        suffixes=("", "_pt"),
    )
    merged = merged.rename(columns={"Precio unitario": "_Precio_PT"})

    # AG: merge with AG_PRECIOS if available
    if ag_precios is not None and not ag_precios.empty:
        ag_norm = ag_precios.copy()
        ag_norm["_Producto_norm"] = normalize_producto_for_match(ag_norm["Producto"])
        merged = merged.merge(
            ag_norm[["_Producto_norm", "Precio unitario"]],
            left_on="_Producto_lookup",
            right_on="_Producto_norm",
            how="left",
            suffixes=("", "_ag"),
        )
        merged = merged.rename(columns={"Precio unitario": "_Precio_AG"})
    else:
        merged["_Precio_AG"] = float("nan")

    # Store before values
    merged["Costo_before"] = merged["Costo"]
    merged["Costo unitario_before"] = merged["Costo unitario"]

    # Apply PT price where origin is PRODUCTO TERMINADO
    is_pt = merged["_Almacen_origen"].str.contains("PRODUCTO TERMINADO")
    pt_matched = is_pt & merged["_Precio_PT"].notna()
    merged.loc[pt_matched, "Costo unitario"] = merged.loc[pt_matched, "_Precio_PT"]

    # Apply AG price where origin is GENERAL and we have AG_PRECIOS
    is_ag = merged["_Almacen_origen"].str.contains("ALMACEN GENERAL")
    ag_matched = is_ag & merged["_Precio_AG"].notna()
    merged.loc[ag_matched, "Costo unitario"] = merged.loc[ag_matched, "_Precio_AG"]

    merged["Costo"] = merged["Cantidad"] * merged["Costo unitario"]
    merged["Costo_after"] = merged["Costo"]

    all_matched = pt_matched | ag_matched
    unmatched = merged["Producto"][~all_matched].unique()
    if len(unmatched) > 0:
        logger.warning("Unmatched Producto (kept original prices): %s", list(unmatched)[:20])

    drop_cols = ["_Producto_norm", "_Producto_norm_ag", "_Producto_lookup", "_Almacen_origen", "_Precio_PT", "_Precio_AG", "Costo unitario_before"]
    out = merged.drop(columns=[c for c in drop_cols if c in merged.columns])

    return out, out


def compute_cost_by_dest_branch(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate total Costo before/after by Sucursal destino."""
    if df.empty or "Costo_before" not in df.columns:
        return pd.DataFrame()
    agg = df.groupby("Sucursal destino", as_index=False).agg(
        Total_Before=("Costo_before", "sum"),
        Total_After=("Costo_after", "sum"),
    )
    agg["Difference"] = agg["Total_After"] - agg["Total_Before"]
    agg["Pct_Change"] = (agg["Difference"] / agg["Total_Before"].replace(0, float("nan"))) * 100
    return agg


def compute_weekly_cost_comparison(
    df: pd.DataFrame,
    exclude_cedis_dest: bool = False,
) -> pd.DataFrame:
    """Aggregate total Costo before/after by Week.

    If exclude_cedis_dest=True, excludes transfers TO CEDIS (matches mart behavior).
    """
    if df.empty or "Costo_before" not in df.columns or "Week" not in df.columns:
        return pd.DataFrame()
    work = df.copy()
    if exclude_cedis_dest and "Sucursal destino" in work.columns:
        work = work[work["Sucursal destino"] != "Panem - CEDIS"]
    agg = work.groupby("Week", as_index=False).agg(
        Total_Before=("Costo_before", "sum"),
        Total_After=("Costo_after", "sum"),
    )
    agg["Difference"] = agg["Total_After"] - agg["Total_Before"]
    agg["Pct_Change"] = (agg["Difference"] / agg["Total_Before"].replace(0, float("nan"))) * 100
    return agg


def _write_weekly_breakdown(combined: pd.DataFrame, output_dir: Path) -> None:
    """Write weekly_breakdown.csv with per-week totals by origin and destination.
    Helps reconcile expected totals (e.g. Feb 2-7 ~283k).
    Adds Gold_Reference and Gold_NUMEROS for weeks in GOLD_REFERENCE_BY_WEEK."""
    if combined.empty or "Week" not in combined.columns:
        return
    orig_col = next((c for c in combined.columns if "Almac" in c and "origen" in c.lower()), None)
    if not orig_col:
        return
    rows = []
    for week in combined["Week"].unique():
        wf = combined[combined["Week"] == week]
        total = wf["Costo_after"].sum()
        to_cedis = wf[wf["Sucursal destino"] == "Panem - CEDIS"]["Costo_after"].sum()
        to_branches = total - to_cedis
        apt = wf[wf[orig_col] == "ALMACEN PRODUCTO TERMINADO"]["Costo_after"].sum()
        ag = wf[wf[orig_col] == "ALMACEN GENERAL"]["Costo_after"].sum()
        row = {
            "Week": week,
            "Total_After": round(total, 2),
            "To_CEDIS": round(to_cedis, 2),
            "To_Branches_Only": round(to_branches, 2),
            "APT_Only": round(apt, 2),
            "AG_Only": round(ag, 2),
        }
        if week in GOLD_REFERENCE_BY_WEEK:
            gold_detail, gold_numeros = GOLD_REFERENCE_BY_WEEK[week]
            row["Gold_Reference"] = gold_detail
            row["Gold_NUMEROS"] = gold_numeros
        else:
            row["Gold_Reference"] = float("nan")
            row["Gold_NUMEROS"] = float("nan")
        rows.append(row)
    breakdown = pd.DataFrame(rows)
    breakdown.to_csv(output_dir / "weekly_breakdown.csv", index=False)
    logger.info("Saved breakdown %s", output_dir / "weekly_breakdown.csv")


def save_weekly_csv(df: pd.DataFrame, output_path: Path, drop_report_cols: bool = True) -> None:
    """Save consolidated DataFrame to CSV. Drops Costo_before/Costo_after/Week if requested."""
    out = df.copy()
    if drop_report_cols:
        for c in ["Costo_before", "Costo_after", "Week"]:
            if c in out.columns:
                out = out.drop(columns=[c])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(output_path, index=False)


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch weekly transfers and apply PRECIOS prices")
    parser.add_argument("--data-root", default="data", help="Data root directory")
    parser.add_argument("--precios-path", default="PRECIOS.xlsx", help="Path to PRECIOS.xlsx")
    parser.add_argument("--ag-precios-path", default="AG_PRECIOS.xlsx", help="Path to AG_PRECIOS.xlsx (for ALMACEN GENERAL)")
    parser.add_argument("--branches-file", default="sucursales.json", help="Branches config")
    parser.add_argument("--start", default="2025-12-01", help="Start date (first week Monday)")
    parser.add_argument("--end", default="2026-02-07", help="End date (last week Sunday)")
    cedis_group = parser.add_mutually_exclusive_group()
    cedis_group.add_argument(
        "--exclude-cedis-dest",
        action="store_true",
        help="Exclude transfers TO CEDIS from totals (matches mart_transfers_pivot, default)",
    )
    cedis_group.add_argument(
        "--include-cedis-dest",
        action="store_true",
        help="Include CEDIS in totals (opt-in for backward compatibility)",
    )
    parser.add_argument(
        "--last-week-feb-8",
        action="store_true",
        help="Use Feb 2-8 for last week (matches gold; requires core.fetch to support Feb 8)",
    )
    args = parser.parse_args()

    data_root = Path(args.data_root)
    batch_dir = data_root / "b_clean" / "transfers" / "batch"
    output_dir = data_root / "c_processed" / "transfers" / "weekly"

    # Filter week ranges to requested period
    start_d = date.fromisoformat(args.start)
    end_d = date.fromisoformat(args.end)
    week_ranges = list(WEEK_RANGES)
    if args.last_week_feb_8 and week_ranges:
        # Replace last week with Feb 2-8 for gold alignment
        week_ranges = week_ranges[:-1] + [(date(2026, 2, 2), date(2026, 2, 8))]
    weeks = [(s, e) for s, e in week_ranges if s >= start_d and e <= end_d]
    if not weeks:
        logger.error("No week ranges in [%s, %s]", args.start, args.end)
        return 1

    logger.info("Loading PRECIOS from %s", args.precios_path)
    precios = load_precios(args.precios_path)
    ag_precios = load_ag_precios(args.ag_precios_path)
    if ag_precios is not None:
        logger.info("Loaded AG_PRECIOS from %s (%d products)", args.ag_precios_path, len(ag_precios))

    paths = DataPaths.from_root(data_root, Path(args.branches_file))
    all_transfers: list[pd.DataFrame] = []

    for start_date, end_date in weeks:
        start_str = start_date.isoformat()
        end_str = end_date.isoformat()
        logger.info("Week %s to %s", start_str, end_str)

        core.fetch(paths, start_str, end_str, mode="force")

        csv_paths = collect_branch_csv_paths(batch_dir, start_str, end_str)
        df = read_and_concat_transfers(csv_paths)
        if df.empty:
            logger.warning("No transfer data for week %s-%s", start_str, end_str)
            continue

        df_updated, _ = apply_prices(df, precios, ag_precios=ag_precios)
        df_updated["Week"] = f"{start_str}_{end_str}"
        all_transfers.append(df_updated)

        out_path = output_dir / f"transfers_{start_str}_{end_str}.csv"
        save_weekly_csv(df_updated, out_path)
        logger.info("Saved %s", out_path)

    # Cost-difference reports
    if all_transfers:
        combined = pd.concat(all_transfers, ignore_index=True)
        report = compute_cost_by_dest_branch(combined)
        report_path = output_dir / "price_correction_report.csv"
        report.to_csv(report_path, index=False)
        logger.info("Saved report %s", report_path)

        # Default: exclude CEDIS for gold comparison; use --include-cedis-dest to include
        exclude_cedis = not args.include_cedis_dest
        weekly_report = compute_weekly_cost_comparison(
            combined, exclude_cedis_dest=exclude_cedis
        )
        weekly_report_path = output_dir / "weekly_cost_comparison.csv"
        weekly_report.to_csv(weekly_report_path, index=False)
        logger.info("Saved weekly comparison %s", weekly_report_path)

        # Breakdown report for reconciliation (e.g. Feb 2-7: 283k expected)
        _write_weekly_breakdown(combined, output_dir)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
