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

import pandas as pd

from pos_core import DataPaths
from pos_core.transfers import core

from pos_frontend.config.weekly_transfers import GOLD_REFERENCE_BY_WEEK

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


def build_week_ranges(end_date: date, num_weeks: int) -> list[tuple[date, date]]:
    """Return num_weeks Mon-Sun ranges ending at or before end_date, most recent first."""
    _, last_sunday = get_week_boundaries(end_date)
    return [
        (last_sunday - timedelta(days=6 + i * 7), last_sunday - timedelta(days=i * 7))
        for i in range(num_weeks)
    ]


def load_precios(precios_path: str | Path) -> pd.DataFrame:
    """Load PRECIOS.xlsx and return DataFrame with Producto, Precio unitario.

    When UNIDAD is LT or KG: PRECIO DRIVE is unit price → use as-is.
    When UNIDAD is PZ: PRECIO DRIVE is presentation price → PRECIO UNITARIO = PRECIO DRIVE / PRESENTACION.
    Prefers PRECIO UNITARIO column if present (from update_precios_with_unit_prices.py), else computes it.
    """
    path = Path(precios_path)
    df = pd.read_excel(path, sheet_name=0)
    product_col = "NOMBRE WANSOFT" if "NOMBRE WANSOFT" in df.columns else "Producto"
    df["Producto"] = df[product_col].astype(str).str.strip()
    # Unit price: prefer PRECIO UNITARIO, else compute PRECIO DRIVE / PRESENTACION, else PRECIO DRIVE
    precio_drive_col = "PRECIO DRIVE" if "PRECIO DRIVE" in df.columns else None
    if "PRECIO UNITARIO" in df.columns:
        df["Precio unitario"] = pd.to_numeric(df["PRECIO UNITARIO"], errors="coerce")
    elif precio_drive_col:
        df["Precio unitario"] = pd.to_numeric(df[precio_drive_col], errors="coerce")
        unidad_col = next((c for c in df.columns if str(c).strip().upper() == "UNIDAD"), None)
        present_col = next((c for c in df.columns if "present" in str(c).lower()), None)
        if unidad_col is not None and present_col is not None:
            unidad = df[unidad_col].astype(str).str.strip().str.upper()
            present_num = pd.to_numeric(df[present_col], errors="coerce")
            # UNIDAD in (LT, KG): PRECIO DRIVE is unit price → use as-is
            # UNIDAD = PZ: PRECIO DRIVE is presentation price → divide by PRESENTACION
            mask_pz = (unidad == "PZ") & (present_num > 0) & df["Precio unitario"].notna()
            df.loc[mask_pz, "Precio unitario"] = (
                df.loc[mask_pz, "Precio unitario"] / present_num.loc[mask_pz]
            )
    else:
        df["Precio unitario"] = pd.to_numeric(df.get("Precio unitario", float("nan")), errors="coerce")
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


def compute_weekly_price_changes(df: pd.DataFrame) -> pd.DataFrame:
    """Extract rows where price was changed, with before/after unit price and cost.

    Filters to rows where Costo_before != Costo_after, computes Costo_unitario_before
    from Costo_before / Cantidad, and returns a DataFrame with Producto, Almacen origen,
    Cantidad, Costo_unitario_before, Costo_unitario_after, Costo_before, Costo_after,
    Sucursal destino, and Orden.
    """
    empty_result = pd.DataFrame(
        columns=[
            "Producto",
            "Almacen_origen",
            "Cantidad",
            "Costo_unitario_before",
            "Costo_unitario_after",
            "Costo_before",
            "Costo_after",
        ]
    )
    if df.empty or "Costo_before" not in df.columns or "Costo_after" not in df.columns:
        return empty_result

    changed = df[df["Costo_before"] != df["Costo_after"]].copy()
    if changed.empty:
        return empty_result

    orig_col = next(
        (c for c in changed.columns if "Almac" in c and "origen" in c.lower()),
        "Almacen_origen",
    )
    cant = pd.to_numeric(changed["Costo_before"], errors="coerce")
    qty = pd.to_numeric(changed["Cantidad"], errors="coerce").replace(0, float("nan"))
    changed["Costo_unitario_before"] = cant / qty
    changed["Costo_unitario_after"] = changed["Costo unitario"]
    if orig_col != "Almacen_origen":
        changed["Almacen_origen"] = changed[orig_col]

    out_cols = [
        "Producto",
        "Almacen_origen",
        "Cantidad",
        "Costo_unitario_before",
        "Costo_unitario_after",
        "Costo_before",
        "Costo_after",
    ]
    for opt in ["Sucursal destino", "Orden"]:
        if opt in changed.columns:
            out_cols.append(opt)

    result = changed[out_cols].sort_values(by=["Producto", "Almacen_origen"])
    return result.reset_index(drop=True)


def compute_price_change_alerts(
    df: pd.DataFrame,
    pct_high: float = 50,
    pct_medium: float = 25,
) -> pd.DataFrame:
    """Products whose corrected unit price differs significantly from weighted-avg original.

    Flags potential wrong corrections. Returns DataFrame sorted by |Pct_change_unit| desc.
    """
    if df.empty or "Costo_before" not in df.columns or "Costo_after" not in df.columns:
        return pd.DataFrame()

    changed = df[df["Costo_before"] != df["Costo_after"]].copy()
    if changed.empty:
        return pd.DataFrame()

    orig_col = next(
        (c for c in changed.columns if "Almac" in c and "origen" in c.lower()),
        "Almacen_origen",
    )
    if orig_col != "Almacen_origen":
        changed["Almacen_origen"] = changed[orig_col]

    cant = pd.to_numeric(changed["Costo_before"], errors="coerce")
    qty = pd.to_numeric(changed["Cantidad"], errors="coerce").replace(0, float("nan"))
    changed["_weighted_avg"] = cant / qty

    agg = (
        changed.groupby(["Producto", "Almacen_origen"], as_index=False)
        .agg(
            Total_Cantidad=("Cantidad", "sum"),
            Costo_before_sum=("Costo_before", "sum"),
            Costo_after_sum=("Costo_after", "sum"),
            Unit_after=("Costo unitario", "first"),
        )
    )
    agg["Weighted_avg_unit_before"] = agg["Costo_before_sum"] / agg["Total_Cantidad"].replace(
        0, float("nan")
    )
    agg["Pct_change_unit"] = (
        (agg["Unit_after"] - agg["Weighted_avg_unit_before"])
        / agg["Weighted_avg_unit_before"].replace(0, float("nan"))
        * 100
    )
    agg["Cost_diff"] = agg["Costo_after_sum"] - agg["Costo_before_sum"]

    pct_abs = agg["Pct_change_unit"].abs()
    agg["Alert"] = ""
    agg.loc[pct_abs > pct_high, "Alert"] = "HIGH"
    agg.loc[(pct_abs > pct_medium) & (pct_abs <= pct_high), "Alert"] = "MEDIUM"

    out_cols = [
        "Producto",
        "Almacen_origen",
        "Total_Cantidad",
        "Weighted_avg_unit_before",
        "Unit_after",
        "Pct_change_unit",
        "Costo_before_sum",
        "Costo_after_sum",
        "Cost_diff",
        "Alert",
    ]
    result = agg[out_cols].sort_values(
        "Pct_change_unit", key=lambda s: s.abs(), ascending=False
    )
    return result.reset_index(drop=True)


def compute_origin_totals(
    df: pd.DataFrame,
    exclude_cedis_dest: bool = False,
) -> pd.DataFrame:
    """Aggregate cost by origin (AG vs PT) before and after correction, by week."""
    if df.empty or "Costo_before" not in df.columns or "Week" not in df.columns:
        return pd.DataFrame()

    work = df.copy()
    if exclude_cedis_dest and "Sucursal destino" in work.columns:
        work = work[work["Sucursal destino"] != "Panem - CEDIS"]

    orig_col = next(
        (c for c in work.columns if "Almac" in c and "origen" in c.lower()),
        "Almacen_origen",
    )
    if orig_col != "Almacen_origen":
        work["Almacen_origen"] = work[orig_col]

    work["_origin_type"] = "OTHER"
    work.loc[
        work["Almacen_origen"].str.contains("ALMACEN GENERAL", na=False), "_origin_type"
    ] = "AG"
    work.loc[
        work["Almacen_origen"].str.contains("PRODUCTO TERMINADO", na=False), "_origin_type"
    ] = "PT"

    by_week_origin = (
        work.groupby(["Week", "_origin_type"], as_index=False)
        .agg(
            Costo_before=("Costo_before", "sum"),
            Costo_after=("Costo_after", "sum"),
        )
    )

    ag_rows = by_week_origin[by_week_origin["_origin_type"] == "AG"].drop(
        columns=["_origin_type"]
    )
    pt_rows = by_week_origin[by_week_origin["_origin_type"] == "PT"].drop(
        columns=["_origin_type"]
    )

    ag_rows = ag_rows.rename(
        columns={"Costo_before": "AG_Before", "Costo_after": "AG_After"}
    )
    pt_rows = pt_rows.rename(
        columns={"Costo_before": "PT_Before", "Costo_after": "PT_After"}
    )

    merged = ag_rows.merge(pt_rows, on="Week", how="outer").fillna(0)
    merged["AG_Diff"] = merged["AG_After"] - merged["AG_Before"]
    merged["AG_Pct"] = (
        merged["AG_Diff"] / merged["AG_Before"].replace(0, float("nan")) * 100
    )
    merged["PT_Diff"] = merged["PT_After"] - merged["PT_Before"]
    merged["PT_Pct"] = (
        merged["PT_Diff"] / merged["PT_Before"].replace(0, float("nan")) * 100
    )

    ag_before = merged["AG_Before"].sum()
    ag_after = merged["AG_After"].sum()
    pt_before = merged["PT_Before"].sum()
    pt_after = merged["PT_After"].sum()
    all_row = pd.DataFrame(
        [
            {
                "Week": "All",
                "AG_Before": ag_before,
                "AG_After": ag_after,
                "AG_Diff": ag_after - ag_before,
                "AG_Pct": ((ag_after - ag_before) / ag_before * 100) if ag_before != 0 else float("nan"),
                "PT_Before": pt_before,
                "PT_After": pt_after,
                "PT_Diff": pt_after - pt_before,
                "PT_Pct": ((pt_after - pt_before) / pt_before * 100) if pt_before != 0 else float("nan"),
            }
        ]
    )
    result = pd.concat([all_row, merged], ignore_index=True)
    return result[["Week", "AG_Before", "AG_After", "AG_Diff", "AG_Pct", "PT_Before", "PT_After", "PT_Diff", "PT_Pct"]]


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


def main(argv: list[str] | None = None) -> int:
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
    parser.add_argument(
        "--weeks",
        type=int,
        default=None,
        metavar="N",
        help="Generate last N Mon-Sun weeks from --end (or today). Overrides hardcoded WEEK_RANGES. Default when used: 12.",
    )
    args = parser.parse_args(argv)

    data_root = Path(args.data_root)
    batch_dir = data_root / "b_clean" / "transfers" / "batch"
    output_dir = data_root / "c_processed" / "transfers" / "weekly"

    # Build week ranges: dynamic (--weeks) or legacy (filter WEEK_RANGES by --start/--end)
    if args.weeks is not None:
        if args.weeks <= 0:
            logger.error("--weeks must be > 0, got %d", args.weeks)
            return 1
        end_d = date.fromisoformat(args.end) if args.end else date.today()
        weeks = build_week_ranges(end_d, args.weeks)
    else:
        start_d = date.fromisoformat(args.start)
        end_d = date.fromisoformat(args.end)
        week_ranges = list(WEEK_RANGES)
        if args.last_week_feb_8 and week_ranges:
            week_ranges = week_ranges[:-1] + [(date(2026, 2, 2), date(2026, 2, 8))]
        weeks = [(s, e) for s, e in week_ranges if s >= start_d and e <= end_d]
        if not weeks:
            logger.error("No week ranges in [%s, %s]", args.start, args.end)
            return 1

    if not weeks:
        logger.error("No week ranges to process")
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

        price_changes = compute_weekly_price_changes(df_updated)
        price_changes_path = output_dir / f"price_changes_{start_str}_{end_str}.csv"
        price_changes_path.parent.mkdir(parents=True, exist_ok=True)
        price_changes.to_csv(price_changes_path, index=False, encoding="utf-8-sig")
        logger.info("Saved %s", price_changes_path)

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

        # Correction summary: AG/PT totals and price change alerts
        origin_totals = compute_origin_totals(combined, exclude_cedis_dest=exclude_cedis)
        origin_totals_path = output_dir / "correction_summary_totals.csv"
        origin_totals.to_csv(origin_totals_path, index=False, encoding="utf-8-sig")
        logger.info("Saved %s", origin_totals_path)

        alerts = compute_price_change_alerts(combined, pct_high=50, pct_medium=25)
        alerts_path = output_dir / "correction_summary_alerts.csv"
        alerts.to_csv(alerts_path, index=False, encoding="utf-8-sig")
        logger.info("Saved %s", alerts_path)

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
