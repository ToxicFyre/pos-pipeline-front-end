"""
Investigate transfer cost discrepancy vs golden reference (TRANSFERENCIAS DEL 02 AL 08 FEBRERO.xlsx).
Parses gold, compares line-level, outputs report, extracts AG unit costs, creates AG_PRECIOS.xlsx.
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import pandas as pd

from pos_frontend.config.paths import get_project_root
from pos_frontend.config.weekly_transfers import AG_EXCLUDED_ORDERS, PT_EXCLUDED_ORDERS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Map gold sheet branch codes to our Sucursal destino
SHEET_TO_SUCURSAL = {
    "KAVIA": "Panem - Hotel Kavia N",
    "PV": "Panem - Punto Valle",
    "QIN": "Panem - Plaza QIN N",
    "Q": "Panem - Plaza QIN N",
    "HZ": "Panem - Hospital Zambrano N",
    "CARRETA": "Panem - La Carreta N",
    "C": "Panem - La Carreta N",
    "NATIVA": "Panem - Plaza Nativa",
    "N": "Panem - Plaza Nativa",
    "CC": "Panem - Credi Club",
}

GOLD_Fecha_START = "2026-02-02"
GOLD_Fecha_END = "2026-02-08"

# Known NUMEROS Kavia total (fallback if parse fails)
GOLD_NUMEROS_KAVIA = 62_083.0


def parse_numeros(path: Path) -> pd.DataFrame:
    """Read NUMEROS sheet from golden Excel and extract per-branch totals.

    NUMEROS is typically a summary table with branch names and cost columns.
    Returns DataFrame with Branch, Total columns. Layout may vary; this uses heuristics.
    """
    try:
        df = pd.read_excel(path, sheet_name="NUMEROS", header=None)
    except Exception as e:
        logger.warning("Could not read NUMEROS sheet: %s", e)
        return pd.DataFrame()
    if df.empty or len(df) < 2:
        return pd.DataFrame()
    # Heuristic: first column often has branch names; last numeric column often has total
    # Try header row (row 0)
    first_row = df.iloc[0].astype(str).tolist()
    branch_col = None
    total_col = None
    for i, v in enumerate(first_row):
        v_lower = str(v).lower()
        if any(x in v_lower for x in ("sucursal", "branch", "suc", "destino")):
            branch_col = i
        if any(x in v_lower for x in ("total", "costo", "suma", "total")):
            total_col = i
    if branch_col is None:
        branch_col = 0
    if total_col is None:
        # Use last numeric column
        for j in range(len(first_row) - 1, -1, -1):
            try:
                float(str(first_row[j]).replace(",", "").replace(" ", ""))
                total_col = j
                break
            except (ValueError, TypeError):
                continue
    if total_col is None:
        total_col = 1
    # Data starts at row 1
    result = pd.DataFrame({
        "Branch": df.iloc[1:, branch_col].astype(str).str.strip(),
        "Total": pd.to_numeric(df.iloc[1:, total_col], errors="coerce"),
    })
    result = result[result["Total"].notna() & (result["Branch"].str.len() > 0)]
    return result.reset_index(drop=True)


def extract_kavia_total(numeros_df: pd.DataFrame) -> float:
    """Extract Kavia row total from NUMEROS DataFrame. Returns GOLD_NUMEROS_KAVIA if not found."""
    if numeros_df.empty or "Branch" not in numeros_df.columns:
        return GOLD_NUMEROS_KAVIA
    branch_col = numeros_df["Branch"].astype(str).str.strip().str.upper()
    kavia_mask = branch_col.str.contains("KAVIA", na=False) | (branch_col == "K")
    kavia_rows = numeros_df[kavia_mask]
    if kavia_rows.empty:
        return GOLD_NUMEROS_KAVIA
    return float(kavia_rows["Total"].iloc[0])


def compute_ours_ag_pt_by_branch(ours: pd.DataFrame) -> pd.DataFrame:
    """Compute AG+PT cost sum by Sucursal destino."""
    if ours.empty:
        return pd.DataFrame()
    df = ours.copy()
    dest_col = next((c for c in df.columns if "destino" in c.lower()), "Sucursal destino")
    if dest_col not in df.columns:
        return pd.DataFrame()
    # Prefer Costo_after (post price correction), then Costo, else Costo unitario * Cantidad
    if "Costo_after" in df.columns:
        df["_Costo"] = df["Costo_after"]
    elif "Costo" in df.columns:
        df["_Costo"] = df["Costo"]
    elif "Costo unitario" in df.columns and "Cantidad" in df.columns:
        df["_Costo"] = df["Costo unitario"] * df["Cantidad"]
    else:
        return pd.DataFrame()
    agg = df.groupby(dest_col, as_index=False).agg(Total=("_Costo", "sum"))
    agg = agg.rename(columns={dest_col: "Sucursal_destino", "Total": "AG_plus_PT_Total"})
    return agg


def detect_header_row(df: pd.DataFrame) -> int:
    """Find row containing 'Orden'."""
    for i in range(min(15, len(df))):
        row = df.iloc[i].astype(str)
        if any("Orden" in str(v) for v in row):
            return i
    return -1


def parse_sheet(df: pd.DataFrame, sheet_name: str, sucursal: str | None) -> pd.DataFrame:
    """Parse a detail sheet into structured rows."""
    hdr = detect_header_row(df)
    if hdr < 0:
        return pd.DataFrame()
    data = df.iloc[hdr + 1 :].copy()
    # Use header row for column names
    cols = df.iloc[hdr].astype(str).tolist()
    data.columns = [c if c != "nan" else f"col_{i}" for i, c in enumerate(cols)]
    # Find column indices
    orden_col = next((i for i, c in enumerate(cols) if "Orden" in str(c)), 1)
    origen_col = next((i for i, c in enumerate(cols) if "Almac" in str(c) and "origen" in str(c).lower()), 2)
    dest_col = next((i for i, c in enumerate(cols) if "destino" in str(c).lower()), 3)
    fecha_col = next((i for i, c in enumerate(cols) if "Fecha" in str(c)), 5)
    cant_col = next((i for i, c in enumerate(cols) if "Cantidad" in str(c)), 6)
    depto_col = next((i for i, c in enumerate(cols) if "Departamento" in str(c)), 7)
    prod_col = next((i for i, c in enumerate(cols) if "Producto" in str(c)), 8)
    costo_col = next((i for i, c in enumerate(cols) if "Costo" in str(c)), 10)
    if costo_col >= len(cols):
        costo_col = len(cols) - 1
    out = pd.DataFrame()
    out["Orden"] = data.iloc[:, orden_col].astype(str)
    out["Almacen_origen"] = data.iloc[:, origen_col].astype(str).str.strip().str.upper()
    out["Sucursal_destino"] = sucursal or data.iloc[:, dest_col].astype(str).str.strip()
    out["Fecha"] = pd.to_datetime(data.iloc[:, fecha_col], errors="coerce")
    out["Cantidad"] = pd.to_numeric(data.iloc[:, cant_col], errors="coerce")
    out["Departamento"] = data.iloc[:, depto_col].astype(str)
    out["Producto"] = data.iloc[:, prod_col].astype(str).str.strip()
    out["Costo"] = pd.to_numeric(data.iloc[:, costo_col], errors="coerce")
    out = out[out["Producto"].notna() & (out["Producto"].str.len() > 2)]
    out = out[out["Costo"].notna() & out["Cantidad"].notna()]
    out["UnitCost"] = out["Costo"] / out["Cantidad"]
    return out


def parse_gold_excel(path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Parse all detail sheets from golden Excel. Returns (all_gold, ag_gold).

    Correct data source per origin:
    - *-AG sheets: correct for ALMACEN GENERAL rows only
    - *-PT-R sheets: correct for ALMACEN PRODUCTO TERMINADO rows only
    """
    xl = pd.ExcelFile(path)
    all_rows = []
    ag_rows = []
    for sheet in xl.sheet_names:
        if sheet == "NUMEROS":
            continue
        if "-PT-W" in sheet:
            continue  # PT-W = before correction
        parts = sheet.split("-")
        if len(parts) < 2:
            continue
        branch = parts[0]
        sucursal = SHEET_TO_SUCURSAL.get(branch, f"Panem - {branch}")
        df = pd.read_excel(path, sheet_name=sheet, header=None)
        parsed = parse_sheet(df, sheet, sucursal)
        if parsed.empty:
            continue
        parsed["Sheet"] = sheet
        if "-AG" in sheet:
            parsed = parsed[
                parsed["Almacen_origen"].str.contains("ALMACEN GENERAL")
            ]
            if not parsed.empty:
                ag_rows.append(parsed)
                all_rows.append(parsed)
        elif "-PT-R" in sheet:
            parsed = parsed[
                parsed["Almacen_origen"].str.contains("ALMACEN PRODUCTO TERMINADO")
            ]
            if not parsed.empty:
                all_rows.append(parsed)
    if not all_rows:
        return pd.DataFrame(), pd.DataFrame()
    all_gold = pd.concat(all_rows, ignore_index=True)
    all_gold = all_gold[
        (all_gold["Fecha"] >= GOLD_Fecha_START) & (all_gold["Fecha"] <= GOLD_Fecha_END)
    ]
    ag_gold = (
        pd.concat(ag_rows, ignore_index=True)
        if ag_rows
        else pd.DataFrame()
    )
    ag_gold = ag_gold[
        (ag_gold["Fecha"] >= GOLD_Fecha_START) & (ag_gold["Fecha"] <= GOLD_Fecha_END)
    ]
    return all_gold, ag_gold


def build_gold_lookup(gold: pd.DataFrame) -> dict:
    """Build lookup (Orden, Producto) -> (UnitCost, Costo)."""
    lookup = {}
    for _, row in gold.iterrows():
        key = (str(row["Orden"]).strip(), str(row["Producto"]).strip())
        lookup[key] = (row["UnitCost"], row["Costo"])
    return lookup


def load_ours(path: Path) -> pd.DataFrame:
    """Load our transfers CSV."""
    df = pd.read_csv(path)
    orig_col = next((c for c in df.columns if "Almac" in c and "origen" in c.lower()), None)
    if orig_col:
        df["Almacen_origen"] = df[orig_col].astype(str).str.strip().str.upper()
    return df


def filter_orders_for_gold_alignment(df: pd.DataFrame) -> pd.DataFrame:
    """Filter out orders excluded in golden dataset (bleed-through from prior weeks).

    Used only for gold-aligned diagnostic comparison, NOT in production pipeline.
    - AG rows: exclude Orden in AG_EXCLUDED_ORDERS
    - PT rows: exclude Orden in PT_EXCLUDED_ORDERS
    """
    if df.empty:
        return df
    df = df.copy()
    if "Almacen_origen" not in df.columns:
        orig_col = next((c for c in df.columns if "Almac" in c and "origen" in c.lower()), None)
        if orig_col:
            df["Almacen_origen"] = df[orig_col].astype(str).str.strip().str.upper()
        else:
            return df
    orden_col = "Orden" if "Orden" in df.columns else None
    if not orden_col:
        return df
    orden_str = df[orden_col].astype(str).str.strip()
    is_ag = df["Almacen_origen"].str.contains("ALMACEN GENERAL", na=False)
    is_pt = df["Almacen_origen"].str.contains("ALMACEN PRODUCTO TERMINADO", na=False)
    mask_ag_excl = is_ag & orden_str.isin(AG_EXCLUDED_ORDERS)
    mask_pt_excl = is_pt & orden_str.isin(PT_EXCLUDED_ORDERS)
    keep = ~(mask_ag_excl | mask_pt_excl)
    return df[keep].reset_index(drop=True)


def match_and_compare(ours: pd.DataFrame, gold_lookup: dict) -> pd.DataFrame:
    """Match our rows to gold, compare unit costs, return report. Includes unmatched rows."""
    rows = []
    for i, row in ours.iterrows():
        orden = str(row.get("Orden", "")).strip()
        producto = str(row.get("Producto", "")).strip()
        key = (orden, producto)
        gold_val = gold_lookup.get(key)
        ours_unit = row.get("Costo unitario", row.get("Costo", 0) / max(row.get("Cantidad", 1), 1e-9))
        ours_costo = row.get("Costo", 0)
        if gold_val:
            gold_unit, gold_costo = gold_val
            diff_unit = ours_unit - gold_unit
            diff_costo = ours_costo - gold_costo
            rows.append({
                "Orden": orden,
                "Producto": producto,
                "Sucursal_destino": row.get("Sucursal destino", ""),
                "Almacen_origen": row.get("Almacen_origen", ""),
                "Ours_UnitCost": ours_unit,
                "Gold_UnitCost": gold_unit,
                "Ours_Costo": ours_costo,
                "Gold_Costo": gold_costo,
                "Diff_UnitCost": diff_unit,
                "Diff_Costo": diff_costo,
                "Matched": True,
            })
        else:
            rows.append({
                "Orden": orden,
                "Producto": producto,
                "Sucursal_destino": row.get("Sucursal destino", ""),
                "Almacen_origen": row.get("Almacen_origen", ""),
                "Ours_UnitCost": ours_unit,
                "Gold_UnitCost": float("nan"),
                "Ours_Costo": ours_costo,
                "Gold_Costo": float("nan"),
                "Diff_UnitCost": float("nan"),
                "Diff_Costo": float("nan"),
                "Matched": False,
            })
    return pd.DataFrame(rows)


def derive_ag_precios(ag_gold: pd.DataFrame) -> pd.DataFrame:
    """Derive canonical AG unit cost per Producto from gold."""
    if ag_gold.empty:
        return pd.DataFrame()
    agg = ag_gold.groupby("Producto", as_index=False).agg(
        Precio_unitario=("UnitCost", "median"),
        Count=("UnitCost", "count"),
    )
    agg = agg[agg["Producto"].str.len() > 2]
    agg = agg.rename(columns={"Precio_unitario": "Precio unitario"})
    return agg[["Producto", "Precio unitario"]]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Investigate transfer cost discrepancy")
    parser.add_argument("--gold", default="TRANSFERENCIAS DEL 02 AL 08 FEBRERO.xlsx")
    parser.add_argument("--ours", default="data/c_processed/transfers/weekly/transfers_2026-02-02_2026-02-07.csv")
    parser.add_argument("--output-dir", default="data/c_processed/transfers/weekly")
    args = parser.parse_args(argv)
    gold_path = Path(args.gold)
    ours_path = Path(args.ours)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if not gold_path.exists():
        logger.error("Gold file not found: %s", gold_path)
        return 1
    if not ours_path.exists():
        logger.error("Ours file not found: %s", ours_path)
        return 1

    logger.info("Parsing gold Excel: %s", gold_path)
    all_gold, ag_gold = parse_gold_excel(gold_path)
    logger.info("Gold rows: %d all, %d AG", len(all_gold), len(ag_gold))

    gold_lookup = build_gold_lookup(all_gold)
    logger.info("Gold lookup keys: %d", len(gold_lookup))

    logger.info("Loading ours: %s", ours_path)
    ours = load_ours(ours_path)
    ours = filter_orders_for_gold_alignment(ours)
    logger.info("Ours rows after order exclusions: %d", len(ours))

    report = match_and_compare(ours, gold_lookup)
    if not report.empty:
        report_path = output_dir / "investigation_report.csv"
        report.to_csv(report_path, index=False)
        logger.info("Saved %s (%d matched, %d unmatched)", report_path,
                   (report["Matched"] == True).sum(), (report["Matched"] == False).sum())

    # Aggregate diff by Almacen origen (matched rows only)
    if not report.empty and "Almacen_origen" in report.columns:
        matched_only = report[report["Matched"] == True]
        if not matched_only.empty:
            agg = matched_only.groupby("Almacen_origen", as_index=False).agg(
                Diff_Sum=("Diff_Costo", "sum"),
                Count=("Matched", "count"),
            )
            logger.info("Diff by origin (matched): %s", agg.to_string())

    # AG unit costs -> AG_PRECIOS.xlsx (project root)
    ag_precios = derive_ag_precios(ag_gold)
    if not ag_precios.empty:
        ag_path = get_project_root() / "AG_PRECIOS.xlsx"
        ag_precios.to_excel(ag_path, index=False)
        logger.info("Saved AG_PRECIOS.xlsx: %d products", len(ag_precios))

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
