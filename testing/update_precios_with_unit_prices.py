"""Update PRECIOS.xlsx with a computed PRECIO UNITARIO column.

PRECIO DRIVE is the price for the quantity in PRESENTACION (e.g. $91.08 for 0.42 L).
PRECIO UNITARIO = PRECIO DRIVE / PRESENTACION gives the true unit price per liter/unit.

Adds PRECIO UNITARIO column and saves back to the same file (or --output path).
"""
import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))
from shim_bootstrap import add_src_to_syspath
add_src_to_syspath()

from pos_frontend.config.paths import get_project_root


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Add PRECIO UNITARIO column to PRECIOS.xlsx (PRECIO DRIVE / PRESENTACION)"
    )
    parser.add_argument("--precios", default="PRECIOS.xlsx", help="Path to PRECIOS.xlsx")
    parser.add_argument("--output", help="Output path (default: overwrite input)")
    parser.add_argument("--dry-run", action="store_true", help="Show changes without writing")
    args = parser.parse_args(argv or [])

    project_root = get_project_root()
    precios_path = project_root / args.precios
    if not precios_path.exists():
        print(f"Error: {precios_path} not found")
        return 1

    df = pd.read_excel(precios_path, sheet_name=0)

    # Find PRESENTACION column (may be PRESENTACION, PRESENTACIÓN, etc.)
    present_col = None
    for c in df.columns:
        if "present" in str(c).lower():
            present_col = c
            break
    if present_col is None:
        print("Error: No PRESENTACION column found in PRECIOS.xlsx")
        return 1

    precio_col = "PRECIO DRIVE" if "PRECIO DRIVE" in df.columns else None
    if precio_col is None:
        # Try alternate names
        for c in df.columns:
            if "precio" in str(c).lower() and "drive" in str(c).lower():
                precio_col = c
                break
    if precio_col is None:
        print("Error: No PRECIO DRIVE column found")
        return 1

    df["PRESENTACION_num"] = pd.to_numeric(df[present_col], errors="coerce")
    df["PRECIO_num"] = pd.to_numeric(df[precio_col], errors="coerce")

    # PRECIO UNITARIO = PRECIO DRIVE / PRESENTACION
    # When PRESENTACION is 0 or NaN, use PRECIO DRIVE as fallback (avoids inf/NaN)
    mask_valid = (df["PRESENTACION_num"] > 0) & df["PRECIO_num"].notna()
    df["PRECIO UNITARIO"] = np.where(
        mask_valid,
        df["PRECIO_num"] / df["PRESENTACION_num"],
        df["PRECIO_num"],
    )

    # Drop helper columns
    df = df.drop(columns=["PRESENTACION_num", "PRECIO_num"], errors="ignore")

    # Round for readability
    if "PRECIO UNITARIO" in df.columns:
        df["PRECIO UNITARIO"] = df["PRECIO UNITARIO"].round(6)

    if args.dry_run:
        cols = [c for c in df.columns if "NOMBRE" in str(c) or "Producto" in str(c) or "precio" in str(c).lower() or "PRECIO" in str(c) or c == present_col]
        print("Sample (first 5 rows):")
        print(df[cols].head().to_string())
        print(f"\nWould add PRECIO UNITARIO column. {len(df)} rows.")
        return 0

    out_path = (project_root / args.output) if args.output else precios_path

    xl = pd.ExcelFile(precios_path)
    sheet_name = xl.sheet_names[0] if xl.sheet_names else "Sheet1"
    df.to_excel(out_path, index=False, sheet_name=sheet_name)
    print(f"Saved {out_path} with PRECIO UNITARIO column ({len(df)} rows)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
