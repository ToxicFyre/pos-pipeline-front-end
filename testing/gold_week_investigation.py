"""Gold-week investigation: Fetch Feb 2-8, apply PRECIOS/AG_PRECIOS, filter orders like golden, compare to NUMEROS.

Investigation-only script. Production pipeline remains unchanged (no order exclusions).
"""
import sys
from pathlib import Path

_root = Path(__file__).resolve().parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))
from shim_bootstrap import add_src_to_syspath
add_src_to_syspath()

import argparse
import logging

import pandas as pd

from pos_core import DataPaths
from pos_core.transfers import core

from pos_frontend.config.paths import get_project_root
from pos_frontend.transfers.gold_investigation import (
    filter_orders_for_gold_alignment,
    parse_numeros,
    extract_kavia_total,
    compute_ours_ag_pt_by_branch,
)
from pos_frontend.transfers.weekly_with_prices import (
    load_precios,
    load_ag_precios,
    collect_branch_csv_paths,
    read_and_concat_transfers,
    apply_prices,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAVIA_SUCURSAL = "Panem - Hotel Kavia N"
START_STR = "2026-02-02"
END_STR = "2026-02-08"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Gold-week investigation: fetch Feb 2-8, compare to NUMEROS")
    parser.add_argument("--gold", default="TRANSFERENCIAS DEL 02 AL 08 FEBRERO.xlsx")
    parser.add_argument("--data-root", default="data")
    parser.add_argument("--precios-path", default="PRECIOS.xlsx")
    parser.add_argument("--ag-precios-path", default="AG_PRECIOS.xlsx")
    parser.add_argument("--branches-file", default="sucursales.json")
    parser.add_argument("--output-dir", default="data/c_processed/transfers/weekly")
    args = parser.parse_args(argv or [])

    project_root = get_project_root()
    gold_path = project_root / args.gold
    data_root = project_root / args.data_root
    batch_dir = data_root / "b_clean" / "transfers" / "batch"
    output_dir = project_root / args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    if not gold_path.exists():
        logger.error("Gold file not found: %s", gold_path)
        return 1

    # 1. Load PRECIOS/AG_PRECIOS
    precios_path = project_root / args.precios_path
    ag_path = project_root / args.ag_precios_path
    if not precios_path.exists():
        logger.error("PRECIOS not found: %s", precios_path)
        return 1
    precios = load_precios(precios_path)
    ag_precios = load_ag_precios(ag_path)
    logger.info("Loaded PRECIOS: %d, AG_PRECIOS: %d", len(precios), len(ag_precios) if ag_precios is not None else 0)

    # 2. Fetch Feb 2-8 transfers
    paths = DataPaths.from_root(data_root, project_root / args.branches_file)
    logger.info("Fetching transfers %s to %s", START_STR, END_STR)
    core.fetch(paths, START_STR, END_STR, mode="force")

    # 3. Load and apply prices (production logic)
    csv_paths = collect_branch_csv_paths(batch_dir, START_STR, END_STR)
    df = read_and_concat_transfers(csv_paths)
    if df.empty:
        logger.error("No transfer data for %s to %s", START_STR, END_STR)
        return 1
    df_updated, _ = apply_prices(df, precios, ag_precios=ag_precios)
    logger.info("Applied prices: %d rows", len(df_updated))

    # 4. Apply order exclusions (investigation-only)
    df_filtered = filter_orders_for_gold_alignment(df_updated)
    logger.info("After order exclusions: %d rows", len(df_filtered))

    # 5. Exclude CEDIS destinations
    if "Sucursal destino" in df_filtered.columns:
        df_filtered = df_filtered[df_filtered["Sucursal destino"] != "Panem - CEDIS"]
    logger.info("After excluding CEDIS: %d rows", len(df_filtered))

    # 6. Compute our Kavia AG+PT total
    costo_col = "Costo_after" if "Costo_after" in df_filtered.columns else "Costo"
    df_kavia = df_filtered[df_filtered["Sucursal destino"] == KAVIA_SUCURSAL]
    ours_kavia_total = df_kavia[costo_col].sum()
    logger.info("Ours Kavia AG+PT total: %.2f (%d rows)", ours_kavia_total, len(df_kavia))

    # 7. Parse NUMEROS and extract gold Kavia total
    numeros_df = parse_numeros(gold_path)
    gold_kavia_total = extract_kavia_total(numeros_df)
    logger.info("Gold NUMEROS Kavia total: %.2f", gold_kavia_total)

    # 8. Compare and write report
    diff = ours_kavia_total - gold_kavia_total
    report_rows = [{
        "Metric": "Kavia AG+PT Total",
        "Ours": round(ours_kavia_total, 2),
        "Gold_NUMEROS": round(gold_kavia_total, 2),
        "Diff": round(diff, 2),
    }]
    report = pd.DataFrame(report_rows)
    report_path = output_dir / "kavia_numeros_comparison.csv"
    report.to_csv(report_path, index=False)
    logger.info("Saved %s", report_path)

    # Also add per-branch breakdown for reference
    by_branch = compute_ours_ag_pt_by_branch(df_filtered)
    if not by_branch.empty:
        by_branch_path = output_dir / "gold_week_by_branch.csv"
        by_branch.to_csv(by_branch_path, index=False)
        logger.info("Saved %s", by_branch_path)

    logger.info("Kavia comparison: Ours=%.2f, Gold=%.2f, Diff=%.2f", ours_kavia_total, gold_kavia_total, diff)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
