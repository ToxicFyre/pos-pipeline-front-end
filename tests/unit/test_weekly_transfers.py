"""Unit tests for get_weekly_transfers_with_prices (12-week analysis corrections)."""

import sys
from datetime import date
from pathlib import Path

import pandas as pd

# Add src to path for pos_frontend
_root = Path(__file__).resolve().parent.parent.parent
if str(_root / "src") not in sys.path:
    sys.path.insert(0, str(_root / "src"))

from pos_frontend.transfers.weekly_with_prices import (
    apply_prices,
    build_week_ranges,
    compute_origin_totals,
    compute_price_change_alerts,
    compute_weekly_cost_comparison,
    compute_weekly_price_changes,
    normalize_producto_for_match,
    _write_weekly_breakdown,
)


def test_build_week_ranges_num_weeks_one_end_sunday() -> None:
    """num_weeks=1 with end_date on Sunday returns that week's Mon-Sun."""
    end = date(2026, 2, 8)  # Sunday
    ranges = build_week_ranges(end, 1)
    assert len(ranges) == 1
    mon, sun = ranges[0]
    assert mon == date(2026, 2, 2)
    assert sun == date(2026, 2, 8)


def test_build_week_ranges_num_weeks_one_end_monday() -> None:
    """num_weeks=1 with end_date on Monday returns that week's Mon-Sun."""
    end = date(2026, 2, 2)  # Monday
    ranges = build_week_ranges(end, 1)
    assert len(ranges) == 1
    mon, sun = ranges[0]
    assert mon == date(2026, 2, 2)
    assert sun == date(2026, 2, 8)


def test_build_week_ranges_num_weeks_two() -> None:
    """num_weeks=2 returns two consecutive weeks, most recent first."""
    end = date(2026, 2, 8)  # Sunday
    ranges = build_week_ranges(end, 2)
    assert len(ranges) == 2
    mon1, sun1 = ranges[0]
    mon2, sun2 = ranges[1]
    assert mon1 == date(2026, 2, 2)
    assert sun1 == date(2026, 2, 8)
    assert mon2 == date(2026, 1, 26)
    assert sun2 == date(2026, 2, 1)


def test_build_week_ranges_boundary_midweek() -> None:
    """end_date mid-week (Wed) uses that week's Sun as anchor."""
    end = date(2026, 2, 4)  # Wednesday
    ranges = build_week_ranges(end, 1)
    assert len(ranges) == 1
    mon, sun = ranges[0]
    assert mon == date(2026, 2, 2)
    assert sun == date(2026, 2, 8)


def test_build_week_ranges_twelve_weeks() -> None:
    """num_weeks=12 produces 12 consecutive Mon-Sun ranges."""
    end = date(2026, 2, 8)
    ranges = build_week_ranges(end, 12)
    assert len(ranges) == 12
    for i, (mon, sun) in enumerate(ranges):
        assert mon.weekday() == 0
        assert sun.weekday() == 6
        assert (sun - mon).days == 6
        if i < 11:
            prev_sun = ranges[i + 1][1]
            assert (mon - prev_sun).days == 1


def test_integration_weeks_arg_rejected_when_zero() -> None:
    """CLI rejects --weeks 0 (validation)."""
    from pos_frontend.transfers.weekly_with_prices import main

    # --weeks 0 should return 1
    result = main(["--weeks", "0"])
    assert result == 1


def test_apply_prices_with_alias() -> None:
    """Mayones de Panem * resolves to Mayonesa de Panem * price via alias."""
    precios = pd.DataFrame({
        "Producto": ["Mayonesa de Panem *"],
        "Precio unitario": [25.50],
    })
    transfers = pd.DataFrame({
        "Producto": ["Mayones de Panem *"],
        "Almacen_origen": ["ALMACEN PRODUCTO TERMINADO"],
        "Cantidad": [10],
        "Costo": [0.0],
        "Costo unitario": [0.0],
    })
    out, _ = apply_prices(transfers, precios, ag_precios=None)
    assert out["Costo unitario"].iloc[0] == 25.50
    assert out["Costo"].iloc[0] == 255.0


def test_apply_prices_asterisk_normalization() -> None:
    """Sopa de tomate* and Sopa de tomate * both resolve to same price."""
    precios = pd.DataFrame({
        "Producto": ["Sopa de tomate *"],
        "Precio unitario": [15.00],
    })
    transfers = pd.DataFrame({
        "Producto": ["Sopa de tomate*", "Sopa de tomate *"],
        "Almacen_origen": ["ALMACEN PRODUCTO TERMINADO", "ALMACEN PRODUCTO TERMINADO"],
        "Cantidad": [2, 3],
        "Costo": [0.0, 0.0],
        "Costo unitario": [0.0, 0.0],
    })
    out, _ = apply_prices(transfers, precios, ag_precios=None)
    assert out["Costo unitario"].iloc[0] == 15.00
    assert out["Costo unitario"].iloc[1] == 15.00
    assert out["Costo"].iloc[0] == 30.0
    assert out["Costo"].iloc[1] == 45.0


def test_normalize_producto_asterisk() -> None:
    """Asterisk variants normalize to canonical form."""
    s = pd.Series(["CEBOLLA ENCURTIDA *", "CEBOLLA ENCURTIDA*"])
    out = normalize_producto_for_match(s)
    assert out.iloc[0] == "cebolla encurtida *"
    assert out.iloc[1] == "cebolla encurtida *"


def test_write_weekly_breakdown_gold_columns() -> None:
    """Gold_Reference and Gold_NUMEROS present for configured weeks."""
    import tempfile
    combined = pd.DataFrame({
        "Week": ["2026-02-02_2026-02-07", "2026-01-26_2026-02-01"],
        "Costo_after": [311000.0, 290000.0],
        "Sucursal destino": ["Branch A", "Branch B"],
        "Almacen_origen": ["ALMACEN PRODUCTO TERMINADO", "ALMACEN GENERAL"],
    })
    with tempfile.TemporaryDirectory() as tmp:
        out_dir = Path(tmp)
        _write_weekly_breakdown(combined, out_dir)
        breakdown = pd.read_csv(out_dir / "weekly_breakdown.csv")
        assert "Gold_Reference" in breakdown.columns
        assert "Gold_NUMEROS" in breakdown.columns
        feb_row = breakdown[breakdown["Week"] == "2026-02-02_2026-02-07"].iloc[0]
        assert feb_row["Gold_Reference"] == 311794.0
        assert feb_row["Gold_NUMEROS"] == 283368.0
        # Other week has NaN
        other_row = breakdown[breakdown["Week"] == "2026-01-26_2026-02-01"].iloc[0]
        assert pd.isna(other_row["Gold_Reference"])
        assert pd.isna(other_row["Gold_NUMEROS"])


def test_compute_weekly_price_changes() -> None:
    """compute_weekly_price_changes returns only rows where price changed, with derived columns."""
    df = pd.DataFrame({
        "Producto": ["Item A", "Item B", "Item C"],
        "Almacen_origen": [
            "ALMACEN PRODUCTO TERMINADO",
            "ALMACEN GENERAL",
            "ALMACEN PRODUCTO TERMINADO",
        ],
        "Cantidad": [10, 5, 3],
        "Costo_before": [100.0, 50.0, 30.0],
        "Costo_after": [120.0, 50.0, 36.0],
        "Costo unitario": [12.0, 10.0, 12.0],
    })
    result = compute_weekly_price_changes(df)
    assert len(result) == 2
    assert "Item B" not in result["Producto"].values
    row_a = result[result["Producto"] == "Item A"].iloc[0]
    assert row_a["Costo_unitario_before"] == 10.0
    assert row_a["Costo_unitario_after"] == 12.0
    assert row_a["Costo_before"] == 100.0
    assert row_a["Costo_after"] == 120.0
    row_c = result[result["Producto"] == "Item C"].iloc[0]
    assert row_c["Costo_unitario_before"] == 10.0
    assert row_c["Costo_unitario_after"] == 12.0


def test_compute_weekly_price_changes_empty_when_no_changes() -> None:
    """compute_weekly_price_changes returns empty DataFrame with columns when no changes."""
    df = pd.DataFrame({
        "Producto": ["Item A"],
        "Almacen_origen": ["ALMACEN PRODUCTO TERMINADO"],
        "Cantidad": [10],
        "Costo_before": [100.0],
        "Costo_after": [100.0],
        "Costo unitario": [10.0],
    })
    result = compute_weekly_price_changes(df)
    assert len(result) == 0
    assert list(result.columns) == [
        "Producto",
        "Almacen_origen",
        "Cantidad",
        "Costo_unitario_before",
        "Costo_unitario_after",
        "Costo_before",
        "Costo_after",
    ]


def test_compute_price_change_alerts() -> None:
    """compute_price_change_alerts flags products with large price changes."""
    df = pd.DataFrame({
        "Producto": ["Item A", "Item A", "Item B"],
        "Almacen_origen": ["ALMACEN GENERAL", "ALMACEN GENERAL", "ALMACEN PRODUCTO TERMINADO"],
        "Cantidad": [10, 5, 4],
        "Costo_before": [100.0, 50.0, 40.0],
        "Costo_after": [200.0, 100.0, 44.0],
        "Costo unitario": [20.0, 20.0, 11.0],
    })
    result = compute_price_change_alerts(df, pct_high=50, pct_medium=25)
    assert len(result) == 2
    row_a = result[result["Producto"] == "Item A"].iloc[0]
    assert row_a["Weighted_avg_unit_before"] == 10.0
    assert row_a["Unit_after"] == 20.0
    assert row_a["Pct_change_unit"] == 100.0
    assert row_a["Alert"] == "HIGH"
    row_b = result[result["Producto"] == "Item B"].iloc[0]
    assert row_b["Pct_change_unit"] == 10.0
    assert row_b["Alert"] == ""


def test_compute_origin_totals() -> None:
    """compute_origin_totals returns AG/PT before/after by week."""
    df = pd.DataFrame({
        "Week": ["W1", "W1", "W2"],
        "Almacen_origen": ["ALMACEN GENERAL", "ALMACEN PRODUCTO TERMINADO", "ALMACEN GENERAL"],
        "Costo_before": [100.0, 200.0, 50.0],
        "Costo_after": [110.0, 180.0, 55.0],
    })
    result = compute_origin_totals(df, exclude_cedis_dest=False)
    assert "All" in result["Week"].values
    all_row = result[result["Week"] == "All"].iloc[0]
    assert all_row["AG_Before"] == 150.0
    assert all_row["AG_After"] == 165.0
    assert all_row["PT_Before"] == 200.0
    assert all_row["PT_After"] == 180.0


def test_exclude_cedis_dest_behavior() -> None:
    """To_Branches_Only < Total when exclude_cedis_dest=True."""
    combined = pd.DataFrame({
        "Week": ["2026-02-02_2026-02-07", "2026-02-02_2026-02-07"],
        "Costo_before": [100.0, 200.0],
        "Costo_after": [100.0, 200.0],
        "Sucursal destino": ["Panem - CEDIS", "Branch A"],
    })
    agg_exclude = compute_weekly_cost_comparison(combined, exclude_cedis_dest=True)
    agg_include = compute_weekly_cost_comparison(combined, exclude_cedis_dest=False)
    total_exclude = agg_exclude["Total_After"].iloc[0]
    total_include = agg_include["Total_After"].iloc[0]
    assert total_exclude == 200.0  # branches only
    assert total_include == 300.0  # branches + CEDIS
    assert total_exclude < total_include
