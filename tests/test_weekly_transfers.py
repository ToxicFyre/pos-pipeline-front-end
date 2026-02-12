"""Unit tests for get_weekly_transfers_with_prices (10-week analysis corrections)."""

import sys
from pathlib import Path

import pandas as pd

# Add project root for imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from testing.get_weekly_transfers_with_prices import (
    apply_prices,
    compute_weekly_cost_comparison,
    normalize_producto_for_match,
    _write_weekly_breakdown,
)


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
