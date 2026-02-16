"""Configuration constants for weekly transfer analysis (10-week corrections)."""

# Gold reference totals for reconciliation: week_str -> (detail_total, numeros_total)
# Use when comparing weekly_breakdown to golden database investigation.
GOLD_REFERENCE_BY_WEEK: dict[str, tuple[float, float]] = {
    "2026-02-02_2026-02-07": (311794.0, 283368.0),
}

GOLD_NUMEROS_BY_WEEK: dict[str, float] = {
    "2026-02-02_2026-02-07": 283368.0,
}

# Orders excluded in golden dataset (bleed-through from prior weeks)
# Used only for gold-aligned diagnostic comparison, NOT in production pipeline
AG_EXCLUDED_ORDERS = {"9980-11588-2609294", "9980-11588-2609295", "9980-11588-2609296"}
PT_EXCLUDED_ORDERS = {"9982-11588-2607562", "9982-11588-2607690"}
