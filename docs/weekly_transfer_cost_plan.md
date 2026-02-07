# Weekly Transfer Cost Planning Document

## Goal
Create a workflow that:
1. Downloads transfer data **weekly** for the last three months using the existing transfer code.
2. Enriches each weekly dataset with unit costs from a provided cost file.
3. Computes:
   - Total cost per item (per week).
   - Total cost for the entire week.
4. Produces clear outputs (CSV files, logs) that can be audited and reused.

This document describes the plan and intended structure; it does **not** implement the code yet.

## Current Building Blocks
The repo already includes a helper script that demonstrates how to fetch transfers:
- `testing/get_transfer_data.py`
  - `run_transfers_core()` calls `pos_core.transfers.core.fetch()` and writes per-branch data to `data/b_clean/transfers/batch/`.
  - `run_transfers_mart()` calls `pos_core.transfers.marts.fetch_pivot()` and writes a CSV to `data/c_processed/transfers/mart_transfers_pivot_<start>_<end>.csv`.

We can reuse `run_transfers_mart()` for weekly transfer data, because it yields a consolidated CSV for a given date range.

## Proposed Inputs
1. **Date range**: Last 3 months (rolling window from “today” back 3 months).
2. **Weekly segmentation**: Each week should be a separate date range (e.g., Monday–Sunday). The week boundaries can be calculated with Python’s `datetime`.
3. **Unit cost file**: A CSV (or similar) containing item IDs and their unit costs. Example columns:
   - `item_id` (or SKU)
   - `unit_cost`
   - (optional) `effective_date` if costs change over time
4. **Item name normalization map**: A mapping between transfer item names and price-file item names.
   - Example: `"Concha Vainilla *"` (transfer data) → `"Concha Vainilla"` (price file)
   - This can live in a simple CSV or JSON file, or be derived with cleanup rules.

## Proposed Outputs
For each weekly window:
- **Raw transfer CSV** (from `run_transfers_mart()`):
  - `data/c_processed/transfers/mart_transfers_pivot_<start>_<end>.csv`
- **Weekly enriched CSV** (new):
  - `data/c_processed/transfers/weekly_costs_<start>_<end>.csv`
  - Contains original transfer data + unit cost + computed totals.
- **Weekly summary CSV** (new):
  - `data/c_processed/transfers/weekly_cost_summary_<start>_<end>.csv`
  - Includes totals per item and the overall weekly total.

## Intended Workflow (Step-by-Step)
1. **Generate weekly ranges** for the last 3 months:
   - Determine `end_date` as today (or last completed day).
   - Determine `start_date` = end_date minus ~90 days.
   - Build weekly windows, e.g. `[week_start, week_end]` for each week.

2. **Download weekly transfers**:
   - For each weekly window, call `run_transfers_mart(start, end)`.
   - Capture the CSV path for each week.

3. **Load unit costs**:
   - Read the cost file into a DataFrame.
   - Normalize key columns (e.g., `item_id` or SKU) to align with transfer data.
   - If there are effective dates, pick the correct unit cost for each transfer line item.

4. **Normalize item names before joining**:
   - Apply a mapping table or cleanup rules to align transfer item names with price-file names.
   - Example rule: trim trailing asterisks or extra whitespace.

5. **Enrich transfer data with costs**:
   - Join transfer data with unit costs on `item_id`.
   - Add a computed column: `total_cost = quantity * unit_cost`.

6. **Aggregate costs**:
   - Group by `item_id` to calculate per-item totals per week.
   - Sum all `total_cost` values to get the weekly total.

7. **Save outputs**:
   - Store enriched data and summary files in a new processed folder.

## Planned Structure for the Implementation
We can introduce a new script, for example:
- `reporting/weekly_transfer_costs.py` (or `testing/weekly_transfer_costs.py` during early validation)

Suggested function structure:
- `generate_weekly_ranges(start_date, end_date, week_start=0) -> list[tuple[str, str]]`
- `download_weekly_transfers(ranges) -> list[Path]`
- `load_unit_costs(path) -> pd.DataFrame`
- `apply_costs(transfer_df, cost_df) -> pd.DataFrame`
- `normalize_item_names(transfer_df, mapping_df) -> pd.DataFrame`
- `summarize_weekly_costs(enriched_df) -> pd.DataFrame`
- `run_weekly_pipeline(start_date, end_date, cost_file)`

## Edge Cases to Consider
- **Missing costs**: Some items might not have unit cost entries. Decide whether to drop, flag, or default to 0.
- **Multiple cost records**: If costs vary by date, ensure the correct effective cost is applied for each transfer date.
- **Empty weeks**: A week might have no transfers; still write a summary row with 0 total.
- **Data consistency**: Ensure quantity columns are numeric and no negative quantities unless expected (returns).

## Next Steps (When You’re Ready to Code)
1. Confirm the cost file format and key column names.
2. Decide how week boundaries should be defined (Mon–Sun vs Sun–Sat).
3. Pick output folder names and CSV schemas.
4. Implement the script and run it for a small sample date range.
5. Expand to last 3 months and validate totals.
