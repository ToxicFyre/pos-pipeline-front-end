# Mac Code Kit

Shell scripts to run the 10-week transfer workflow on macOS / Linux.

## Setup

1. **Secrets** – In the project root, create `secrets.env`:

   ```bash
   export WS_USER="your_email"
   export WS_PASS="your_password"
   export WS_BASE="https://www.wansoft.net/Wansoft.Web"
   ```

2. **Make scripts executable**:

   ```bash
   chmod +x mac-code-kit/*.sh
   ```

3. **Price files** – Ensure `PRECIOS.xlsx` and `AG_PRECIOS.xlsx` exist (see SETUP.md and TEN_WEEK_ANALYSIS_CORRECTIONS.md).

## Usage

From the project root:

```bash
# Optional: refresh PRECIOS/AG_PRECIOS from golden reference
./mac-code-kit/run_pre_weekly_check.sh

# Run 10-week transfer analysis (fetches from Wansoft, applies prices)
./mac-code-kit/run_weekly_transfers.sh

# Build pivot marts from corrected weekly CSVs
./mac-code-kit/run_weekly_transfer_pivots.sh
```

Or run with full path:

```bash
./mac-code-kit/run_weekly_transfers.sh --data-root data --precios-path PRECIOS.xlsx --ag-precios-path AG_PRECIOS.xlsx
```

## Scripts

| Script | Purpose |
|--------|---------|
| `run_pre_weekly_check.sh` | Run compare_unit_prices_full.py to refresh PRECIOS/AG_PRECIOS from gold |
| `run_weekly_transfers.sh` | Fetch transfers, apply prices, output weekly CSVs and reports |
| `run_weekly_transfer_pivots.sh` | Build mart_transfers_pivot_*.csv from corrected weekly data |
