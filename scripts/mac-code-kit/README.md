# Mac Code Kit

Shell scripts to run the 12-week transfer workflow on macOS / Linux.

## Setup

1. **Secrets** – In the project root, create `secrets.env`:

   ```bash
   export WS_USER="your_email"
   export WS_PASS="your_password"
   export WS_BASE="https://www.wansoft.net/Wansoft.Web"
   ```

2. **Make scripts executable**:

   ```bash
   chmod +x scripts/mac-code-kit/*.sh
   ```

3. **Price files** – Ensure `PRECIOS.xlsx` and `AG_PRECIOS.xlsx` exist (see [docs/setup.md](../../docs/setup.md) and [docs/ten_week_analysis_corrections.md](../../docs/ten_week_analysis_corrections.md)).

## 12-Week Production Workflow

Production order (run from project root):

```bash
# 1. Update PRECIOS with PRECIO UNITARIO (UNIDAD logic: LT/KG = unit, PZ = PRECIO DRIVE / PRESENTACION)
./scripts/mac-code-kit/run_update_precios_unit_prices.sh

# 2. Run 12-week transfer analysis (fetches last 12 Mon–Sun weeks from Wansoft, applies PRECIOS/AG_PRECIOS, excludes CEDIS)
./scripts/mac-code-kit/run_weekly_transfers.sh

# 3. Build pivot marts from corrected weekly CSVs
./scripts/mac-code-kit/run_weekly_transfer_pivots.sh
```

`run_weekly_transfers.sh` automatically runs `run_update_precios_unit_prices.sh` first, so in practice you can run step 2 alone.

## Usage

From the project root:

```bash
# Run 12-week transfer analysis (runs update_precios first, then fetches, applies prices, outputs weekly CSVs and reports)
./scripts/mac-code-kit/run_weekly_transfers.sh

# Build pivot marts from corrected weekly data
./scripts/mac-code-kit/run_weekly_transfer_pivots.sh
```

Pass-through arguments (e.g. `--precios-path`, `--end`, `--ag-precios-path`, `--weeks`):

```bash
./scripts/mac-code-kit/run_weekly_transfers.sh --data-root data --precios-path PRECIOS.xlsx --end 2026-02-08 --weeks 8
```

## Prerequisites

- **run_update_precios_unit_prices.sh** – Run before the weekly transfer pipeline so PRECIOS.xlsx has a current PRECIO UNITARIO column. `run_weekly_transfers.sh` invokes it automatically.
- **PRECIOS with UNIDAD** – PRECIOS.xlsx is the source of truth for unit prices. The UNIDAD column semantics:
  - **LT** or **KG**: PRECIO DRIVE is already the unit price.
  - **PZ**: PRECIO DRIVE is the presentation price; PRECIO UNITARIO = PRECIO DRIVE / PRESENTACION.
- **run_pre_weekly_check.sh** – Investigation only. Refreshes PRECIOS/AG_PRECIOS from golden; do **not** run before production if it would overwrite your corrected PRECIOS with UNIDAD.

## Scripts

| Script | Purpose |
|--------|---------|
| `run_update_precios_unit_prices.sh` | Add PRECIO UNITARIO column to PRECIOS.xlsx using UNIDAD logic (LT/KG/PZ) |
| `run_pre_weekly_check.sh` | Investigation only: refresh PRECIOS/AG_PRECIOS from golden |
| `run_weekly_transfers.sh` | Fetch last 12 weeks, apply prices, output weekly CSVs and reports |
| `run_weekly_transfer_pivots.sh` | Build mart_transfers_pivot_*.csv from corrected weekly data |
