# Setup on a New Machine

After cloning, set up the following before running scripts.

## 1. Python environment

```bash
pip install -r requirements.txt
```

Requires `pos-core-etl` (from https://github.com/ToxicFyre/pos-pipeline-core-etl) — install via pip or `pip install -e ../pos-pipeline-core-etl`.

## 2. Secrets

Copy `secrets.env.example` to `secrets.env` and fill in your credentials:

- **Wansoft** (WS_USER, WS_PASS, WS_BASE): Required for transfer/sales/payments ETL
- **Telegram**: For daily forecast notifications
- **Google Drive**: For Zapier upload

Source before running: `set -a && source secrets.env && set +a` (bash) or set env vars manually in your shell.

## 3. Branch config

Copy `sucursales.example.json` to `sucursales.json` and replace `XXXX` placeholder codes with your branch codes from Wansoft.

## 4. Price files (for 10-week transfer analysis)

Run `python testing/compare_unit_prices_full.py` and `python testing/investigate_transfer_cost.py` after placing the golden reference file (`TRANSFERENCIAS DEL 02 AL 08 FEBRERO.xlsx` or similar) where the scripts expect it. Copy `PRECIOS_UPDATED.xlsx` → `PRECIOS.xlsx` and `AG_PRECIOS_UPDATED.xlsx` → `AG_PRECIOS.xlsx` as needed.

See `TEN_WEEK_ANALYSIS_CORRECTIONS.md` for the full pre-run checklist.

## 5. Run scripts

**Mac / Linux:** Use the `mac-code-kit/` folder:

```bash
chmod +x mac-code-kit/*.sh
./mac-code-kit/run_weekly_transfers.sh
./mac-code-kit/run_weekly_transfer_pivots.sh
```

See `mac-code-kit/README.md` for details.

**Windows:** The `.cmd` files are not in the repo. Run directly:

```bash
python testing/get_weekly_transfers_with_prices.py --data-root data --precios-path PRECIOS.xlsx
python testing/build_weekly_transfer_pivots.py
```

Ensure `WS_USER`, `WS_PASS`, `WS_BASE` are set in your environment before running transfer scripts.
