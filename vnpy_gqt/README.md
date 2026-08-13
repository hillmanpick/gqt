# GQT vn.py USDT-M research module

This directory is the isolated vn.py-based research path for BTC/ETH USDT-M
perpetual futures. It does not replace the running event-contract client or the
existing Freqtrade dry-run process.

## Safety boundary

- Research and backtesting only by default.
- Only `BTCUSDT` and `ETHUSDT` are accepted.
- Leverage is capped at 3x by the local configuration model.
- Position size is derived from a fixed fraction of account equity at risk.
- No forced entries, pyramiding, martingale sizing, API keys, or live order
  entry are implemented in this module.
- BTC and ETH are evaluated separately before any portfolio result is shown.

## Environment

Use Python 3.10-3.12 and install the project into a dedicated virtual
environment:

```powershell
cd D:\x\gqt\vnpy_gqt
py -3.12 -m venv .venv
.venv\Scripts\python -m pip install -e ".[test]"
```

The backtest adapter uses vn.py's modular packages:

- `vnpy`
- `vnpy_ctastrategy`
- `vnpy_sqlite`

Exact reviewed upstream versions and commits are recorded in `UPSTREAM.md`.

`vnpy_binance` is intentionally optional. Install it only when a reviewed
paper-trading gateway is added later. This package never reads the existing GQT
exchange credentials.

## Import existing Freqtrade candles

The desktop application already has BTC/ETH Feather files. Import one symbol
and timeframe into the vn.py database with:

```powershell
.venv\Scripts\python -m gqt_vnpy.cli import-freqtrade `
  --source "$env:LOCALAPPDATA\HillmanPick\GQT Trader\data\trading\user_data\data\binance\futures" `
  --symbol BTCUSDT --timeframe 15m
```

Repeat for `ETHUSDT`. The importer validates monotonic timestamps, OHLC
relationships, duplicate bars, positive volume, and missing candle intervals.
Files with missing intervals are rejected before anything is written to SQLite.
When a dedicated 1-hour futures file is absent, it derives 1-hour candles from
UTC-aligned groups of exactly four 15-minute bars and rejects incomplete hours.

## Backtest

Edit `config/research.json`, then run:

```powershell
.venv\Scripts\python -m gqt_vnpy.cli backtest `
  --config config/research.json --start 2021-01-01 --end 2026-01-01
```

The command runs each symbol independently and writes JSON results under
`results/`. It also runs rolling train/test windows; strategy parameters are
fixed, so the training window is an embargoed context window rather than an
optimization pass. A result is marked eligible only when every out-of-sample
fold meets the configured trade-count, profit-factor, expectancy, and drawdown
limits.

Because vn.py stores only coarse `MINUTE` and `HOUR` interval enums, imported
aggregated candles use isolated research symbols such as
`BTCUSDT_GQT_15M.GLOBAL`. This prevents 15-minute, 1-hour, and 4-hour datasets
from overwriting each other. Slippage is converted from basis points to a
fixed price amount using each test fold's median price. Results also deduct a
configurable adverse 8-hour funding-rate stress from every closed trade.
Walk-forward test windows use right-open date ranges, and pre-test bars are
loaded only through the strategy warm-up callback so they cannot open trades
or carry positions into an out-of-sample fold.

The bar simulator uses a conservative ambiguity rule: when one OHLC candle
touches both stop and target, the stop wins. Entries are stop orders placed from
the previous closed-bar breakout signal. A newly filled entry receives an
immediate protective stop, while its profit target starts after the entry bar.
Any position still open at a fold boundary is settled at the final close with
fees, slippage, and funding stress included. CLI dates are interpreted as UTC;
database queries are converted to vn.py's configured database timezone.

Do not enable real trading based only on an in-sample backtest.
