from __future__ import annotations

from dataclasses import dataclass
from datetime import timezone
from pathlib import Path

from .config import ALLOWED_SYMBOLS, ALLOWED_TIMEFRAMES


@dataclass(frozen=True)
class ImportReport:
    source: str
    symbol: str
    timeframe: str
    bars: int
    first_timestamp: str
    last_timestamp: str
    missing_intervals: int


def freqtrade_filename(symbol: str, timeframe: str) -> str:
    normalized = symbol.upper()
    if normalized not in ALLOWED_SYMBOLS:
        raise ValueError(f"unsupported symbol: {symbol}")
    if timeframe not in ALLOWED_TIMEFRAMES:
        raise ValueError(f"unsupported timeframe: {timeframe}")
    base = normalized.removesuffix("USDT")
    return f"{base}_USDT_USDT-{timeframe}-futures.feather"


def storage_symbol(symbol: str, timeframe: str) -> str:
    """Keep aggregated intervals isolated in vn.py's minute/hour database keys."""
    normalized = symbol.upper()
    if normalized not in ALLOWED_SYMBOLS:
        raise ValueError(f"unsupported symbol: {symbol}")
    if timeframe not in ALLOWED_TIMEFRAMES:
        raise ValueError(f"unsupported timeframe: {timeframe}")
    return f"{normalized}_GQT_{timeframe.upper()}"


def load_freqtrade_frame(source_dir: str | Path, symbol: str, timeframe: str):
    try:
        import pandas as pd
    except ImportError as exc:
        raise RuntimeError("pandas and pyarrow are required to read Feather data") from exc

    path = Path(source_dir) / freqtrade_filename(symbol, timeframe)
    derived_from_15m = timeframe == "1h" and not path.is_file()
    if derived_from_15m:
        path = Path(source_dir) / freqtrade_filename(symbol, "15m")
    if not path.is_file():
        raise FileNotFoundError(path)
    frame = pd.read_feather(path)
    required = {"date", "open", "high", "low", "close", "volume"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"missing Feather columns: {sorted(missing)}")

    result = frame.loc[:, ["date", "open", "high", "low", "close", "volume"]].copy()
    result["date"] = pd.to_datetime(result["date"], utc=True)
    result = result.sort_values("date", kind="stable").reset_index(drop=True)
    if result.empty:
        raise ValueError("candle file is empty")
    if result["date"].duplicated().any():
        raise ValueError("duplicate candle timestamps detected")
    numeric = ["open", "high", "low", "close", "volume"]
    if result[numeric].isna().any().any():
        raise ValueError("candle data contains null numeric values")
    if (result[["open", "high", "low", "close"]] <= 0).any().any():
        raise ValueError("OHLC prices must be positive")
    if (result["volume"] < 0).any():
        raise ValueError("volume cannot be negative")
    if (result["high"] < result[["open", "close", "low"]].max(axis=1)).any():
        raise ValueError("high price violates OHLC ordering")
    if (result["low"] > result[["open", "close", "high"]].min(axis=1)).any():
        raise ValueError("low price violates OHLC ordering")
    if derived_from_15m:
        result = _resample_complete_hours(result)
    return path, result


def _resample_complete_hours(frame):
    """Aggregate complete groups of four UTC 15-minute bars into 1-hour bars."""
    indexed = frame.set_index("date")
    counts = indexed["close"].resample("1h", label="left", closed="left").count()
    complete = counts[counts == 4]
    if complete.empty:
        raise ValueError("cannot derive 1h data: no complete UTC hours")
    first_complete, last_complete = complete.index[0], complete.index[-1]
    interior = counts.loc[first_complete:last_complete]
    incomplete = interior[interior != 4]
    if not incomplete.empty:
        first = incomplete.index[0].isoformat()
        raise ValueError(f"cannot derive 1h data: incomplete UTC hour at {first}")
    result = indexed.resample("1h", label="left", closed="left").agg(
        {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
        }
    )
    return result.loc[first_complete:last_complete].reset_index()


def import_to_vnpy(source_dir: str | Path, symbol: str, timeframe: str) -> ImportReport:
    try:
        from vnpy.trader.constant import Exchange, Interval
        from vnpy.trader.database import DB_TZ, get_database
        from vnpy.trader.object import BarData
    except ImportError as exc:
        raise RuntimeError("vn.py and vnpy_sqlite must be installed before importing bars") from exc

    path, frame = load_freqtrade_frame(source_dir, symbol, timeframe)
    expected_seconds = {"15m": 900, "1h": 3600, "4h": 14400}[timeframe]
    deltas = frame["date"].diff().dt.total_seconds().dropna()
    missing_intervals = int(((deltas / expected_seconds).round() - 1).clip(lower=0).sum())
    if missing_intervals:
        raise ValueError(f"candle data has {missing_intervals} missing {timeframe} intervals")
    intervals = {
        "15m": Interval.MINUTE,
        "1h": Interval.HOUR,
        "4h": Interval.HOUR,
    }
    bars = []
    for row in frame.itertuples(index=False):
        dt = row.date.to_pydatetime().astimezone(timezone.utc).astimezone(DB_TZ)
        bars.append(
            BarData(
                symbol=storage_symbol(symbol, timeframe),
                exchange=Exchange.GLOBAL,
                datetime=dt,
                interval=intervals[timeframe],
                volume=float(row.volume),
                open_price=float(row.open),
                high_price=float(row.high),
                low_price=float(row.low),
                close_price=float(row.close),
                gateway_name="GQT_FREQTRADE_IMPORT",
            )
        )
    get_database().save_bar_data(bars)

    return ImportReport(
        source=str(path),
        symbol=symbol.upper(),
        timeframe=timeframe,
        bars=len(frame),
        first_timestamp=frame["date"].iloc[0].isoformat(),
        last_timestamp=frame["date"].iloc[-1].isoformat(),
        missing_intervals=missing_intervals,
    )
