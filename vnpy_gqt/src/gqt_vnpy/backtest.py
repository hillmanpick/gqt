from __future__ import annotations

import json
import math
import statistics as stats
from dataclasses import asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from .config import ResearchConfig
from .data_import import storage_symbol
from .walkforward import build_folds, evaluate_fold


def _vnpy_runtime():
    try:
        from vnpy.trader.constant import Exchange, Interval
    except ImportError as exc:
        raise RuntimeError("vnpy and vnpy_ctastrategy must be installed to run backtests") from exc
    from .engine import ConservativeBarBacktestingEngine
    from .strategy import GqtUsdtTrendStrategy

    return Exchange, Interval, ConservativeBarBacktestingEngine, GqtUsdtTrendStrategy


def _strategy_settings(config: ResearchConfig) -> dict[str, Any]:
    interval_value = {"15m": "1m", "1h": "1h", "4h": "1h"}[config.timeframe]
    return {
        **asdict(config.strategy),
        "starting_capital": config.capital,
        "leverage": config.leverage,
        "risk_per_trade": config.risk_per_trade,
        "max_margin_per_trade": config.max_margin_per_trade,
        "fee_rate": config.fee_rate,
        "slippage_bps": config.slippage_bps,
        "min_volume": config.min_volume,
        "contract_size": config.contract_size,
        "warmup_days": _warmup_days(config),
        "data_interval": interval_value,
    }


def _warmup_days(config: ResearchConfig) -> int:
    minutes = {"15m": 15, "1h": 60, "4h": 240}[config.timeframe]
    return max(2, math.ceil(config.warmup_bars * minutes / 1440) + 1)


def _database_bounds(start: datetime, end: datetime, timeframe: str) -> tuple[datetime, datetime]:
    """Convert right-open UTC research dates to vn.py's naive DB wall clock."""
    if start.tzinfo is None or end.tzinfo is None:
        raise ValueError("backtest dates must be timezone-aware")
    try:
        from vnpy.trader.database import DB_TZ
    except ImportError as exc:
        raise RuntimeError("vn.py is required to resolve database time boundaries") from exc

    interval = {"15m": timedelta(minutes=15), "1h": timedelta(hours=1), "4h": timedelta(hours=4)}[
        timeframe
    ]
    query_start = start.astimezone(DB_TZ).replace(tzinfo=None)
    query_end = end.astimezone(DB_TZ).replace(tzinfo=None) - interval
    if query_end < query_start:
        raise ValueError("backtest range is shorter than one candle")
    return query_start, query_end


def _run_period(config: ResearchConfig, symbol: str, start: datetime, end: datetime) -> dict[str, Any]:
    if end <= start:
        raise ValueError("backtest end must be later than start")
    Exchange, Interval, BacktestingEngine, strategy_class = _vnpy_runtime()
    interval = {"15m": Interval.MINUTE, "1h": Interval.HOUR, "4h": Interval.HOUR}[config.timeframe]
    engine = BacktestingEngine()
    query_start, query_end = _database_bounds(start, end, config.timeframe)
    engine.set_parameters(
        vt_symbol=f"{storage_symbol(symbol, config.timeframe)}.{Exchange.GLOBAL.value}",
        interval=interval,
        start=query_start,
        end=query_end,
        rate=config.fee_rate,
        slippage=0.0,
        size=config.contract_size,
        pricetick=config.price_tick,
        capital=config.capital,
    )
    # vn.py expands end to 23:59:59. Restore the exact right-open fold bound.
    engine.end = query_end
    engine.add_strategy(strategy_class, _strategy_settings(config))
    engine.load_data()
    evaluation_bars = list(engine.history_data)
    if not evaluation_bars:
        raise ValueError(f"no {symbol} bars found for {start.date()} to {end.date()}")
    reference_price = stats.median(float(bar.close_price) for bar in evaluation_bars)
    slippage_price = reference_price * config.slippage_bps / 10_000.0
    engine.slippage = slippage_price
    engine.run_backtesting()
    daily = engine.calculate_result()
    if daily is None or daily.empty:
        built_in: dict[str, Any] = {}
    else:
        evaluation_daily = daily.copy()
        built_in = engine.calculate_statistics(df=evaluation_daily, output=False)
    built_in_drawdown = abs(float(built_in.get("max_ddpercent", 0.0) or 0.0))
    final_bar = evaluation_bars[-1]
    completed = _closed_trade_statistics(
        engine.get_all_trades(),
        start=start,
        capital=config.capital,
        contract_size=config.contract_size,
        fee_rate=config.fee_rate,
        slippage_price=slippage_price,
        funding_rate_8h_stress=config.funding_rate_8h_stress,
        final_price=float(final_bar.close_price),
        final_datetime=final_bar.datetime,
    )
    completed["max_drawdown_percent"] = max(
        float(completed["max_drawdown_percent"]),
        built_in_drawdown,
    )
    return {
        **{f"vnpy_{key}": _json_value(value) for key, value in built_in.items()},
        **completed,
        "reference_price": reference_price,
        "assumed_slippage_price": slippage_price,
        "slippage_bps": config.slippage_bps,
        "funding_rate_8h_stress": config.funding_rate_8h_stress,
    }


def _closed_trade_statistics(
    trades,
    *,
    start: datetime,
    capital: float,
    contract_size: float,
    fee_rate: float,
    slippage_price: float,
    funding_rate_8h_stress: float,
    final_price: float,
    final_datetime: datetime,
) -> dict[str, Any]:
    try:
        from vnpy.trader.constant import Direction, Offset
    except ImportError as exc:
        raise RuntimeError("vn.py is required to evaluate backtest trades") from exc

    entry = None
    outcomes: list[float] = []
    total_commission = 0.0
    total_slippage = 0.0
    total_funding_stress = 0.0
    forced_exit_count = 0

    def settle(open_trade, close_price: float, close_datetime: datetime) -> None:
        nonlocal total_commission, total_slippage, total_funding_stress
        volume = float(open_trade.volume)
        direction = 1.0 if open_trade.direction == Direction.LONG else -1.0
        gross = (close_price - float(open_trade.price)) * volume * contract_size * direction
        turnover = (float(open_trade.price) + close_price) * volume * contract_size
        commission = turnover * fee_rate
        slippage = 2.0 * volume * contract_size * slippage_price
        held_hours = max(0.0, (close_datetime - open_trade.datetime).total_seconds() / 3600.0)
        funding_stress = (
            float(open_trade.price)
            * volume
            * contract_size
            * funding_rate_8h_stress
            * held_hours
            / 8.0
        )
        outcomes.append(gross - commission - slippage - funding_stress)
        total_commission += commission
        total_slippage += slippage
        total_funding_stress += funding_stress

    for trade in sorted(trades, key=lambda item: item.datetime):
        if trade.datetime < start:
            continue
        if trade.offset == Offset.OPEN:
            if entry is not None:
                raise RuntimeError("pyramiding is not supported by the research evaluator")
            entry = trade
            continue
        if entry is None:
            continue

        if float(trade.volume) != float(entry.volume):
            raise RuntimeError("partial fills are not supported by the research evaluator")
        settle(entry, float(trade.price), trade.datetime)
        entry = None

    if entry is not None:
        settle(entry, final_price, final_datetime)
        forced_exit_count = 1
        entry = None

    positive = sum(value for value in outcomes if value > 0)
    negative = abs(sum(value for value in outcomes if value < 0))
    profit_factor_capped = negative == 0 and positive > 0
    profit_factor = positive / negative if negative > 0 else (999.0 if positive > 0 else 0.0)
    equity = capital
    peak = capital
    maximum_drawdown = 0.0
    for value in outcomes:
        equity += value
        peak = max(peak, equity)
        if peak > 0:
            maximum_drawdown = max(maximum_drawdown, (peak - equity) / peak)
    return {
        "total_trade_count": len(outcomes),
        "winning_trade_count": sum(value > 0 for value in outcomes),
        "losing_trade_count": sum(value < 0 for value in outcomes),
        "win_rate": sum(value > 0 for value in outcomes) / len(outcomes) if outcomes else 0.0,
        "profit_factor": _json_value(profit_factor),
        "profit_factor_capped": profit_factor_capped,
        "expectancy": sum(outcomes) / len(outcomes) if outcomes else 0.0,
        "cost_adjusted_total_net_pnl": sum(outcomes),
        "max_drawdown_percent": maximum_drawdown * 100.0,
        "total_commission": total_commission,
        "total_slippage": total_slippage,
        "total_funding_stress": total_funding_stress,
        "forced_exit_count": forced_exit_count,
        "unclosed_position_count": 0,
    }


def _json_value(value: Any) -> Any:
    if hasattr(value, "item"):
        value = value.item()
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, float) and (value != value or value in (float("inf"), float("-inf"))):
        return None
    return value


def run_research(config: ResearchConfig, start: datetime, end: datetime) -> dict[str, Any]:
    folds = build_folds(
        start,
        end,
        train_days=config.train_days,
        test_days=config.test_days,
        step_days=config.step_days,
        embargo_days=config.embargo_days,
    )
    if not folds:
        raise ValueError("date range is too short to create one walk-forward fold")

    symbols: dict[str, Any] = {}
    for symbol in config.symbols:
        fold_results = []
        for fold in folds:
            statistics = _run_period(config, symbol, fold.test_start, fold.test_end)
            passed, reasons = evaluate_fold(
                statistics,
                minimum_trades=config.minimum_test_trades,
                minimum_profit_factor=config.minimum_profit_factor,
                minimum_expectancy=config.minimum_expectancy,
                maximum_drawdown=config.maximum_drawdown,
            )
            fold_results.append(
                {
                    **asdict(fold),
                    "passed": passed,
                    "reasons": reasons,
                    "statistics": statistics,
                }
            )
        symbols[symbol] = {
            "eligible": bool(fold_results) and all(item["passed"] for item in fold_results),
            "folds": fold_results,
        }

    return {
        "mode": config.mode,
        "timeframe": config.timeframe,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "portfolio_eligible": all(item["eligible"] for item in symbols.values()),
        "symbols": symbols,
    }


def write_result(result: dict[str, Any], output_dir: str | Path) -> Path:
    directory = Path(output_dir)
    directory.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    path = directory / f"walk-forward-{stamp}.json"
    path.write_text(json.dumps(result, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    return path
