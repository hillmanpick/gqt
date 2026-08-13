from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass(frozen=True)
class WalkForwardFold:
    number: int
    train_start: datetime
    train_end: datetime
    test_start: datetime
    test_end: datetime


def build_folds(
    start: datetime,
    end: datetime,
    *,
    train_days: int,
    test_days: int,
    step_days: int,
    embargo_days: int,
) -> list[WalkForwardFold]:
    if end <= start:
        raise ValueError("end must be later than start")
    if min(train_days, test_days, step_days) <= 0 or embargo_days < 0:
        raise ValueError("walk-forward window sizes are invalid")

    train_delta = timedelta(days=train_days)
    test_delta = timedelta(days=test_days)
    step_delta = timedelta(days=step_days)
    embargo_delta = timedelta(days=embargo_days)
    folds: list[WalkForwardFold] = []
    train_start = start

    while True:
        train_end = train_start + train_delta
        test_start = train_end + embargo_delta
        test_end = test_start + test_delta
        if test_end > end:
            break
        folds.append(
            WalkForwardFold(
                number=len(folds) + 1,
                train_start=train_start,
                train_end=train_end,
                test_start=test_start,
                test_end=test_end,
            )
        )
        train_start += step_delta
    return folds


def evaluate_fold(
    statistics: dict[str, float],
    *,
    minimum_trades: int,
    minimum_profit_factor: float,
    minimum_expectancy: float,
    maximum_drawdown: float,
) -> tuple[bool, list[str]]:
    reasons: list[str] = []
    trades = int(statistics.get("total_trade_count", 0) or 0)
    profit_factor = float(statistics.get("profit_factor", 0) or 0)
    expectancy = float(statistics.get("expectancy", 0) or 0)
    drawdown = float(statistics.get("max_drawdown_percent", 100)) / 100.0
    if trades < minimum_trades:
        reasons.append(f"trades {trades} < {minimum_trades}")
    if profit_factor < minimum_profit_factor:
        reasons.append(f"profit factor {profit_factor:.3f} < {minimum_profit_factor:.3f}")
    if expectancy <= minimum_expectancy:
        reasons.append(f"expectancy {expectancy:.6f} <= {minimum_expectancy:.6f}")
    if drawdown > maximum_drawdown:
        reasons.append(f"drawdown {drawdown:.2%} > {maximum_drawdown:.2%}")
    return not reasons, reasons
