from __future__ import annotations

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class PositionSizing:
    volume: float
    margin: float
    notional: float
    cash_risk: float
    round_trip_cost: float


def floor_to_step(value: float, step: float) -> float:
    if value <= 0 or step <= 0:
        return 0.0
    return math.floor((value + 1e-12) / step) * step


def size_position(
    *,
    equity: float,
    entry_price: float,
    stop_price: float,
    leverage: int,
    risk_fraction: float,
    max_margin: float,
    fee_rate: float,
    slippage_bps: float,
    min_volume: float,
) -> PositionSizing:
    """Size from worst-case cash loss, including two-sided trading costs."""
    if equity <= 0 or entry_price <= 0 or leverage < 1:
        raise ValueError("equity, entry price, and leverage must be positive")
    if not 0 < risk_fraction <= 0.01:
        raise ValueError("risk_fraction must be in (0, 0.01]")
    if max_margin <= 0 or fee_rate < 0 or slippage_bps < 0:
        raise ValueError("margin and cost assumptions are invalid")

    stop_distance = abs(entry_price - stop_price)
    if stop_distance <= 0:
        raise ValueError("stop price must differ from entry price")

    one_way_cost_rate = fee_rate + slippage_bps / 10_000.0
    unit_round_trip_cost = entry_price * 2.0 * one_way_cost_rate
    unit_worst_loss = stop_distance + unit_round_trip_cost
    risk_budget = equity * risk_fraction

    risk_volume = risk_budget / unit_worst_loss
    margin_volume = max_margin * leverage / entry_price
    volume = floor_to_step(min(risk_volume, margin_volume), min_volume)
    if volume < min_volume:
        return PositionSizing(0.0, 0.0, 0.0, 0.0, 0.0)

    notional = volume * entry_price
    margin = notional / leverage
    round_trip_cost = volume * unit_round_trip_cost
    cash_risk = volume * stop_distance + round_trip_cost
    return PositionSizing(volume, margin, notional, cash_risk, round_trip_cost)
