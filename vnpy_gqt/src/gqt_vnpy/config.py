from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field, fields
from pathlib import Path
from typing import Any


ALLOWED_SYMBOLS = frozenset({"BTCUSDT", "ETHUSDT"})
ALLOWED_TIMEFRAMES = frozenset({"15m", "1h", "4h"})


@dataclass(frozen=True)
class StrategySettings:
    fast_window: int = 24
    slow_window: int = 96
    atr_window: int = 20
    breakout_window: int = 48
    risk_reward: float = 1.8
    atr_stop_multiple: float = 2.2
    minimum_atr_percent: float = 0.002
    maximum_atr_percent: float = 0.04
    cooldown_bars: int = 4

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "StrategySettings":
        allowed = {item.name for item in fields(cls)}
        unknown = set(value) - allowed
        if unknown:
            raise ValueError(f"unknown strategy settings: {sorted(unknown)}")
        settings = cls(**value)
        if not 2 <= settings.fast_window < settings.slow_window:
            raise ValueError("fast_window must be at least 2 and below slow_window")
        if settings.atr_window < 2 or settings.breakout_window < 2:
            raise ValueError("ATR and breakout windows must be at least 2")
        if settings.risk_reward <= 1.0:
            raise ValueError("risk_reward must be greater than 1")
        if settings.atr_stop_multiple <= 0:
            raise ValueError("atr_stop_multiple must be positive")
        if not 0 < settings.minimum_atr_percent < settings.maximum_atr_percent:
            raise ValueError("ATR percentage bounds are invalid")
        if settings.cooldown_bars < 0:
            raise ValueError("cooldown_bars cannot be negative")
        return settings


@dataclass(frozen=True)
class ResearchConfig:
    mode: str = "research"
    symbols: tuple[str, ...] = ("BTCUSDT", "ETHUSDT")
    timeframe: str = "15m"
    capital: float = 1000.0
    leverage: int = 2
    risk_per_trade: float = 0.005
    max_margin_per_trade: float = 120.0
    fee_rate: float = 0.0005
    slippage_bps: float = 3.0
    funding_rate_8h_stress: float = 0.0001
    contract_size: float = 1.0
    price_tick: float = 0.1
    min_volume: float = 0.001
    warmup_bars: int = 240
    train_days: int = 365
    test_days: int = 90
    step_days: int = 90
    embargo_days: int = 2
    minimum_test_trades: int = 30
    minimum_profit_factor: float = 1.05
    minimum_expectancy: float = 0.0
    maximum_drawdown: float = 0.15
    minimum_sharpe_ratio: float = 0.0
    maximum_cost_ratio: float = 0.60
    maximum_consecutive_losses: int = 12
    selection_min_train_trades: int = 30
    selection_min_profit_factor: float = 0.90
    selection_max_drawdown: float = 0.25
    strategy: StrategySettings = field(default_factory=StrategySettings)
    candidate_strategies: tuple[StrategySettings, ...] = ()

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "ResearchConfig":
        value = dict(raw)
        value["symbols"] = tuple(
            str(item).upper()
            for item in value.get("symbols", ("BTCUSDT", "ETHUSDT"))
        )
        value["strategy"] = StrategySettings.from_dict(value.get("strategy", {}))
        candidate_values = value.pop("candidate_strategies", ())
        if not isinstance(candidate_values, (list, tuple)):
            raise ValueError("candidate_strategies must be a list")
        base = asdict(value["strategy"])
        if any(not isinstance(candidate, dict) for candidate in candidate_values):
            raise ValueError("each candidate strategy must be an object")
        value["candidate_strategies"] = tuple(
            StrategySettings.from_dict({**base, **candidate})
            for candidate in candidate_values
        )
        config = cls(**value)
        config.validate()
        return config

    @classmethod
    def load(cls, path: str | Path) -> "ResearchConfig":
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("research config root must be an object")
        return cls.from_dict(raw)

    def validate(self) -> None:
        if self.mode != "research":
            raise ValueError("only mode=research is supported; live trading is disabled")
        if not self.symbols or len(set(self.symbols)) != len(self.symbols):
            raise ValueError("symbols must be non-empty and unique")
        unsupported = set(self.symbols) - ALLOWED_SYMBOLS
        if unsupported:
            raise ValueError(f"unsupported symbols: {sorted(unsupported)}")
        if self.timeframe not in ALLOWED_TIMEFRAMES:
            raise ValueError(f"unsupported timeframe: {self.timeframe}")
        if self.capital <= 0:
            raise ValueError("capital must be positive")
        if not 1 <= self.leverage <= 3:
            raise ValueError("research leverage must be between 1 and 3")
        if not 0 < self.risk_per_trade <= 0.01:
            raise ValueError("risk_per_trade must be in (0, 0.01]")
        if not 0 < self.max_margin_per_trade <= self.capital * 0.25:
            raise ValueError("max_margin_per_trade must be positive and at most 25% of capital")
        if not 0 <= self.fee_rate <= 0.002:
            raise ValueError("fee_rate is outside the research safety range")
        if not 0 <= self.slippage_bps <= 20:
            raise ValueError("slippage_bps is outside the research safety range")
        if not 0 <= self.funding_rate_8h_stress <= 0.001:
            raise ValueError("funding_rate_8h_stress is outside the research safety range")
        if self.contract_size <= 0 or self.price_tick <= 0 or self.min_volume <= 0:
            raise ValueError("contract size, price tick, and minimum volume must be positive")
        if self.warmup_bars < self.strategy.slow_window + 2:
            raise ValueError("warmup_bars must exceed the slow strategy window")
        if min(self.train_days, self.test_days, self.step_days) <= 0:
            raise ValueError("walk-forward windows must be positive")
        if self.embargo_days < 0 or self.embargo_days >= self.test_days:
            raise ValueError("embargo_days must be non-negative and below test_days")
        if self.minimum_test_trades < 1:
            raise ValueError("minimum_test_trades must be positive")
        if self.minimum_profit_factor < 1:
            raise ValueError("minimum_profit_factor must be at least 1")
        if not 0 < self.maximum_drawdown < 1:
            raise ValueError("maximum_drawdown must be between 0 and 1")
        if self.minimum_sharpe_ratio < -5:
            raise ValueError("minimum_sharpe_ratio is outside the research range")
        if not 0 < self.maximum_cost_ratio <= 2:
            raise ValueError("maximum_cost_ratio must be in (0, 2]")
        if self.maximum_consecutive_losses < 1:
            raise ValueError("maximum_consecutive_losses must be positive")
        if self.selection_min_train_trades < 1:
            raise ValueError("selection_min_train_trades must be positive")
        if self.selection_min_profit_factor <= 0:
            raise ValueError("selection_min_profit_factor must be positive")
        if not 0 < self.selection_max_drawdown < 1:
            raise ValueError("selection_max_drawdown must be between 0 and 1")
        if len(self.candidate_strategies) > 12:
            raise ValueError("at most 12 candidate strategies are allowed")
        for candidate in self.candidate_strategies:
            if candidate.slow_window + 2 > self.warmup_bars:
                raise ValueError("warmup_bars must exceed every candidate slow window")
