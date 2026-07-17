import json
from datetime import datetime
from pathlib import Path

import numpy as np
import talib.abstract as ta
from pandas import DataFrame, Series

from freqtrade.strategy import IStrategy


class FuturesFactorStrategy(IStrategy):
    """Multi-factor Binance futures baseline for research and dry-run."""

    INTERFACE_VERSION = 3

    can_short = True
    timeframe = "4h"
    process_only_new_candles = True
    startup_candle_count = 180
    position_adjustment_enable = False
    _user_data = Path("/freqtrade/user_data")

    minimal_roi = {
        "0": 0.10,
        "360": 0.05,
        "1440": 0.0,
    }
    stoploss = -0.08

    trailing_stop = True
    trailing_stop_positive = 0.025
    trailing_stop_positive_offset = 0.055
    trailing_only_offset_is_reached = True

    use_exit_signal = True
    exit_profit_only = False
    ignore_roi_if_entry_signal = False

    @property
    def protections(self) -> list[dict]:
        return [
            {"method": "CooldownPeriod", "stop_duration_candles": 2},
            {
                "method": "StoplossGuard",
                "lookback_period_candles": 36,
                "trade_limit": 3,
                "stop_duration_candles": 18,
                "only_per_pair": False,
            },
            {
                "method": "MaxDrawdown",
                "lookback_period_candles": 72,
                "trade_limit": 8,
                "stop_duration_candles": 24,
                "max_allowed_drawdown": 0.12,
            },
        ]

    @staticmethod
    def bounded(series: Series, low: float, high: float) -> Series:
        if high <= low:
            return series * 0.0
        return ((series - low) / (high - low)).clip(lower=0.0, upper=1.0).fillna(0.0)

    @staticmethod
    def center_score(series: Series, center: float, half_width: float) -> Series:
        if half_width <= 0:
            return series * 0.0
        return (1.0 - (series - center).abs() / half_width).clip(lower=0.0, upper=1.0).fillna(0.0)

    def _config(self) -> dict:
        try:
            value = json.loads((self._user_data / "ai_config.json").read_text(encoding="utf-8"))
            return value if isinstance(value, dict) else {}
        except (OSError, ValueError, TypeError):
            return {}

    @staticmethod
    def _threshold(config: dict, name: str, fallback: float) -> float:
        try:
            value = float(config.get(name, fallback))
            return value if np.isfinite(value) else fallback
        except (TypeError, ValueError):
            return fallback

    @staticmethod
    def rolling_zscore(series: Series, window: int) -> Series:
        mean = series.rolling(window, min_periods=max(20, window // 3)).mean()
        std = series.rolling(window, min_periods=max(20, window // 3)).std().replace(0, np.nan)
        return ((series - mean) / std).clip(lower=-4.0, upper=4.0).fillna(0.0)

    @staticmethod
    def weighted_score(parts: list[tuple[Series, float]]) -> Series:
        total_weight = sum(weight for _, weight in parts)
        if total_weight <= 0:
            return parts[0][0] * 0.0
        score = sum(part.clip(lower=0.0, upper=1.0).fillna(0.0) * weight for part, weight in parts)
        return (score / total_weight).clip(lower=0.0, upper=1.0).fillna(0.0)

    def populate_indicators(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        close = dataframe["close"]

        dataframe["mom_6"] = close.pct_change(6)
        dataframe["mom_42"] = close.pct_change(42)
        dataframe["ret_1"] = np.log(close / close.shift(1))
        dataframe["realized_vol"] = dataframe["ret_1"].rolling(20, min_periods=20).std()

        dataframe["ema_fast"] = ta.EMA(dataframe, timeperiod=20)
        dataframe["ema_mid"] = ta.EMA(dataframe, timeperiod=50)
        dataframe["ema_slow"] = ta.EMA(dataframe, timeperiod=100)
        dataframe["trend"] = dataframe["ema_fast"] / dataframe["ema_mid"] - 1.0

        dataframe["adx"] = ta.ADX(dataframe, timeperiod=14)
        dataframe["rsi"] = ta.RSI(dataframe, timeperiod=14)
        macd = ta.MACD(dataframe, fastperiod=12, slowperiod=26, signalperiod=9)
        dataframe["macd_hist"] = macd["macdhist"]

        dataframe["atr_pct"] = ta.ATR(dataframe, timeperiod=14) / close
        dataframe["volume_ratio"] = dataframe["volume"] / dataframe["volume"].rolling(42).mean() - 1.0

        dataframe["donchian_high"] = dataframe["high"].rolling(55, min_periods=55).max().shift(1)
        dataframe["donchian_low"] = dataframe["low"].rolling(55, min_periods=55).min().shift(1)
        donchian_range = (dataframe["donchian_high"] - dataframe["donchian_low"]).replace(0, np.nan)
        dataframe["breakout_position"] = (
            (close - (dataframe["donchian_high"] + dataframe["donchian_low"]) * 0.5)
            / donchian_range
            * 2.0
        ).clip(lower=-1.5, upper=1.5)
        candle_range = (dataframe["high"] - dataframe["low"]).replace(0, np.nan)
        dataframe["close_location"] = ((close - dataframe["low"]) / candle_range * 2.0 - 1.0).clip(
            lower=-1.0,
            upper=1.0,
        )

        trend_scale = dataframe["atr_pct"].clip(lower=0.003) * 4.0
        trend_long = self.weighted_score(
            [
                ((close > dataframe["ema_fast"]).astype(float), 0.18),
                ((dataframe["ema_fast"] > dataframe["ema_mid"]).astype(float), 0.24),
                ((dataframe["ema_mid"] > dataframe["ema_slow"]).astype(float), 0.18),
                (self.bounded(dataframe["adx"], 16.0, 35.0), 0.25),
                ((dataframe["trend"] / trend_scale).clip(lower=0.0, upper=1.0), 0.15),
            ]
        )
        trend_short = self.weighted_score(
            [
                ((close < dataframe["ema_fast"]).astype(float), 0.18),
                ((dataframe["ema_fast"] < dataframe["ema_mid"]).astype(float), 0.24),
                ((dataframe["ema_mid"] < dataframe["ema_slow"]).astype(float), 0.18),
                (self.bounded(dataframe["adx"], 16.0, 35.0), 0.25),
                ((-dataframe["trend"] / trend_scale).clip(lower=0.0, upper=1.0), 0.15),
            ]
        )

        macd_zscore = self.rolling_zscore(dataframe["macd_hist"], 126)
        momentum_long = self.weighted_score(
            [
                (self.bounded(dataframe["mom_6"], 0.0, 0.018), 0.30),
                (self.bounded(dataframe["mom_42"], 0.0, 0.060), 0.30),
                (self.bounded(macd_zscore, 0.0, 2.0), 0.25),
                ((dataframe["macd_hist"] > 0).astype(float), 0.15),
            ]
        )
        momentum_short = self.weighted_score(
            [
                (self.bounded(-dataframe["mom_6"], 0.0, 0.018), 0.30),
                (self.bounded(-dataframe["mom_42"], 0.0, 0.060), 0.30),
                (self.bounded(-macd_zscore, 0.0, 2.0), 0.25),
                ((dataframe["macd_hist"] < 0).astype(float), 0.15),
            ]
        )

        dataframe["rsi_long"] = self.center_score(dataframe["rsi"], 60.0, 18.0) * self.bounded(
            dataframe["rsi"],
            46.0,
            53.0,
        )
        dataframe["rsi_short"] = self.center_score(dataframe["rsi"], 40.0, 18.0) * self.bounded(
            54.0 - dataframe["rsi"],
            0.0,
            8.0,
        )
        dataframe["volume_confirmation"] = self.bounded(dataframe["volume_ratio"], -0.10, 0.85)

        dataframe["volatility_quality"] = 0.0
        low_vol = (dataframe["atr_pct"] >= 0.0004) & (dataframe["atr_pct"] < 0.0010)
        good_vol = (dataframe["atr_pct"] >= 0.0010) & (dataframe["atr_pct"] <= 0.0350)
        high_vol = (dataframe["atr_pct"] > 0.0350) & (dataframe["atr_pct"] <= 0.0900)
        dataframe.loc[low_vol, "volatility_quality"] = self.bounded(
            dataframe.loc[low_vol, "atr_pct"],
            0.0004,
            0.0010,
        )
        dataframe.loc[good_vol, "volatility_quality"] = 1.0
        dataframe.loc[high_vol, "volatility_quality"] = (
            1.0 - self.bounded(dataframe.loc[high_vol, "atr_pct"], 0.0350, 0.0900) * 0.85
        )

        dataframe["long_score"] = self.weighted_score(
            [
                (trend_long, 0.25),
                (momentum_long, 0.20),
                (dataframe["rsi_long"], 0.14),
                (self.bounded(dataframe["breakout_position"], 0.20, 0.85), 0.16),
                (dataframe["volume_confirmation"], 0.12),
                (dataframe["volatility_quality"], 0.08),
                (self.bounded(dataframe["close_location"], 0.10, 0.85), 0.05),
            ]
        )
        dataframe["short_score"] = self.weighted_score(
            [
                (trend_short, 0.25),
                (momentum_short, 0.20),
                (dataframe["rsi_short"], 0.14),
                (self.bounded(-dataframe["breakout_position"], 0.20, 0.85), 0.16),
                (dataframe["volume_confirmation"], 0.12),
                (dataframe["volatility_quality"], 0.08),
                (self.bounded(-dataframe["close_location"], 0.10, 0.85), 0.05),
            ]
        )
        dataframe["factor_score"] = dataframe["long_score"] - dataframe["short_score"]
        dataframe["trend_quality"] = np.maximum(trend_long, trend_short)

        return dataframe.replace([np.inf, -np.inf], np.nan)

    def populate_entry_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        config = self._config()
        minimum_long_score = self._threshold(config, "minimum_long_score", 0.68)
        minimum_short_score = self._threshold(config, "minimum_short_score", 0.68)
        minimum_factor_score = self._threshold(config, "minimum_factor_score", 0.25)
        minimum_trend_quality = self._threshold(config, "minimum_trend_quality", 0.52)
        minimum_adx = self._threshold(config, "minimum_adx", 18.0)
        minimum_volume_ratio = self._threshold(config, "minimum_volume_ratio", 0.0)
        liquid_market = (
            (dataframe["volume"] > 0)
            & (dataframe["atr_pct"] >= 0.0004)
            & (dataframe["atr_pct"] <= 0.0900)
            & (dataframe["adx"] >= minimum_adx)
            & (dataframe["volume_ratio"] >= minimum_volume_ratio)
        )

        dataframe.loc[
            liquid_market
            & (dataframe["long_score"] >= minimum_long_score)
            & (dataframe["factor_score"] >= minimum_factor_score)
            & (dataframe["trend_quality"] >= minimum_trend_quality)
            & (dataframe["rsi"].between(46.0, 76.0))
            & (dataframe["close"] > dataframe["ema_fast"]),
            ["enter_long", "enter_tag"],
        ] = (1, "multi_factor_long")

        dataframe.loc[
            liquid_market
            & (dataframe["short_score"] >= minimum_short_score)
            & (dataframe["factor_score"] <= -minimum_factor_score)
            & (dataframe["trend_quality"] >= minimum_trend_quality)
            & (dataframe["rsi"].between(24.0, 54.0))
            & (dataframe["close"] < dataframe["ema_fast"]),
            ["enter_short", "enter_tag"],
        ] = (1, "multi_factor_short")

        return dataframe

    def populate_exit_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        dataframe.loc[
            (dataframe["volume"] > 0)
            & (
                (dataframe["factor_score"] < -0.10)
                | (dataframe["short_score"] > 0.60)
                | (dataframe["close"] < dataframe["ema_mid"])
                | (dataframe["rsi"] > 82)
            ),
            ["exit_long", "exit_tag"],
        ] = (1, "multi_factor_long_exit")

        dataframe.loc[
            (dataframe["volume"] > 0)
            & (
                (dataframe["factor_score"] > 0.10)
                | (dataframe["long_score"] > 0.60)
                | (dataframe["close"] > dataframe["ema_mid"])
                | (dataframe["rsi"] < 18)
            ),
            ["exit_short", "exit_tag"],
        ] = (1, "multi_factor_short_exit")

        return dataframe

    def leverage(
        self,
        pair: str,
        current_time: datetime,
        current_rate: float,
        proposed_leverage: float,
        max_leverage: float,
        entry_tag: str | None,
        side: str,
        **kwargs,
    ) -> float:
        return min(2.0, max_leverage)
