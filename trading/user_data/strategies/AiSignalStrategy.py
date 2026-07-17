import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import talib.abstract as ta
from pandas import DataFrame, Series

from freqtrade.strategy import IStrategy


class AiSignalStrategy(IStrategy):
    """Executes AI signals only when the local multi-factor gate agrees."""

    INTERFACE_VERSION = 3
    can_short = True
    timeframe = "4h"
    process_only_new_candles = True
    startup_candle_count = 180
    position_adjustment_enable = False

    minimal_roi = {"0": 100.0}
    stoploss = -0.10
    use_exit_signal = True
    exit_profit_only = False

    _user_data = Path("/freqtrade/user_data")

    def _read_json(self, name: str) -> dict:
        try:
            value = json.loads((self._user_data / name).read_text(encoding="utf-8"))
            return value if isinstance(value, dict) else {}
        except (OSError, ValueError, TypeError):
            return {}

    def _config(self) -> dict:
        return self._read_json("ai_config.json")

    @staticmethod
    def _float(value, fallback: float) -> float:
        try:
            parsed = float(value)
            return parsed if parsed == parsed else fallback
        except (TypeError, ValueError):
            return fallback

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

    def _signal(self, pair: str) -> dict | None:
        signal = self._read_json("ai_signals.json").get(pair)
        return signal if isinstance(signal, dict) else None

    def _valid_signal(self, pair: str) -> dict | None:
        config = self._config()
        signal = self._signal(pair)
        if not config.get("enabled", False) or signal is None:
            return None
        symbol = pair.replace("/", "").replace(":USDT", "")
        if symbol not in config.get("symbol_whitelist", []):
            return None
        if signal.get("symbol") != symbol:
            return None
        if signal.get("timeframe") != config.get("timeframe"):
            return None
        if float(signal.get("confidence", 0)) < float(config.get("minimum_confidence", 1)):
            return None
        now = int(datetime.now(timezone.utc).timestamp())
        if int(signal.get("valid_until", 0)) <= now:
            return None
        if signal.get("action") not in {"long", "short", "close"}:
            return None
        return signal

    def populate_indicators(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        close = dataframe["close"]

        dataframe["mom_6"] = close.pct_change(6)
        dataframe["mom_42"] = close.pct_change(42)
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

    @staticmethod
    def _threshold(config: dict, name: str, fallback: float) -> float:
        try:
            value = float(config.get(name, fallback))
            return value if np.isfinite(value) else fallback
        except (TypeError, ValueError):
            return fallback

    def _factor_allows(self, dataframe: DataFrame, side: str) -> bool:
        if dataframe.empty:
            return False
        config = self._config()
        minimum_factor_score = self._threshold(config, "minimum_factor_score", 0.25)
        minimum_trend_quality = self._threshold(config, "minimum_trend_quality", 0.52)
        minimum_adx = self._threshold(config, "minimum_adx", 18.0)
        minimum_volume_ratio = self._threshold(config, "minimum_volume_ratio", 0.0)
        row = dataframe.iloc[-1]
        if side == "long":
            minimum_long_score = self._threshold(config, "minimum_long_score", 0.68)
            return bool(
                row.get("long_score", 0) >= minimum_long_score
                and row.get("factor_score", 0) >= minimum_factor_score
                and row.get("trend_quality", 0) >= minimum_trend_quality
                and row.get("adx", 0) >= minimum_adx
                and 46.0 <= row.get("rsi", 0) <= 76.0
                and row.get("volume_ratio", -1) >= minimum_volume_ratio
            )
        minimum_short_score = self._threshold(config, "minimum_short_score", 0.68)
        return bool(
            row.get("short_score", 0) >= minimum_short_score
            and row.get("factor_score", 0) <= -minimum_factor_score
            and row.get("trend_quality", 0) >= minimum_trend_quality
            and row.get("adx", 0) >= minimum_adx
            and 24.0 <= row.get("rsi", 100) <= 54.0
            and row.get("volume_ratio", -1) >= minimum_volume_ratio
        )

    def populate_entry_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        dataframe["enter_long"] = 0
        dataframe["enter_short"] = 0
        signal = self._valid_signal(metadata["pair"])
        if signal is None or dataframe.empty:
            return dataframe
        tag = f'ai:{signal["decision_id"]}'
        if signal["action"] == "long" and self._factor_allows(dataframe, "long"):
            dataframe.loc[dataframe.index[-1], ["enter_long", "enter_tag"]] = (1, tag)
        elif signal["action"] == "short" and self._factor_allows(dataframe, "short"):
            dataframe.loc[dataframe.index[-1], ["enter_short", "enter_tag"]] = (1, tag)
        return dataframe

    def populate_exit_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        dataframe["exit_long"] = 0
        dataframe["exit_short"] = 0
        signal = self._valid_signal(metadata["pair"])
        if signal is not None and signal["action"] == "close" and not dataframe.empty:
            dataframe.loc[dataframe.index[-1], ["exit_long", "exit_tag"]] = (1, "ai_close")
            dataframe.loc[dataframe.index[-1], ["exit_short", "exit_tag"]] = (1, "ai_close")

        if not dataframe.empty:
            last_index = dataframe.index[-1]
            row = dataframe.iloc[-1]
            if (
                row.get("factor_score", 0) < -0.10
                or row.get("short_score", 0) > 0.60
                or row.get("close", 0) < row.get("ema_mid", 0)
                or row.get("rsi", 0) > 82
            ):
                dataframe.loc[last_index, ["exit_long", "exit_tag"]] = (1, "factor_flip_long_exit")
            if (
                row.get("factor_score", 0) > 0.10
                or row.get("long_score", 0) > 0.60
                or row.get("close", 0) > row.get("ema_mid", 0)
                or row.get("rsi", 100) < 18
            ):
                dataframe.loc[last_index, ["exit_short", "exit_tag"]] = (1, "factor_flip_short_exit")
        return dataframe

    def confirm_trade_entry(
        self, pair: str, order_type: str, amount: float, rate: float,
        time_in_force: str, current_time: datetime, entry_tag: str | None,
        side: str, **kwargs,
    ) -> bool:
        signal = self._valid_signal(pair)
        return signal is not None and entry_tag == f'ai:{signal["decision_id"]}'

    def custom_stake_amount(
        self, pair: str, current_time: datetime, current_rate: float,
        proposed_stake: float, min_stake: float | None, max_stake: float,
        leverage: float, entry_tag: str | None, side: str, **kwargs,
    ) -> float:
        config = self._config()
        configured_cap = self._float(config.get("max_stake_amount"), proposed_stake)
        usage_cap = max_stake * self._float(config.get("capital_usage_percent"), 10.0) / 100.0
        stake = min(configured_cap, usage_cap, max_stake)
        signal = self._valid_signal(pair)
        if config.get("allow_ai_risk_sizing", False) and signal is not None:
            stake = min(stake, self._float(signal.get("stake_amount"), stake))
        if min_stake is not None:
            stake = max(stake, min_stake)
        return max(0.0, min(stake, max_stake))

    def custom_exit(
        self, pair: str, trade, current_time: datetime, current_rate: float,
        current_profit: float, **kwargs,
    ) -> str | None:
        signal = self._signal(pair)
        stop_loss = self._float(
            signal.get("stop_loss_percent") if signal else None,
            1.5,
        ) / 100.0
        reward = stop_loss * self._float(self._config().get("risk_reward_ratio"), 2.0)
        if current_profit >= reward:
            return "ai_risk_reward_take_profit"
        if current_profit <= -stop_loss:
            return "ai_risk_stop_loss"
        return None

    def leverage(
        self, pair: str, current_time: datetime, current_rate: float,
        proposed_leverage: float, max_leverage: float, entry_tag: str | None,
        side: str, **kwargs,
    ) -> float:
        requested = float(self._config().get("leverage", 1))
        return max(1.0, min(requested, max_leverage, 125.0))
