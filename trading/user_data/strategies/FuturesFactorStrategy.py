from datetime import datetime

import numpy as np
import talib.abstract as ta
from pandas import DataFrame, Series

from freqtrade.strategy import IStrategy


class FuturesFactorStrategy(IStrategy):
    """A conservative 4h USDT-margined futures baseline for research and dry-run."""

    INTERFACE_VERSION = 3

    can_short = True
    timeframe = "4h"
    process_only_new_candles = True
    startup_candle_count = 400
    position_adjustment_enable = False

    # Profit and stop values include leverage. At 2x, -8% is roughly a 4% adverse move.
    minimal_roi = {
        "0": 0.08,
        "720": 0.04,
        "1440": 0.0,
    }
    stoploss = -0.08

    trailing_stop = True
    trailing_stop_positive = 0.03
    trailing_stop_positive_offset = 0.06
    trailing_only_offset_is_reached = True

    use_exit_signal = True
    exit_profit_only = False
    ignore_roi_if_entry_signal = False

    @property
    def protections(self) -> list[dict]:
        return [
            {"method": "CooldownPeriod", "stop_duration_candles": 1},
            {
                "method": "StoplossGuard",
                "lookback_period_candles": 24,
                "trade_limit": 3,
                "stop_duration_candles": 12,
                "only_per_pair": False,
            },
            {
                "method": "MaxDrawdown",
                "lookback_period_candles": 42,
                "trade_limit": 8,
                "stop_duration_candles": 12,
                "max_allowed_drawdown": 0.15,
            },
        ]

    @staticmethod
    def rolling_zscore(series: Series, window: int) -> Series:
        mean = series.rolling(window, min_periods=window).mean()
        std = series.rolling(window, min_periods=window).std().replace(0, np.nan)
        return (series - mean) / std

    def populate_indicators(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        close = dataframe["close"]

        dataframe["ret_1"] = np.log(close / close.shift(1))
        dataframe["mom_6"] = close.pct_change(6)
        dataframe["mom_42"] = close.pct_change(42)
        dataframe["realized_vol"] = dataframe["ret_1"].rolling(12).std()

        dataframe["ema_fast"] = ta.EMA(dataframe, timeperiod=20)
        dataframe["ema_slow"] = ta.EMA(dataframe, timeperiod=50)
        dataframe["trend"] = dataframe["ema_fast"] / dataframe["ema_slow"] - 1.0
        dataframe["atr_pct"] = ta.ATR(dataframe, timeperiod=14) / close
        dataframe["volume_ratio"] = (
            dataframe["volume"] / dataframe["volume"].rolling(42).mean() - 1.0
        )

        norm_window = 126
        dataframe["factor_score"] = (
            0.30 * self.rolling_zscore(dataframe["mom_6"], norm_window)
            + 0.35 * self.rolling_zscore(dataframe["mom_42"], norm_window)
            + 0.25 * self.rolling_zscore(dataframe["trend"], norm_window)
            + 0.10 * self.rolling_zscore(dataframe["volume_ratio"], norm_window)
            - 0.15 * self.rolling_zscore(dataframe["realized_vol"], norm_window)
        )

        return dataframe

    def populate_entry_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        liquid_market = (
            (dataframe["volume"] > 0)
            & (dataframe["atr_pct"] > 0.003)
            & (dataframe["atr_pct"] < 0.08)
        )

        dataframe.loc[
            liquid_market
            & (dataframe["factor_score"] > 0.80)
            & (dataframe["ema_fast"] > dataframe["ema_slow"])
            & (dataframe["close"] > dataframe["ema_fast"]),
            ["enter_long", "enter_tag"],
        ] = (1, "factor_long")

        dataframe.loc[
            liquid_market
            & (dataframe["factor_score"] < -0.80)
            & (dataframe["ema_fast"] < dataframe["ema_slow"])
            & (dataframe["close"] < dataframe["ema_fast"]),
            ["enter_short", "enter_tag"],
        ] = (1, "factor_short")

        return dataframe

    def populate_exit_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        dataframe.loc[
            (dataframe["volume"] > 0)
            & (dataframe["factor_score"] < 0.0),
            ["exit_long", "exit_tag"],
        ] = (1, "factor_long_exit")

        dataframe.loc[
            (dataframe["volume"] > 0)
            & (dataframe["factor_score"] > 0.0),
            ["exit_short", "exit_tag"],
        ] = (1, "factor_short_exit")

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
