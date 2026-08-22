import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import talib.abstract as ta
from pandas import DataFrame, Series

from freqtrade.strategy import IStrategy


PROFILE_ALIASES = {
    "compound_alpha_scalp": "balanced",
    "factor_alpha": "balanced",
    "default": "balanced",
}

MAJOR_BASES = {
    "BTC", "ETH", "BNB", "SOL", "XRP", "ADA", "DOGE", "TRX", "AVAX", "LINK",
    "DOT", "LTC", "BCH", "TON", "NEAR", "UNI", "ATOM", "ETC", "FIL", "APT", "SUI",
}

PROFILE_DEFAULTS = {
    "conservative": {
        "minimum_long_score": 0.68,
        "minimum_short_score": 0.68,
        "minimum_factor_score": 0.18,
        "minimum_trend_quality": 0.50,
        "minimum_adx": 14.0,
        "minimum_volume_ratio": -0.10,
        "long_rsi_min": 38.0,
        "long_rsi_max": 74.0,
        "short_rsi_min": 26.0,
        "short_rsi_max": 62.0,
        "atr_min": 0.00060,
        "atr_max": 0.045,
        "long_vwap_factor": 0.998,
        "short_vwap_factor": 1.002,
        "gqt_compound_capital_usage_percent": 8.0,
        "gqt_compound_take_profit": 0.014,
        "gqt_compound_stop_loss": 0.010,
        "gqt_compound_pyramid_profit": 0.008,
        "gqt_compound_pyramid_stake_ratio": 0.30,
        "gqt_compound_leverage": 2.0,
        "gqt_compound_add_window": 0.78,
        "gqt_fee_rate": 0.0005,
        "gqt_slippage_rate": 0.0002,
        "gqt_min_net_profit": 0.0040,
        "gqt_min_pyramid_net_profit": 0.0025,
        "gqt_time_roll_net_profit": 0.0025,
        "gqt_daily_profit_lock_enabled": True,
        "gqt_daily_profit_force_exit": True,
        "gqt_daily_profit_target": 0.10,
        "gqt_daily_profit_timezone_offset_hours": 8.0,
        "factor_flip_threshold": 0.10,
        "opposite_score_exit": 0.62,
        "time_roll_hours": 8.0,
        "time_roll_profit": 0.002,
        "cooldown_candles": 2,
        "stoploss_guard_trade_limit": 3,
        "stoploss_guard_stop_candles": 24,
        "max_drawdown_allowed": 0.10,
        "max_drawdown_stop_candles": 36,
    },
    "balanced": {
        "minimum_long_score": 0.62,
        "minimum_short_score": 0.62,
        "minimum_factor_score": 0.12,
        "minimum_trend_quality": 0.42,
        "minimum_adx": 10.0,
        "minimum_volume_ratio": -0.35,
        "long_rsi_min": 34.0,
        "long_rsi_max": 78.0,
        "short_rsi_min": 22.0,
        "short_rsi_max": 66.0,
        "atr_min": 0.00045,
        "atr_max": 0.060,
        "long_vwap_factor": 0.995,
        "short_vwap_factor": 1.005,
        "gqt_compound_capital_usage_percent": 12.0,
        "gqt_compound_take_profit": 0.018,
        "gqt_compound_stop_loss": 0.014,
        "gqt_compound_pyramid_profit": 0.006,
        "gqt_compound_pyramid_stake_ratio": 0.45,
        "gqt_compound_leverage": 2.0,
        "gqt_compound_add_window": 0.85,
        "gqt_fee_rate": 0.0005,
        "gqt_slippage_rate": 0.0002,
        "gqt_min_net_profit": 0.0060,
        "gqt_min_pyramid_net_profit": 0.0025,
        "gqt_time_roll_net_profit": 0.0025,
        "gqt_daily_profit_lock_enabled": True,
        "gqt_daily_profit_force_exit": True,
        "gqt_daily_profit_target": 0.10,
        "gqt_daily_profit_timezone_offset_hours": 8.0,
        "factor_flip_threshold": 0.12,
        "opposite_score_exit": 0.64,
        "time_roll_hours": 12.0,
        "time_roll_profit": 0.002,
        "cooldown_candles": 1,
        "stoploss_guard_trade_limit": 4,
        "stoploss_guard_stop_candles": 16,
        "max_drawdown_allowed": 0.16,
        "max_drawdown_stop_candles": 24,
    },
    "aggressive": {
        "minimum_long_score": 0.54,
        "minimum_short_score": 0.54,
        "minimum_factor_score": 0.05,
        "minimum_trend_quality": 0.30,
        "minimum_adx": 6.0,
        "minimum_volume_ratio": -0.60,
        "long_rsi_min": 30.0,
        "long_rsi_max": 82.0,
        "short_rsi_min": 18.0,
        "short_rsi_max": 70.0,
        "atr_min": 0.00035,
        "atr_max": 0.075,
        "long_vwap_factor": 0.992,
        "short_vwap_factor": 1.008,
        "gqt_compound_capital_usage_percent": 18.0,
        "gqt_compound_take_profit": 0.010,
        "gqt_compound_stop_loss": 0.010,
        "gqt_compound_pyramid_profit": 0.003,
        "gqt_compound_pyramid_stake_ratio": 0.60,
        "gqt_compound_leverage": 100.0,
        "gqt_compound_add_window": 0.92,
        "gqt_fee_rate": 0.0005,
        "gqt_slippage_rate": 0.0003,
        "gqt_min_net_profit": 0.0025,
        "gqt_min_pyramid_net_profit": 0.0010,
        "gqt_time_roll_net_profit": 0.0010,
        "gqt_daily_profit_lock_enabled": True,
        "gqt_daily_profit_force_exit": True,
        "gqt_daily_profit_target": 0.10,
        "gqt_daily_profit_timezone_offset_hours": 8.0,
        "factor_flip_threshold": 0.08,
        "opposite_score_exit": 0.58,
        "time_roll_hours": 6.0,
        "time_roll_profit": 0.001,
        "cooldown_candles": 1,
        "stoploss_guard_trade_limit": 5,
        "stoploss_guard_stop_candles": 12,
        "max_drawdown_allowed": 0.20,
        "max_drawdown_stop_candles": 18,
    },
}


class FuturesFactorStrategy(IStrategy):
    """profiled_compound_alpha_scalp: factor futures scalp with controlled roll-in sizing."""

    INTERFACE_VERSION = 3
    can_short = True
    timeframe = "5m"
    process_only_new_candles = False
    startup_candle_count = 320

    position_adjustment_enable = True
    max_entry_position_adjustment = 2

    # ROI is intentionally high so custom_exit owns normal exits.
    minimal_roi = {"0": 0.99}
    stoploss = -0.045
    trailing_stop = False
    trailing_stop_positive = 0.0
    trailing_stop_positive_offset = 0.0
    trailing_only_offset_is_reached = False

    use_exit_signal = True
    exit_profit_only = False
    ignore_roi_if_entry_signal = False

    _user_data = Path("/freqtrade/user_data")

    @property
    def protections(self) -> list[dict]:
        if self._paper_data_collection_enabled():
            return []
        return [
            {
                "method": "CooldownPeriod",
                "stop_duration_candles": self._profile_int("cooldown_candles", 1),
            },
            {
                "method": "StoplossGuard",
                "lookback_period_candles": 48,
                "trade_limit": self._profile_int("stoploss_guard_trade_limit", 4),
                "stop_duration_candles": self._profile_int("stoploss_guard_stop_candles", 16),
                "only_per_pair": False,
            },
            {
                "method": "MaxDrawdown",
                "lookback_period_candles": 96,
                "trade_limit": 10,
                "stop_duration_candles": self._profile_int("max_drawdown_stop_candles", 24),
                "max_allowed_drawdown": self._profile_float("max_drawdown_allowed", 0.16),
            },
        ]

    def _read_json(self, name: str) -> dict:
        try:
            value = json.loads((self._user_data / name).read_text(encoding="utf-8"))
            return value if isinstance(value, dict) else {}
        except (OSError, ValueError, TypeError):
            return {}

    def _settings(self) -> dict:
        settings = self._read_json("ai_config.json")
        runtime_config = getattr(self, "config", {})
        if isinstance(runtime_config, dict):
            settings.update(runtime_config)
        return settings

    @staticmethod
    def _float(value, fallback: float) -> float:
        try:
            parsed = float(value)
            return parsed if np.isfinite(parsed) else fallback
        except (TypeError, ValueError):
            return fallback

    def _profile_name(self) -> str:
        value = str(self._settings().get("gqt_strategy_profile", "balanced")).strip().lower()
        value = PROFILE_ALIASES.get(value, value)
        return value if value in PROFILE_DEFAULTS else "balanced"

    def _profile(self) -> dict:
        return PROFILE_DEFAULTS[self._profile_name()]

    def _float_setting(self, name: str, fallback: float) -> float:
        return self._float(self._settings().get(name), fallback)

    def _profile_float(self, name: str, fallback: float) -> float:
        profile_value = self._profile().get(name, fallback)
        return self._float(self._settings().get(name), self._float(profile_value, fallback))

    def _profile_int(self, name: str, fallback: int) -> int:
        return int(round(self._profile_float(name, float(fallback))))

    @staticmethod
    def _bool(value, fallback: bool) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            value = value.strip().lower()
            if value in {"1", "true", "yes", "on"}:
                return True
            if value in {"0", "false", "no", "off"}:
                return False
        return fallback

    def _profile_bool(self, name: str, fallback: bool) -> bool:
        profile_value = self._profile().get(name, fallback)
        return self._bool(self._settings().get(name), self._bool(profile_value, fallback))

    def _continuous_dry_run_entries_enabled(self) -> bool:
        runtime_config = getattr(self, "config", {})
        return (
            isinstance(runtime_config, dict)
            and runtime_config.get("dry_run") is True
            and self._profile_bool("gqt_continuous_dry_run_entries", False)
        )

    def _paper_data_collection_enabled(self) -> bool:
        runtime_config = getattr(self, "config", {})
        return (
            isinstance(runtime_config, dict)
            and runtime_config.get("dry_run") is True
            and str(self._settings().get("gqt_execution_mode", "paper")).strip().lower()
            == "paper"
            and self._profile_bool("gqt_paper_data_collection", True)
        )

    def _cost_leverage(self) -> float:
        requested = self._profile_float(
            "gqt_compound_leverage",
            self._float_setting("leverage", 2.0),
        )
        return max(1.0, min(requested, 50.0))

    def _execution_mode(self) -> str:
        return str(self._settings().get("gqt_execution_mode", "paper")).strip().lower()

    def _leverage_cap(self, pair: str) -> float:
        symbol = pair.replace("/", "").replace(":USDT", "").upper()
        base = symbol[:-4] if symbol.endswith("USDT") else symbol
        setting = "gqt_major_leverage_cap" if base in MAJOR_BASES else "gqt_alt_leverage_cap"
        fallback = 50.0 if base in MAJOR_BASES else 5.0
        return max(1.0, min(self._float_setting(setting, fallback), fallback))

    def _round_trip_cost_floor(self) -> float:
        fee_rate = max(0.0, self._profile_float("gqt_fee_rate", 0.0005))
        slippage_rate = max(0.0, self._profile_float("gqt_slippage_rate", 0.0002))
        return 2.0 * (fee_rate + slippage_rate) * self._cost_leverage()

    def _cost_adjusted_profit_floor(
        self,
        profit_name: str,
        profit_fallback: float,
        net_name: str,
        net_fallback: float,
    ) -> float:
        raw_target = self._profile_float(profit_name, profit_fallback)
        net_buffer = max(0.0, self._profile_float(net_name, net_fallback))
        return max(raw_target, self._round_trip_cost_floor() + net_buffer)

    def _take_profit_target(self) -> float:
        return self._cost_adjusted_profit_floor(
            "gqt_compound_take_profit",
            0.018,
            "gqt_min_net_profit",
            0.0060,
        )

    def _pyramid_profit_trigger(self) -> float:
        return self._cost_adjusted_profit_floor(
            "gqt_compound_pyramid_profit",
            0.006,
            "gqt_min_pyramid_net_profit",
            0.0025,
        )

    def _time_roll_profit_target(self) -> float:
        return self._cost_adjusted_profit_floor(
            "time_roll_profit",
            0.002,
            "gqt_time_roll_net_profit",
            0.0025,
        )

    def _daily_lock_path(self) -> Path:
        return self._user_data / "gqt_daily_profit_lock.json"

    @staticmethod
    def _as_utc(value) -> datetime:
        if hasattr(value, "to_pydatetime"):
            value = value.to_pydatetime()
        if not isinstance(value, datetime):
            return datetime.now(timezone.utc)
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    def _day_start(self, current_time: datetime) -> datetime:
        current_time = self._as_utc(current_time)
        offset = self._daily_timezone_offset()
        local_time = current_time + offset
        local_midnight = datetime(
            local_time.year,
            local_time.month,
            local_time.day,
            tzinfo=timezone.utc,
        )
        return local_midnight - offset

    def _daily_day_label(self, current_time: datetime) -> str:
        current_time = self._as_utc(current_time)
        return (current_time + self._daily_timezone_offset()).date().isoformat()

    def _daily_timezone_offset(self) -> timedelta:
        hours = self._profile_float("gqt_daily_profit_timezone_offset_hours", 8.0)
        hours = max(-12.0, min(hours, 14.0))
        return timedelta(hours=hours)

    def _utc_day_start(self, current_time: datetime) -> datetime:
        current_time = self._as_utc(current_time)
        return datetime(
            current_time.year,
            current_time.month,
            current_time.day,
            tzinfo=timezone.utc,
        )

    def _dataframe_time(self, dataframe: DataFrame) -> datetime:
        if "date" in dataframe.columns and not dataframe.empty:
            return self._as_utc(dataframe["date"].iloc[-1])
        return datetime.now(timezone.utc)

    def _account_equity(self) -> float | None:
        wallets = getattr(self, "wallets", None)
        stake_currency = str(self._settings().get("stake_currency", "USDT"))
        if wallets is not None:
            for method_name, args in (
                ("get_total", (stake_currency,)),
                ("get_total_stake_amount", ()),
            ):
                method = getattr(wallets, method_name, None)
                if callable(method):
                    try:
                        value = self._float(method(*args), 0.0)
                        if value > 0.0:
                            return value
                    except Exception:
                        pass
        wallet = self._float_setting("dry_run_wallet", 0.0)
        if wallet > 0.0:
            return wallet + self._daily_realized_profit_abs(datetime.now(timezone.utc))
        return None

    def _daily_realized_profit_abs(self, current_time: datetime) -> float:
        try:
            from freqtrade.persistence import Trade

            start = self._day_start(current_time)
            try:
                trades = Trade.get_trades_proxy(is_open=False, close_date=start)
            except TypeError:
                trades = Trade.get_trades_proxy(
                    is_open=False,
                    close_date=start.replace(tzinfo=None),
                )
            total = 0.0
            for trade in trades:
                total += self._float(
                    getattr(trade, "close_profit_abs", None),
                    self._float(getattr(trade, "realized_profit", 0.0), 0.0),
                )
            return total
        except Exception:
            return 0.0

    def _trade_profit_abs(self, trade, current_profit: float) -> float:
        stake = self._float(getattr(trade, "stake_amount", 0.0), 0.0)
        return max(0.0, stake * current_profit)

    def _read_daily_lock_state(self) -> dict:
        cached = getattr(self, "_daily_lock_cache", None)
        if isinstance(cached, dict):
            return cached
        state = self._read_json("gqt_daily_profit_lock.json")
        setattr(self, "_daily_lock_cache", state)
        return state

    def _write_daily_lock_state(self, state: dict) -> None:
        setattr(self, "_daily_lock_cache", state)
        try:
            self._daily_lock_path().write_text(
                json.dumps(state, ensure_ascii=True, indent=2),
                encoding="utf-8",
            )
        except OSError:
            pass

    def _daily_lock_state(self, current_time: datetime) -> dict:
        current_time = self._as_utc(current_time)
        day = self._daily_day_label(current_time)
        state = self._read_daily_lock_state()
        if state.get("date") == day and self._float(state.get("start_balance"), 0.0) > 0.0:
            return state

        equity = self._account_equity()
        realized = self._daily_realized_profit_abs(current_time)
        start_balance = (equity - realized) if equity is not None else 0.0
        if not np.isfinite(start_balance) or start_balance <= 0.0:
            start_balance = max(equity or 0.0, self._float_setting("dry_run_wallet", 1000.0))
        state = {
            "date": day,
            "start_balance": start_balance,
            "locked": False,
            "target": self._profile_float("gqt_daily_profit_target", 0.10),
        }
        self._write_daily_lock_state(state)
        return state

    def _daily_profit_ratio(self, current_time: datetime, extra_profit_abs: float = 0.0) -> float:
        state = self._daily_lock_state(current_time)
        start_balance = self._float(state.get("start_balance"), 0.0)
        if start_balance <= 0.0:
            return 0.0
        equity = self._account_equity()
        if equity is None:
            equity = start_balance + self._daily_realized_profit_abs(current_time)
        return (equity + extra_profit_abs - start_balance) / start_balance

    def _daily_target_reached(self, current_time: datetime, extra_profit_abs: float = 0.0) -> bool:
        if self._paper_data_collection_enabled():
            return False
        if not self._profile_bool("gqt_daily_profit_lock_enabled", True):
            return False
        state = self._daily_lock_state(current_time)
        if state.get("locked") is True:
            return True
        target = max(0.0, self._profile_float("gqt_daily_profit_target", 0.10))
        if target <= 0.0:
            return False
        ratio = self._daily_profit_ratio(current_time, extra_profit_abs)
        if ratio >= target:
            state.update(
                {
                    "locked": True,
                    "locked_at": self._as_utc(current_time).isoformat(),
                    "profit_ratio": ratio,
                    "target": target,
                }
            )
            self._write_daily_lock_state(state)
            return True
        return False

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
        minimum = max(20, window // 3)
        mean = series.rolling(window, min_periods=minimum).mean()
        std = series.rolling(window, min_periods=minimum).std().replace(0, np.nan)
        return ((series - mean) / std).clip(lower=-4.0, upper=4.0).fillna(0.0)

    @staticmethod
    def weighted_score(parts: list[tuple[Series, float]]) -> Series:
        total_weight = sum(weight for _, weight in parts)
        if total_weight <= 0:
            return parts[0][0] * 0.0
        score = sum(part.clip(lower=0.0, upper=1.0).fillna(0.0) * weight for part, weight in parts)
        return (score / total_weight).clip(lower=0.0, upper=1.0).fillna(0.0)

    @staticmethod
    def signed_score(positive: Series, negative: Series) -> Series:
        return (positive.fillna(0.0) - negative.fillna(0.0)).clip(lower=-1.0, upper=1.0)

    @staticmethod
    def rolling_aroon(series: Series, window: int, high: bool) -> Series:
        def score(values: np.ndarray) -> float:
            index = int(np.argmax(values) if high else np.argmin(values))
            return index / max(len(values) - 1, 1) * 100.0

        return series.rolling(window + 1, min_periods=window).apply(score, raw=True).fillna(50.0)

    def populate_indicators(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        close = dataframe["close"]
        high = dataframe["high"]
        low = dataframe["low"]
        volume = dataframe["volume"]
        typical = (high + low + close) / 3.0

        dataframe["ema_8"] = ta.EMA(dataframe, timeperiod=8)
        dataframe["ema_21"] = ta.EMA(dataframe, timeperiod=21)
        dataframe["ema_55"] = ta.EMA(dataframe, timeperiod=55)
        dataframe["ema_144"] = ta.EMA(dataframe, timeperiod=144)
        dataframe["trend_fast"] = dataframe["ema_8"] / dataframe["ema_21"] - 1.0
        dataframe["trend_slow"] = dataframe["ema_55"] / dataframe["ema_144"] - 1.0

        dataframe["ret_1"] = close.pct_change(1)
        dataframe["ret_3"] = close.pct_change(3)
        dataframe["ret_12"] = close.pct_change(12)
        dataframe["ret_48"] = close.pct_change(48)
        dataframe["realized_vol"] = dataframe["ret_1"].rolling(32, min_periods=16).std() * np.sqrt(96)

        dataframe["adx"] = ta.ADX(dataframe, timeperiod=14)
        dataframe["rsi"] = ta.RSI(dataframe, timeperiod=14)
        dataframe["cci"] = ta.CCI(dataframe, timeperiod=20)
        macd = ta.MACD(dataframe, fastperiod=12, slowperiod=26, signalperiod=9)
        dataframe["macd_hist"] = macd["macdhist"]
        dataframe["atr_pct"] = (ta.ATR(dataframe, timeperiod=14) / close).replace([np.inf, -np.inf], np.nan)
        dataframe["volume_ratio"] = volume / volume.rolling(48, min_periods=24).mean() - 1.0

        cumulative_volume = volume.rolling(96, min_periods=24).sum().replace(0, np.nan)
        dataframe["rolling_vwap"] = (typical * volume).rolling(96, min_periods=24).sum() / cumulative_volume
        dataframe["vwap_distance"] = close / dataframe["rolling_vwap"] - 1.0

        dataframe["donchian_high_32"] = high.rolling(32, min_periods=32).max().shift(1)
        dataframe["donchian_low_32"] = low.rolling(32, min_periods=32).min().shift(1)
        dataframe["donchian_high_96"] = high.rolling(96, min_periods=48).max().shift(1)
        dataframe["donchian_low_96"] = low.rolling(96, min_periods=48).min().shift(1)
        donchian_range = (dataframe["donchian_high_96"] - dataframe["donchian_low_96"]).replace(0, np.nan)
        dataframe["breakout_position"] = (
            (close - (dataframe["donchian_high_96"] + dataframe["donchian_low_96"]) * 0.5)
            / donchian_range
            * 2.0
        ).clip(lower=-1.5, upper=1.5)
        candle_range = (high - low).replace(0, np.nan)
        dataframe["close_location"] = ((close - low) / candle_range * 2.0 - 1.0).clip(
            lower=-1.0,
            upper=1.0,
        )

        bands = ta.BBANDS(dataframe, timeperiod=20, nbdevup=2.0, nbdevdn=2.0)
        dataframe["bb_width"] = (bands["upperband"] - bands["lowerband"]) / bands["middleband"]
        dataframe["bb_z"] = ((close - bands["middleband"]) / (bands["upperband"] - bands["lowerband"])).clip(
            lower=-2.0,
            upper=2.0,
        )
        rsi_low = dataframe["rsi"].rolling(14, min_periods=8).min()
        rsi_high = dataframe["rsi"].rolling(14, min_periods=8).max()
        dataframe["stoch_rsi"] = ((dataframe["rsi"] - rsi_low) / (rsi_high - rsi_low).replace(0, np.nan)).fillna(0.5)
        dataframe["aroon_up"] = self.rolling_aroon(high, 25, True)
        dataframe["aroon_down"] = self.rolling_aroon(low, 25, False)
        dataframe["aroon_osc"] = (dataframe["aroon_up"] - dataframe["aroon_down"]) / 100.0

        dataframe["price_volume_corr"] = dataframe["ret_3"].rolling(24, min_periods=12).corr(
            dataframe["volume_ratio"]
        )
        dataframe["liquidity_impulse"] = self.rolling_zscore(volume * candle_range / close, 96)
        dataframe["volatility_squeeze"] = -self.rolling_zscore(dataframe["bb_width"], 96)
        dataframe["range_absorption"] = (
            dataframe["volume_ratio"].clip(lower=-1.0, upper=3.0)
            * (1.0 - dataframe["close_location"].abs()).clip(lower=0.0, upper=1.0)
        )

        trend_scale = dataframe["atr_pct"].clip(lower=0.002) * 3.5
        trend_long = self.weighted_score(
            [
                ((close > dataframe["ema_8"]).astype(float), 0.10),
                ((dataframe["ema_8"] > dataframe["ema_21"]).astype(float), 0.16),
                ((dataframe["ema_21"] > dataframe["ema_55"]).astype(float), 0.16),
                ((dataframe["ema_55"] > dataframe["ema_144"]).astype(float), 0.10),
                (self.bounded(dataframe["adx"], 10.0, 32.0), 0.22),
                ((dataframe["trend_fast"] / trend_scale).clip(lower=0.0, upper=1.0), 0.14),
                (self.bounded(dataframe["aroon_osc"], 0.0, 0.65), 0.12),
            ]
        )
        trend_short = self.weighted_score(
            [
                ((close < dataframe["ema_8"]).astype(float), 0.10),
                ((dataframe["ema_8"] < dataframe["ema_21"]).astype(float), 0.16),
                ((dataframe["ema_21"] < dataframe["ema_55"]).astype(float), 0.16),
                ((dataframe["ema_55"] < dataframe["ema_144"]).astype(float), 0.10),
                (self.bounded(dataframe["adx"], 10.0, 32.0), 0.22),
                ((-dataframe["trend_fast"] / trend_scale).clip(lower=0.0, upper=1.0), 0.14),
                (self.bounded(-dataframe["aroon_osc"], 0.0, 0.65), 0.12),
            ]
        )

        macd_zscore = self.rolling_zscore(dataframe["macd_hist"], 96)
        momentum_long = self.weighted_score(
            [
                (self.bounded(dataframe["ret_3"], 0.0, 0.012), 0.22),
                (self.bounded(dataframe["ret_12"], 0.0, 0.035), 0.24),
                (self.bounded(dataframe["ret_48"], 0.0, 0.080), 0.16),
                (self.bounded(macd_zscore, 0.0, 2.0), 0.18),
                (self.bounded(dataframe["stoch_rsi"], 0.35, 0.85), 0.10),
                (self.bounded(dataframe["cci"], -40.0, 120.0), 0.10),
            ]
        )
        momentum_short = self.weighted_score(
            [
                (self.bounded(-dataframe["ret_3"], 0.0, 0.012), 0.22),
                (self.bounded(-dataframe["ret_12"], 0.0, 0.035), 0.24),
                (self.bounded(-dataframe["ret_48"], 0.0, 0.080), 0.16),
                (self.bounded(-macd_zscore, 0.0, 2.0), 0.18),
                (self.bounded(1.0 - dataframe["stoch_rsi"], 0.35, 0.85), 0.10),
                (self.bounded(-dataframe["cci"], -40.0, 120.0), 0.10),
            ]
        )

        dataframe["alpha_liquidity_long"] = self.weighted_score(
            [
                (self.bounded(dataframe["liquidity_impulse"], 0.0, 2.2), 0.28),
                (self.bounded(dataframe["volume_ratio"], -0.35, 1.4), 0.26),
                (self.bounded(dataframe["close_location"], 0.0, 0.85), 0.20),
                (self.bounded(dataframe["vwap_distance"], -0.002, 0.014), 0.16),
                (self.bounded(dataframe["price_volume_corr"], 0.0, 0.75), 0.10),
            ]
        )
        dataframe["alpha_liquidity_short"] = self.weighted_score(
            [
                (self.bounded(dataframe["liquidity_impulse"], 0.0, 2.2), 0.28),
                (self.bounded(dataframe["volume_ratio"], -0.35, 1.4), 0.26),
                (self.bounded(-dataframe["close_location"], 0.0, 0.85), 0.20),
                (self.bounded(-dataframe["vwap_distance"], -0.002, 0.014), 0.16),
                (self.bounded(-dataframe["price_volume_corr"], 0.0, 0.75), 0.10),
            ]
        )
        dataframe["alpha_breakout_long"] = self.weighted_score(
            [
                ((close > dataframe["donchian_high_32"]).astype(float), 0.24),
                (self.bounded(dataframe["breakout_position"], 0.12, 0.85), 0.24),
                (self.bounded(dataframe["volatility_squeeze"], -0.4, 1.8), 0.18),
                (self.bounded(dataframe["close_location"], 0.15, 0.95), 0.18),
                (trend_long, 0.16),
            ]
        )
        dataframe["alpha_breakout_short"] = self.weighted_score(
            [
                ((close < dataframe["donchian_low_32"]).astype(float), 0.24),
                (self.bounded(-dataframe["breakout_position"], 0.12, 0.85), 0.24),
                (self.bounded(dataframe["volatility_squeeze"], -0.4, 1.8), 0.18),
                (self.bounded(-dataframe["close_location"], 0.15, 0.95), 0.18),
                (trend_short, 0.16),
            ]
        )
        dataframe["alpha_reversion_long"] = self.weighted_score(
            [
                (self.bounded(-dataframe["bb_z"], 0.18, 1.15), 0.26),
                (self.center_score(dataframe["rsi"], 43.0, 15.0), 0.22),
                (self.bounded(dataframe["range_absorption"], 0.10, 1.20), 0.18),
                (self.bounded(-(close / dataframe["ema_21"] - 1.0), 0.002, 0.025), 0.18),
                (self.bounded(dataframe["stoch_rsi"], 0.05, 0.35), 0.16),
            ]
        )
        dataframe["alpha_reversion_short"] = self.weighted_score(
            [
                (self.bounded(dataframe["bb_z"], 0.18, 1.15), 0.26),
                (self.center_score(dataframe["rsi"], 57.0, 15.0), 0.22),
                (self.bounded(dataframe["range_absorption"], 0.10, 1.20), 0.18),
                (self.bounded(close / dataframe["ema_21"] - 1.0, 0.002, 0.025), 0.18),
                (self.bounded(1.0 - dataframe["stoch_rsi"], 0.05, 0.35), 0.16),
            ]
        )

        dataframe["volume_confirmation"] = self.bounded(dataframe["volume_ratio"], -0.35, 1.25)
        dataframe["volatility_quality"] = 0.0
        low_vol = (dataframe["atr_pct"] >= 0.00045) & (dataframe["atr_pct"] < 0.0010)
        good_vol = (dataframe["atr_pct"] >= 0.0010) & (dataframe["atr_pct"] <= 0.0350)
        high_vol = (dataframe["atr_pct"] > 0.0350) & (dataframe["atr_pct"] <= 0.0800)
        dataframe.loc[low_vol, "volatility_quality"] = self.bounded(
            dataframe.loc[low_vol, "atr_pct"],
            0.00045,
            0.0010,
        )
        dataframe.loc[good_vol, "volatility_quality"] = 1.0
        dataframe.loc[high_vol, "volatility_quality"] = (
            1.0 - self.bounded(dataframe.loc[high_vol, "atr_pct"], 0.0350, 0.0800) * 0.80
        )

        dataframe["long_score"] = self.weighted_score(
            [
                (trend_long, 0.20),
                (momentum_long, 0.18),
                (dataframe["alpha_liquidity_long"], 0.17),
                (dataframe["alpha_breakout_long"], 0.16),
                (dataframe["alpha_reversion_long"], 0.09),
                (dataframe["volume_confirmation"], 0.10),
                (dataframe["volatility_quality"], 0.07),
                (self.bounded(dataframe["close_location"], -0.10, 0.90), 0.03),
            ]
        )
        dataframe["short_score"] = self.weighted_score(
            [
                (trend_short, 0.20),
                (momentum_short, 0.18),
                (dataframe["alpha_liquidity_short"], 0.17),
                (dataframe["alpha_breakout_short"], 0.16),
                (dataframe["alpha_reversion_short"], 0.09),
                (dataframe["volume_confirmation"], 0.10),
                (dataframe["volatility_quality"], 0.07),
                (self.bounded(-dataframe["close_location"], -0.10, 0.90), 0.03),
            ]
        )
        dataframe["factor_score"] = dataframe["long_score"] - dataframe["short_score"]
        dataframe["trend_quality"] = np.maximum(trend_long, trend_short)
        dataframe["regime_score"] = self.signed_score(trend_long, trend_short)

        return dataframe.replace([np.inf, -np.inf], np.nan)

    def _threshold(self, name: str, fallback: float) -> float:
        return self._profile_float(name, fallback)

    def _entry_thresholds(self) -> tuple[float, float, float, float, float, float]:
        return (
            self._threshold("minimum_long_score", 0.62),
            self._threshold("minimum_short_score", 0.62),
            self._threshold("minimum_factor_score", 0.12),
            self._threshold("minimum_trend_quality", 0.42),
            self._threshold("minimum_adx", 10.0),
            self._threshold("minimum_volume_ratio", -0.35),
        )

    def populate_entry_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        dataframe["enter_long"] = 0
        dataframe["enter_short"] = 0
        if dataframe.empty:
            return dataframe
        if self._daily_target_reached(self._dataframe_time(dataframe)):
            return dataframe

        min_long, min_short, min_factor, min_trend, min_adx, min_volume = self._entry_thresholds()
        long_rsi_min = self._profile_float("long_rsi_min", 34.0)
        long_rsi_max = self._profile_float("long_rsi_max", 78.0)
        short_rsi_min = self._profile_float("short_rsi_min", 22.0)
        short_rsi_max = self._profile_float("short_rsi_max", 66.0)
        atr_min = self._profile_float("atr_min", 0.00045)
        atr_max = self._profile_float("atr_max", 0.060)
        long_vwap_factor = self._profile_float("long_vwap_factor", 0.995)
        short_vwap_factor = self._profile_float("short_vwap_factor", 1.005)
        long_conditions = (
            (dataframe["volume"] > 0)
            & (dataframe["long_score"] >= min_long)
            & (dataframe["factor_score"] >= min_factor)
            & (dataframe["trend_quality"] >= min_trend)
            & (dataframe["adx"] >= min_adx)
            & (dataframe["volume_ratio"] >= min_volume)
            & (dataframe["rsi"].between(long_rsi_min, long_rsi_max))
            & (dataframe["atr_pct"].between(atr_min, atr_max))
            & (dataframe["close"] > dataframe["rolling_vwap"] * long_vwap_factor)
        )
        short_conditions = (
            (dataframe["volume"] > 0)
            & (dataframe["short_score"] >= min_short)
            & (dataframe["factor_score"] <= -min_factor)
            & (dataframe["trend_quality"] >= min_trend)
            & (dataframe["adx"] >= min_adx)
            & (dataframe["volume_ratio"] >= min_volume)
            & (dataframe["rsi"].between(short_rsi_min, short_rsi_max))
            & (dataframe["atr_pct"].between(atr_min, atr_max))
            & (dataframe["close"] < dataframe["rolling_vwap"] * short_vwap_factor)
        )

        dataframe.loc[long_conditions, ["enter_long", "enter_tag"]] = (1, "alpha_compound_long")
        dataframe.loc[short_conditions, ["enter_short", "enter_tag"]] = (1, "alpha_compound_short")

        if self._continuous_dry_run_entries_enabled():
            latest_index = dataframe.index[-1]
            latest_factor = dataframe.at[latest_index, "factor_score"]
            latest_volume = dataframe.at[latest_index, "volume"]
            if (
                np.isfinite(latest_factor)
                and np.isfinite(latest_volume)
                and latest_volume > 0
                and dataframe.at[latest_index, "enter_long"] != 1
                and dataframe.at[latest_index, "enter_short"] != 1
            ):
                if latest_factor >= 0:
                    dataframe.at[latest_index, "enter_long"] = 1
                    dataframe.at[latest_index, "enter_tag"] = "dry_run_continuous_factor_long"
                else:
                    dataframe.at[latest_index, "enter_short"] = 1
                    dataframe.at[latest_index, "enter_tag"] = "dry_run_continuous_factor_short"
        return dataframe

    def populate_exit_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        dataframe["exit_long"] = 0
        dataframe["exit_short"] = 0
        if dataframe.empty:
            return dataframe

        flip_threshold = self._profile_float("factor_flip_threshold", 0.12)
        opposite_exit = self._profile_float("opposite_score_exit", 0.64)
        long_exit = (
            (dataframe["factor_score"] < -flip_threshold)
            | ((dataframe["short_score"] > opposite_exit) & (dataframe["close"] < dataframe["ema_21"]))
            | ((dataframe["rsi"] > 84.0) & (dataframe["close_location"] < 0.15))
        )
        short_exit = (
            (dataframe["factor_score"] > flip_threshold)
            | ((dataframe["long_score"] > opposite_exit) & (dataframe["close"] > dataframe["ema_21"]))
            | ((dataframe["rsi"] < 16.0) & (dataframe["close_location"] > -0.15))
        )
        dataframe.loc[long_exit, ["exit_long", "exit_tag"]] = (1, "factor_flip_long_exit")
        dataframe.loc[short_exit, ["exit_short", "exit_tag"]] = (1, "factor_flip_short_exit")
        return dataframe

    def _last_analyzed_row(self, pair: str) -> Series | None:
        try:
            dataframe, _ = self.dp.get_analyzed_dataframe(pair=pair, timeframe=self.timeframe)
            if dataframe is None or dataframe.empty:
                return None
            return dataframe.iloc[-1]
        except Exception:
            return None

    def _side_still_valid(self, row: Series | None, side: str) -> bool:
        if row is None:
            return False
        min_long, min_short, min_factor, min_trend, min_adx, min_volume = self._entry_thresholds()
        if side == "long":
            return bool(
                row.get("long_score", 0.0) >= min_long * 0.92
                and row.get("factor_score", 0.0) >= min_factor * 0.70
                and row.get("trend_quality", 0.0) >= min_trend
                and row.get("adx", 0.0) >= min_adx
                and row.get("volume_ratio", -1.0) >= min_volume
            )
        return bool(
            row.get("short_score", 0.0) >= min_short * 0.92
            and row.get("factor_score", 0.0) <= -min_factor * 0.70
            and row.get("trend_quality", 0.0) >= min_trend
            and row.get("adx", 0.0) >= min_adx
            and row.get("volume_ratio", -1.0) >= min_volume
        )

    def confirm_trade_entry(
        self, pair: str, order_type: str, amount: float, rate: float,
        time_in_force: str, current_time: datetime, entry_tag: str | None,
        side: str, **kwargs,
    ) -> bool:
        if self._execution_mode() == "recommend":
            return False
        if not bool(getattr(self, "config", {}).get("dry_run", True)) and self._execution_mode() != "live":
            return False
        return not self._daily_target_reached(current_time)

    def custom_stake_amount(
        self, pair: str, current_time: datetime, current_rate: float,
        proposed_stake: float, min_stake: float | None, max_stake: float,
        leverage: float, entry_tag: str | None, side: str, **kwargs,
    ) -> float:
        configured_cap = self._float_setting(
            "gqt_max_stake_amount",
            self._float_setting("max_stake_amount", proposed_stake),
        )
        usage_percent = self._profile_float("gqt_compound_capital_usage_percent", 12.0)
        usage_cap = max_stake * usage_percent / 100.0
        stake = min(configured_cap, usage_cap, max_stake)
        if min_stake is not None:
            stake = max(stake, min_stake)
        return max(0.0, min(stake, max_stake))

    def adjust_trade_position(
        self, trade, current_time: datetime, current_rate: float, current_profit: float,
        min_stake: float | None, max_stake: float, current_entry_rate: float,
        current_exit_rate: float, current_entry_profit: float, current_exit_profit: float,
        **kwargs,
    ):
        if not self._settings().get("gqt_compound_enabled", True):
            return None
        if self._daily_target_reached(current_time):
            return None
        add_trigger = self._pyramid_profit_trigger()
        take_profit = self._take_profit_target()
        add_window = self._profile_float("gqt_compound_add_window", 0.85)
        if current_profit < add_trigger or current_profit > take_profit * add_window:
            return None

        entries = int(getattr(trade, "nr_of_successful_entries", 1) or 1)
        if entries >= self.max_entry_position_adjustment + 1:
            return None

        side = "short" if getattr(trade, "is_short", False) else "long"
        if not self._side_still_valid(self._last_analyzed_row(trade.pair), side):
            return None

        ratio = self._profile_float("gqt_compound_pyramid_stake_ratio", 0.45)
        stake = min(max_stake, float(getattr(trade, "stake_amount", 0.0) or 0.0) * ratio)
        if min_stake is not None and stake < min_stake:
            return None
        if stake <= 0.0:
            return None
        return stake, "compound_roll_add"

    def custom_exit(
        self, pair: str, trade, current_time: datetime, current_rate: float,
        current_profit: float, **kwargs,
    ) -> str | None:
        force_exit = self._profile_bool("gqt_daily_profit_force_exit", True)
        extra_profit = self._trade_profit_abs(trade, current_profit)
        if force_exit and self._daily_target_reached(current_time, extra_profit):
            return "daily_profit_target_lock"

        if self._paper_data_collection_enabled():
            open_date = getattr(trade, "open_date_utc", None)
            if open_date is not None:
                open_date = self._as_utc(open_date)
                hold_minutes = max(
                    1.0,
                    self._profile_float("gqt_paper_collection_hold_minutes", 3.0),
                )
                if (self._as_utc(current_time) - open_date).total_seconds() >= hold_minutes * 60.0:
                    return "paper_data_collection_roll"

        take_profit = self._take_profit_target()
        stop_loss = self._profile_float("gqt_compound_stop_loss", 0.014)
        if current_profit >= take_profit:
            return "compound_take_profit"
        if current_profit <= -stop_loss:
            return "compound_cut_loss"

        row = self._last_analyzed_row(pair)
        side = "short" if getattr(trade, "is_short", False) else "long"
        flip_threshold = self._profile_float("factor_flip_threshold", 0.12)
        opposite_exit = self._profile_float("opposite_score_exit", 0.64)
        if row is not None:
            if side == "long" and (
                row.get("factor_score", 0.0) < -flip_threshold
                or row.get("short_score", 0.0) > opposite_exit
                or row.get("close", 0.0) < row.get("ema_55", 0.0)
            ):
                return "compound_factor_flip_long"
            if side == "short" and (
                row.get("factor_score", 0.0) > flip_threshold
                or row.get("long_score", 0.0) > opposite_exit
                or row.get("close", 0.0) > row.get("ema_55", 0.0)
            ):
                return "compound_factor_flip_short"

        open_date = getattr(trade, "open_date_utc", None)
        if open_date is not None and open_date.tzinfo is None:
            open_date = open_date.replace(tzinfo=timezone.utc)
        if open_date is not None:
            age_hours = (current_time - open_date).total_seconds() / 3600.0
            if (
                age_hours >= self._profile_float("time_roll_hours", 12.0)
                and current_profit >= self._time_roll_profit_target()
            ):
                return "compound_time_roll"
        return None

    def leverage(
        self, pair: str, current_time: datetime, current_rate: float,
        proposed_leverage: float, max_leverage: float, entry_tag: str | None,
        side: str, **kwargs,
    ) -> float:
        requested = self._profile_float(
            "gqt_compound_leverage",
            self._float_setting("leverage", 2.0),
        )
        return max(1.0, min(requested, self._leverage_cap(pair), max_leverage))
