import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import talib.abstract as ta
from pandas import DataFrame, Series

from freqtrade.strategy import IStrategy


class AiSignalStrategy(IStrategy):
    """Executes AI signals only when the local multi-factor gate agrees."""

    INTERFACE_VERSION = 3
    can_short = True
    timeframe = "15m"
    process_only_new_candles = False
    startup_candle_count = 240
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
        config = self._read_json("ai_config.json")
        runtime_config = getattr(self, "config", {})
        if isinstance(runtime_config, dict):
            config.update(runtime_config)
        return config

    @staticmethod
    def _float(value, fallback: float) -> float:
        try:
            parsed = float(value)
            return parsed if parsed == parsed else fallback
        except (TypeError, ValueError):
            return fallback

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

    def _round_trip_cost_floor(self) -> float:
        config = self._config()
        fee_rate = max(0.0, self._float(config.get("gqt_fee_rate"), 0.0005))
        slippage_rate = max(0.0, self._float(config.get("gqt_slippage_rate"), 0.0002))
        leverage = max(1.0, self._float(config.get("gqt_compound_leverage", config.get("leverage")), 1.0))
        return 2.0 * (fee_rate + slippage_rate) * leverage

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
        config = self._config()
        hours = self._float(config.get("gqt_daily_profit_timezone_offset_hours"), 8.0)
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

    def _account_equity(self) -> float | None:
        config = self._config()
        wallets = getattr(self, "wallets", None)
        stake_currency = str(config.get("stake_currency", "USDT"))
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
        wallet = self._float(config.get("dry_run_wallet"), 0.0)
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
        config = self._config()
        current_time = self._as_utc(current_time)
        day = self._daily_day_label(current_time)
        state = self._read_daily_lock_state()
        if state.get("date") == day and self._float(state.get("start_balance"), 0.0) > 0.0:
            return state

        equity = self._account_equity()
        realized = self._daily_realized_profit_abs(current_time)
        start_balance = (equity - realized) if equity is not None else 0.0
        if not np.isfinite(start_balance) or start_balance <= 0.0:
            start_balance = max(equity or 0.0, self._float(config.get("dry_run_wallet"), 1000.0))
        state = {
            "date": day,
            "start_balance": start_balance,
            "locked": False,
            "target": self._float(config.get("gqt_daily_profit_target"), 0.10),
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
        config = self._config()
        if not self._bool(config.get("gqt_daily_profit_lock_enabled"), True):
            return False
        state = self._daily_lock_state(current_time)
        if state.get("locked") is True:
            return True
        target = max(0.0, self._float(config.get("gqt_daily_profit_target"), 0.10))
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
        minimum_factor_score = self._threshold(config, "minimum_factor_score", 0.12)
        minimum_trend_quality = self._threshold(config, "minimum_trend_quality", 0.42)
        minimum_adx = self._threshold(config, "minimum_adx", 10.0)
        minimum_volume_ratio = self._threshold(config, "minimum_volume_ratio", -0.35)
        row = dataframe.iloc[-1]
        if side == "long":
            minimum_long_score = self._threshold(config, "minimum_long_score", 0.62)
            return bool(
                row.get("long_score", 0) >= minimum_long_score
                and row.get("factor_score", 0) >= minimum_factor_score
                and row.get("trend_quality", 0) >= minimum_trend_quality
                and row.get("adx", 0) >= minimum_adx
                and 46.0 <= row.get("rsi", 0) <= 76.0
                and row.get("volume_ratio", -1) >= minimum_volume_ratio
            )
        minimum_short_score = self._threshold(config, "minimum_short_score", 0.62)
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
        return (
            signal is not None
            and entry_tag == f'ai:{signal["decision_id"]}'
            and not self._daily_target_reached(current_time)
        )

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
        config = self._config()
        force_exit = self._bool(config.get("gqt_daily_profit_force_exit"), True)
        extra_profit = self._trade_profit_abs(trade, current_profit)
        if force_exit and self._daily_target_reached(current_time, extra_profit):
            return "daily_profit_target_lock"

        signal = self._signal(pair)
        stop_loss = self._float(
            signal.get("stop_loss_percent") if signal else None,
            1.5,
        ) / 100.0
        min_net_profit = max(0.0, self._float(config.get("gqt_min_net_profit"), 0.006))
        reward = max(
            stop_loss * self._float(config.get("risk_reward_ratio"), 1.4),
            self._round_trip_cost_floor() + min_net_profit,
        )
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
        config = self._config()
        requested = self._float(config.get("gqt_compound_leverage", config.get("leverage")), 1.0)
        return max(1.0, min(requested, max_leverage))
