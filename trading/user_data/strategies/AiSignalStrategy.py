import json
from datetime import datetime, timezone
from pathlib import Path

from pandas import DataFrame

from freqtrade.strategy import IStrategy


class AiSignalStrategy(IStrategy):
    """Executes pre-validated AI signals written by the native GQT client."""

    INTERFACE_VERSION = 3
    can_short = True
    timeframe = "1h"
    process_only_new_candles = True
    startup_candle_count = 2
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
        return dataframe

    def populate_entry_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        dataframe["enter_long"] = 0
        dataframe["enter_short"] = 0
        signal = self._valid_signal(metadata["pair"])
        if signal is None or dataframe.empty:
            return dataframe
        tag = f'ai:{signal["decision_id"]}'
        if signal["action"] == "long":
            dataframe.loc[dataframe.index[-1], ["enter_long", "enter_tag"]] = (1, tag)
        elif signal["action"] == "short":
            dataframe.loc[dataframe.index[-1], ["enter_short", "enter_tag"]] = (1, tag)
        return dataframe

    def populate_exit_trend(self, dataframe: DataFrame, metadata: dict) -> DataFrame:
        dataframe["exit_long"] = 0
        dataframe["exit_short"] = 0
        signal = self._valid_signal(metadata["pair"])
        if signal is not None and signal["action"] == "close" and not dataframe.empty:
            dataframe.loc[dataframe.index[-1], ["exit_long", "exit_tag"]] = (1, "ai_close")
            dataframe.loc[dataframe.index[-1], ["exit_short", "exit_tag"]] = (1, "ai_close")
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
