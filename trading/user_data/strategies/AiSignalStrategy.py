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

    def leverage(
        self, pair: str, current_time: datetime, current_rate: float,
        proposed_leverage: float, max_leverage: float, entry_tag: str | None,
        side: str, **kwargs,
    ) -> float:
        requested = float(self._config().get("leverage", 1))
        return max(1.0, min(requested, max_leverage, 125.0))
