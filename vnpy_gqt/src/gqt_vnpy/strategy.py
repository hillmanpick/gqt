from __future__ import annotations

try:
    from vnpy.trader.constant import Direction, Interval, Offset
    from vnpy.trader.object import BarData, OrderData, TickData, TradeData
    from vnpy_ctastrategy import ArrayManager, BarGenerator, CtaTemplate, StopOrder
except ImportError as exc:  # pragma: no cover - exercised after optional runtime install
    raise RuntimeError("vnpy and vnpy_ctastrategy are required to load the CTA strategy") from exc

from .risk import size_position


class GqtUsdtTrendStrategy(CtaTemplate):
    """Closed-bar BTC/ETH trend strategy with explicit cash-risk sizing."""

    author = "GQT"

    fast_window = 24
    slow_window = 96
    atr_window = 20
    breakout_window = 48
    risk_reward = 1.8
    atr_stop_multiple = 2.2
    minimum_atr_percent = 0.002
    maximum_atr_percent = 0.04
    cooldown_bars = 4

    starting_capital = 1000.0
    leverage = 2
    risk_per_trade = 0.005
    max_margin_per_trade = 120.0
    fee_rate = 0.0005
    slippage_bps = 3.0
    min_volume = 0.001
    contract_size = 1.0
    warmup_days = 10
    data_interval = "1m"

    parameters = [
        "fast_window",
        "slow_window",
        "atr_window",
        "breakout_window",
        "risk_reward",
        "atr_stop_multiple",
        "minimum_atr_percent",
        "maximum_atr_percent",
        "cooldown_bars",
        "starting_capital",
        "leverage",
        "risk_per_trade",
        "max_margin_per_trade",
        "fee_rate",
        "slippage_bps",
        "min_volume",
        "contract_size",
        "warmup_days",
        "data_interval",
    ]
    variables = [
        "strategy_equity",
        "entry_price",
        "stop_price",
        "target_price",
        "cooldown_remaining",
    ]

    def __init__(self, cta_engine, strategy_name: str, vt_symbol: str, setting: dict) -> None:
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)
        research_symbol = vt_symbol.split(".", maxsplit=1)[0]
        supported_symbols = {
            f"{symbol}_GQT_{timeframe}"
            for symbol in ("BTCUSDT", "ETHUSDT")
            for timeframe in ("15M", "1H", "4H")
        }
        if research_symbol not in supported_symbols:
            raise ValueError("GqtUsdtTrendStrategy only supports BTCUSDT and ETHUSDT")
        if not 1 <= int(self.leverage) <= 3:
            raise ValueError("strategy leverage must be between 1 and 3")
        self.bg = BarGenerator(self.on_bar)
        self.am = ArrayManager(max(self.slow_window, self.breakout_window, self.atr_window) + 10)
        self.entry_price = 0.0
        self.stop_price = 0.0
        self.target_price = 0.0
        self.entry_volume = 0.0
        self.entry_direction = 0
        self.planned_stop_distance = 0.0
        self.strategy_equity = float(self.starting_capital)
        self.cooldown_remaining = 0

    def on_init(self) -> None:
        self.write_log("Initializing GQT BTC/ETH USDT-M research strategy")
        self.load_bar(int(self.warmup_days), interval=Interval(self.data_interval))

    def on_start(self) -> None:
        self.write_log("GQT research strategy started")

    def on_stop(self) -> None:
        self.write_log("GQT research strategy stopped")

    def on_tick(self, tick: TickData) -> None:
        self.bg.update_tick(tick)

    def on_bar(self, bar: BarData) -> None:
        self.cancel_all()
        self.am.update_bar(bar)
        if not self.am.inited or not self.trading:
            return

        if self.cooldown_remaining > 0:
            self.cooldown_remaining -= 1

        fast = float(self.am.sma(self.fast_window))
        slow = float(self.am.sma(self.slow_window))
        atr = float(self.am.atr(self.atr_window))
        if atr <= 0 or bar.close_price <= 0:
            return
        atr_percent = atr / bar.close_price

        if self.pos > 0:
            self._place_exits(include_target=True)
        elif self.pos < 0:
            self._place_exits(include_target=True)
        elif self.cooldown_remaining == 0 and self.minimum_atr_percent <= atr_percent <= self.maximum_atr_percent:
            prior_high = float(max(self.am.high[-self.breakout_window - 1 : -1]))
            prior_low = float(min(self.am.low[-self.breakout_window - 1 : -1]))
            if fast > slow and bar.close_price > prior_high:
                self._open(bar.close_price, atr, long=True)
            elif fast < slow and bar.close_price < prior_low:
                self._open(bar.close_price, atr, long=False)
        self.put_event()

    def _open(self, entry: float, atr: float, *, long: bool) -> None:
        stop_distance = atr * self.atr_stop_multiple
        stop = entry - stop_distance if long else entry + stop_distance
        sizing = size_position(
            equity=self.strategy_equity,
            entry_price=entry,
            stop_price=stop,
            leverage=int(self.leverage),
            risk_fraction=self.risk_per_trade,
            max_margin=self.max_margin_per_trade,
            fee_rate=self.fee_rate,
            slippage_bps=self.slippage_bps,
            min_volume=self.min_volume,
        )
        if sizing.volume <= 0:
            return
        self.entry_price = entry
        self.planned_stop_distance = stop_distance
        self.stop_price = stop
        target_distance = stop_distance * self.risk_reward
        self.target_price = entry + target_distance if long else entry - target_distance
        if long:
            self.buy(entry, sizing.volume, stop=True)
        else:
            self.short(entry, sizing.volume, stop=True)

    def _place_exits(self, *, include_target: bool) -> None:
        volume = abs(self.pos)
        if volume <= 0:
            return
        if self.pos > 0:
            self.sell(self.stop_price, volume, stop=True)
            if include_target:
                self.sell(self.target_price, volume)
        else:
            self.cover(self.stop_price, volume, stop=True)
            if include_target:
                self.cover(self.target_price, volume)

    def on_trade(self, trade: TradeData) -> None:
        if trade.offset == Offset.OPEN:
            self.entry_price = float(trade.price)
            self.entry_volume = float(trade.volume)
            self.entry_direction = 1 if trade.direction == Direction.LONG else -1
            if self.entry_direction > 0:
                self.stop_price = self.entry_price - self.planned_stop_distance
                self.target_price = self.entry_price + self.planned_stop_distance * self.risk_reward
            else:
                self.stop_price = self.entry_price + self.planned_stop_distance
                self.target_price = self.entry_price - self.planned_stop_distance * self.risk_reward
            # The conservative engine checks this stop once more on the entry
            # bar. The target starts after that bar to avoid optimistic OHLC
            # path assumptions when entry and target are both touched.
            self._place_exits(include_target=False)
        elif self.entry_volume > 0:
            # Cancel the sibling stop/target immediately when either exit fills.
            self.cancel_all()
            volume = min(self.entry_volume, float(trade.volume))
            gross = (
                (float(trade.price) - self.entry_price)
                * volume
                * float(self.contract_size)
                * self.entry_direction
            )
            turnover = (
                self.entry_price + float(trade.price)
            ) * volume * float(self.contract_size)
            cost_rate = self.fee_rate + self.slippage_bps / 10_000.0
            self.strategy_equity = max(0.0, self.strategy_equity + gross - turnover * cost_rate)
            self.entry_volume = max(0.0, self.entry_volume - volume)

        if self.pos == 0 and trade.offset != Offset.OPEN:
            self.entry_price = 0.0
            self.stop_price = 0.0
            self.target_price = 0.0
            self.entry_volume = 0.0
            self.entry_direction = 0
            self.planned_stop_distance = 0.0
            self.cooldown_remaining = self.cooldown_bars
        self.put_event()

    def on_order(self, order: OrderData) -> None:
        pass

    def on_stop_order(self, stop_order: StopOrder) -> None:
        pass
