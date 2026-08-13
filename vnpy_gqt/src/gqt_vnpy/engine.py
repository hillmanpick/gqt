from __future__ import annotations

from vnpy.trader.object import BarData
from vnpy_ctastrategy.backtesting import BacktestingEngine


class ConservativeBarBacktestingEngine(BacktestingEngine):
    """Resolve ambiguous OHLC exits against the strategy, with stops first."""

    def new_bar(self, bar: BarData) -> None:
        self.bar = bar
        self.datetime = bar.datetime

        # First pass handles carried stops and stop-entry orders. A stop entry
        # can create its protective stop, which the second pass checks against
        # the same OHLC bar before any profit target is allowed to fill.
        self.cross_stop_order()
        self.cross_stop_order()
        self.cross_limit_order()
        self.strategy.on_bar(bar)

        self.update_daily_close(bar.close_price)
