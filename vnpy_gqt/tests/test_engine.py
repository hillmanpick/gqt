from __future__ import annotations

import unittest
from datetime import datetime, timedelta

from gqt_vnpy.engine import ConservativeBarBacktestingEngine
from vnpy.trader.constant import Exchange, Interval, Offset
from vnpy.trader.object import BarData, TradeData
from vnpy_ctastrategy import CtaTemplate


class AmbiguousExitStrategy(CtaTemplate):
    author = "test"

    def __init__(self, cta_engine, strategy_name: str, vt_symbol: str, setting: dict) -> None:
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)
        self.entry_submitted = False

    def on_init(self) -> None:
        pass

    def on_bar(self, bar: BarData) -> None:
        if not self.entry_submitted:
            self.buy(100.0, 1.0, stop=True)
            self.entry_submitted = True
        elif self.pos > 0:
            self.sell(95.0, 1.0, stop=True)
            self.sell(110.0, 1.0)

    def on_trade(self, trade: TradeData) -> None:
        if trade.offset == Offset.CLOSE:
            self.cancel_all()


class ConservativeEngineTests(unittest.TestCase):
    def test_stop_wins_when_one_bar_touches_stop_and_target(self) -> None:
        engine = ConservativeBarBacktestingEngine()
        start = datetime(2024, 1, 1)
        engine.set_parameters(
            vt_symbol="BTCUSDT_GQT_15M.GLOBAL",
            interval=Interval.MINUTE,
            start=start,
            end=start + timedelta(days=1),
            rate=0,
            slippage=0,
            size=1,
            pricetick=0.1,
            capital=1000,
        )
        engine.add_strategy(AmbiguousExitStrategy, {})
        engine.strategy.inited = True
        engine.strategy.trading = True

        bars = [
            self._bar(start, 99, 99, 99, 99),
            self._bar(start + timedelta(minutes=15), 99, 101, 99, 100),
            self._bar(start + timedelta(minutes=30), 100, 115, 90, 105),
        ]
        for bar in bars:
            engine.new_bar(bar)

        trades = engine.get_all_trades()
        self.assertEqual(len(trades), 2)
        self.assertEqual(trades[0].offset, Offset.OPEN)
        self.assertEqual(trades[1].offset, Offset.CLOSE)
        self.assertEqual(trades[1].price, 95.0)
        self.assertEqual(engine.strategy.pos, 0)
        self.assertFalse(engine.active_limit_orders)
        self.assertFalse(engine.active_stop_orders)

    @staticmethod
    def _bar(dt: datetime, open_: float, high: float, low: float, close: float) -> BarData:
        return BarData(
            symbol="BTCUSDT_GQT_15M",
            exchange=Exchange.GLOBAL,
            datetime=dt,
            interval=Interval.MINUTE,
            volume=1,
            open_price=open_,
            high_price=high,
            low_price=low,
            close_price=close,
            gateway_name="TEST",
        )


if __name__ == "__main__":
    unittest.main()
