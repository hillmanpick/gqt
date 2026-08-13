from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from gqt_vnpy.backtest import _closed_trade_statistics, _database_bounds


class BacktestAdapterTests(unittest.TestCase):
    def test_database_bounds_cover_exact_utc_day(self) -> None:
        from vnpy.trader.database import DB_TZ

        start = datetime(2024, 1, 3, tzinfo=timezone.utc)
        end = start + timedelta(days=1)
        query_start, query_end = _database_bounds(start, end, "15m")

        expected = [
            (start + timedelta(minutes=15 * index)).astimezone(DB_TZ).replace(tzinfo=None)
            for index in range(96)
        ]
        selected = [item for item in expected if query_start <= item <= query_end]
        self.assertEqual(len(selected), 96)
        self.assertEqual(query_start, expected[0])
        self.assertEqual(query_end, expected[-1])

    def test_open_position_is_settled_at_final_close(self) -> None:
        from vnpy.trader.constant import Direction, Exchange, Offset
        from vnpy.trader.object import TradeData

        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        entry = TradeData(
            symbol="BTCUSDT_GQT_15M",
            exchange=Exchange.GLOBAL,
            orderid="1",
            tradeid="1",
            direction=Direction.LONG,
            offset=Offset.OPEN,
            price=100.0,
            volume=1.0,
            datetime=start,
            gateway_name="TEST",
        )
        result = _closed_trade_statistics(
            [entry],
            start=start,
            capital=1000.0,
            contract_size=1.0,
            fee_rate=0.001,
            slippage_price=0.5,
            funding_rate_8h_stress=0.001,
            final_price=90.0,
            final_datetime=start + timedelta(hours=8),
        )

        self.assertEqual(result["total_trade_count"], 1)
        self.assertEqual(result["forced_exit_count"], 1)
        self.assertEqual(result["unclosed_position_count"], 0)
        self.assertAlmostEqual(result["cost_adjusted_total_net_pnl"], -11.29)


if __name__ == "__main__":
    unittest.main()
