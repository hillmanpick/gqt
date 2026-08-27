from __future__ import annotations

import unittest

from gqt_vnpy.risk import floor_to_step, size_position


class RiskTests(unittest.TestCase):
    def test_floor_to_step(self) -> None:
        self.assertAlmostEqual(floor_to_step(0.0129, 0.001), 0.012)

    def test_position_respects_risk_and_margin(self) -> None:
        sizing = size_position(
            equity=1000,
            entry_price=60_000,
            stop_price=59_000,
            leverage=2,
            risk_fraction=0.005,
            max_margin=120,
            fee_rate=0.0005,
            slippage_bps=3,
            min_volume=0.001,
        )
        self.assertLessEqual(sizing.cash_risk, 5.0 + 1e-9)
        self.assertLessEqual(sizing.margin, 120 + 1e-9)
        self.assertGreater(sizing.volume, 0)

    def test_cost_is_not_multiplied_twice_by_leverage(self) -> None:
        sizing = size_position(
            equity=1000,
            entry_price=60_000,
            stop_price=59_000,
            leverage=3,
            risk_fraction=0.005,
            max_margin=120,
            fee_rate=0.0005,
            slippage_bps=0,
            min_volume=0.001,
        )
        self.assertAlmostEqual(sizing.round_trip_cost, sizing.notional * 0.001)


if __name__ == "__main__":
    unittest.main()
