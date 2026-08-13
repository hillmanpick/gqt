from __future__ import annotations

import unittest
from datetime import datetime, timezone

from gqt_vnpy.walkforward import build_folds, evaluate_fold


class WalkForwardTests(unittest.TestCase):
    def test_builds_non_overlapping_embargo(self) -> None:
        folds = build_folds(
            datetime(2022, 1, 1, tzinfo=timezone.utc),
            datetime(2024, 1, 1, tzinfo=timezone.utc),
            train_days=365,
            test_days=90,
            step_days=90,
            embargo_days=2,
        )
        self.assertGreaterEqual(len(folds), 3)
        self.assertEqual((folds[0].test_start - folds[0].train_end).days, 2)
        self.assertLessEqual(folds[0].test_end, folds[1].test_start)

    def test_rejects_weak_fold(self) -> None:
        passed, reasons = evaluate_fold(
            {
                "total_trade_count": 12,
                "profit_factor": 0.9,
                "expectancy": -1.0,
                "max_drawdown_percent": 22.0,
            },
            minimum_trades=30,
            minimum_profit_factor=1.05,
            minimum_expectancy=0,
            maximum_drawdown=0.15,
        )
        self.assertFalse(passed)
        self.assertEqual(len(reasons), 4)

    def test_zero_drawdown_is_not_treated_as_missing(self) -> None:
        passed, reasons = evaluate_fold(
            {
                "total_trade_count": 30,
                "profit_factor": 1.2,
                "expectancy": 0.1,
                "max_drawdown_percent": 0.0,
            },
            minimum_trades=30,
            minimum_profit_factor=1.05,
            minimum_expectancy=0,
            maximum_drawdown=0.15,
        )
        self.assertTrue(passed, reasons)

    def test_capped_all_wins_profit_factor_passes(self) -> None:
        passed, reasons = evaluate_fold(
            {
                "total_trade_count": 30,
                "profit_factor": 999.0,
                "profit_factor_capped": True,
                "expectancy": 0.1,
                "max_drawdown_percent": 0.0,
            },
            minimum_trades=30,
            minimum_profit_factor=1.05,
            minimum_expectancy=0,
            maximum_drawdown=0.15,
        )
        self.assertTrue(passed, reasons)


if __name__ == "__main__":
    unittest.main()
