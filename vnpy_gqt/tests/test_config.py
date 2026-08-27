from __future__ import annotations

import unittest

from gqt_vnpy.config import ResearchConfig


class ResearchConfigTests(unittest.TestCase):
    def test_default_is_research_only_and_low_leverage(self) -> None:
        config = ResearchConfig()
        config.validate()
        self.assertEqual(config.symbols, ("BTCUSDT", "ETHUSDT"))
        self.assertLessEqual(config.leverage, 3)

    def test_rejects_live_mode(self) -> None:
        with self.assertRaisesRegex(ValueError, "mode=research"):
            ResearchConfig.from_dict({"mode": "live"})

    def test_rejects_other_symbols(self) -> None:
        with self.assertRaisesRegex(ValueError, "unsupported symbols"):
            ResearchConfig.from_dict({"symbols": ["SOLUSDT"]})

    def test_rejects_high_leverage(self) -> None:
        with self.assertRaisesRegex(ValueError, "between 1 and 3"):
            ResearchConfig.from_dict({"leverage": 100})

    def test_partial_config_keeps_default_symbols(self) -> None:
        config = ResearchConfig.from_dict({"leverage": 1})
        self.assertEqual(config.symbols, ("BTCUSDT", "ETHUSDT"))

    def test_candidate_parameters_are_explicit_and_validated(self) -> None:
        config = ResearchConfig.from_dict(
            {
                "candidate_strategies": [
                    {"fast_window": 16, "slow_window": 64, "breakout_window": 32}
                ]
            }
        )
        self.assertEqual(len(config.candidate_strategies), 1)
        self.assertEqual(config.candidate_strategies[0].fast_window, 16)

    def test_rejects_unknown_strategy_parameter(self) -> None:
        with self.assertRaisesRegex(ValueError, "unknown strategy settings"):
            ResearchConfig.from_dict({"strategy": {"lookahead": 1}})


if __name__ == "__main__":
    unittest.main()
