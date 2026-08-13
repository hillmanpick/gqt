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


if __name__ == "__main__":
    unittest.main()
