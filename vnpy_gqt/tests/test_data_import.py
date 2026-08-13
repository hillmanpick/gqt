from __future__ import annotations

import unittest

from gqt_vnpy.data_import import freqtrade_filename, storage_symbol


class DataImportTests(unittest.TestCase):
    def test_freqtrade_filename(self) -> None:
        self.assertEqual(
            freqtrade_filename("BTCUSDT", "15m"),
            "BTC_USDT_USDT-15m-futures.feather",
        )

    def test_timeframes_use_distinct_storage_symbols(self) -> None:
        symbols = {storage_symbol("ETHUSDT", timeframe) for timeframe in ("15m", "1h", "4h")}
        self.assertEqual(len(symbols), 3)

    def test_rejects_unsupported_symbol(self) -> None:
        with self.assertRaisesRegex(ValueError, "unsupported symbol"):
            storage_symbol("SOLUSDT", "15m")


if __name__ == "__main__":
    unittest.main()
