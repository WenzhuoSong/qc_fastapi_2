import unittest

try:
    import pandas as pd
except ImportError:  # pragma: no cover - local minimal env may omit pandas
    pd = None

from services.yfinance_backfill import compute_feature_rows_from_frame


@unittest.skipIf(pd is None, "pandas not installed")
class YfinanceBackfillTest(unittest.TestCase):
    def test_compute_feature_rows_from_frame(self):
        dates = pd.date_range("2026-01-01", periods=260, freq="B")
        frame = pd.DataFrame({
            "Open": [100 + i for i in range(260)],
            "High": [101 + i for i in range(260)],
            "Low": [99 + i for i in range(260)],
            "Close": [100 + i for i in range(260)],
            "Adj Close": [100 + i for i in range(260)],
            "Volume": [1000 + i for i in range(260)],
        }, index=dates)

        rows = compute_feature_rows_from_frame("SPY", frame)
        last = rows[-1]

        self.assertEqual(len(rows), 260)
        self.assertEqual(last["ticker"], "SPY")
        self.assertEqual(last["data_quality_flag"], "ok")
        self.assertIsNotNone(last["return_1d"])
        self.assertIsNotNone(last["return_5d"])
        self.assertIsNotNone(last["return_252d"])
        self.assertIsNotNone(last["sma_200"])
        self.assertIsNotNone(last["hist_vol_20d"])
        self.assertGreater(last["dollar_volume"], 0)

    def test_missing_required_columns_returns_empty(self):
        frame = pd.DataFrame({"Close": [1.0, 2.0]})

        self.assertEqual(compute_feature_rows_from_frame("SPY", frame), [])


if __name__ == "__main__":
    unittest.main()
