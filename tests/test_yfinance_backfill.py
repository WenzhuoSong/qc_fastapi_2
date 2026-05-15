import unittest
from datetime import date, timedelta

try:
    import pandas as pd
except ImportError:  # pragma: no cover - local minimal env may omit pandas
    pd = None

from services.yfinance_backfill import (
    LOOKBACK_DAYS_DEFAULT,
    _lookback_days_for_ticker,
    _rows_needed_for_ticker,
    compute_feature_rows_from_frame,
)


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
        self.assertIsNotNone(last["rsi_14"])
        self.assertIsNotNone(last["atr_pct"])
        self.assertIsNotNone(last["bb_position"])
        self.assertGreater(last["dollar_volume"], 0)

    def test_missing_required_columns_returns_empty(self):
        frame = pd.DataFrame({"Close": [1.0, 2.0]})

        self.assertEqual(compute_feature_rows_from_frame("SPY", frame), [])


class YfinanceBackfillPlanningTest(unittest.TestCase):
    def test_new_ticker_uses_full_lookback(self):
        self.assertEqual(
            _lookback_days_for_ticker(None, LOOKBACK_DAYS_DEFAULT),
            LOOKBACK_DAYS_DEFAULT,
        )

    def test_existing_ticker_fetches_recent_gap_with_rolling_context(self):
        latest = date.today() - timedelta(days=2)

        self.assertEqual(_lookback_days_for_ticker(latest, 420), 289)

    def test_existing_ticker_writes_only_gap_and_small_refresh_window(self):
        latest = date(2026, 5, 10)
        rows = [
            {"trading_date": date(2026, 5, 1), "ticker": "SPY"},
            {"trading_date": date(2026, 5, 7), "ticker": "SPY"},
            {"trading_date": date(2026, 5, 10), "ticker": "SPY"},
            {"trading_date": date(2026, 5, 11), "ticker": "SPY"},
        ]

        needed = _rows_needed_for_ticker(rows, latest)

        self.assertEqual(needed, rows[1:])


if __name__ == "__main__":
    unittest.main()
