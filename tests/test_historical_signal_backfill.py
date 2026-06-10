import unittest
from datetime import date, datetime, timezone

from services.historical_signal_backfill import build_historical_signal_backfill_plan


TICKERS = ["SPY", "QQQ", "TQQQ", "UVXY", "TECL", "SPXL", "SQQQ", "TECS", "BSV"]


def _rows_for_dates(dates):
    price_path = {
        "SPY": [100, 101, 99, 102, 103, 104],
        "QQQ": [100, 102, 100, 103, 104, 105],
        "TQQQ": [100, 110, 105, 116, 118, 120],
        "UVXY": [20, 19, 21, 18, 17, 16],
        "TECL": [80, 84, 82, 85, 86, 87],
        "SPXL": [90, 93, 91, 95, 96, 97],
        "SQQQ": [15, 14, 15, 13, 12, 11],
        "TECS": [12, 11, 12, 10, 9, 8],
        "BSV": [75, 75.1, 75.2, 75.25, 75.3, 75.35],
    }
    rows = []
    for idx, trading_date in enumerate(dates):
        for ticker in TICKERS:
            close = price_path[ticker][idx]
            rows.append({
                "ticker": ticker,
                "trading_date": trading_date,
                "source": "yfinance",
                "close_price": close,
                "adj_close_price": close,
                "sma_20": close * 0.98,
                "sma_200": close * 0.90,
                "rsi_10": 55,
                "rsi_14": 55,
                "atr_pct": 0.02,
                "hist_vol_20d": 0.18,
                "return_1d": 0.0,
                "return_5d": 0.03 + idx * 0.001,
                "return_20d": 0.08 + idx * 0.001,
                "return_60d": 0.15 + idx * 0.001,
                "return_252d": 0.22 + idx * 0.001,
            })
    return rows


class HistoricalSignalBackfillTest(unittest.TestCase):
    def test_backfill_plan_marks_historical_prior_and_uses_recent_window(self):
        dates = [
            date(2020, 1, 1),
            date(2020, 1, 2),
            date(2020, 1, 3),
            date(2020, 1, 6),
            date(2020, 1, 7),
            date(2020, 1, 8),
        ]

        plan = build_historical_signal_backfill_plan(
            _rows_for_dates(dates),
            strategy_names=["leveraged_etf_momentum_allocator"],
            horizons=(1,),
            max_dates=2,
            generated_at=datetime(2020, 1, 8, tzinfo=timezone.utc),
        )

        self.assertEqual(plan.selected_start_date, date(2020, 1, 7))
        self.assertEqual(plan.selected_end_date, date(2020, 1, 8))
        self.assertGreater(len(plan.replay.signals), 0)
        self.assertGreater(len(plan.replay.outcomes), 0)
        self.assertEqual(plan.summary["execution_authority"], "none")
        self.assertEqual(plan.summary["target_weight_mutation"], "none")
        self.assertEqual(plan.summary["source_bucket"], "historical_prior")
        self.assertEqual(plan.summary["reliability"], "historical_prior")
        self.assertTrue(plan.summary["requires_live_confirmation"])

        self.assertTrue(all(signal.signal_source == "yfinance_replay" for signal in plan.replay.signals))
        self.assertTrue(
            all(
                (signal.diagnostics or {}).get("source_bucket") == "historical_prior"
                for signal in plan.replay.signals
            )
        )
        self.assertTrue(
            all(outcome.label_date > outcome.signal_date for outcome in plan.replay.outcomes)
        )

    def test_backfill_maps_return_features_to_momentum_aliases(self):
        dates = [
            date(2020, 1, 1),
            date(2020, 1, 2),
            date(2020, 1, 3),
            date(2020, 1, 6),
            date(2020, 1, 7),
            date(2020, 1, 8),
        ]

        plan = build_historical_signal_backfill_plan(
            _rows_for_dates(dates),
            strategy_names=["momentum_lite_v1"],
            horizons=(1,),
            max_dates=2,
            generated_at=datetime(2020, 1, 8, tzinfo=timezone.utc),
        )

        self.assertGreater(len(plan.replay.signals), 0)
        self.assertNotIn("momentum_lite_v1:not_ready", plan.summary["skipped"])


if __name__ == "__main__":
    unittest.main()
