import unittest
from datetime import date, datetime, timezone

from services.historical_signal_replay import (
    FrozenSignal,
    assert_no_future_features,
    label_signal_outcomes,
    replay_historical_signals,
)


TICKERS = ["SPY", "QQQ", "TQQQ", "UVXY", "TECL", "SPXL", "SQQQ", "TECS", "BSV"]


def _rows_for_dates(dates):
    rows = []
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
    rsi = {
        "SPY": 55,
        "QQQ": 60,
        "TQQQ": 58,
        "UVXY": 40,
        "TECL": 52,
        "SPXL": 50,
        "SQQQ": 45,
        "TECS": 48,
        "BSV": 50,
    }
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
                "rsi_10": rsi[ticker],
                "return_1d": 0.0,
            })
    return rows


class HistoricalSignalReplayTest(unittest.TestCase):
    def test_assert_no_future_features_catches_leak(self):
        with self.assertRaisesRegex(AssertionError, "Feature leak detected"):
            assert_no_future_features(
                [{"ticker": "SPY", "trading_date": date(2020, 1, 2)}],
                date(2020, 1, 1),
            )

    def test_replay_starts_outcomes_from_next_trading_day(self):
        dates = [
            date(2020, 1, 1),
            date(2020, 1, 2),
            date(2020, 1, 3),
            date(2020, 1, 6),
            date(2020, 1, 7),
            date(2020, 1, 8),
        ]

        result = replay_historical_signals(
            _rows_for_dates(dates),
            strategy_names=["leveraged_etf_momentum_allocator"],
            horizons=(1, 5),
            end_date=date(2020, 1, 1),
            generated_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
        )

        self.assertTrue(result.signals)
        tqqq = next(
            signal
            for signal in result.signals
            if signal.ticker == "TQQQ" and signal.action == "increase"
        )
        self.assertEqual(tqqq.signal_date, date(2020, 1, 1))
        self.assertEqual(tqqq.tradable_from_date, date(2020, 1, 2))
        self.assertEqual(tqqq.feature_data_date, date(2020, 1, 1))
        self.assertEqual(tqqq.data_lag_days, 0)

        one_day = next(
            outcome
            for outcome in result.outcomes
            if outcome.signal_id == tqqq.signal_id and outcome.horizon_days == 1
        )
        self.assertEqual(one_day.label_date, date(2020, 1, 2))
        self.assertAlmostEqual(one_day.forward_return, 0.10)
        self.assertEqual(one_day.excess_calculation_method, "raw")
        self.assertTrue(one_day.hit)

    def test_replay_slices_full_history_without_lookahead(self):
        dates = [
            date(2020, 1, 1),
            date(2020, 1, 2),
            date(2020, 1, 3),
        ]

        result = replay_historical_signals(
            _rows_for_dates(dates),
            strategy_names=["leveraged_etf_momentum_allocator"],
            horizons=(1,),
            end_date=date(2020, 1, 1),
            generated_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
        )

        self.assertEqual(result.summary["signal_source"], "yfinance_replay")
        self.assertEqual(result.summary["reliability"], "historical_prior")
        self.assertGreater(result.summary["signals_generated"], 0)
        self.assertGreater(result.summary["outcomes_generated"], 0)

    def test_hedge_hit_uses_spy_stress_not_uvxy_return(self):
        signal = FrozenSignal(
            signal_id="sig-uvxy",
            signal_source="yfinance_replay",
            signal_date=date(2020, 1, 1),
            generated_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
            tradable_from_date=date(2020, 1, 2),
            strategy_id="test_strategy",
            strategy_version="1.0",
            ticker="UVXY",
            role="vol_hedge",
            branch="hedge_branch",
            action="hedge",
            signal_type="tail_risk_hedge",
            confidence=1.0,
            raw_score=1.0,
            normalized_score=1.0,
            max_reasonable_weight=0.03,
            risk_budget_cost=1.0,
            feature_data_date=date(2020, 1, 1),
            data_lag_days=0,
            feature_source="yfinance",
            feature_authority="daily_research",
            regime_at_signal="high_vol",
            vix_at_signal=None,
            evidence_contract_version="v1",
            diagnostics={},
            created_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
        )
        price_by_ticker = {
            "UVXY": {
                date(2020, 1, 1): 20,
                date(2020, 1, 2): 19,
            },
            "SPY": {
                date(2020, 1, 1): 100,
                date(2020, 1, 2): 97,
            },
        }

        outcomes = label_signal_outcomes(
            signal,
            price_by_ticker=price_by_ticker,
            trading_dates=[date(2020, 1, 1), date(2020, 1, 2)],
            horizons=(1,),
            created_at=datetime(2020, 1, 2, tzinfo=timezone.utc),
        )

        self.assertEqual(len(outcomes), 1)
        self.assertLess(outcomes[0].forward_return, 0)
        self.assertTrue(outcomes[0].hit)
        self.assertIn("spy_forward_return", outcomes[0].hit_definition)

    def test_watch_and_neutral_have_no_hit_label(self):
        signal = FrozenSignal(
            signal_id="sig-watch",
            signal_source="yfinance_replay",
            signal_date=date(2020, 1, 1),
            generated_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
            tradable_from_date=date(2020, 1, 2),
            strategy_id="test_strategy",
            strategy_version="1.0",
            ticker="SPY",
            role="core_market",
            branch=None,
            action="watch",
            signal_type="fallback",
            confidence=0.2,
            raw_score=0.2,
            normalized_score=0.2,
            max_reasonable_weight=0.0,
            risk_budget_cost=0.4,
            feature_data_date=date(2020, 1, 1),
            data_lag_days=0,
            feature_source="yfinance",
            feature_authority="daily_research",
            regime_at_signal="unknown",
            vix_at_signal=None,
            evidence_contract_version="v1",
            diagnostics={},
            created_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
        )

        outcomes = label_signal_outcomes(
            signal,
            price_by_ticker={
                "SPY": {
                    date(2020, 1, 1): 100,
                    date(2020, 1, 2): 101,
                }
            },
            trading_dates=[date(2020, 1, 1), date(2020, 1, 2)],
            horizons=(1,),
            created_at=datetime(2020, 1, 2, tzinfo=timezone.utc),
        )

        self.assertIsNone(outcomes[0].hit)
        self.assertIn("no_hit_label", outcomes[0].hit_definition)


if __name__ == "__main__":
    unittest.main()
