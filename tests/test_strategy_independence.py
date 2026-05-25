import unittest
from datetime import date, timedelta

from services.strategy_independence import (
    build_strategy_independence_diagnostics_from_snapshots,
    build_strategy_independence_summary,
)


class StrategyIndependenceTest(unittest.TestCase):
    def test_summary_flags_correlated_variants_and_separates_inverse_pairs(self):
        days = [date(2026, 1, 1) + timedelta(days=idx) for idx in range(6)]
        base = [0.010, -0.004, 0.006, 0.012, -0.005, 0.003]
        return_series = {
            "momentum_lite_v1": [
                {"date": day.isoformat(), "return": value}
                for day, value in zip(days, base)
            ],
            "absolute_trend_following_lite": [
                {"date": day.isoformat(), "return": value * 0.9}
                for day, value in zip(days, base)
            ],
            "relative_value_reversion_lite": [
                {"date": day.isoformat(), "return": -value}
                for day, value in zip(days, base)
            ],
        }
        metadata = {
            "momentum_lite_v1": {"canonical_family": "momentum", "alpha_source": True},
            "absolute_trend_following_lite": {"canonical_family": "momentum", "alpha_source": True},
            "relative_value_reversion_lite": {"canonical_family": "mean_reversion", "alpha_source": True},
        }

        summary = build_strategy_independence_summary(
            return_series=return_series,
            strategy_metadata=metadata,
            min_overlap=4,
        )

        self.assertEqual(summary["status"], "available")
        self.assertEqual(summary["execution_authority"], "none")
        self.assertEqual(summary["target_weight_mutation"], "none")
        self.assertEqual(summary["alpha_strategy_count"], 3)
        self.assertLess(summary["effective_independent_alpha_count"], 3)
        self.assertEqual(summary["high_correlation_pairs"][0]["left"], "absolute_trend_following_lite")
        self.assertEqual(summary["high_correlation_pairs"][0]["right"], "momentum_lite_v1")
        self.assertTrue(summary["inverse_correlation_pairs"])
        self.assertIn(
            "high_strategy_correlation:absolute_trend_following_lite:momentum_lite_v1:1.0",
            summary["warnings"],
        )

    def test_replay_builds_diagnostics_without_lookahead(self):
        snapshots = [
            _snapshot(day, idx)
            for idx, day in enumerate(date(2026, 1, 1) + timedelta(days=i) for i in range(6))
        ]

        summary = build_strategy_independence_diagnostics_from_snapshots(
            snapshots=snapshots,
            strategy_names=["momentum_lite_v1", "equal_weight_benchmark"],
            min_overlap=2,
        )

        self.assertEqual(summary["contract_version"], "strategy_independence_diagnostics_v1")
        self.assertEqual(summary["status"], "available")
        self.assertEqual(summary["snapshot_count"], 6)
        self.assertIn("momentum_lite_v1", summary["correlation_matrix"])
        self.assertGreaterEqual(
            summary["replay_summary"]["ready_counts"]["momentum_lite_v1"],
            2,
        )

    def test_replay_rejects_future_feature_rows(self):
        snapshots = [
            _snapshot(date(2026, 1, 1), 0, feature_date=date(2026, 1, 2)),
            _snapshot(date(2026, 1, 2), 1),
        ]

        with self.assertRaisesRegex(AssertionError, "Feature leak detected"):
            build_strategy_independence_diagnostics_from_snapshots(
                snapshots=snapshots,
                strategy_names=["momentum_lite_v1"],
                min_overlap=2,
            )


def _snapshot(day: date, idx: int, *, feature_date: date | None = None) -> dict:
    rows = [
        _row("SPY", day, idx, 0.01 + idx * 0.001, feature_date=feature_date),
        _row("QQQ", day, idx, 0.015 + idx * 0.001, feature_date=feature_date),
        _row("IWM", day, idx, -0.002 + idx * 0.001, feature_date=feature_date),
    ]
    return {
        "packet_type": "yfinance_historical",
        "trading_date": day.isoformat(),
        "features": rows,
        "holdings": rows,
        "portfolio": {},
    }


def _row(
    ticker: str,
    day: date,
    idx: int,
    daily_return: float,
    *,
    feature_date: date | None = None,
) -> dict:
    feature_day = feature_date or day
    mom_base = {"SPY": 0.04, "QQQ": 0.06, "IWM": 0.01}[ticker]
    return {
        "ticker": ticker,
        "universe_role": "research",
        "price": 100 + idx,
        "close_price": 100 + idx,
        "daily_return_pct": daily_return,
        "return_1d": daily_return,
        "return_5d": daily_return * 2,
        "return_20d": mom_base + idx * 0.001,
        "return_60d": mom_base * 2 + idx * 0.001,
        "return_252d": mom_base * 3 + idx * 0.001,
        "mom_20d": mom_base + idx * 0.001,
        "mom_60d": mom_base * 2 + idx * 0.001,
        "mom_252d": mom_base * 3 + idx * 0.001,
        "sma_200": 95,
        "hist_vol_20d": 0.15 + idx * 0.001,
        "rsi_14": 55 + idx,
        "atr_pct": 0.02 + idx * 0.001,
        "feature_sources": [{
            "source": "yfinance_historical",
            "filled_fields": [
                "daily_return_pct",
                "return_1d",
                "return_5d",
                "mom_20d",
                "mom_60d",
                "mom_252d",
                "rsi_14",
                "atr_pct",
                "hist_vol_20d",
            ],
            "authority_by_field": {},
            "trading_date": feature_day.isoformat(),
        }],
    }


if __name__ == "__main__":
    unittest.main()

