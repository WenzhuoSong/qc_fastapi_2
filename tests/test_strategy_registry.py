import unittest

from strategies import STRATEGY_REGISTRY, get_strategy


SAMPLE_HOLDINGS = [
    {
        "ticker": "SPY",
        "mom_20d": 0.02,
        "mom_60d": 0.05,
        "mom_252d": 0.12,
        "rsi_14": 55,
        "atr_pct": 0.011,
        "bb_position": 0.55,
        "hist_vol_20d": 0.14,
        "beta_vs_spy": 1.0,
        "unrealized_pnl_pct": 0.01,
    },
    {
        "ticker": "QQQ",
        "mom_20d": 0.04,
        "mom_60d": 0.08,
        "mom_252d": 0.20,
        "rsi_14": 68,
        "atr_pct": 0.018,
        "bb_position": 0.80,
        "hist_vol_20d": 0.22,
        "beta_vs_spy": 1.2,
        "unrealized_pnl_pct": 0.03,
    },
    {
        "ticker": "TLT",
        "mom_20d": -0.01,
        "mom_60d": 0.01,
        "mom_252d": 0.04,
        "rsi_14": 42,
        "atr_pct": 0.009,
        "bb_position": 0.25,
        "hist_vol_20d": 0.10,
        "beta_vs_spy": 0.2,
        "unrealized_pnl_pct": -0.01,
    },
    {
        "ticker": "GLD",
        "mom_20d": 0.01,
        "mom_60d": 0.03,
        "mom_252d": 0.08,
        "rsi_14": 50,
        "atr_pct": 0.012,
        "bb_position": 0.45,
        "hist_vol_20d": 0.12,
        "beta_vs_spy": 0.1,
        "unrealized_pnl_pct": 0.01,
    },
]


class StrategyRegistryTest(unittest.TestCase):
    def test_all_registered_strategies_have_data_requirements(self):
        for name in STRATEGY_REGISTRY:
            strategy = get_strategy(name)
            req = strategy.data_requirements()
            self.assertIn("required_fields", req)
            self.assertIn("min_required_coverage", req)

    def test_registered_strategies_produce_normalized_weights_with_sample_data(self):
        context = {
            "risk_params": {"max_single_position": 0.20, "min_cash_pct": 0.05},
            "direction_bias": "neutral",
            "confidence": 0.6,
        }
        for name in STRATEGY_REGISTRY:
            with self.subTest(strategy=name):
                strategy = get_strategy(name)
                self.assertTrue(strategy.data_readiness(SAMPLE_HOLDINGS)["ready"])
                scored = strategy.score(SAMPLE_HOLDINGS, context)
                weights = strategy.optimize(scored, context)
                self.assertIn("CASH", weights)
                self.assertAlmostEqual(sum(weights.values()), 1.0, places=4)

    def test_missing_required_fields_marks_strategy_not_ready(self):
        strategy = get_strategy("dual_momentum_rotation")
        readiness = strategy.data_readiness([
            {"ticker": "SPY", "mom_60d": 0.1},
            {"ticker": "QQQ", "mom_60d": 0.2},
        ])
        self.assertFalse(readiness["ready"])
        self.assertIn("mom_252d", readiness["missing_fields"])
        self.assertIn("hist_vol_20d", readiness["missing_fields"])

    def test_watchlist_tickers_are_not_strategy_eligible(self):
        strategy = get_strategy("momentum_lite_v1")
        readiness = strategy.data_readiness([
            {
                "ticker": "SPY",
                "universe_role": "core",
                "mom_20d": 0.02,
                "mom_60d": 0.05,
                "mom_252d": 0.12,
                "rsi_14": 55,
                "atr_pct": 0.011,
                "bb_position": 0.55,
                "hist_vol_20d": 0.14,
                "unrealized_pnl_pct": 0.01,
            },
            {
                "ticker": "SPXS",
                "universe_role": "watchlist",
                "mom_20d": 0.50,
                "mom_60d": 0.50,
                "mom_252d": 0.50,
                "rsi_14": 90,
                "atr_pct": 0.05,
                "bb_position": 0.9,
                "hist_vol_20d": 0.5,
                "unrealized_pnl_pct": 0.1,
            },
        ])

        self.assertEqual(readiness["eligible_tickers"], ["SPY"])


if __name__ == "__main__":
    unittest.main()
