import unittest

from services.strategy_diversity import CANONICAL_FAMILIES
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

LEVERAGED_ALLOCATOR_HOLDINGS = [
    {
        "ticker": ticker,
        "close_price": close,
        "price": close,
        "sma_20": sma20,
        "sma_200": sma200,
        "rsi_10": rsi,
        "return_1d": ret,
        "hist_vol_20d": 0.25,
    }
    for ticker, close, sma20, sma200, rsi, ret in [
        ("SPY", 500.0, 495.0, 450.0, 55.0, 0.002),
        ("QQQ", 430.0, 420.0, 380.0, 65.0, 0.003),
        ("TQQQ", 70.0, 68.0, 55.0, 64.0, 0.009),
        ("UVXY", 20.0, 22.0, 30.0, 40.0, -0.010),
        ("TECL", 85.0, 80.0, 70.0, 58.0, 0.006),
        ("SPXL", 150.0, 145.0, 130.0, 57.0, 0.005),
        ("SQQQ", 18.0, 19.0, 24.0, 45.0, -0.008),
        ("TECS", 12.0, 13.0, 18.0, 48.0, -0.006),
        ("BSV", 76.0, 75.8, 75.0, 52.0, 0.0005),
    ]
]


class StrategyRegistryTest(unittest.TestCase):
    def test_all_registered_strategies_have_data_requirements(self):
        for name in STRATEGY_REGISTRY:
            strategy = get_strategy(name)
            req = strategy.data_requirements()
            self.assertIn("required_fields", req)
            self.assertIn("min_required_coverage", req)

    def test_all_registered_strategies_have_english_strategy_cards(self):
        for name in STRATEGY_REGISTRY:
            strategy = get_strategy(name)
            card = strategy.strategy_card()
            self.assertEqual(card["name"], strategy.name)
            self.assertTrue(card["family"])
            self.assertIn(card["canonical_family"], CANONICAL_FAMILIES)
            self.assertIsInstance(card["alpha_source"], bool)
            self.assertTrue(card["core_idea"])
            self.assertTrue(card["agent_guidance"])
            self.assertIsInstance(card["failure_modes"], list)

    def test_benchmark_like_strategies_are_not_alpha_sources(self):
        self.assertFalse(get_strategy("equal_weight_benchmark").strategy_card()["alpha_source"])
        self.assertFalse(get_strategy("risk_parity_lite").strategy_card()["alpha_source"])
        self.assertTrue(get_strategy("momentum_lite_v1").strategy_card()["alpha_source"])

    def test_registered_strategies_produce_normalized_weights_with_sample_data(self):
        context = {
            "risk_params": {"max_single_position": 0.20, "min_cash_pct": 0.05},
            "direction_bias": "neutral",
            "confidence": 0.6,
        }
        for name in STRATEGY_REGISTRY:
            with self.subTest(strategy=name):
                strategy = get_strategy(name)
                holdings = _sample_holdings_for_strategy(name)
                self.assertTrue(strategy.data_readiness(holdings)["ready"])
                scored = strategy.score(holdings, context)
                weights = strategy.optimize(scored, context)
                self.assertIn("CASH", weights)
                self.assertAlmostEqual(sum(weights.values()), 1.0, places=4)

    def test_leveraged_allocator_ports_qc_branch_logic_as_playground_only(self):
        strategy = get_strategy("leveraged_etf_momentum_allocator")

        scored = strategy.score(LEVERAGED_ALLOCATOR_HOLDINGS, {})
        weights = strategy.optimize(scored, {})

        self.assertEqual(scored[0].ticker, "TQQQ")
        self.assertEqual(weights, {"TQQQ": 1.0, "CASH": 0.0})
        self.assertIn("playground-only", strategy.agent_guidance.lower())

    def test_missing_required_fields_marks_strategy_not_ready(self):
        strategy = get_strategy("dual_momentum_rotation")
        readiness = strategy.data_readiness([
            {"ticker": "SPY", "mom_60d": 0.1},
            {"ticker": "QQQ", "mom_60d": 0.2},
        ])
        self.assertFalse(readiness["ready"])
        self.assertIn("mom_252d", readiness["missing_fields"])
        self.assertIn("hist_vol_20d", readiness["missing_fields"])

    def test_hedge_tickers_are_not_strategy_eligible(self):
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
                "universe_role": "hedge",
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


def _sample_holdings_for_strategy(name: str) -> list[dict]:
    if name == "leveraged_etf_momentum_allocator":
        return LEVERAGED_ALLOCATOR_HOLDINGS
    return SAMPLE_HOLDINGS


if __name__ == "__main__":
    unittest.main()
