import unittest

from services.strategy_diversity import CANONICAL_FAMILIES
from strategies import STRATEGY_REGISTRY, get_strategy


SAMPLE_HOLDINGS = [
    {
        "ticker": "SPY",
        "return_5d": 0.01,
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
        "return_5d": -0.01,
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
        "return_5d": 0.002,
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
        "return_5d": 0.004,
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

SECTOR_THEME_HOLDINGS = [
    {
        "ticker": ticker,
        "sector_group": group,
        "return_5d": ret5,
        "mom_20d": mom20,
        "mom_60d": mom60,
        "mom_252d": mom252,
        "hist_vol_20d": vol,
        "atr_pct": atr,
        "rsi_14": rsi,
    }
    for ticker, group, ret5, mom20, mom60, mom252, vol, atr, rsi in [
        ("SOXX", "semiconductors", 0.020, 0.080, 0.160, 0.300, 0.30, 0.024, 64.0),
        ("XSD", "semiconductors", 0.010, 0.050, 0.120, 0.220, 0.34, 0.028, 60.0),
        ("XLK", "tech_growth", 0.008, 0.040, 0.100, 0.180, 0.22, 0.018, 59.0),
        ("XLE", "cyclicals", 0.004, 0.020, 0.060, 0.110, 0.25, 0.021, 54.0),
    ]
]

SECTOR_THEME_REVERSION_HOLDINGS = [
    {
        "ticker": ticker,
        "sector_group": group,
        "return_5d": ret5,
        "mom_20d": mom20,
        "mom_60d": mom60,
        "hist_vol_20d": vol,
        "atr_pct": atr,
        "rsi_14": rsi,
    }
    for ticker, group, ret5, mom20, mom60, vol, atr, rsi in [
        ("QQQ", "tech_growth", 0.030, 0.050, 0.120, 0.22, 0.018, 62.0),
        ("XLK", "tech_growth", 0.025, 0.045, 0.110, 0.20, 0.016, 60.0),
        ("AIQ", "tech_growth", -0.035, 0.015, 0.080, 0.28, 0.026, 43.0),
        ("CIBR", "tech_growth", 0.010, 0.030, 0.075, 0.24, 0.020, 54.0),
        ("SOXX", "semiconductors", 0.040, 0.080, 0.160, 0.30, 0.024, 66.0),
        ("XSD", "semiconductors", -0.040, 0.010, 0.090, 0.34, 0.030, 41.0),
        ("PSI", "semiconductors", 0.020, 0.050, 0.130, 0.32, 0.026, 58.0),
        ("FTXL", "semiconductors", -0.015, 0.020, 0.100, 0.31, 0.025, 47.0),
        ("XLE", "cyclicals", 0.015, 0.025, 0.080, 0.24, 0.020, 56.0),
        ("XLI", "cyclicals", -0.020, 0.010, 0.060, 0.20, 0.018, 45.0),
        ("XLRE", "real_estate", -0.010, 0.005, 0.030, 0.18, 0.016, 46.0),
    ]
]

MACRO_RATE_HOLDINGS = [
    {
        "ticker": ticker,
        "return_5d": ret5,
        "mom_20d": mom20,
        "mom_60d": mom60,
        "hist_vol_20d": vol,
        "atr_pct": atr,
        "rsi_14": rsi,
    }
    for ticker, ret5, mom20, mom60, vol, atr, rsi in [
        ("SGOV", 0.001, 0.004, 0.012, 0.01, 0.002, 50.0),
        ("BSV", 0.001, 0.003, 0.009, 0.03, 0.005, 51.0),
        ("BND", -0.001, -0.004, -0.015, 0.06, 0.010, 45.0),
        ("IEF", -0.003, -0.012, -0.030, 0.08, 0.014, 42.0),
        ("TLT", -0.008, -0.025, -0.070, 0.14, 0.030, 38.0),
    ]
]

MACRO_CYCLE_HOLDINGS = [
    {
        "ticker": ticker,
        "mom_20d": mom20,
        "mom_60d": mom60,
        "mom_252d": mom252,
        "hist_vol_20d": vol,
        "atr_pct": atr,
        "rsi_14": rsi,
    }
    for ticker, mom20, mom60, mom252, vol, atr, rsi in [
        ("XLE", 0.070, 0.150, 0.250, 0.24, 0.022, 64.0),
        ("XLI", 0.055, 0.120, 0.210, 0.19, 0.018, 61.0),
        ("IWM", 0.060, 0.110, 0.180, 0.25, 0.024, 60.0),
        ("XLRE", 0.015, 0.035, 0.060, 0.18, 0.016, 54.0),
        ("SGOV", 0.004, 0.012, 0.045, 0.01, 0.002, 50.0),
        ("TLT", -0.010, -0.030, -0.070, 0.14, 0.030, 42.0),
    ]
]

INVERSE_HEDGE_HOLDINGS = [
    {
        "ticker": ticker,
        "return_5d": ret5,
        "mom_20d": mom20,
        "mom_60d": mom60,
        "hist_vol_20d": vol,
        "atr_pct": atr,
        "rsi_14": rsi,
        "close_price": close,
        "sma_200": sma200,
    }
    for ticker, ret5, mom20, mom60, vol, atr, rsi, close, sma200 in [
        ("SPY", -0.040, -0.060, -0.100, 0.20, 0.026, 32.0, 390.0, 420.0),
        ("QQQ", -0.060, -0.080, -0.140, 0.25, 0.033, 30.0, 350.0, 390.0),
        ("SQQQ", 0.120, 0.160, 0.220, 0.70, 0.080, 65.0, 24.0, 18.0),
        ("SPXS", 0.090, 0.110, 0.170, 0.62, 0.070, 62.0, 18.0, 14.0),
        ("SOXS", 0.160, 0.220, 0.300, 0.85, 0.100, 70.0, 36.0, 25.0),
        ("TECS", 0.110, 0.140, 0.210, 0.72, 0.085, 66.0, 20.0, 15.0),
    ]
]

LEVERAGED_AMPLIFIER_HOLDINGS = [
    {
        "ticker": ticker,
        "mom_20d": mom20,
        "mom_60d": mom60,
        "mom_252d": mom252,
        "hist_vol_20d": vol,
        "atr_pct": atr,
        "rsi_14": rsi,
        "close_price": close,
        "sma_200": sma200,
    }
    for ticker, mom20, mom60, mom252, vol, atr, rsi, close, sma200 in [
        ("QQQ", 0.050, 0.100, 0.220, 0.18, 0.016, 62.0, 440.0, 390.0),
        ("SOXX", 0.080, 0.160, 0.300, 0.26, 0.022, 64.0, 220.0, 185.0),
        ("XLK", 0.040, 0.090, 0.180, 0.20, 0.018, 60.0, 190.0, 165.0),
        ("SPY", 0.030, 0.070, 0.140, 0.14, 0.012, 58.0, 510.0, 470.0),
        ("TQQQ", 0.140, 0.240, 0.480, 0.55, 0.055, 68.0, 70.0, 55.0),
        ("SOXL", 0.200, 0.340, 0.650, 0.70, 0.070, 70.0, 32.0, 24.0),
        ("TECL", 0.120, 0.220, 0.420, 0.50, 0.050, 66.0, 85.0, 70.0),
        ("SPXL", 0.090, 0.180, 0.340, 0.42, 0.042, 63.0, 150.0, 130.0),
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
    if name == "sector_theme_relative_strength_lite":
        return SECTOR_THEME_HOLDINGS
    if name == "sector_theme_relative_value_reversion_lite":
        return SECTOR_THEME_REVERSION_HOLDINGS
    if name == "macro_rate_duration_lite":
        return MACRO_RATE_HOLDINGS
    if name == "macro_cyclical_inflation_rotation_lite":
        return MACRO_CYCLE_HOLDINGS
    if name == "inverse_equity_hedge_lite":
        return INVERSE_HEDGE_HOLDINGS
    if name == "leveraged_long_amplifier_lite":
        return LEVERAGED_AMPLIFIER_HOLDINGS
    return SAMPLE_HOLDINGS


if __name__ == "__main__":
    unittest.main()
