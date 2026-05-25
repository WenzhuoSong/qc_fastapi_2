import unittest
from datetime import date
from pathlib import Path

from services.knowledge_base import build_knowledge_context
from services.strategy_evidence import build_evidence_cards
from strategies import STRATEGY_REGISTRY, get_strategy


def defensive_holding(ticker: str, vol: float, atr: float, mom20: float, mom60: float, rsi: float = 52.0) -> dict:
    return {
        "ticker": ticker,
        "hist_vol_20d": vol,
        "atr_pct": atr,
        "mom_20d": mom20,
        "mom_60d": mom60,
        "rsi_14": rsi,
        "close_price": 100.0,
        "sma_200": 99.0,
    }


def leveraged_amplifier_holdings() -> list[dict]:
    return [
        {"ticker": "QQQ", "mom_20d": 0.050, "mom_60d": 0.100, "mom_252d": 0.220, "hist_vol_20d": 0.18, "atr_pct": 0.016, "rsi_14": 62.0, "close_price": 440.0, "sma_200": 390.0},
        {"ticker": "SOXX", "mom_20d": 0.080, "mom_60d": 0.160, "mom_252d": 0.300, "hist_vol_20d": 0.26, "atr_pct": 0.022, "rsi_14": 64.0, "close_price": 220.0, "sma_200": 185.0},
        {"ticker": "XLK", "mom_20d": 0.040, "mom_60d": 0.090, "mom_252d": 0.180, "hist_vol_20d": 0.20, "atr_pct": 0.018, "rsi_14": 60.0, "close_price": 190.0, "sma_200": 165.0},
        {"ticker": "SPY", "mom_20d": 0.030, "mom_60d": 0.070, "mom_252d": 0.140, "hist_vol_20d": 0.14, "atr_pct": 0.012, "rsi_14": 58.0, "close_price": 510.0, "sma_200": 470.0},
        {"ticker": "TQQQ", "mom_20d": 0.140, "mom_60d": 0.240, "mom_252d": 0.480, "hist_vol_20d": 0.55, "atr_pct": 0.055, "rsi_14": 68.0},
        {"ticker": "SOXL", "mom_20d": 0.200, "mom_60d": 0.340, "mom_252d": 0.650, "hist_vol_20d": 0.70, "atr_pct": 0.070, "rsi_14": 70.0},
        {"ticker": "TECL", "mom_20d": 0.120, "mom_60d": 0.220, "mom_252d": 0.420, "hist_vol_20d": 0.50, "atr_pct": 0.050, "rsi_14": 66.0},
        {"ticker": "SPXL", "mom_20d": 0.090, "mom_60d": 0.180, "mom_252d": 0.340, "hist_vol_20d": 0.42, "atr_pct": 0.042, "rsi_14": 63.0},
    ]


class AlphaStrategyPluginTests(unittest.TestCase):
    def test_new_alpha_families_are_registered_and_default_playground_enabled(self):
        self.assertIn("absolute_trend_following_lite", STRATEGY_REGISTRY)
        self.assertIn("seasonality_month_end_lite", STRATEGY_REGISTRY)
        self.assertIn("sector_theme_relative_strength_lite", STRATEGY_REGISTRY)
        self.assertIn("macro_rate_duration_lite", STRATEGY_REGISTRY)
        self.assertIn("carry_cash_proxy_lite", STRATEGY_REGISTRY)
        self.assertIn("volatility_hedge_lite", STRATEGY_REGISTRY)
        self.assertIn("inverse_equity_hedge_lite", STRATEGY_REGISTRY)
        self.assertIn("leveraged_long_amplifier_lite", STRATEGY_REGISTRY)
        self.assertIn("relative_value_reversion_lite", STRATEGY_REGISTRY)
        self.assertIn("defensive_quality_rotation_lite", STRATEGY_REGISTRY)
        playground_source = Path("services/playground.py").read_text()
        self.assertIn('"absolute_trend_following_lite"', playground_source)
        self.assertIn('"seasonality_month_end_lite"', playground_source)
        self.assertIn('"sector_theme_relative_strength_lite"', playground_source)
        self.assertIn('"macro_rate_duration_lite"', playground_source)
        self.assertIn('"carry_cash_proxy_lite"', playground_source)
        self.assertIn('"volatility_hedge_lite"', playground_source)
        self.assertIn('"inverse_equity_hedge_lite"', playground_source)
        self.assertIn('"leveraged_long_amplifier_lite"', playground_source)
        self.assertIn('"relative_value_reversion_lite"', playground_source)
        self.assertIn('"defensive_quality_rotation_lite"', playground_source)

        trend = get_strategy("absolute_trend_following_lite")
        seasonality = get_strategy("seasonality_month_end_lite")
        sector_theme = get_strategy("sector_theme_relative_strength_lite")
        macro_rate = get_strategy("macro_rate_duration_lite")
        carry = get_strategy("carry_cash_proxy_lite")
        hedge = get_strategy("volatility_hedge_lite")
        inverse_hedge = get_strategy("inverse_equity_hedge_lite")
        leveraged_amplifier = get_strategy("leveraged_long_amplifier_lite")
        relative_value = get_strategy("relative_value_reversion_lite")
        defensive_quality = get_strategy("defensive_quality_rotation_lite")
        self.assertEqual(trend.strategy_card()["canonical_family"], "momentum")
        self.assertEqual(seasonality.strategy_card()["canonical_family"], "seasonality_flow")
        self.assertEqual(sector_theme.strategy_card()["canonical_family"], "momentum")
        self.assertEqual(macro_rate.strategy_card()["canonical_family"], "carry_or_cash_proxy")
        self.assertEqual(carry.strategy_card()["canonical_family"], "carry_or_cash_proxy")
        self.assertEqual(hedge.strategy_card()["canonical_family"], "volatility_hedge")
        self.assertEqual(inverse_hedge.strategy_card()["canonical_family"], "event_risk_avoidance")
        self.assertEqual(leveraged_amplifier.strategy_card()["canonical_family"], "momentum")
        self.assertEqual(relative_value.strategy_card()["canonical_family"], "mean_reversion")
        self.assertEqual(defensive_quality.strategy_card()["canonical_family"], "low_vol_defensive")
        self.assertTrue(trend.strategy_card()["alpha_source"])
        self.assertTrue(seasonality.strategy_card()["alpha_source"])
        self.assertTrue(sector_theme.strategy_card()["alpha_source"])
        self.assertTrue(macro_rate.strategy_card()["alpha_source"])
        self.assertTrue(carry.strategy_card()["alpha_source"])
        self.assertTrue(hedge.strategy_card()["alpha_source"])
        self.assertTrue(inverse_hedge.strategy_card()["alpha_source"])
        self.assertTrue(leveraged_amplifier.strategy_card()["alpha_source"])
        self.assertTrue(relative_value.strategy_card()["alpha_source"])
        self.assertTrue(defensive_quality.strategy_card()["alpha_source"])

    def test_absolute_trend_following_scores_non_leveraged_trend_with_cash_buffer(self):
        strategy = get_strategy("absolute_trend_following_lite")
        holdings = [
            {
                "ticker": "SPY",
                "mom_60d": 0.060,
                "mom_252d": 0.140,
                "hist_vol_20d": 0.14,
                "atr_pct": 0.012,
                "close_price": 510.0,
                "sma_200": 470.0,
            },
            {
                "ticker": "QQQ",
                "mom_60d": 0.090,
                "mom_252d": 0.220,
                "hist_vol_20d": 0.22,
                "atr_pct": 0.019,
                "close_price": 440.0,
                "sma_200": 390.0,
            },
            {
                "ticker": "IWM",
                "mom_60d": -0.020,
                "mom_252d": 0.020,
                "hist_vol_20d": 0.26,
                "atr_pct": 0.023,
                "close_price": 210.0,
                "sma_200": 215.0,
            },
            defensive_holding("SGOV", 0.01, 0.002, 0.003, 0.010, rsi=50.0) | {"mom_252d": 0.045},
            defensive_holding("IEF", 0.05, 0.008, 0.004, 0.015, rsi=51.0) | {"mom_252d": 0.030},
        ]
        context = {"regime": "trending_bull", "risk_params": {"max_single_position": 0.20}}

        scored = strategy.score(holdings, context)
        weights = strategy.optimize(scored, context)

        self.assertGreater(len(scored), 0)
        self.assertIn(scored[0].ticker, {"SPY", "QQQ"})
        self.assertLessEqual(sum(weight for ticker, weight in weights.items() if ticker != "CASH"), 0.30)
        self.assertLessEqual(max(weight for ticker, weight in weights.items() if ticker != "CASH"), 0.08)
        self.assertGreaterEqual(weights["CASH"], 0.70)

    def test_absolute_trend_following_builds_non_fallback_evidence_cards(self):
        strategy = get_strategy("absolute_trend_following_lite")
        holdings = [
            {"ticker": "SPY", "mom_60d": 0.060, "mom_252d": 0.140, "hist_vol_20d": 0.14, "atr_pct": 0.012, "close_price": 510.0, "sma_200": 470.0},
            {"ticker": "QQQ", "mom_60d": 0.090, "mom_252d": 0.220, "hist_vol_20d": 0.22, "atr_pct": 0.019, "close_price": 440.0, "sma_200": 390.0},
            {"ticker": "IWM", "mom_60d": -0.020, "mom_252d": 0.020, "hist_vol_20d": 0.26, "atr_pct": 0.023, "close_price": 210.0, "sma_200": 215.0},
            defensive_holding("SGOV", 0.01, 0.002, 0.003, 0.010, rsi=50.0) | {"mom_252d": 0.045},
            defensive_holding("IEF", 0.05, 0.008, 0.004, 0.015, rsi=51.0) | {"mom_252d": 0.030},
        ]
        scored = strategy.score(holdings, {"regime": "trending_bull"})
        knowledge = build_knowledge_context(
            tickers=[item.ticker for item in scored],
            strategy_names=["absolute_trend_following_lite"],
            regime="trending_bull",
            max_assets=8,
        )

        cards = build_evidence_cards(
            strategy=strategy,
            scored=scored,
            knowledge_context=knowledge,
            mode="playground",
        )

        self.assertTrue(cards)
        self.assertTrue(any(card.action in {"increase", "de_risk", "watch", "neutral"} for card in cards))
        self.assertFalse(any("missing_compatibility_mapping" in card.reason for card in cards))
        self.assertFalse(any("missing_required_safety_field" in card.reason for card in cards))

    def test_seasonality_month_end_scores_only_small_month_end_sleeve(self):
        strategy = get_strategy("seasonality_month_end_lite")
        holdings = [
            {"ticker": "SPY", "mom_20d": 0.025, "mom_60d": 0.060, "hist_vol_20d": 0.14, "atr_pct": 0.012},
            {"ticker": "QQQ", "mom_20d": 0.040, "mom_60d": 0.090, "hist_vol_20d": 0.23, "atr_pct": 0.020},
            {"ticker": "IWM", "mom_20d": -0.010, "mom_60d": 0.010, "hist_vol_20d": 0.25, "atr_pct": 0.024},
        ]
        context = {
            "regime": "risk_on",
            "signal_date": date(2026, 5, 29),
            "risk_params": {"max_single_position": 0.20},
        }

        scored = strategy.score(holdings, context)
        weights = strategy.optimize(scored, context)

        self.assertGreater(len(scored), 0)
        self.assertIn(scored[0].ticker, {"SPY", "QQQ"})
        self.assertEqual(scored[0].raw_factors["calendar_branch"], "month_end_flow")
        self.assertLessEqual(sum(weight for ticker, weight in weights.items() if ticker != "CASH"), 0.12)
        self.assertLessEqual(max(weight for ticker, weight in weights.items() if ticker != "CASH"), 0.04)
        self.assertGreaterEqual(weights["CASH"], 0.88)

    def test_seasonality_month_end_stays_cash_outside_window(self):
        strategy = get_strategy("seasonality_month_end_lite")
        holdings = [
            {"ticker": "SPY", "mom_20d": 0.050, "mom_60d": 0.100, "hist_vol_20d": 0.12, "atr_pct": 0.010},
            {"ticker": "QQQ", "mom_20d": 0.060, "mom_60d": 0.120, "hist_vol_20d": 0.18, "atr_pct": 0.016},
        ]
        scored = strategy.score(holdings, {"regime": "risk_on", "signal_date": date(2026, 5, 15)})

        self.assertTrue(scored)
        self.assertEqual(strategy.optimize(scored, {}), {"CASH": 1.0})

    def test_seasonality_month_end_builds_non_fallback_evidence_cards(self):
        strategy = get_strategy("seasonality_month_end_lite")
        holdings = [
            {"ticker": "SPY", "mom_20d": 0.025, "mom_60d": 0.060, "hist_vol_20d": 0.14, "atr_pct": 0.012},
            {"ticker": "QQQ", "mom_20d": 0.040, "mom_60d": 0.090, "hist_vol_20d": 0.23, "atr_pct": 0.020},
            {"ticker": "IWM", "mom_20d": -0.010, "mom_60d": 0.010, "hist_vol_20d": 0.25, "atr_pct": 0.024},
        ]
        scored = strategy.score(holdings, {"regime": "risk_on", "signal_date": date(2026, 5, 29)})
        knowledge = build_knowledge_context(
            tickers=[item.ticker for item in scored],
            strategy_names=["seasonality_month_end_lite"],
            regime="risk_on",
            max_assets=8,
        )

        cards = build_evidence_cards(
            strategy=strategy,
            scored=scored,
            knowledge_context=knowledge,
            mode="playground",
        )

        self.assertTrue(cards)
        self.assertTrue(any(card.action in {"increase", "watch", "neutral"} for card in cards))
        self.assertFalse(any("missing_compatibility_mapping" in card.reason for card in cards))
        self.assertFalse(any("missing_required_safety_field" in card.reason for card in cards))

    def test_sector_theme_relative_strength_scores_leadership_with_group_caps(self):
        strategy = get_strategy("sector_theme_relative_strength_lite")
        holdings = [
            {"ticker": "SOXX", "sector_group": "semiconductors", "mom_20d": 0.090, "mom_60d": 0.180, "mom_252d": 0.320, "hist_vol_20d": 0.30, "atr_pct": 0.025, "rsi_14": 66.0},
            {"ticker": "XSD", "sector_group": "semiconductors", "mom_20d": 0.060, "mom_60d": 0.140, "mom_252d": 0.260, "hist_vol_20d": 0.34, "atr_pct": 0.028, "rsi_14": 64.0},
            {"ticker": "XLK", "sector_group": "tech_growth", "mom_20d": 0.040, "mom_60d": 0.100, "mom_252d": 0.210, "hist_vol_20d": 0.22, "atr_pct": 0.018, "rsi_14": 61.0},
            {"ticker": "XLE", "sector_group": "cyclicals", "mom_20d": 0.010, "mom_60d": 0.030, "mom_252d": 0.080, "hist_vol_20d": 0.25, "atr_pct": 0.020, "rsi_14": 55.0},
            {"ticker": "XLRE", "sector_group": "real_estate", "mom_20d": -0.010, "mom_60d": -0.020, "mom_252d": 0.010, "hist_vol_20d": 0.20, "atr_pct": 0.017, "rsi_14": 48.0},
        ]
        context = {"regime": "trending_bull", "risk_params": {"max_single_position": 0.20}}

        scored = strategy.score(holdings, context)
        weights = strategy.optimize(scored, context)

        self.assertGreater(len(scored), 0)
        self.assertEqual(scored[0].ticker, "SOXX")
        self.assertLessEqual(sum(weight for ticker, weight in weights.items() if ticker != "CASH"), 0.18)
        self.assertLessEqual(max(weight for ticker, weight in weights.items() if ticker != "CASH"), 0.05)
        semiconductor_weight = weights.get("SOXX", 0.0) + weights.get("XSD", 0.0)
        self.assertLessEqual(semiconductor_weight, 0.10)
        self.assertGreaterEqual(weights["CASH"], 0.82)

    def test_sector_theme_relative_strength_builds_non_fallback_evidence_cards(self):
        strategy = get_strategy("sector_theme_relative_strength_lite")
        holdings = [
            {"ticker": "SOXX", "sector_group": "semiconductors", "mom_20d": 0.090, "mom_60d": 0.180, "mom_252d": 0.320, "hist_vol_20d": 0.30, "atr_pct": 0.025, "rsi_14": 66.0},
            {"ticker": "XSD", "sector_group": "semiconductors", "mom_20d": 0.060, "mom_60d": 0.140, "mom_252d": 0.260, "hist_vol_20d": 0.34, "atr_pct": 0.028, "rsi_14": 64.0},
            {"ticker": "XLK", "sector_group": "tech_growth", "mom_20d": 0.040, "mom_60d": 0.100, "mom_252d": 0.210, "hist_vol_20d": 0.22, "atr_pct": 0.018, "rsi_14": 61.0},
            {"ticker": "XLE", "sector_group": "cyclicals", "mom_20d": 0.010, "mom_60d": 0.030, "mom_252d": 0.080, "hist_vol_20d": 0.25, "atr_pct": 0.020, "rsi_14": 55.0},
        ]
        scored = strategy.score(holdings, {"regime": "trending_bull"})
        knowledge = build_knowledge_context(
            tickers=[item.ticker for item in scored],
            strategy_names=["sector_theme_relative_strength_lite"],
            regime="trending_bull",
            max_assets=12,
        )

        cards = build_evidence_cards(
            strategy=strategy,
            scored=scored,
            knowledge_context=knowledge,
            mode="playground",
        )

        self.assertTrue(cards)
        self.assertTrue(any(card.action in {"increase", "watch", "neutral"} for card in cards))
        self.assertFalse(any("missing_compatibility_mapping" in card.reason for card in cards))
        self.assertFalse(any("missing_required_safety_field" in card.reason for card in cards))

    def test_carry_cash_proxy_scores_and_optimizes_with_policy_sized_caps(self):
        strategy = get_strategy("carry_cash_proxy_lite")
        holdings = [
            defensive_holding("SGOV", 0.01, 0.002, 0.004, 0.012),
            defensive_holding("BND", 0.04, 0.006, 0.002, 0.006),
            defensive_holding("IEF", 0.05, 0.008, 0.003, 0.008),
            defensive_holding("TLT", 0.10, 0.018, -0.010, -0.020),
        ]
        context = {"regime": "defensive", "risk_params": {"max_single_position": 0.20}}

        scored = strategy.score(holdings, context)
        weights = strategy.optimize(scored, context)

        self.assertGreater(len(scored), 0)
        self.assertEqual(scored[0].ticker, "SGOV")
        self.assertLessEqual(sum(weight for ticker, weight in weights.items() if ticker != "CASH"), 0.20)
        self.assertLessEqual(max(weight for ticker, weight in weights.items() if ticker != "CASH"), 0.05)
        self.assertGreaterEqual(weights["CASH"], 0.80)

    def test_macro_rate_duration_prefers_cash_like_assets_when_rates_rise(self):
        strategy = get_strategy("macro_rate_duration_lite")
        holdings = [
            defensive_holding("SGOV", 0.01, 0.002, 0.004, 0.012),
            defensive_holding("BSV", 0.03, 0.005, 0.003, 0.009),
            defensive_holding("BND", 0.06, 0.010, -0.004, -0.015),
            defensive_holding("IEF", 0.08, 0.014, -0.012, -0.030),
            defensive_holding("TLT", 0.14, 0.030, -0.025, -0.070),
        ]
        context = {
            "regime": "defensive",
            "rate_regime_label": "rising_rate_expectation",
            "risk_params": {"max_single_position": 0.20},
        }

        scored = strategy.score(holdings, context)
        weights = strategy.optimize(scored, context)

        self.assertGreater(len(scored), 0)
        self.assertIn(scored[0].ticker, {"SGOV", "BSV"})
        self.assertEqual(weights.get("TLT", 0.0), 0.0)
        self.assertLessEqual(sum(weight for ticker, weight in weights.items() if ticker != "CASH"), 0.20)
        self.assertLessEqual(max(weight for ticker, weight in weights.items() if ticker != "CASH"), 0.05)
        self.assertGreaterEqual(weights["CASH"], 0.80)

    def test_macro_rate_duration_allows_duration_when_rates_fall(self):
        strategy = get_strategy("macro_rate_duration_lite")
        holdings = [
            defensive_holding("SGOV", 0.01, 0.002, 0.002, 0.006),
            defensive_holding("BSV", 0.03, 0.005, 0.004, 0.010),
            defensive_holding("BND", 0.05, 0.009, 0.010, 0.030),
            defensive_holding("IEF", 0.07, 0.012, 0.020, 0.060),
            defensive_holding("TLT", 0.12, 0.021, 0.040, 0.120),
        ]
        context = {
            "regime": "risk_off",
            "rate_regime_label": "falling_rate_expectation",
            "risk_params": {"max_single_position": 0.20},
        }

        scored = strategy.score(holdings, context)
        weights = strategy.optimize(scored, context)

        self.assertGreater(len(scored), 0)
        self.assertIn(scored[0].ticker, {"IEF", "TLT"})
        self.assertTrue(weights.get("IEF", 0.0) > 0.0 or weights.get("TLT", 0.0) > 0.0)
        self.assertLessEqual(sum(weight for ticker, weight in weights.items() if ticker != "CASH"), 0.20)
        self.assertGreaterEqual(weights["CASH"], 0.80)

    def test_macro_rate_duration_builds_non_fallback_evidence_cards(self):
        strategy = get_strategy("macro_rate_duration_lite")
        holdings = [
            defensive_holding("SGOV", 0.01, 0.002, 0.004, 0.012),
            defensive_holding("BSV", 0.03, 0.005, 0.003, 0.009),
            defensive_holding("BND", 0.06, 0.010, -0.004, -0.015),
            defensive_holding("IEF", 0.08, 0.014, -0.012, -0.030),
            defensive_holding("TLT", 0.14, 0.030, -0.025, -0.070),
        ]
        scored = strategy.score(holdings, {"regime": "defensive", "rate_regime_label": "rising_rate_expectation"})
        knowledge = build_knowledge_context(
            tickers=[item.ticker for item in scored],
            strategy_names=["macro_rate_duration_lite"],
            regime="defensive",
            max_assets=8,
        )

        cards = build_evidence_cards(
            strategy=strategy,
            scored=scored,
            knowledge_context=knowledge,
            mode="playground",
        )

        self.assertTrue(cards)
        self.assertTrue(any(card.action in {"de_risk", "watch", "neutral"} for card in cards))
        self.assertFalse(any("missing_compatibility_mapping" in card.reason for card in cards))
        self.assertFalse(any("missing_required_safety_field" in card.reason for card in cards))

    def test_volatility_hedge_scores_only_small_hedge_sleeve_under_stress(self):
        strategy = get_strategy("volatility_hedge_lite")
        holdings = [
            defensive_holding("SPY", 0.18, 0.025, -0.060, -0.100, rsi=32.0) | {"close_price": 390.0, "sma_200": 420.0},
            defensive_holding("UVXY", 0.80, 0.090, 0.180, 0.120, rsi=65.0),
            defensive_holding("VIXY", 0.55, 0.060, 0.120, 0.080, rsi=62.0),
            defensive_holding("SGOV", 0.01, 0.002, 0.003, 0.010, rsi=50.0),
            defensive_holding("TLT", 0.13, 0.020, 0.020, 0.030, rsi=55.0),
        ]
        context = {"regime": "high_vol", "risk_params": {"max_single_position": 0.20}}

        scored = strategy.score(holdings, context)
        weights = strategy.optimize(scored, context)

        self.assertGreater(len(scored), 0)
        self.assertGreater(max(item.raw_factors["market_stress"] for item in scored), 0.70)
        self.assertLessEqual(weights.get("UVXY", 0.0), 0.03)
        self.assertLessEqual(weights.get("VIXY", 0.0), 0.03)
        self.assertLessEqual(sum(weight for ticker, weight in weights.items() if ticker != "CASH"), 0.11)

    def test_inverse_equity_hedge_scores_only_tiny_hedge_under_breakdown(self):
        strategy = get_strategy("inverse_equity_hedge_lite")
        holdings = [
            defensive_holding("SPY", 0.20, 0.026, -0.060, -0.100, rsi=32.0) | {"close_price": 390.0, "sma_200": 420.0},
            defensive_holding("QQQ", 0.25, 0.033, -0.080, -0.140, rsi=30.0) | {"close_price": 350.0, "sma_200": 390.0},
            defensive_holding("SOXX", 0.32, 0.040, -0.120, -0.200, rsi=28.0) | {"close_price": 150.0, "sma_200": 190.0},
            defensive_holding("XLK", 0.24, 0.030, -0.070, -0.130, rsi=31.0) | {"close_price": 160.0, "sma_200": 185.0},
            defensive_holding("SQQQ", 0.70, 0.080, 0.160, 0.220, rsi=65.0),
            defensive_holding("SPXS", 0.62, 0.070, 0.110, 0.170, rsi=62.0),
            defensive_holding("SOXS", 0.85, 0.100, 0.220, 0.300, rsi=70.0),
            defensive_holding("TECS", 0.72, 0.085, 0.140, 0.210, rsi=66.0),
        ]
        context = {"regime": "risk_off", "market_breakdown": True}

        scored = strategy.score(holdings, context)
        weights = strategy.optimize(scored, context)

        self.assertGreater(len(scored), 0)
        self.assertIn(scored[0].ticker, {"SQQQ", "SPXS", "SOXS", "TECS"})
        self.assertLessEqual(sum(weight for ticker, weight in weights.items() if ticker != "CASH"), 0.05)
        self.assertLessEqual(max(weight for ticker, weight in weights.items() if ticker != "CASH"), 0.02)
        self.assertGreaterEqual(weights["CASH"], 0.95)

    def test_inverse_equity_hedge_stays_cash_in_risk_on(self):
        strategy = get_strategy("inverse_equity_hedge_lite")
        holdings = [
            defensive_holding("SPY", 0.14, 0.012, 0.040, 0.090, rsi=58.0) | {"close_price": 510.0, "sma_200": 460.0},
            defensive_holding("QQQ", 0.18, 0.016, 0.060, 0.120, rsi=62.0) | {"close_price": 440.0, "sma_200": 390.0},
            defensive_holding("SQQQ", 0.60, 0.080, 0.030, -0.050, rsi=50.0),
            defensive_holding("SPXS", 0.55, 0.070, 0.010, -0.040, rsi=49.0),
        ]
        scored = strategy.score(holdings, {"regime": "risk_on"})

        self.assertTrue(scored)
        self.assertEqual(strategy.optimize(scored, {"regime": "risk_on"}), {"CASH": 1.0})

    def test_inverse_equity_hedge_builds_non_fallback_evidence_cards(self):
        strategy = get_strategy("inverse_equity_hedge_lite")
        holdings = [
            defensive_holding("SPY", 0.20, 0.026, -0.060, -0.100, rsi=32.0) | {"close_price": 390.0, "sma_200": 420.0},
            defensive_holding("QQQ", 0.25, 0.033, -0.080, -0.140, rsi=30.0) | {"close_price": 350.0, "sma_200": 390.0},
            defensive_holding("SQQQ", 0.70, 0.080, 0.160, 0.220, rsi=65.0),
            defensive_holding("SPXS", 0.62, 0.070, 0.110, 0.170, rsi=62.0),
        ]
        scored = strategy.score(holdings, {"regime": "risk_off", "market_breakdown": True})
        knowledge = build_knowledge_context(
            tickers=[item.ticker for item in scored],
            strategy_names=["inverse_equity_hedge_lite"],
            regime="risk_off",
            max_assets=8,
        )

        cards = build_evidence_cards(
            strategy=strategy,
            scored=scored,
            knowledge_context=knowledge,
            mode="playground",
        )

        self.assertTrue(cards)
        self.assertTrue(any(card.action in {"hedge", "watch", "avoid"} for card in cards))
        self.assertFalse(any("missing_compatibility_mapping" in card.reason for card in cards))
        self.assertFalse(any("missing_required_safety_field" in card.reason for card in cards))

    def test_leveraged_long_amplifier_scores_only_tiny_risk_on_sleeve(self):
        strategy = get_strategy("leveraged_long_amplifier_lite")
        context = {"regime": "trending_bull", "risk_params": {"max_single_position": 0.20}}

        scored = strategy.score(leveraged_amplifier_holdings(), context)
        weights = strategy.optimize(scored, context)

        self.assertGreater(len(scored), 0)
        self.assertIn(scored[0].ticker, {"TQQQ", "SOXL", "TECL", "SPXL"})
        self.assertLessEqual(sum(weight for ticker, weight in weights.items() if ticker != "CASH"), 0.06)
        self.assertLessEqual(max(weight for ticker, weight in weights.items() if ticker != "CASH"), 0.02)
        self.assertGreaterEqual(weights["CASH"], 0.94)

    def test_leveraged_long_amplifier_stays_cash_outside_risk_on(self):
        strategy = get_strategy("leveraged_long_amplifier_lite")
        scored = strategy.score(leveraged_amplifier_holdings(), {"regime": "high_vol"})

        self.assertTrue(scored)
        self.assertEqual(strategy.optimize(scored, {"regime": "high_vol"}), {"CASH": 1.0})

    def test_leveraged_long_amplifier_builds_non_fallback_evidence_cards(self):
        strategy = get_strategy("leveraged_long_amplifier_lite")
        scored = strategy.score(leveraged_amplifier_holdings(), {"regime": "trending_bull"})
        knowledge = build_knowledge_context(
            tickers=[item.ticker for item in scored],
            strategy_names=["leveraged_long_amplifier_lite"],
            regime="trending_bull",
            max_assets=8,
        )

        cards = build_evidence_cards(
            strategy=strategy,
            scored=scored,
            knowledge_context=knowledge,
            mode="playground",
        )

        self.assertTrue(cards)
        self.assertTrue(any(card.action in {"increase", "watch", "neutral"} for card in cards))
        self.assertFalse(any("missing_compatibility_mapping" in card.reason for card in cards))
        self.assertFalse(any("missing_required_safety_field" in card.reason for card in cards))

    def test_new_strategy_knowledge_builds_non_fallback_evidence_cards(self):
        strategy = get_strategy("carry_cash_proxy_lite")
        holdings = [
            defensive_holding("SGOV", 0.01, 0.002, 0.004, 0.012),
            defensive_holding("BND", 0.04, 0.006, 0.002, 0.006),
            defensive_holding("IEF", 0.05, 0.008, 0.003, 0.008),
            defensive_holding("TLT", 0.10, 0.018, -0.010, -0.020),
        ]
        scored = strategy.score(holdings, {"regime": "defensive"})
        knowledge = build_knowledge_context(
            tickers=[item.ticker for item in scored],
            strategy_names=["carry_cash_proxy_lite"],
            regime="defensive",
            max_assets=8,
        )

        cards = build_evidence_cards(
            strategy=strategy,
            scored=scored,
            knowledge_context=knowledge,
            mode="playground",
        )

        self.assertTrue(cards)
        self.assertTrue(any(card.action in {"de_risk", "watch", "neutral"} for card in cards))
        self.assertFalse(any("missing_compatibility_mapping" in card.reason for card in cards))
        self.assertFalse(any("missing_required_safety_field" in card.reason for card in cards))

    def test_relative_value_reversion_scores_underperforming_core_etf_with_small_caps(self):
        strategy = get_strategy("relative_value_reversion_lite")
        holdings = [
            {
                "ticker": "SPY",
                "return_5d": 0.015,
                "mom_20d": 0.030,
                "mom_60d": 0.050,
                "hist_vol_20d": 0.16,
                "rsi_14": 58.0,
            },
            {
                "ticker": "QQQ",
                "return_5d": -0.035,
                "mom_20d": -0.020,
                "mom_60d": 0.040,
                "hist_vol_20d": 0.20,
                "rsi_14": 43.0,
            },
            {
                "ticker": "IWM",
                "return_5d": 0.005,
                "mom_20d": 0.010,
                "mom_60d": 0.020,
                "hist_vol_20d": 0.24,
                "rsi_14": 52.0,
            },
        ]
        context = {"regime": "mean_reverting", "risk_params": {"max_single_position": 0.20}}

        scored = strategy.score(holdings, context)
        weights = strategy.optimize(scored, context)

        self.assertGreater(len(scored), 0)
        self.assertEqual(scored[0].ticker, "QQQ")
        self.assertLessEqual(sum(weight for ticker, weight in weights.items() if ticker != "CASH"), 0.15)
        self.assertLessEqual(max(weight for ticker, weight in weights.items() if ticker != "CASH"), 0.05)
        self.assertGreaterEqual(weights["CASH"], 0.85)

    def test_relative_value_reversion_builds_non_fallback_evidence_cards(self):
        strategy = get_strategy("relative_value_reversion_lite")
        holdings = [
            {"ticker": "SPY", "return_5d": 0.015, "mom_20d": 0.030, "mom_60d": 0.050, "hist_vol_20d": 0.16, "rsi_14": 58.0},
            {"ticker": "QQQ", "return_5d": -0.035, "mom_20d": -0.020, "mom_60d": 0.040, "hist_vol_20d": 0.20, "rsi_14": 43.0},
            {"ticker": "IWM", "return_5d": 0.005, "mom_20d": 0.010, "mom_60d": 0.020, "hist_vol_20d": 0.24, "rsi_14": 52.0},
        ]
        scored = strategy.score(holdings, {"regime": "mean_reverting"})
        knowledge = build_knowledge_context(
            tickers=[item.ticker for item in scored],
            strategy_names=["relative_value_reversion_lite"],
            regime="mean_reverting",
            max_assets=8,
        )

        cards = build_evidence_cards(
            strategy=strategy,
            scored=scored,
            knowledge_context=knowledge,
            mode="playground",
        )

        self.assertTrue(cards)
        self.assertTrue(any(card.action in {"increase", "watch", "neutral"} for card in cards))
        self.assertFalse(any("missing_compatibility_mapping" in card.reason for card in cards))
        self.assertFalse(any("missing_required_safety_field" in card.reason for card in cards))

    def test_defensive_quality_rotation_scores_low_vol_defensive_assets_with_caps(self):
        strategy = get_strategy("defensive_quality_rotation_lite")
        holdings = [
            defensive_holding("SGOV", 0.01, 0.002, 0.004, 0.012, rsi=50.0) | {"return_5d": 0.001},
            defensive_holding("BND", 0.04, 0.006, 0.002, 0.006, rsi=51.0) | {"return_5d": 0.002},
            defensive_holding("IEF", 0.05, 0.008, 0.003, 0.008, rsi=52.0) | {"return_5d": -0.001},
            defensive_holding("TLT", 0.10, 0.018, -0.010, -0.020, rsi=41.0) | {"return_5d": -0.018},
        ]
        context = {"regime": "defensive", "risk_params": {"max_single_position": 0.20}}

        scored = strategy.score(holdings, context)
        weights = strategy.optimize(scored, context)

        self.assertGreater(len(scored), 0)
        self.assertEqual(scored[0].ticker, "SGOV")
        self.assertLessEqual(sum(weight for ticker, weight in weights.items() if ticker != "CASH"), 0.18)
        self.assertLessEqual(max(weight for ticker, weight in weights.items() if ticker != "CASH"), 0.05)
        self.assertGreaterEqual(weights["CASH"], 0.82)

    def test_defensive_quality_rotation_builds_non_fallback_evidence_cards(self):
        strategy = get_strategy("defensive_quality_rotation_lite")
        holdings = [
            defensive_holding("SGOV", 0.01, 0.002, 0.004, 0.012, rsi=50.0) | {"return_5d": 0.001},
            defensive_holding("BND", 0.04, 0.006, 0.002, 0.006, rsi=51.0) | {"return_5d": 0.002},
            defensive_holding("IEF", 0.05, 0.008, 0.003, 0.008, rsi=52.0) | {"return_5d": -0.001},
            defensive_holding("TLT", 0.10, 0.018, -0.010, -0.020, rsi=41.0) | {"return_5d": -0.018},
        ]
        scored = strategy.score(holdings, {"regime": "defensive"})
        knowledge = build_knowledge_context(
            tickers=[item.ticker for item in scored],
            strategy_names=["defensive_quality_rotation_lite"],
            regime="defensive",
            max_assets=8,
        )

        cards = build_evidence_cards(
            strategy=strategy,
            scored=scored,
            knowledge_context=knowledge,
            mode="playground",
        )

        self.assertTrue(cards)
        self.assertTrue(any(card.action in {"de_risk", "watch", "neutral"} for card in cards))
        self.assertFalse(any("missing_compatibility_mapping" in card.reason for card in cards))
        self.assertFalse(any("missing_required_safety_field" in card.reason for card in cards))


if __name__ == "__main__":
    unittest.main()
