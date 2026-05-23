import unittest

from services.quant_baseline import classify_market_regime


class QuantBaselineRegimeTest(unittest.TestCase):
    def test_bull_with_bond_relative_strength_sets_defensive_rotation_subtype(self):
        result = classify_market_regime(
            {"vix": 18, "current_drawdown_pct": 0.02, "breadth_pct": 0.55},
            {
                "ticker": "SPY",
                "mom_20d": 0.02,
                "mom_60d": 0.06,
                "mom_252d": 0.12,
                "rsi_14": 60,
                "atr_pct": 0.012,
            },
            holdings=[
                {"ticker": "SPY", "mom_20d": 0.02},
                {"ticker": "IEF", "mom_20d": 0.055},
            ],
        )

        self.assertEqual(result.regime.value, "trending_bull")
        self.assertEqual(result.signals["regime_subtype"], "bull_with_defensive_rotation")
        self.assertTrue(result.signals["regime_bond_adjusted"])
        self.assertAlmostEqual(result.signals["ief_vs_spy_relative_strength_20d"], 0.035)

    def test_bull_with_good_breadth_sets_broad_participation_subtype(self):
        result = classify_market_regime(
            {"vix": 18, "current_drawdown_pct": 0.02, "breadth_pct": 0.70},
            {
                "ticker": "SPY",
                "mom_20d": 0.02,
                "mom_60d": 0.06,
                "mom_252d": 0.12,
                "rsi_14": 60,
                "atr_pct": 0.012,
            },
            holdings=[
                {"ticker": "SPY", "mom_20d": 0.02},
                {"ticker": "IEF", "mom_20d": 0.01},
            ],
        )

        self.assertEqual(result.regime.value, "trending_bull")
        self.assertEqual(result.signals["regime_subtype"], "bull_broad_participation")
        self.assertFalse(result.signals["regime_bond_adjusted"])

    def test_regime_confidence_is_capped_when_research_features_lack_authority(self):
        result = classify_market_regime(
            {"vix": 18, "current_drawdown_pct": 0.02, "breadth_pct": 0.70},
            {
                "ticker": "SPY",
                "mom_20d": 0.02,
                "mom_60d": 0.06,
                "mom_252d": 0.12,
                "rsi_14": 60,
                "atr_pct": 0.012,
            },
            holdings=[],
        )

        self.assertEqual(result.regime.value, "trending_bull")
        self.assertEqual(result.confidence, "low")
        self.assertTrue(result.signals["feature_authority"]["has_fallback_or_unknown"])

    def test_regime_confidence_uses_yfinance_daily_research_authority(self):
        result = classify_market_regime(
            {"vix": 18, "current_drawdown_pct": 0.02, "breadth_pct": 0.70},
            {
                "ticker": "SPY",
                "return_20d": 0.02,
                "return_60d": 0.06,
                "return_252d": 0.12,
                "rsi_14": 60,
                "atr_pct": 0.012,
                "feature_sources": [
                    {
                        "source": "yfinance",
                        "filled_fields": ["return_20d", "return_60d", "return_252d", "rsi_14", "atr_pct"],
                    }
                ],
            },
            holdings=[],
        )

        self.assertEqual(result.regime.value, "trending_bull")
        self.assertEqual(result.confidence, "high")
        self.assertFalse(result.signals["feature_authority"]["has_fallback_or_unknown"])


if __name__ == "__main__":
    unittest.main()
