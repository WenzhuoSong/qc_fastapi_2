import unittest

from services.macro_regime_builder import build_deterministic_macro_regime


class MacroRegimeBuilderTests(unittest.TestCase):
    def test_detects_rising_rates_commodity_strength_and_stable_growth(self):
        context = build_deterministic_macro_regime([
            {"ticker": "SPY", "mom_60d": 0.06, "atr_pct": 0.014},
            {"ticker": "QQQ", "mom_60d": 0.07, "atr_pct": 0.018},
            {"ticker": "IWM", "mom_60d": 0.04, "atr_pct": 0.022},
            {"ticker": "XLI", "mom_60d": 0.05, "atr_pct": 0.017},
            {"ticker": "XLE", "mom_60d": 0.12, "mom_20d": 0.04},
            {"ticker": "TLT", "mom_60d": -0.08, "mom_20d": -0.03},
            {"ticker": "IEF", "mom_60d": -0.04},
            {"ticker": "SGOV", "mom_60d": 0.01},
        ])

        self.assertEqual(context["rate_regime_label"], "rising_rate_expectation")
        self.assertEqual(context["inflation_regime_label"], "commodity_strength")
        self.assertEqual(context["growth_regime_label"], "reacceleration")
        self.assertEqual(context["source"], "deterministic_price_proxy")
        self.assertTrue(context["has_data"])

    def test_detects_falling_rates_and_growth_scare(self):
        context = build_deterministic_macro_regime([
            {"ticker": "SPY", "mom_60d": -0.07, "atr_pct": 0.040},
            {"ticker": "QQQ", "mom_60d": -0.08, "atr_pct": 0.045},
            {"ticker": "IWM", "mom_60d": -0.10, "atr_pct": 0.050},
            {"ticker": "XLI", "mom_60d": -0.06, "atr_pct": 0.035},
            {"ticker": "XLE", "mom_60d": -0.04, "mom_20d": -0.02},
            {"ticker": "TLT", "mom_60d": 0.08, "mom_20d": 0.02},
            {"ticker": "IEF", "mom_60d": 0.05},
            {"ticker": "SGOV", "mom_60d": 0.01},
        ])

        self.assertEqual(context["rate_regime_label"], "falling_rate_expectation")
        self.assertIn(context["growth_regime_label"], {"growth_scare", "recession_risk"})
        self.assertIn(context["inflation_regime_label"], {"disinflationary", "neutral_inflation"})

    def test_news_overlay_is_auxiliary_not_required(self):
        context = build_deterministic_macro_regime(
            [{"ticker": "TLT", "mom_60d": 0.01, "mom_20d": 0.0}],
            news_context={"_fallback": True, "macro_signals": []},
        )

        self.assertEqual(context["news_overlay"]["data_quality"], "missing")
        self.assertIn("price proxies remain primary", " ".join(context["warnings"]))


if __name__ == "__main__":
    unittest.main()
