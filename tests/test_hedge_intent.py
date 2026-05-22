import unittest

from services.hedge_intent import evaluate_hedge_intent


class HedgeIntentTests(unittest.TestCase):
    def test_no_trigger_in_normal_market(self):
        plan = evaluate_hedge_intent(
            vix_level=18,
            portfolio_drawdown_pct=-0.01,
            net_long_exposure=0.60,
            market_regime_raw="bull",
            current_holdings={"QQQ": 0.10},
            scorecard_requires_human=False,
            market_breadth_pct=0.65,
        )

        self.assertFalse(plan.triggered)
        self.assertFalse(plan.add_hedge_etf)

    def test_vix_and_weak_breadth_trigger_trim_and_cash(self):
        plan = evaluate_hedge_intent(
            vix_level=30,
            portfolio_drawdown_pct=-0.02,
            net_long_exposure=0.80,
            market_regime_raw="range",
            current_holdings={"QQQ": 0.12, "PSI": 0.04, "SPY": 0.30},
            scorecard_requires_human=False,
            market_breadth_pct=0.25,
        )

        self.assertTrue(plan.triggered)
        self.assertIn("QQQ", plan.trim_targets)
        self.assertIn("PSI", plan.trim_targets)
        self.assertGreater(plan.target_cash_raise_pct, 0.0)

    def test_severe_stress_adds_small_hedge_etf(self):
        plan = evaluate_hedge_intent(
            vix_level=50,
            portfolio_drawdown_pct=-0.12,
            net_long_exposure=0.85,
            market_regime_raw="defensive",
            current_holdings={"QQQ": 0.12, "XLK": 0.10},
            scorecard_requires_human=True,
            market_breadth_pct=0.10,
        )

        self.assertTrue(plan.triggered)
        self.assertTrue(plan.add_hedge_etf)
        self.assertEqual(plan.hedge_instrument, "UVXY")
        self.assertLessEqual(plan.hedge_weight, 0.03)


if __name__ == "__main__":
    unittest.main()
