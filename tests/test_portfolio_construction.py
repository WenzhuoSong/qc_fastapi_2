import unittest

from services.portfolio_construction import PortfolioConstructionModel, build_construction_signal_strengths


class PortfolioConstructionTests(unittest.TestCase):
    def test_factor_limits_scale_excess_exposure_to_cash(self):
        out = PortfolioConstructionModel().construct(
            base_weights={
                "QQQ": 0.18,
                "XLK": 0.12,
                "SOXX": 0.10,
                "PSI": 0.08,
                "CASH": 0.52,
            },
            current_weights={
                "QQQ": 0.18,
                "XLK": 0.12,
                "SOXX": 0.10,
                "PSI": 0.08,
                "CASH": 0.52,
            },
            signal_strengths={},
            basket_reviews=None,
            scorecard_permission="normal_rebalance",
            turnover_budget=None,
        ).to_dict()

        self.assertLessEqual(out["factor_exposures"]["tech_growth"], 0.350001)
        self.assertGreater(out["target_weights"]["CASH"], 0.52)
        self.assertTrue(any(item.startswith("factor_limit:tech_growth") for item in out["violations"]))
        self.assertFalse(out["diagnostics"]["consumes_raw_llm_adjusted_weights"])

    def test_basket_review_tightens_group_to_multiplier_limit(self):
        out = PortfolioConstructionModel().construct(
            base_weights={
                "SOXX": 0.10,
                "PSI": 0.08,
                "FTXL": 0.07,
                "CASH": 0.75,
            },
            current_weights={
                "SOXX": 0.10,
                "PSI": 0.08,
                "FTXL": 0.07,
                "CASH": 0.75,
            },
            signal_strengths={},
            basket_reviews=[{"group": "semiconductors", "tickers": ["SOXX", "PSI", "FTXL"]}],
            scorecard_permission="normal_rebalance",
            turnover_budget=None,
        ).to_dict()

        self.assertLessEqual(out["factor_exposures"]["semiconductors"], 0.175001)
        self.assertIn("semiconductors", out["diagnostics"]["active_basket_reviews"])
        self.assertTrue(any(item.startswith("basket_limit:semiconductors") for item in out["violations"]))

    def test_turnover_budget_preserves_stronger_signal_adjustment(self):
        out = PortfolioConstructionModel().construct(
            base_weights={"SPY": 0.20, "QQQ": 0.20, "CASH": 0.60},
            current_weights={"SPY": 0.10, "QQQ": 0.10, "CASH": 0.80},
            signal_strengths={"SPY": 0.9, "QQQ": 0.1},
            basket_reviews=None,
            scorecard_permission="normal_rebalance",
            turnover_budget=0.10,
        ).to_dict()

        self.assertAlmostEqual(out["target_weights"]["SPY"], 0.20)
        self.assertAlmostEqual(out["target_weights"]["QQQ"], 0.10)
        self.assertLessEqual(out["turnover"]["estimated"], 0.100001)
        self.assertTrue(any(item.startswith("turnover_budget:") for item in out["violations"]))

    def test_no_add_permission_clips_targets_to_current(self):
        out = PortfolioConstructionModel().construct(
            base_weights={"SPY": 0.20, "CASH": 0.80},
            current_weights={"SPY": 0.10, "CASH": 0.90},
            signal_strengths={"SPY": 1.0},
            basket_reviews=None,
            scorecard_permission="reduce_risk_only",
            turnover_budget=None,
        ).to_dict()

        self.assertAlmostEqual(out["target_weights"]["SPY"], 0.10)
        self.assertTrue(any(item.startswith("scorecard_no_add:SPY") for item in out["violations"]))

    def test_same_input_is_repeatable(self):
        payload = dict(
            base_weights={"SOXX": 0.08, "SPY": 0.20, "CASH": 0.72},
            current_weights={"SOXX": 0.04, "SPY": 0.20, "CASH": 0.76},
            signal_strengths={"SOXX": 0.7, "SPY": 0.2},
            basket_reviews={"semiconductors": {"reason": "cluster"}},
            scorecard_permission="normal_rebalance",
            turnover_budget=0.03,
        )

        first = PortfolioConstructionModel().construct(**payload).to_dict()
        second = PortfolioConstructionModel().construct(**payload).to_dict()

        self.assertEqual(first, second)

    def test_build_construction_signals_merges_strategy_and_rotation(self):
        signals = build_construction_signal_strengths(
            {
                "strategies": {
                    "strategy_results": [
                        {
                            "strategy_name": "momentum_lite_v1",
                            "suggested_use": "advisory",
                            "confidence_score": 0.80,
                            "selected_tickers": ["XLK", "XLP"],
                        },
                        {
                            "strategy_name": "watch_only",
                            "suggested_use": "watch_only",
                            "confidence_score": 1.0,
                            "selected_tickers": ["SOXX"],
                        },
                    ],
                },
                "rotation": {
                    "signals": {
                        "XLK": 1.0,
                        "XLP": -1.0,
                        "SOXX": 0.5,
                    }
                },
            }
        )

        self.assertAlmostEqual(signals["XLK"], 0.88)
        self.assertAlmostEqual(signals["XLP"], 0.08)
        self.assertAlmostEqual(signals["SOXX"], 0.20)


if __name__ == "__main__":
    unittest.main()
