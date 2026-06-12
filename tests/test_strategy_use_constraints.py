import unittest

from services.strategy_use_constraints import apply_strategy_use_constraints


class StrategyUseConstraintsTest(unittest.TestCase):
    def test_primary_strategy_leaves_weights_unchanged(self):
        out, log = apply_strategy_use_constraints(
            base_weights={"SPY": 0.50, "CASH": 0.50},
            adjusted_weights={"SPY": 0.60, "CASH": 0.40},
            strategy_evidence={
                "playground_available": True,
                "strategy_use_summary": {
                    "primary": [{"strategy_name": "momentum_lite_v1"}],
                    "advisory": [],
                },
            },
        )

        self.assertEqual(out, {"SPY": 0.6, "CASH": 0.4})
        self.assertEqual(log, [])

    def test_missing_cash_does_not_amplify_risk_weight(self):
        out, log = apply_strategy_use_constraints(
            base_weights={"CASH": 1.0},
            adjusted_weights={"SPY": 0.20},
            strategy_evidence={"playground_available": False},
        )

        self.assertEqual(out, {"SPY": 0.2, "CASH": 0.8})
        self.assertEqual(log, [])

    def test_advisory_only_caps_delta_and_blocks_unsupported_new_position(self):
        out, log = apply_strategy_use_constraints(
            base_weights={"SPY": 0.50, "CASH": 0.50},
            adjusted_weights={"SPY": 0.60, "QQQ": 0.08, "CASH": 0.32},
            strategy_evidence={
                "playground_available": True,
                "strategy_use_summary": {
                    "primary": [],
                    "advisory": [{"strategy_name": "momentum_lite_v1"}],
                },
                "strategy_results": [
                    {
                        "strategy_name": "momentum_lite_v1",
                        "suggested_use": "advisory",
                        "selected_tickers": ["SPY"],
                    }
                ],
            },
        )

        self.assertAlmostEqual(out["SPY"], 0.53, places=4)
        self.assertNotIn("QQQ", out)
        self.assertAlmostEqual(out["CASH"], 0.47, places=4)
        self.assertTrue(any("strategy_advisory_only:max_delta:SPY" in item for item in log))
        self.assertTrue(any("strategy_advisory_only:new_position_blocked:QQQ" in item for item in log))

    def test_certified_advisory_uses_advisory_constraints(self):
        out, log = apply_strategy_use_constraints(
            base_weights={"SPY": 0.50, "CASH": 0.50},
            adjusted_weights={"SPY": 0.60, "QQQ": 0.08, "CASH": 0.32},
            strategy_evidence={
                "playground_available": True,
                "strategy_results": [
                    {
                        "strategy_name": "momentum_lite_v1",
                        "suggested_use": "advisory",
                        "selected_tickers": ["SPY"],
                    }
                ],
                "strategy_certification": {
                    "items": {
                        "momentum_lite_v1": {
                            "approved_use": "advisory",
                            "execution_evidence_status": "execution_grade_validated",
                        }
                    }
                },
            },
        )

        self.assertAlmostEqual(out["SPY"], 0.53, places=4)
        self.assertNotIn("QQQ", out)
        self.assertTrue(any("strategy_advisory_only:max_delta:SPY" in item for item in log))

    def test_uncertified_advisory_is_treated_as_no_actionable_strategy(self):
        out, log = apply_strategy_use_constraints(
            base_weights={"SPY": 0.50, "CASH": 0.50},
            adjusted_weights={"SPY": 0.60, "QQQ": 0.08, "CASH": 0.32},
            strategy_evidence={
                "playground_available": True,
                "strategy_results": [
                    {
                        "strategy_name": "momentum_lite_v1",
                        "suggested_use": "advisory",
                        "selected_tickers": ["SPY"],
                    }
                ],
                "strategy_certification": {
                    "items": {
                        "momentum_lite_v1": {
                            "approved_use": "research_only",
                            "execution_evidence_status": "insufficient_execution_evidence",
                        }
                    }
                },
            },
        )

        self.assertAlmostEqual(out["SPY"], 0.51, places=4)
        self.assertNotIn("QQQ", out)
        self.assertTrue(any("no_actionable_strategy:max_delta:SPY" in item for item in log))
        self.assertTrue(any("no_actionable_strategy:new_position_blocked:QQQ" in item for item in log))

    def test_no_actionable_strategy_caps_to_one_percent_and_blocks_new_positions(self):
        out, log = apply_strategy_use_constraints(
            base_weights={"SPY": 0.50, "CASH": 0.50},
            adjusted_weights={"SPY": 0.56, "QQQ": 0.04, "CASH": 0.40},
            strategy_evidence={
                "playground_available": True,
                "strategy_use_summary": {
                    "primary": [],
                    "advisory": [],
                    "watch_only": [{"strategy_name": "mean_reversion_lite"}],
                },
            },
        )

        self.assertAlmostEqual(out["SPY"], 0.51, places=4)
        self.assertNotIn("QQQ", out)
        self.assertAlmostEqual(out["CASH"], 0.49, places=4)
        self.assertTrue(any("no_actionable_strategy:max_delta:SPY" in item for item in log))
        self.assertTrue(any("no_actionable_strategy:new_position_blocked:QQQ" in item for item in log))

    def test_missing_playground_does_not_clip(self):
        out, log = apply_strategy_use_constraints(
            base_weights={"SPY": 0.50, "CASH": 0.50},
            adjusted_weights={"SPY": 0.60, "CASH": 0.40},
            strategy_evidence={"playground_available": False},
        )

        self.assertEqual(out, {"SPY": 0.6, "CASH": 0.4})
        self.assertEqual(log, [])


if __name__ == "__main__":
    unittest.main()
