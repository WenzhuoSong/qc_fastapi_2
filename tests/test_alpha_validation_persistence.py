import unittest
from datetime import datetime

from services.alpha_validation_persistence import build_alpha_validation_run_record


class AlphaValidationPersistenceTest(unittest.TestCase):
    def test_builds_compact_alpha_validation_record(self):
        record = build_alpha_validation_run_record(
            analysis_id=123,
            analyzed_at=datetime(2026, 5, 25, 12, 0, 0),
            trigger_type="scheduled",
            execution_status="pre_execution_diagnostic",
            risk_out={
                "approved": True,
                "transaction_cost_gate": {
                    "status": "observe_warning",
                    "warnings": ["TQQQ: edge_to_cost_ratio 1.20 below 2.00"],
                    "rows": [
                        {
                            "ticker": "TQQQ",
                            "trade_action": "buy",
                            "edge_to_cost_ratio": 1.2,
                            "verdict": "low_edge_to_cost",
                        },
                        {
                            "ticker": "SPY",
                            "trade_action": "buy",
                            "edge_to_cost_ratio": 3.0,
                            "verdict": "cost_supported",
                        },
                    ],
                },
                "portfolio_risk_diagnostic": {
                    "status": "ok",
                    "data_quality": "historical_supported",
                    "summary": {
                        "target_var_95_loss": 0.021,
                        "target_cvar_95_loss": 0.034,
                        "max_target_scenario_loss": 0.09,
                    },
                    "warnings": [],
                },
                "portfolio_construction_shadow": {
                    "signal_weighted_effective_n_after": 4.2,
                    "signal_alignment_score_after": 0.68,
                    "signal_objective_metrics": {"warnings": ["x"]},
                },
            },
            evidence_bundle={
                "strategies": {
                    "strategy_diversity": {
                        "independent_alpha_family_count": 1,
                        "actionable_alpha_strategy_count": 2,
                        "warnings": ["same_family_not_independent:momentum:a,b"],
                    },
                    "strategy_results": [
                        {
                            "evidence_cards": [
                                {"conviction_status": "calibrated"},
                                {"conviction_status": "early_estimate"},
                                {"conviction_status": "insufficient_samples"},
                            ]
                        }
                    ],
                }
            },
        )

        self.assertEqual(record["analysis_id"], 123)
        self.assertEqual(record["status"], "observe_warning")
        self.assertEqual(record["data_quality"], "diagnostic_supported")
        self.assertEqual(record["low_edge_trade_count"], 1)
        self.assertAlmostEqual(record["min_edge_to_cost_ratio"], 1.2)
        self.assertAlmostEqual(record["avg_edge_to_cost_ratio"], 2.1)
        self.assertAlmostEqual(record["var_95_loss"], 0.021)
        self.assertAlmostEqual(record["cvar_95_loss"], 0.034)
        self.assertAlmostEqual(record["max_scenario_loss"], 0.09)
        self.assertAlmostEqual(record["signal_weighted_effective_n"], 4.2)
        self.assertAlmostEqual(record["signal_alignment_score"], 0.68)
        self.assertEqual(record["independent_alpha_family_count"], 1)
        self.assertEqual(record["actionable_alpha_strategy_count"], 2)
        self.assertEqual(record["calibrated_conviction_count"], 1)
        self.assertEqual(record["early_conviction_count"], 1)
        self.assertEqual(record["insufficient_conviction_count"], 1)
        self.assertIn("content_hash", record)

    def test_missing_diagnostics_marks_insufficient_data(self):
        record = build_alpha_validation_run_record(
            analysis_id=1,
            analyzed_at=None,
            trigger_type="scheduled",
            risk_out={},
            evidence_bundle={},
        )

        self.assertEqual(record["status"], "insufficient_data")
        self.assertEqual(record["data_quality"], "missing")
        self.assertEqual(record["independent_alpha_family_count"], 0)


if __name__ == "__main__":
    unittest.main()
