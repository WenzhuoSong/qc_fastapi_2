import unittest

from services.target_path_visibility import build_target_path_visibility


class TargetPathVisibilityTests(unittest.TestCase):
    def test_builds_executable_truths_and_diagnostic_surfaces(self):
        risk_out = {
            "target_weights": {"SPY": 0.18, "QQQ": 0.10, "CASH": 0.72},
            "diagnostic_llm_adjusted_weights": {"SPY": 0.30, "CASH": 0.70},
            "portfolio_construction_shadow": {
                "target_weights": {"QQQ": 0.25, "CASH": 0.75},
            },
            "target_envelope": {
                "current_weights": {"SPY": 0.20, "QQQ": 0.12, "CASH": 0.68},
                "risk_approved_target": {"SPY": 0.20, "QQQ": 0.12, "CASH": 0.68},
                "final_target": {"SPY": 0.18, "QQQ": 0.10, "CASH": 0.72},
                "accounting_ok": True,
                "ledger": {
                    "mutations": [
                        {
                            "type": "loss_trim",
                            "ticker": "SPY",
                            "before": 0.20,
                            "after": 0.18,
                            "tighten_only": True,
                            "conditional": False,
                            "reason": "trim",
                            "metadata": {"stage": "position_governance"},
                        },
                    ],
                },
                "stage_snapshots": [
                    {
                        "stage": "position_governance",
                        "cash_actual": 0.70,
                        "cash_matches_requested": True,
                    },
                ],
            },
        }

        visibility = build_target_path_visibility(risk_out)

        self.assertTrue(visibility["available"])
        truth_keys = {row["key"] for row in visibility["truth_rows"]}
        diagnostic_keys = {row["key"] for row in visibility["diagnostic_surface_rows"]}
        self.assertEqual(
            truth_keys,
            {"actual_holdings", "risk_approved_target", "envelope_final_target"},
        )
        self.assertEqual(
            diagnostic_keys,
            {"legacy_dict_final_target", "advisory_llm_weights", "pc_shadow_reference_weights"},
        )
        self.assertTrue(all(row["executable"] for row in visibility["truth_rows"]))
        self.assertTrue(all(not row["executable"] for row in visibility["diagnostic_surface_rows"]))

    def test_mutation_rows_show_stage_and_safety_effect(self):
        risk_out = {
            "target_envelope": {
                "current_weights": {"XLE": 0.12, "CASH": 0.88},
                "risk_approved_target": {"XLE": 0.12, "CASH": 0.88},
                "final_target": {"XLE": 0.09, "CASH": 0.91},
                "accounting_ok": True,
                "ledger": {
                    "mutations": [
                        {
                            "type": "loss_trim",
                            "ticker": "XLE",
                            "before": 0.12,
                            "after": 0.09,
                            "tighten_only": True,
                            "conditional": False,
                            "reason": "hard risk trim",
                            "metadata": {"stage": "position_governance"},
                        },
                    ],
                },
                "stage_snapshots": [],
            },
        }

        visibility = build_target_path_visibility(risk_out)
        row = visibility["mutation_rows"][0]

        self.assertEqual(row["stage"], "position_governance")
        self.assertEqual(row["ticker"], "XLE")
        self.assertEqual(row["mutation_type"], "loss_trim")
        self.assertEqual(row["safety_effect"], "reduce")
        self.assertEqual(row["stage_effect"], "reduce")

    def test_missing_envelope_is_visible_not_silent(self):
        visibility = build_target_path_visibility({"target_weights": {"SPY": 0.1}})

        self.assertFalse(visibility["available"])
        self.assertIn("target_envelope_unavailable", visibility["warnings"])
        self.assertEqual(visibility["execution_authority"], "unknown")


if __name__ == "__main__":
    unittest.main()
