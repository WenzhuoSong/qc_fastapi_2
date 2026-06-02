import unittest
from pathlib import Path

from services.final_risk_validation import validate_final_execution_target
from services.mutation_ownership import (
    REGIME_CONSTRAINT_MUTATION_TYPE,
    legacy_mutation_classification_summary,
)


class MutationOwnershipTests(unittest.TestCase):
    def test_legacy_mutation_paths_are_classified_with_exit_criteria(self):
        summary = legacy_mutation_classification_summary()

        self.assertEqual(summary["contract_version"], "legacy_mutation_ownership_v1")
        self.assertEqual(summary["unresolved_count"], 0)
        self.assertEqual(
            summary["unclassified_observe_criteria"]["min_cycles_before_decision"],
            20,
        )
        self.assertEqual(
            summary["unclassified_observe_criteria"]["decision_deadline"],
            "2026-07-01",
        )
        self.assertEqual(
            summary["classifications"]["apply_regime_constraints"]["mutation_type"],
            REGIME_CONSTRAINT_MUTATION_TYPE,
        )
        self.assertEqual(
            summary["classifications"]["enforce_pm_constraints"]["status"],
            "deprecated_inactive",
        )
        self.assertEqual(
            summary["classifications"]["enforce_pm_constraints_v2"]["execution_authority"],
            "none",
        )

    def test_final_validation_allows_regime_constraint_tighten_mutation(self):
        out = validate_final_execution_target(
            risk_approved_target={"SPY": 0.25, "CASH": 0.75},
            final_target={"SPY": 0.20, "CASH": 0.80},
            current_weights={"SPY": 0.25, "CASH": 0.75},
            policy_context={
                "post_risk_mutation_types": [REGIME_CONSTRAINT_MUTATION_TYPE],
                "post_risk_mutation_ledgers": [
                    {
                        "mutations": [
                            {
                                "type": REGIME_CONSTRAINT_MUTATION_TYPE,
                                "ticker": "SPY",
                                "before": 0.25,
                                "after": 0.20,
                                "reason": "regime hard constraint",
                            }
                        ]
                    }
                ],
                "material_drift_threshold": 0.001,
            },
            mode="blocking",
        )

        self.assertTrue(out["approved"], out)
        self.assertIn(REGIME_CONSTRAINT_MUTATION_TYPE, out["allowed_mutation_types"])
        self.assertEqual(out["unknown_mutation_types"], [])

    def test_pipeline_registers_regime_constraint_mutation_type(self):
        text = Path("services/pipeline.py").read_text()

        self.assertIn("legacy_mutation_classification_summary()", text)
        self.assertIn("REGIME_CONSTRAINT_MUTATION_TYPE", text)
        self.assertIn('"target_weight_mutation": "tighten_only"', text)
        self.assertIn("post_risk_mutation_ledgers", text)


if __name__ == "__main__":
    unittest.main()
