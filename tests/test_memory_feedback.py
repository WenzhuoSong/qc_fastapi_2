import unittest
from types import SimpleNamespace

from services.memory_feedback import (
    build_strategy_memory_feedback_from_records,
    extract_playground_strategy_names,
)


class MemoryFeedbackTests(unittest.TestCase):
    def test_extracts_strategy_names_from_flexible_assessment_shapes(self):
        assessment = {
            "selected_strategy": "momentum_lite_v1",
            "strategy_blend": [
                {"strategy_name": "low_vol_factor"},
                "risk_parity_lite",
            ],
        }

        names = extract_playground_strategy_names(assessment)

        self.assertEqual(
            names,
            ["low_vol_factor", "momentum_lite_v1", "risk_parity_lite"],
        )

    def test_discounts_underperforming_strategy_in_same_regime(self):
        records = [
            SimpleNamespace(
                regime_label="trending_bull",
                decision_quality_score=0.30,
                decision={"playground_strategy_assessment": {"selected_strategy": "momentum_lite_v1"}},
                raw_researcher_output={},
            ),
            SimpleNamespace(
                regime_label="trending_bull",
                decision_quality_score=0.40,
                decision={"playground_strategy_assessment": {"selected_strategy": "momentum_lite_v1"}},
                raw_researcher_output={},
            ),
            SimpleNamespace(
                regime_label="trending_bull",
                decision_quality_score=0.50,
                decision={"playground_strategy_assessment": {"selected_strategy": "momentum_lite_v1"}},
                raw_researcher_output={},
            ),
            SimpleNamespace(
                regime_label="high_vol",
                decision_quality_score=0.10,
                decision={"playground_strategy_assessment": {"selected_strategy": "momentum_lite_v1"}},
                raw_researcher_output={},
            ),
        ]

        feedback = build_strategy_memory_feedback_from_records(
            "trending_bull",
            ["momentum_lite_v1", "low_vol_factor"],
            records,
        )

        self.assertEqual(feedback["momentum_lite_v1"]["sample_size"], 3)
        self.assertEqual(feedback["momentum_lite_v1"]["discount_multiplier"], 0.70)
        self.assertFalse(feedback["momentum_lite_v1"]["can_bypass_risk_manager"])
        self.assertEqual(feedback["low_vol_factor"]["discount_multiplier"], 1.0)
        self.assertIn("insufficient", feedback["low_vol_factor"]["advisory_note"])


if __name__ == "__main__":
    unittest.main()
