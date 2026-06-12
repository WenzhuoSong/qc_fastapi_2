import unittest

from services.scorecard_execution_semantics import (
    scorecard_blocks_new_risk,
    scorecard_no_add_reason,
)


class ScorecardExecutionSemanticsTest(unittest.TestCase):
    def test_human_required_data_quality_does_not_block_new_risk(self):
        scorecard = {
            "investment_permission": "small_overweight_only",
            "require_human_confirmation": True,
            "triggered_rules": ["limited_data_quality"],
        }

        self.assertFalse(scorecard_blocks_new_risk(scorecard))
        self.assertIsNone(scorecard_no_add_reason(scorecard))

    def test_strategy_advisory_only_blocks_automatic_adds(self):
        scorecard = {
            "investment_permission": "small_overweight_only",
            "require_human_confirmation": True,
            "triggered_rules": ["strategy_advisory_only"],
        }

        self.assertTrue(scorecard_blocks_new_risk(scorecard))
        self.assertEqual(scorecard_no_add_reason(scorecard), "scorecard_strategy_advisory_only")

    def test_insufficient_execution_evidence_blocks_automatic_adds(self):
        scorecard = {
            "investment_permission": "small_overweight_only",
            "require_human_confirmation": True,
            "triggered_rules": ["insufficient_execution_evidence"],
        }

        self.assertTrue(scorecard_blocks_new_risk(scorecard))
        self.assertEqual(scorecard_no_add_reason(scorecard), "scorecard_insufficient_execution_evidence")

    def test_hard_no_add_permission_blocks_new_risk(self):
        scorecard = {
            "investment_permission": "reduce_risk_only",
            "require_human_confirmation": False,
        }

        self.assertTrue(scorecard_blocks_new_risk(scorecard))
        self.assertEqual(scorecard_no_add_reason(scorecard), "scorecard_no_add")


if __name__ == "__main__":
    unittest.main()
