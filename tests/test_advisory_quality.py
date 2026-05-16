import unittest

from services.advisory_quality import (
    build_advisory_quality_diagnostics,
    score_advisory_outcomes,
)


class AdvisoryQualityTest(unittest.TestCase):
    def test_builds_current_run_validator_diagnostics(self):
        diagnostics = build_advisory_quality_diagnostics([
            {"ticker": "QQQ", "llm_advisory": "trim", "validator_result": "accepted_as_trim_1.00%"},
            {"ticker": "SPY", "llm_advisory": "add", "validator_result": "rejected_human_required_add"},
            {"ticker": "XLE", "llm_advisory": "exit", "validator_result": "converted_exit_to_hold_review"},
        ])

        current = diagnostics["current_run"]
        self.assertTrue(diagnostics["diagnostic_only"])
        self.assertEqual(current["total"], 3)
        self.assertEqual(current["accepted"], 1)
        self.assertEqual(current["rejected"], 1)
        self.assertEqual(current["converted"], 1)
        self.assertEqual(current["accepted_tickers"], ["QQQ"])
        self.assertEqual(diagnostics["historical_feedback"]["verdict"], "insufficient")

    def test_scores_forward_outcomes_by_advisory_direction(self):
        scored = score_advisory_outcomes(
            [
                {"ticker": "SPY", "llm_advisory": "add", "validator_result": "accepted_as_add_1.00%"},
                {"ticker": "QQQ", "llm_advisory": "trim", "validator_result": "accepted_as_trim_1.00%"},
                {"ticker": "IWM", "llm_advisory": "add", "validator_result": "rejected_add_not_allowed"},
            ],
            forward_returns_by_ticker={"SPY": 0.02, "QQQ": -0.01, "IWM": 0.03},
            benchmark_return=0.005,
        )

        by_ticker = {row["ticker"]: row for row in scored}
        self.assertEqual(by_ticker["SPY"]["outcome_score"], 1.0)
        self.assertEqual(by_ticker["QQQ"]["outcome_score"], 1.0)
        self.assertNotIn("IWM", by_ticker)

    def test_historical_feedback_becomes_positive_after_enough_samples(self):
        diagnostics = build_advisory_quality_diagnostics(
            [],
            historical_records=[
                {"outcome_score": 1.0},
                {"outcome_score": 1.0},
                {"outcome_score": 0.5},
                {"outcome_score": 1.0},
                {"outcome_score": 0.0},
            ],
        )

        self.assertEqual(diagnostics["historical_feedback"]["sample_size"], 5)
        self.assertEqual(diagnostics["historical_feedback"]["verdict"], "positive")
        self.assertEqual(diagnostics["execution_impact"], "none")


if __name__ == "__main__":
    unittest.main()
