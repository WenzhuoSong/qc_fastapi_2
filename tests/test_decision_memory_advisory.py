import unittest

from services.advisory_quality import build_advisory_outcome_backfill


class DecisionMemoryAdvisoryBackfillTest(unittest.TestCase):
    def test_builds_advisory_outcome_backfill_payload(self):
        payload = build_advisory_outcome_backfill(
            {
                "position_advisory_overrides": [
                    {
                        "ticker": "SPY",
                        "llm_advisory": "add",
                        "validator_result": "accepted_as_add_1.00%",
                    },
                    {
                        "ticker": "QQQ",
                        "llm_advisory": "trim",
                        "validator_result": "accepted_as_trim_1.00%",
                    },
                    {
                        "ticker": "IWM",
                        "llm_advisory": "add",
                        "validator_result": "rejected_add_not_allowed",
                    },
                ]
            },
            forward_returns_by_ticker={"SPY": 0.02, "QQQ": -0.01, "IWM": 0.03},
            benchmark_return=0.005,
        )

        outcomes = {row["ticker"]: row for row in payload["position_advisory_outcomes"]}
        self.assertEqual(outcomes["SPY"]["outcome_score"], 1.0)
        self.assertEqual(outcomes["QQQ"]["outcome_score"], 1.0)
        self.assertNotIn("IWM", outcomes)
        self.assertEqual(payload["position_advisory_benchmark_return"], 0.005)
        self.assertEqual(payload["position_advisory_quality"]["historical_feedback"]["verdict"], "insufficient")


if __name__ == "__main__":
    unittest.main()
