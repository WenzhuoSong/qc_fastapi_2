import unittest

from services.advisory_quality import build_advisory_outcome_backfill
from services.decision_ledger_memory import (
    build_decision_ledger_review,
    compact_decision_ledger_for_memory,
)


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

    def test_compacts_decision_ledger_for_memory_without_execution_impact(self):
        compact = compact_decision_ledger_for_memory(
            {
                "phase": "phase_3_sparse_lifecycle",
                "portfolio_summary": {
                    "risk_approved": False,
                    "execution_status": "not_sent",
                    "governance_available": True,
                    "ticker_count": 2,
                },
                "tickers": {
                    "QQQ": {
                        "ticker": "QQQ",
                        "proposed_action": "trim",
                        "final_action": "none",
                        "execution_status": "not_sent",
                        "risk_result": "blocked",
                        "reason_codes": ["risk_rejected", "human_required"],
                        "trade_lifecycle": {
                            "final_target": 0.12,
                            "changed_by": ["risk_rejected_final_target_current"],
                        },
                        "evidence_used": {
                            "position_governance": {
                                "decision": "trim_review",
                            }
                        },
                        "explanation": {"position_state": "risk_budget_review"},
                    },
                    "XLK": {
                        "ticker": "XLK",
                        "proposed_action": "hold",
                        "final_action": "hold",
                        "execution_status": "not_sent",
                        "risk_result": "blocked",
                        "reason_codes": [],
                        "trade_lifecycle": {"final_target": 0.10, "changed_by": []},
                        "evidence_used": {"position_governance": {"decision": "hold"}},
                    },
                },
                "warnings": ["example_warning"],
            }
        )

        self.assertTrue(compact["available"])
        self.assertEqual(compact["execution_impact"], "none")
        self.assertEqual(compact["counts"]["blocked_count"], 1)
        self.assertEqual(compact["counts"]["changed_count"], 1)
        self.assertEqual(compact["top_decisions"][0]["ticker"], "QQQ")
        self.assertEqual(compact["top_decisions"][0]["final_action"], "none")
        self.assertEqual(compact["warnings"], ["example_warning"])

        review = build_decision_ledger_review(compact)
        self.assertTrue(review["available"])
        self.assertTrue(review["diagnostic_only"])
        self.assertEqual(review["execution_impact"], "none")
        self.assertEqual(review["blocked_count"], 1)
        self.assertEqual(review["changed_count"], 1)
        self.assertIn("proposed=", review["summary"])
        self.assertIn("blocked=QQQ:trim->none", review["summary"])
        self.assertEqual(review["examples"]["blocked"][0]["ticker"], "QQQ")

    def test_missing_decision_ledger_compacts_to_unavailable(self):
        compact = compact_decision_ledger_for_memory({})

        self.assertFalse(compact["available"])
        self.assertEqual(compact["reason"], "missing_decision_ledger")

        review = build_decision_ledger_review(compact)
        self.assertFalse(review["available"])
        self.assertEqual(review["execution_impact"], "none")


if __name__ == "__main__":
    unittest.main()
