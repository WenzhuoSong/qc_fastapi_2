import unittest

from services.decision_live_validation import (
    format_decision_live_validation_report,
    stage_outputs_from_step_logs,
    validate_decision_live_artifacts,
)


class DecisionLiveValidationTests(unittest.TestCase):
    def test_passes_when_required_live_validation_blocks_are_visible(self):
        result = validate_decision_live_artifacts(
            stage_outputs={
                "2d_evidence_scorecard": {
                    "evidence_bundle": {
                        "strategies": {
                            "snapshot_count": 7,
                            "evidence_summary": {"live_fit": "insufficient"},
                        }
                    }
                },
                "5d_proposal_shaper": {
                    "applied": True,
                    "clip_log": ["loss_review_no_add:FTXL"],
                },
                "6ba_position_governance": {
                    "manual_action_hints": [
                        {
                            "ticker": "FTXL",
                            "reason_codes": [
                                "unrealized_loss_review",
                                "basket_review",
                                "advisory_basket_loss_review",
                            ],
                        }
                    ],
                    "portfolio_summary": {
                        "position_explanations": [
                            {
                                "ticker": "XLRE",
                                "position_state": "hard_risk_review",
                                "explanation_facts": {"severity": "hard_risk"},
                            }
                        ]
                    },
                },
                "6d_decision_ledger": {
                    "tickers": {
                        "FTXL": {
                            "proposed_action": "trim",
                            "final_action": "none",
                            "source_effects": {
                                "qc": ["unrealized_loss_review"],
                                "knowledge": ["correlated_basket_review"],
                                "risk": ["risk_rejected"],
                            },
                        }
                    }
                },
                "8_communicator": {
                    "text": (
                        "<b>Data quality detail</b>\n"
                        "<b>Proposal shaping</b>\n"
                        "<b>Decision ledger</b>\n"
                        "  FTXL: trim -> none | sources=risk,knowledge,qc\n"
                        "<b>Position governance</b>\n"
                        "  explain XLRE: hard-risk review\n"
                        "  manual trim review: FTXL 3.0%->2.0% "
                        "(advisory=weak-positive, basket loss review)"
                    )
                },
            }
        )

        self.assertEqual(result["overall"], "pass")
        statuses = {row["name"]: row["status"] for row in result["checks"]}
        self.assertEqual(statuses["proposal_shaping"], "pass")
        self.assertEqual(statuses["advisory_weak_positive"], "pass")
        self.assertEqual(statuses["source_effects"], "pass")

    def test_fails_when_triggered_blocks_are_missing_from_telegram(self):
        result = validate_decision_live_artifacts(
            stage_outputs={
                "2d_evidence_scorecard": {
                    "evidence_bundle": {
                        "strategies": {
                            "snapshot_count": 7,
                            "evidence_summary": {"live_fit": "insufficient"},
                        }
                    }
                },
                "5d_proposal_shaper": {"applied": True, "clip_log": ["clip"]},
                "6ba_position_governance": {
                    "manual_action_hints": [
                        {"ticker": "FTXL", "reason_codes": ["advisory_basket_loss_review"]}
                    ],
                },
                "6d_decision_ledger": {
                    "tickers": {
                        "FTXL": {
                            "proposed_action": "trim",
                            "final_action": "none",
                            "source_effects": {"risk": ["risk_rejected"]},
                        }
                    }
                },
                "8_communicator": {"text": "short message without validation blocks"},
            }
        )

        self.assertEqual(result["overall"], "fail")
        failures = {row["name"] for row in result["checks"] if row["status"] == "fail"}
        self.assertIn("data_quality_detail", failures)
        self.assertIn("proposal_shaping", failures)
        self.assertIn("manual_trim_review", failures)
        self.assertIn("advisory_weak_positive", failures)
        self.assertIn("decision_ledger", failures)

    def test_skips_non_triggered_checks_without_failing(self):
        result = validate_decision_live_artifacts(
            stage_outputs={
                "6d_decision_ledger": {
                    "tickers": {
                        "QQQ": {
                            "proposed_action": "hold",
                            "final_action": "hold",
                            "source_effects": {},
                        }
                    }
                },
                "8_communicator": {
                    "text": "<b>Data quality detail</b>\n<b>Decision ledger</b>\n  QQQ: hold -> hold"
                },
            }
        )

        statuses = {row["name"]: row["status"] for row in result["checks"]}
        self.assertEqual(statuses["proposal_shaping"], "skipped")
        self.assertEqual(statuses["manual_trim_review"], "skipped")
        self.assertEqual(statuses["advisory_weak_positive"], "skipped")
        self.assertEqual(statuses["source_effects"], "skipped")
        self.assertEqual(result["overall"], "pass")

    def test_formats_compact_report(self):
        result = validate_decision_live_artifacts(
            stage_outputs={
                "6d_decision_ledger": {"tickers": {}},
                "8_communicator": {"text": ""},
            }
        )

        report = format_decision_live_validation_report(result)

        self.assertIn("Decision live validation: fail", report)
        self.assertIn("decision_ledger", report)

    def test_normalizes_stage_outputs_from_step_logs(self):
        class Row:
            def __init__(self, stage, output_data):
                self.stage = stage
                self.output_data = output_data

        out = stage_outputs_from_step_logs([
            Row("6d_decision_ledger", {"tickers": {}}),
            Row("8_communicator", {"text": "hello"}),
            Row("", {"ignored": True}),
            Row("bad", "not a dict"),
        ])

        self.assertEqual(out["8_communicator"]["text"], "hello")
        self.assertEqual(out["bad"], {})
        self.assertNotIn("", out)


if __name__ == "__main__":
    unittest.main()
