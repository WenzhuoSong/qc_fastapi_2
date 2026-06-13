from datetime import datetime
import unittest

from services.strategy_execution_evidence_notifications import (
    prepare_strategy_execution_evidence_notification,
)


def _artifact(rows):
    return [{
        "schema_version": "decision_funnel_observability_v1",
        "buy_intents": [
            {"ticker": "SOXX"},
            {"ticker": "FTXL"},
        ],
        "data_quality_flags": {
            "strategy_execution_evidence": {
                "schema_version": "strategy_execution_evidence_summary_v1",
                "rows": rows,
            }
        },
    }]


class StrategyExecutionEvidenceNotificationTest(unittest.TestCase):
    def test_does_not_notify_insufficient_execution_evidence(self):
        message, state = prepare_strategy_execution_evidence_notification(
            analysis_id=1,
            diagnostic_artifacts=_artifact([
                {
                    "strategy_name": "momentum_lite_v1",
                    "execution_evidence_status": "insufficient_execution_evidence",
                    "certification_status": "research_supported",
                    "approved_use": "research_only",
                    "suggested_use": "advisory",
                    "failed_checks": ["live_samples_min"],
                }
            ]),
            previous_state={},
            now=datetime(2026, 6, 12, 15, 0),
        )

        self.assertIsNone(message)
        self.assertEqual(
            state["latest_status_by_strategy"]["momentum_lite_v1"],
            "insufficient_execution_evidence",
        )

    def test_notifies_new_execution_grade_strategy_once(self):
        message, state = prepare_strategy_execution_evidence_notification(
            analysis_id=2,
            diagnostic_artifacts=_artifact([
                {
                    "strategy_name": "momentum_lite_v1",
                    "execution_evidence_status": "execution_grade_validated",
                    "certification_status": "advisory",
                    "approved_use": "advisory",
                    "suggested_use": "advisory",
                    "failed_checks": [],
                    "evidence_checks": {
                        "checks": {
                            "live_samples_min": {"pass": True},
                            "turnover_below_advisory_max": {"pass": True},
                        },
                        "failed": [],
                    },
                }
            ]),
            previous_state={
                "latest_status_by_strategy": {
                    "momentum_lite_v1": "insufficient_execution_evidence",
                }
            },
            now=datetime(2026, 6, 12, 15, 5),
        )

        self.assertIsNotNone(message)
        self.assertIn("Strategy execution evidence certified", message)
        self.assertIn("momentum_lite_v1", message)
        self.assertIn("Current buy-intent tickers: SOXX, FTXL", message)
        self.assertIn("evidence_checks: 2/2 passed", message)
        self.assertIn("failed_checks: none", message)
        self.assertEqual(
            state["latest_status_by_strategy"]["momentum_lite_v1"],
            "execution_grade_validated",
        )
        self.assertIn(
            "momentum_lite_v1",
            state["notified_validated_by_strategy"],
        )

        second_message, _ = prepare_strategy_execution_evidence_notification(
            analysis_id=3,
            diagnostic_artifacts=_artifact([
                {
                    "strategy_name": "momentum_lite_v1",
                    "execution_evidence_status": "execution_grade_validated",
                }
            ]),
            previous_state=state,
            now=datetime(2026, 6, 12, 16, 0),
        )
        self.assertIsNone(second_message)

    def test_reupgrade_after_prior_notification_does_not_notify_again(self):
        prior = {
            "latest_status_by_strategy": {
                "momentum_lite_v1": "insufficient_execution_evidence",
            },
            "notified_validated_by_strategy": {
                "momentum_lite_v1": {
                    "first_notified_at": "2026-06-10T15:00:00",
                    "analysis_id": 10,
                }
            },
        }

        message, state = prepare_strategy_execution_evidence_notification(
            analysis_id=11,
            diagnostic_artifacts=_artifact([
                {
                    "strategy_name": "momentum_lite_v1",
                    "execution_evidence_status": "execution_grade_validated",
                    "certification_status": "advisory",
                    "approved_use": "advisory",
                    "suggested_use": "primary",
                    "failed_checks": [],
                }
            ]),
            previous_state=prior,
            now=datetime(2026, 6, 12, 17, 0),
        )

        self.assertIsNone(message)
        self.assertEqual(
            state["notified_validated_by_strategy"]["momentum_lite_v1"]["analysis_id"],
            10,
        )


if __name__ == "__main__":
    unittest.main()
