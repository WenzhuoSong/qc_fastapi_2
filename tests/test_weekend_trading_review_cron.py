from datetime import UTC, datetime
import asyncio
import unittest

from cron.weekend_trading_review import (
    build_ops_failure_message,
    format_weekend_review_telegram,
    run_weekend_trading_review,
)
from services.weekend_review_loader import build_weekend_review_dataset


class WeekendTradingReviewCronTests(unittest.TestCase):
    def test_skips_market_open_by_default(self):
        calls = {"loader": 0}

        async def fake_loader(**kwargs):
            calls["loader"] += 1
            return build_weekend_review_dataset()

        result = asyncio.run(
            run_weekend_trading_review(
                now=datetime(2026, 6, 5, 14, 0, tzinfo=UTC),  # Friday 10:00 ET.
                dataset_loader=fake_loader,
                persist=False,
            )
        )

        self.assertEqual(result.status, "skipped")
        self.assertEqual(result.reason, "market_open")
        self.assertEqual(result.execution_authority, "none")
        self.assertEqual(result.target_weight_mutation, "none")
        self.assertEqual(calls["loader"], 0)

    def test_market_open_force_still_review_only(self):
        persisted_payloads = []

        async def fake_loader(**kwargs):
            return build_weekend_review_dataset()

        async def fake_persist(payload):
            persisted_payloads.append(payload)
            return "review-row-1"

        result = asyncio.run(
            run_weekend_trading_review(
                now=datetime(2026, 6, 5, 14, 0, tzinfo=UTC),
                allow_market_open=True,
                dataset_loader=fake_loader,
                artifact_persister=fake_persist,
                safety_report_loader=_fake_safety_report,
                persist=True,
            )
        )

        self.assertEqual(result.status, "success")
        self.assertEqual(result.execution_authority, "none")
        self.assertEqual(result.target_weight_mutation, "none")
        self.assertTrue(result.persisted)
        self.assertEqual(result.persisted_ref, "review-row-1")
        self.assertEqual(len(persisted_payloads), 1)
        self.assertEqual(persisted_payloads[0]["execution_authority"], "none")
        self.assertEqual(persisted_payloads[0]["target_weight_mutation"], "none")

    def test_builds_metrics_artifacts_summary_persists_and_notifies(self):
        persisted_payloads = []
        notifications = []

        async def fake_loader(**kwargs):
            return build_weekend_review_dataset(
                execution_logs=[
                    {
                        "command_id": "analysis_1",
                        "command_type": "weight_adjustment",
                        "lifecycle_state": "reconciled",
                        "submitted_at": "2026-06-05T15:00:00+00:00",
                        "command_payload": {"weights": {"SPY": 0.1}},
                    }
                ]
            )

        async def fake_persist(payload):
            persisted_payloads.append(payload)
            return 42

        async def fake_notify(payload):
            notifications.append(payload)

        async def fake_llm(prompt: str) -> str:
            self.assertIn("weekly_execution_truth_review_v1", prompt)
            self.assertIn("weekly_decision_degradation_review_v1", prompt)
            return "Execution truth: metrics are deterministic. Review-only follow-up: inspect blockers."

        result = asyncio.run(
            run_weekend_trading_review(
                now=datetime(2026, 6, 6, 12, 0, tzinfo=UTC),
                dataset_loader=fake_loader,
                artifact_persister=fake_persist,
                notifier=fake_notify,
                llm_complete=fake_llm,
                safety_report_loader=_fake_safety_report,
                notify=True,
                persist=True,
            )
        )

        self.assertEqual(result.status, "success")
        self.assertEqual(result.artifact_count, 9)
        self.assertEqual(result.persisted_ref, 42)
        self.assertTrue(result.notified)
        self.assertEqual(len(persisted_payloads), 1)
        payload = persisted_payloads[0]
        self.assertEqual(payload["schema_version"], "weekend_trading_review_cron_v1")
        self.assertEqual(payload["weekend_review_artifact_count"], 9)
        self.assertIn("safety_invariants", payload)
        self.assertEqual(payload["safety_invariants"]["finding_count"], 0)
        self.assertIn("weekend_review_artifacts", payload)
        self.assertIn("weekend_review_summary", payload)
        self.assertIn("weekend_review_metrics", payload)
        self.assertEqual(
            payload["weekend_review_metrics"]["sections"]["execution_truth"]["metrics"]["commands_sent"],
            1,
        )
        self.assertEqual(len(notifications), 1)
        self.assertIn("execution_authority=none", notifications[0]["text"])

    def test_telegram_summary_marks_review_only(self):
        async def fake_loader(**kwargs):
            return build_weekend_review_dataset()

        result = asyncio.run(
            run_weekend_trading_review(
                now=datetime(2026, 6, 6, 12, 0, tzinfo=UTC),
                dataset_loader=fake_loader,
                safety_report_loader=_fake_safety_report,
                persist=False,
            )
        )

        text = format_weekend_review_telegram({
            "week_start": result.week_start,
            "week_end": result.week_end,
            "weekend_review_artifact_count": result.artifact_count,
            "weekend_review_metrics": result.metrics,
            "weekend_review_summary": result.summary_report,
        })

        self.assertIn("Weekend trading review", text)
        self.assertIn("execution_authority=none", text)
        self.assertIn("target_weight_mutation=none", text)

    def test_telegram_summary_includes_split_execution_outcomes(self):
        text = format_weekend_review_telegram({
            "week_start": "2026-06-01",
            "week_end": "2026-06-07",
            "weekend_review_artifact_count": 9,
            "safety_invariants": {
                "finding_count": 2,
                "fail_safe_required": True,
            },
            "weekend_review_metrics": {
                "sections": {
                    "decision_degradation": {
                        "metrics": {
                            "normal_sample_count": 3,
                            "degraded_sample_count": 1,
                        }
                    },
                    "execution_truth": {
                        "metrics": {
                            "commands_sent": 22,
                            "filled_count": 12,
                            "noop_count": 0,
                            "stuck_in_flight_count": 0,
                            "true_qc_rejected_count": 1,
                            "preflight_blocked_count": 15,
                            "not_sent_count": 15,
                            "timeout_no_ack_count": 4,
                            "timeout_no_execution_confirmed_count": 9,
                            "duplicate_target_count": 4,
                        }
                    },
                    "intent_execution": {
                        "metrics": {
                            "risk_block_count": 0,
                            "final_validation_block_count": 0,
                            "execution_preflight_block_count": 15,
                            "daily_command_cap_block_count": 3,
                            "daily_turnover_cap_block_count": 2,
                            "dedupe_count": 4,
                            "execution_timeout_count": 4,
                            "qc_reject_count": 1,
                        }
                    },
                    "label_maturity": {"metrics": {}},
                    "hedge_review": {"metrics": {}},
                }
            },
            "weekend_review_summary": {},
        })

        self.assertIn("Execution outcomes:", text)
        self.assertIn("Decision degradation:", text)
        self.assertIn("normal=3", text)
        self.assertIn("Safety invariants:", text)
        self.assertIn("findings=2", text)
        self.assertIn("qc_reject=1", text)
        self.assertIn("preflight=15", text)
        self.assertIn("timeout_ack=4", text)
        self.assertIn("no_exec=9", text)
        self.assertIn("daily_cap=3", text)
        self.assertIn("turnover_cap=2", text)
        self.assertIn("timeout=4", text)
        self.assertIn("qc_reject=1", text)

    def test_ops_failure_message_is_not_trading_failure(self):
        message = build_ops_failure_message(RuntimeError("db unavailable"))

        self.assertIn("ops failure", message)
        self.assertIn("no trading action attempted", message)
        self.assertNotIn("trading failed", message.lower())

    def test_cron_source_does_not_import_execution_pipeline(self):
        with open("cron/weekend_trading_review.py", "r", encoding="utf-8") as handle:
            source = handle.read()

        forbidden = [
            "run_full_pipeline",
            "run_executor_async",
            "services.pipeline",
            "agents.executor",
            "qc_command",
        ]
        for token in forbidden:
            self.assertNotIn(token, source)

def _fake_safety_report() -> dict:
    return {
        "schema_version": "safety_config_fail_safe_report_v1",
        "execution_authority": "none",
        "target_weight_mutation": "none",
        "finding_count": 0,
        "fail_safe_required": False,
        "findings": [],
        "effective_states": {},
    }


if __name__ == "__main__":
    unittest.main()
