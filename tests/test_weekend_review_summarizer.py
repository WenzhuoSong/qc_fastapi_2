from datetime import UTC, datetime
import asyncio
import unittest

from services.weekend_review_artifacts import build_weekly_review_artifacts
from services.weekend_review_loader import build_weekend_review_dataset
from services.weekend_review_metrics import build_weekly_review_metrics
from services.weekend_review_summarizer import (
    REQUIRED_FOOTER,
    build_weekend_review_prompt,
    build_weekend_review_summary_report,
    sanitize_weekend_review_summary,
    summarize_weekend_review,
)


class WeekendReviewSummarizerTests(unittest.TestCase):
    def test_prompt_uses_metrics_not_target_builder_or_command_payload(self):
        metrics = build_weekly_review_metrics(
            build_weekend_review_dataset(
                execution_logs=[
                    {
                        "command_id": "analysis_1",
                        "command_type": "weight_adjustment",
                        "lifecycle_state": "reconciled",
                        "command_payload": {"weights": {"SPY": 0.1}},
                    }
                ]
            ),
            review_as_of=datetime(2026, 6, 6, 12, 0, tzinfo=UTC),
        )
        artifacts = build_weekly_review_artifacts(metrics)

        prompt = build_weekend_review_prompt(artifacts)
        text = prompt["prompt"]

        self.assertIn("execution truth", text.lower())
        self.assertIn("weekly_execution_truth_review_v1", text)
        self.assertNotIn('"target_weights"', text)
        self.assertNotIn('"target_builder_input"', text)
        self.assertNotIn('"command_payload"', text)
        self.assertNotIn('"qc_response"', text)
        self.assertNotIn('"section_payload"', text)
        self.assertEqual(prompt["execution_authority"], "none")
        self.assertEqual(prompt["target_weight_mutation"], "none")

    def test_sanitizer_removes_trading_policy_and_strategy_instructions(self):
        sanitized = sanitize_weekend_review_summary(
            "\n".join([
                "Execution truth: two commands reconciled.",
                "Buy PSQ tomorrow to hedge.",
                "Lower hedge threshold to 0.55.",
                "Promote momentum strategy to gated.",
                "Label maturity remains insufficient sample.",
            ])
        )

        self.assertIn("Execution truth", sanitized["summary_text"])
        self.assertIn("Label maturity", sanitized["summary_text"])
        self.assertNotIn("Buy PSQ", sanitized["summary_text"])
        self.assertNotIn("Lower hedge", sanitized["summary_text"])
        self.assertNotIn("Promote momentum", sanitized["summary_text"])
        self.assertIn("buy_order", sanitized["removed_actions"])
        self.assertIn("policy_change", sanitized["removed_actions"])
        self.assertIn("strategy_promotion", sanitized["removed_actions"])

    def test_summary_report_is_review_only_and_appends_required_footer(self):
        prompt = {
            "contract_version": "weekend_review_summary_prompt_v1",
            "artifact_count": 2,
            "included_schema_versions": ["weekly_execution_truth_review_v1"],
        }

        report = build_weekend_review_summary_report(
            raw_summary="Execution truth looked stable.\nSet weights to SPY 10%.",
            prompt_payload=prompt,
            created_at=datetime(2026, 6, 6, 13, 0, tzinfo=UTC),
        )

        self.assertEqual(report["schema_version"], "weekend_review_llm_summary_v1")
        self.assertEqual(report["execution_authority"], "none")
        self.assertEqual(report["target_weight_mutation"], "none")
        self.assertTrue(report["llm_summary_is_explanatory_only"])
        self.assertIn(REQUIRED_FOOTER, report["summary_text"])
        self.assertNotIn("Set weights", report["summary_text"])
        self.assertEqual(report["removed_forbidden_line_count"], 1)

    def test_async_summarize_weekend_review_uses_injected_llm_callable(self):
        metrics = build_weekly_review_metrics(
            build_weekend_review_dataset(),
            review_as_of=datetime(2026, 6, 6, 12, 0, tzinfo=UTC),
        )
        artifacts = build_weekly_review_artifacts(metrics)
        seen_prompt = {}

        async def fake_llm(prompt: str) -> str:
            seen_prompt["text"] = prompt
            return "All headline metrics are deterministic. Review-only follow-up: inspect blockers."

        report = asyncio.run(
            summarize_weekend_review(
                artifacts,
                llm_complete=fake_llm,
                created_at=datetime(2026, 6, 6, 13, 0, tzinfo=UTC),
            )
        )

        self.assertIn("weekly_execution_truth_review_v1", seen_prompt["text"])
        self.assertEqual(report["execution_authority"], "none")
        self.assertEqual(report["target_weight_mutation"], "none")
        self.assertEqual(report["removed_forbidden_line_count"], 0)
        self.assertIn("Review-only", report["summary_text"])

    def test_prompt_includes_insufficient_sample_context(self):
        metrics = build_weekly_review_metrics(
            build_weekend_review_dataset(),
            review_as_of=datetime(2026, 6, 6, 12, 0, tzinfo=UTC),
        )
        artifacts = build_weekly_review_artifacts(metrics)

        prompt = build_weekend_review_prompt(artifacts)

        self.assertIn("insufficient_sample", prompt["prompt"])
        self.assertIn("Use 'insufficient sample'", prompt["prompt"])


if __name__ == "__main__":
    unittest.main()
