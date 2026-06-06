from datetime import UTC, datetime
from pathlib import Path
import unittest

from cron.weekend_trading_review import build_weekend_review_payload
from services.weekend_review_artifacts import build_weekly_review_artifacts, serialize_weekly_review_artifact
from services.weekend_review_loader import build_weekend_review_dataset
from services.weekend_review_metrics import build_weekly_review_metrics
from services.weekend_review_operator_view import (
    build_weekend_review_operator_pack,
    build_weekend_review_operator_view,
    format_weekend_review_operator_text,
)
from services.weekend_review_summarizer import build_weekend_review_summary_report


class WeekendReviewOperatorViewTests(unittest.TestCase):
    def test_builds_read_only_operator_view_from_review_payload(self):
        payload = _sample_review_payload()

        view = build_weekend_review_operator_view(payload)

        self.assertEqual(view["schema_version"], "weekend_review_operator_view_v1")
        self.assertEqual(view["execution_authority"], "none")
        self.assertEqual(view["target_weight_mutation"], "none")
        self.assertTrue(view["review_only"])
        self.assertEqual(view["headline"]["commands_sent"], 1)
        self.assertEqual(view["headline"]["filled_count"], 1)
        self.assertIn("execution_truth", view["sections"])
        self.assertIn("blocker_distribution", view["sections"])
        self.assertIn("label_maturity", view["sections"])
        self.assertIn("hedge_review", view["sections"])
        self.assertIn("debate_value", view["sections"])
        self.assertIn("basket_portfolio", view["sections"])
        self.assertIn("prior_review_self_assessment", view["sections"])
        self.assertEqual(len(view["acceptance_answers"]), 10)
        self.assertTrue(all(item["llm_computed"] is False for item in view["acceptance_answers"]))
        self.assertTrue(all(item["execution_authority"] == "none" for item in view["acceptance_answers"]))

    def test_recommendations_are_visibly_review_only(self):
        payload = _sample_review_payload(
            summary_text=(
                "Execution truth looked stable.\n"
                "Review-only follow-up: inspect daily cap blockers.\n"
                "Operator review suggested: compare false negatives with SPY drawdowns."
            )
        )

        view = build_weekend_review_operator_view(payload)
        text = format_weekend_review_operator_text(view)

        self.assertGreaterEqual(len(view["recommendations"]), 2)
        self.assertTrue(all(item["label"] == "review-only" for item in view["recommendations"]))
        self.assertTrue(all(item["execution_authority"] == "none" for item in view["recommendations"]))
        self.assertIn("review-only:", text)
        self.assertIn("execution_authority=none", text)
        self.assertIn("target_weight_mutation=none", text)

    def test_rejects_execution_authority_or_target_mutation(self):
        payload = _sample_review_payload()
        payload["execution_authority"] = "gated"

        with self.assertRaises(ValueError):
            build_weekend_review_operator_view(payload)

        payload = _sample_review_payload()
        payload["target_weight_mutation"] = "write_target"

        with self.assertRaises(ValueError):
            build_weekend_review_operator_view(payload)

    def test_operator_text_includes_data_first_sections(self):
        payload = _sample_review_payload()
        view = build_weekend_review_operator_view(payload)

        text = format_weekend_review_operator_text(view)

        self.assertIn("Weekend Review Operator View", text)
        self.assertIn("Execution truth:", text)
        self.assertIn("Execution outcomes:", text)
        self.assertIn("Top blocker:", text)
        self.assertIn("Labels:", text)
        self.assertIn("Hedge:", text)
        self.assertIn("Debate change rate:", text)
        self.assertIn("Prior review:", text)

    def test_acceptance_answers_map_to_deterministic_sources(self):
        view = build_weekend_review_operator_view(_sample_review_payload())

        answers = view["acceptance_answers"]
        sources = {item["id"]: item["deterministic_source"] for item in answers}

        self.assertEqual(len(answers), 10)
        self.assertIn("intent_execution", sources[1])
        self.assertIn("execution_truth", sources[2])
        self.assertIn("execution_truth", sources[3])
        self.assertIn("blocker_distribution", sources[4])
        self.assertIn("label_maturity", sources[6])
        self.assertIn("hedge_review", sources[7])
        self.assertIn("debate_value", sources[8])
        self.assertIn("basket_portfolio", sources[9])
        self.assertIn("prior_review_self_assessment", sources[10])

    def test_operator_pack_contains_text_view_and_optional_full_report(self):
        payload = _sample_review_payload()

        compact = build_weekend_review_operator_pack(payload)
        full = build_weekend_review_operator_pack(payload, include_full_report=True)

        self.assertEqual(compact["schema_version"], "weekend_review_operator_pack_v1")
        self.assertTrue(compact["review_only"])
        self.assertEqual(compact["execution_authority"], "none")
        self.assertEqual(compact["target_weight_mutation"], "none")
        self.assertIn("Weekend Review Operator View", compact["text"])
        self.assertIn("acceptance_answers", compact["view"])
        self.assertIsNone(compact["full_report"])
        self.assertEqual(full["full_report"]["schema_version"], "weekend_trading_review_cron_v1")

    def test_ops_api_routes_are_registered_as_read_only_weekend_review_entrypoints(self):
        main_source = Path("main.py").read_text(encoding="utf-8")
        api_source = Path("api/ops.py").read_text(encoding="utf-8")

        self.assertIn("from api.ops import router as ops_router", main_source)
        self.assertIn("app.include_router(ops_router, prefix=\"/api\")", main_source)
        self.assertIn("@router.get(\"/weekend-review/latest\")", api_source)
        self.assertIn("@router.get(\"/weekend-review/latest/text\"", api_source)
        self.assertNotIn("SetWeights", api_source)
        self.assertNotIn("execute_weights", api_source)


def _sample_review_payload(summary_text: str = "Review-only follow-up: inspect blockers.") -> dict:
    now = datetime(2026, 6, 6, 12, 0, tzinfo=UTC)
    dataset = build_weekend_review_dataset(
        execution_logs=[
            {
                "command_id": "analysis_1",
                "command_type": "weight_adjustment",
                "lifecycle_state": "reconciled",
                "submitted_at": "2026-06-05T15:00:00+00:00",
                "command_payload": {"weights": {"SPY": 0.1}},
            }
        ],
        validation_observations=[
            {
                "id": 1,
                "observation_id": "intent-1",
                "observation_type": "intent_vs_execution",
                "schema_version": "intent_vs_execution_v1",
                "observed_at": "2026-06-05T15:00:00+00:00",
                "observation_date": "2026-06-05",
                "observation_payload": {
                    "risk_approved": True,
                    "blocker_events": [{"blocker_category": "execution_daily_cap"}],
                    "blockers": ["daily_command_count_ok"],
                },
                "outcome_payload": {"command_sent": False, "not_sent_reason": "daily_cap"},
                "status": "completed",
            }
        ],
    )
    metrics = build_weekly_review_metrics(dataset, review_as_of=now)
    artifacts = build_weekly_review_artifacts(metrics, created_at=now)
    summary = build_weekend_review_summary_report(
        raw_summary=summary_text,
        prompt_payload={
            "contract_version": "weekend_review_summary_prompt_v1",
            "artifact_count": len(artifacts),
            "included_schema_versions": [],
        },
        created_at=now,
    )
    return build_weekend_review_payload(
        metrics=metrics,
        artifacts=[serialize_weekly_review_artifact(item) for item in artifacts],
        summary_report=summary,
        review_as_of=now,
        week_start=now.date(),
        week_end=now.date(),
        market_status={"is_open": False, "phase": "closed"},
    )


if __name__ == "__main__":
    unittest.main()
