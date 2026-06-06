from datetime import UTC, date, datetime
import json
import unittest

from services.weekend_review_artifacts import (
    ALLOWED_SCHEMA_VERSIONS,
    append_weekly_review_artifacts,
    build_weekly_review_artifacts,
    serialize_weekly_review_artifact,
)
from services.weekend_review_loader import build_weekend_review_dataset
from services.weekend_review_metrics import build_weekly_review_metrics


class WeekendReviewArtifactsTests(unittest.TestCase):
    def test_builds_versioned_artifacts_from_metrics_payload(self):
        metrics = build_weekly_review_metrics(
            build_weekend_review_dataset(),
            review_as_of=datetime(2026, 6, 6, 12, 0, tzinfo=UTC),
        )

        artifacts = build_weekly_review_artifacts(
            metrics,
            created_at=datetime(2026, 6, 6, 13, 0, tzinfo=UTC),
        )
        serialized = [serialize_weekly_review_artifact(item) for item in artifacts]
        schemas = {item["schema_version"] for item in serialized}

        self.assertEqual(schemas, ALLOWED_SCHEMA_VERSIONS)
        self.assertTrue(all(item["artifact_id"] for item in serialized))
        self.assertTrue(all(item["execution_authority"] == "none" for item in serialized))
        self.assertTrue(all(item["target_weight_mutation"] == "none" for item in serialized))
        self.assertTrue(all(item["llm_summary"] is None for item in serialized))
        self.assertTrue(all(item["week_start"] == "2026-06-01" for item in serialized))
        self.assertTrue(all(item["week_end"] == "2026-06-07" for item in serialized))
        json.dumps(serialized)

    def test_execution_truth_artifact_includes_metrics_and_evidence_refs(self):
        dataset = build_weekend_review_dataset(
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
        metrics = build_weekly_review_metrics(
            dataset,
            review_as_of=datetime(2026, 6, 6, 12, 0, tzinfo=UTC),
        )

        artifacts = build_weekly_review_artifacts(metrics)
        execution = next(
            serialize_weekly_review_artifact(item)
            for item in artifacts
            if item.schema_version == "weekly_execution_truth_review_v1"
        )

        self.assertEqual(execution["metrics"]["commands_sent"], 1)
        self.assertEqual(execution["evidence_refs"][0]["command_id"], "analysis_1")
        self.assertEqual(execution["section_payload"]["schema_version"], "weekly_execution_truth_review_v1")
        self.assertEqual(execution["metric_contract_version"], "weekend_review_metrics_v1")

    def test_append_weekly_review_artifacts_is_append_only(self):
        metrics = build_weekly_review_metrics(
            build_weekend_review_dataset(),
            review_as_of=datetime(2026, 6, 6, 12, 0, tzinfo=UTC),
        )
        first = build_weekly_review_artifacts(
            metrics,
            created_at=datetime(2026, 6, 6, 13, 0, tzinfo=UTC),
        )[:1]
        second = build_weekly_review_artifacts(
            metrics,
            created_at=datetime(2026, 6, 6, 14, 0, tzinfo=UTC),
        )[:1]

        payload = append_weekly_review_artifacts({}, first)
        updated = append_weekly_review_artifacts(payload, second)

        self.assertEqual(payload["weekend_review_artifact_count"], 1)
        self.assertEqual(updated["weekend_review_artifact_count"], 2)
        self.assertNotEqual(
            updated["weekend_review_artifacts"][0]["artifact_id"],
            updated["weekend_review_artifacts"][1]["artifact_id"],
        )

    def test_rejects_execution_authority_and_target_mutation(self):
        with self.assertRaises(ValueError):
            serialize_weekly_review_artifact({
                "schema_version": "weekly_execution_truth_review_v1",
                "artifact_type": "execution_truth",
                "execution_authority": "gated",
                "target_weight_mutation": "none",
                "metrics": {},
            })

        with self.assertRaises(ValueError):
            serialize_weekly_review_artifact({
                "schema_version": "weekly_execution_truth_review_v1",
                "artifact_type": "execution_truth",
                "execution_authority": "none",
                "target_weight_mutation": "write_target",
                "metrics": {},
            })

    def test_rejects_llm_summary_in_pr2_artifacts(self):
        with self.assertRaises(ValueError):
            serialize_weekly_review_artifact({
                "schema_version": "weekly_hedge_review_v1",
                "artifact_type": "hedge_review",
                "execution_authority": "none",
                "target_weight_mutation": "none",
                "llm_summary": "Buy PSQ.",
                "metrics": {},
            })

    def test_rejects_unknown_schema(self):
        with self.assertRaises(ValueError):
            serialize_weekly_review_artifact({
                "schema_version": "weekly_unknown_review_v1",
                "artifact_type": "unknown",
                "execution_authority": "none",
                "target_weight_mutation": "none",
                "metrics": {},
            })

    def test_explicit_week_window_overrides_review_as_of(self):
        metrics = build_weekly_review_metrics(
            build_weekend_review_dataset(),
            review_as_of=datetime(2026, 6, 6, 12, 0, tzinfo=UTC),
        )

        artifacts = build_weekly_review_artifacts(
            metrics,
            week_start=date(2026, 5, 25),
            week_end=date(2026, 5, 31),
        )
        payload = serialize_weekly_review_artifact(artifacts[0])

        self.assertEqual(payload["week_start"], "2026-05-25")
        self.assertEqual(payload["week_end"], "2026-05-31")


if __name__ == "__main__":
    unittest.main()
