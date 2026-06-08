import unittest

from services.training_data_authority import (
    assert_training_data_source_authority,
    evaluate_training_data_source,
)


class TrainingDataAuthorityTests(unittest.TestCase):
    def test_legacy_raw_json_is_not_training_authoritative(self):
        verdict = evaluate_training_data_source(
            source_type="agent_step_log",
            payload={"researcher_output": {"market": "risk_off"}},
        )

        self.assertFalse(verdict["allowed"])
        self.assertEqual(verdict["training_data_authority"], "not_authoritative")
        self.assertIn("non_authoritative_source_type", verdict["reasons"])

    def test_diagnostic_artifact_requires_version_and_no_execution_authority(self):
        verdict = evaluate_training_data_source(
            source_type="diagnostic_artifact",
            payload={
                "schema_version": "decision_feature_snapshot_v1",
                "artifact_id": "decision_feature_snapshot_v1:42:abc",
                "execution_authority": "none",
            },
        )

        self.assertTrue(verdict["allowed"])

        bad = evaluate_training_data_source(
            source_type="diagnostic_artifact",
            payload={
                "artifact_id": "decision_feature_snapshot_v1:42:abc",
                "execution_authority": "none",
            },
        )
        self.assertFalse(bad["allowed"])
        self.assertIn("missing_schema_version", bad["reasons"])

    def test_validation_observation_requires_versioned_payload(self):
        verdict = evaluate_training_data_source(
            source_type="validation_observation",
            payload={
                "observation_id": "intent_vs_execution:1",
                "observation_type": "intent_vs_execution",
                "execution_authority": "none",
                "observation_payload": {
                    "schema_version": "intent_vs_execution_v1",
                    "time_axis": {
                        "contract_version": "time_axis_v1",
                        "data_time": "2026-06-05T14:00:00",
                        "knowledge_time": "2026-06-05T14:00:01",
                        "as_of_time": "2026-06-05T14:00:00",
                    },
                },
            },
        )

        self.assertTrue(verdict["allowed"])

        missing_time_axis = evaluate_training_data_source(
            source_type="validation_observation",
            payload={
                "observation_id": "intent_vs_execution:1",
                "observation_type": "intent_vs_execution",
                "execution_authority": "none",
                "observation_payload": {
                    "schema_version": "intent_vs_execution_v1",
                },
            },
        )
        self.assertFalse(missing_time_axis["allowed"])
        self.assertIn("missing_time_axis", missing_time_axis["reasons"])

    def test_outcome_label_must_have_training_authority(self):
        verdict = evaluate_training_data_source(
            source_type="outcome_label",
            payload={
                "label_schema_version": "outcome_label_v1",
                "training_authority": "feature_scope_limited",
                "data_time": "2026-06-10T14:00:00Z",
                "knowledge_time": "2026-06-10T14:00:00Z",
                "as_of_time": "2026-06-10T14:00:00Z",
            },
        )

        self.assertFalse(verdict["allowed"])
        self.assertIn("label_training_authority_not_eligible", verdict["reasons"])

        missing_time_axis = evaluate_training_data_source(
            source_type="outcome_label",
            payload={
                "label_schema_version": "outcome_label_v1",
                "training_authority": "eligible",
            },
        )
        self.assertFalse(missing_time_axis["allowed"])
        self.assertIn("missing_data_time", missing_time_axis["reasons"])

    def test_assertion_raises_for_legacy_source(self):
        with self.assertRaises(ValueError):
            assert_training_data_source_authority(
                source_type="legacy_json",
                payload={"foo": "bar"},
            )


if __name__ == "__main__":
    unittest.main()
