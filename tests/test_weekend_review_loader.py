from datetime import UTC, date, datetime
import json
import unittest

from services.weekend_review_loader import (
    EXECUTION_AUTHORITY,
    LOADER_CONTRACT_VERSION,
    TARGET_WEIGHT_MUTATION,
    build_weekend_review_dataset,
)


class WeekendReviewLoaderTests(unittest.TestCase):
    def test_accepts_versioned_validation_observation(self):
        dataset = build_weekend_review_dataset(
            validation_observations=[
                {
                    "id": 1,
                    "observation_id": "intent_vs_execution:analysis_1",
                    "observation_type": "intent_vs_execution",
                    "observed_at": datetime(2026, 6, 6, 1, 0, tzinfo=UTC),
                    "observation_date": date(2026, 6, 6),
                    "status": "observed",
                    "execution_authority": "none",
                    "target_weight_mutation": "none",
                    "observation_payload": {
                        "schema_version": "intent_vs_execution_v1",
                        "intent": {"action": "trim"},
                    },
                }
            ]
        )

        self.assertEqual(len(dataset.validation_observations), 1)
        self.assertIsNone(dataset.validation_observations[0]["horizon_days"])
        self.assertIsNone(dataset.validation_observations[0]["maturity_date"])
        self.assertEqual(dataset.source_counts["validation_observation"], 1)
        self.assertEqual(dataset.excluded_inputs, [])
        self.assertEqual(dataset.execution_authority, EXECUTION_AUTHORITY)
        self.assertEqual(dataset.target_weight_mutation, TARGET_WEIGHT_MUTATION)

    def test_rejects_unversioned_validation_observation(self):
        dataset = build_weekend_review_dataset(
            validation_observations=[
                {
                    "observation_id": "intent_vs_execution:analysis_1",
                    "observation_type": "intent_vs_execution",
                    "execution_authority": "none",
                    "observation_payload": {"intent": {"action": "trim"}},
                }
            ]
        )

        self.assertEqual(dataset.validation_observations, [])
        self.assertEqual(dataset.exclusion_counts["missing_observation_payload_version"], 1)
        self.assertEqual(dataset.excluded_inputs[0]["source_type"], "validation_observation")

    def test_validation_observation_preserves_maturity_fields(self):
        dataset = build_weekend_review_dataset(
            validation_observations=[
                {
                    "observation_id": "hedge_intent:analysis_1",
                    "observation_type": "hedge_intent",
                    "execution_authority": "none",
                    "horizon_days": 20,
                    "maturity_date": date(2026, 6, 30),
                    "observation_payload": {
                        "contract_version": "validation_observation_loop_v1",
                    },
                }
            ]
        )

        self.assertEqual(len(dataset.validation_observations), 1)
        self.assertEqual(dataset.validation_observations[0]["horizon_days"], 20)
        self.assertEqual(dataset.validation_observations[0]["maturity_date"], "2026-06-30")

    def test_accepts_only_versioned_diagnostic_artifacts(self):
        dataset = build_weekend_review_dataset(
            agent_analyses=[
                {
                    "id": 42,
                    "risk_output": {
                        "diagnostic_artifacts": [
                            {
                                "schema_version": "market_risk_assessment_v1",
                                "artifact_id": "market_risk_assessment_v1:42:abc",
                                "artifact_type": "market_risk_assessment",
                                "execution_authority": "none",
                            },
                            {
                                "schema_version": "market_risk_assessment_v1",
                                "artifact_type": "market_risk_assessment",
                                "execution_authority": "none",
                            },
                        ]
                    },
                }
            ]
        )

        self.assertEqual(len(dataset.diagnostic_artifacts), 1)
        self.assertEqual(dataset.source_counts["diagnostic_artifact"], 1)
        self.assertEqual(dataset.exclusion_counts["missing_artifact_id"], 1)

    def test_counts_mixed_feature_authority_as_scope_limited(self):
        dataset = build_weekend_review_dataset(
            diagnostic_artifacts=[
                {
                    "schema_version": "decision_feature_snapshot_v1",
                    "artifact_id": "decision_feature_snapshot_v1:42:abc",
                    "artifact_type": "decision_feature_snapshot",
                    "execution_authority": "none",
                    "training_authority": "feature_scope_limited",
                    "scope_limit_reasons": ["mixed_feature_authority"],
                }
            ]
        )

        self.assertEqual(len(dataset.diagnostic_artifacts), 1)
        self.assertEqual(dataset.mixed_feature_authority_count, 1)

    def test_rejects_legacy_raw_json_records(self):
        dataset = build_weekend_review_dataset(
            legacy_records=[
                {
                    "source_type": "agent_step_log",
                    "researcher_output": {"market": "risk_off"},
                }
            ]
        )

        self.assertEqual(dataset.validation_observations, [])
        self.assertEqual(dataset.diagnostic_artifacts, [])
        self.assertEqual(dataset.exclusion_counts["non_authoritative_source_type"], 1)

    def test_accepts_typed_execution_log_and_rejects_missing_command_type(self):
        dataset = build_weekend_review_dataset(
            execution_logs=[
                {
                    "id": 1,
                    "command_id": "analysis_1",
                    "command_type": "weight_adjustment",
                    "lifecycle_state": "reconciled",
                    "command_payload": {"weights": {"SPY": 0.1}},
                },
                {
                    "id": 2,
                    "command_id": "analysis_2",
                    "lifecycle_state": "reconciled",
                    "command_payload": {"weights": {"SPY": 0.1}},
                },
            ]
        )

        self.assertEqual(len(dataset.execution_logs), 1)
        self.assertEqual(dataset.source_counts["execution_log"], 1)
        self.assertEqual(dataset.exclusion_counts["missing_command_type"], 1)

    def test_account_snapshot_requires_source_tags(self):
        dataset = build_weekend_review_dataset(
            account_snapshots=[
                {
                    "id": 250,
                    "recorded_at": datetime(2026, 6, 6, 1, 0, tzinfo=UTC),
                    "source_packet_type": "execution_ack",
                    "contract_version": "v1",
                    "holdings_weights": {"SPY": 0.1},
                },
                {
                    "id": 251,
                    "recorded_at": datetime(2026, 6, 6, 1, 0, tzinfo=UTC),
                    "holdings_weights": {"SPY": 0.1},
                },
            ]
        )

        self.assertEqual(len(dataset.account_snapshots), 1)
        self.assertEqual(dataset.source_counts["account_state_snapshot"], 1)
        self.assertEqual(dataset.exclusion_counts["missing_source_packet_type"], 1)
        self.assertEqual(dataset.exclusion_counts["missing_contract_version"], 1)

    def test_market_feature_requires_source_and_price(self):
        dataset = build_weekend_review_dataset(
            market_features=[
                {
                    "id": 1,
                    "trading_date": date(2026, 6, 5),
                    "ticker": "SPY",
                    "source": "yfinance",
                    "adj_close_price": 600.0,
                },
                {
                    "id": 2,
                    "trading_date": date(2026, 6, 5),
                    "ticker": "QQQ",
                    "source": "yfinance",
                },
            ]
        )

        self.assertEqual(len(dataset.market_features), 1)
        self.assertEqual(dataset.source_counts["market_daily_feature"], 1)
        self.assertEqual(dataset.exclusion_counts["missing_price"], 1)

    def test_outcome_label_fallback_is_counted_and_excluded(self):
        dataset = build_weekend_review_dataset(
            outcome_labels=[
                {
                    "label_schema_version": "outcome_label_v1",
                    "training_authority": "feature_scope_limited",
                    "scope_limit_reasons": ["fallback_label_source"],
                    "source_metadata": {"label_source_role": "fallback"},
                }
            ]
        )

        self.assertEqual(dataset.outcome_labels, [])
        self.assertEqual(dataset.fallback_label_count, 1)
        self.assertEqual(dataset.exclusion_counts["label_training_authority_not_eligible"], 1)

    def test_dataset_to_dict_is_json_safe(self):
        dataset = build_weekend_review_dataset(
            validation_observations=[
                {
                    "observation_id": "execution_truth:analysis_1",
                    "observation_type": "execution_truth",
                    "observed_at": datetime(2026, 6, 6, 1, 0, tzinfo=UTC),
                    "observation_date": date(2026, 6, 6),
                    "status": "observed",
                    "execution_authority": "none",
                    "target_weight_mutation": "none",
                    "observation_payload": {
                        "schema_version": "execution_truth_v1",
                    },
                }
            ]
        )

        payload = dataset.to_dict()
        self.assertEqual(payload["contract_version"], LOADER_CONTRACT_VERSION)
        json.dumps(payload)


if __name__ == "__main__":
    unittest.main()
