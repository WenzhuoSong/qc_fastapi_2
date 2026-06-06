from datetime import UTC, datetime, timedelta
import json
import unittest

from pydantic import ValidationError

from services.outcome_label_contract import (
    OutcomeLabel,
    build_outcome_label,
    label_has_training_authority,
    outcome_label_contract_summary,
    serialize_outcome_label,
)


class OutcomeLabelContractTests(unittest.TestCase):
    def setUp(self):
        self.decision_time = datetime(2026, 6, 6, 14, 0, tzinfo=UTC)
        self.as_of_time = self.decision_time + timedelta(days=5)
        self.feature_snapshot = {
            "schema_version": "decision_feature_snapshot_v1",
            "artifact_id": "decision_feature_snapshot_v1:42:abc",
            "as_of_time": self.decision_time.isoformat(),
            "feature_authority": "qc_live",
            "training_authority": "eligible",
        }

    def test_label_requires_source_metadata(self):
        with self.assertRaises(ValidationError):
            OutcomeLabel(
                decision_time=self.decision_time,
                as_of_time=self.as_of_time,
                horizon="5d",
                price_source="fill_price",
                return_value=0.01,
                max_drawdown_after_decision=-0.02,
                decision_feature_snapshot_id=self.feature_snapshot["artifact_id"],
                decision_feature_snapshot_schema_version="decision_feature_snapshot_v1",
                decision_feature_snapshot_as_of_time=self.decision_time,
            )

    def test_contract_summary_defines_point_in_time_label_sources(self):
        summary = outcome_label_contract_summary()

        self.assertEqual(summary["label_schema_version"], "outcome_label_v1")
        self.assertEqual(summary["preferred_training_source"], "qc_execution")
        self.assertEqual(summary["preferred_training_price_source"], "fill_price")
        self.assertEqual(summary["fallback_training_authority"], "feature_scope_limited")
        self.assertEqual(
            summary["label_source_price_sources"]["yfinance"],
            ["yfinance_adjusted_close"],
        )
        self.assertIn("decision_feature_snapshot_id", summary["training_authority_requires"])

    def test_future_as_of_time_is_required_for_outcome(self):
        with self.assertRaises(ValidationError):
            build_outcome_label(
                decision_time=self.decision_time,
                as_of_time=self.decision_time,
                horizon="5d",
                label_source="qc_execution",
                price_source="fill_price",
                return_value=0.01,
                max_drawdown_after_decision=-0.02,
                decision_feature_snapshot=self.feature_snapshot,
            )

    def test_valid_qc_execution_label_has_training_authority(self):
        label = build_outcome_label(
            decision_time=self.decision_time,
            as_of_time=self.as_of_time,
            horizon="5d",
            label_source="qc_execution",
            price_source="fill_price",
            return_value=0.01,
            max_drawdown_after_decision=-0.02,
            decision_feature_snapshot=self.feature_snapshot,
        )

        payload = serialize_outcome_label(label)

        self.assertEqual(payload["label_schema_version"], "outcome_label_v1")
        self.assertEqual(payload["return"], 0.01)
        self.assertEqual(payload["training_authority"], "eligible")
        self.assertTrue(label_has_training_authority(payload))
        json.dumps(payload)

    def test_missing_decision_feature_snapshot_prevents_training_authority(self):
        label = build_outcome_label(
            decision_time=self.decision_time,
            as_of_time=self.as_of_time,
            horizon="5d",
            label_source="qc_snapshot",
            price_source="qc_market_price",
            return_value=0.01,
            max_drawdown_after_decision=-0.02,
            decision_feature_snapshot=None,
        )

        payload = serialize_outcome_label(label)

        self.assertEqual(payload["training_authority"], "feature_scope_limited")
        self.assertIn("missing_decision_feature_snapshot", payload["scope_limit_reasons"])
        self.assertFalse(label_has_training_authority(payload))

    def test_feature_snapshot_after_decision_time_is_scope_limited(self):
        feature_snapshot = dict(self.feature_snapshot)
        feature_snapshot["as_of_time"] = (self.decision_time + timedelta(minutes=1)).isoformat()

        label = build_outcome_label(
            decision_time=self.decision_time,
            as_of_time=self.as_of_time,
            horizon="5d",
            label_source="qc_snapshot",
            price_source="qc_market_price",
            return_value=0.01,
            max_drawdown_after_decision=-0.02,
            decision_feature_snapshot=feature_snapshot,
        )

        payload = serialize_outcome_label(label)

        self.assertEqual(payload["training_authority"], "feature_scope_limited")
        self.assertIn("feature_snapshot_after_decision_time", payload["scope_limit_reasons"])

    def test_yfinance_label_must_be_explicit_and_not_mixed_with_qc_price(self):
        with self.assertRaises(ValidationError):
            build_outcome_label(
                decision_time=self.decision_time,
                as_of_time=self.as_of_time,
                horizon="5d",
                label_source="yfinance",
                price_source="qc_market_price",
                return_value=0.01,
                max_drawdown_after_decision=-0.02,
                decision_feature_snapshot=self.feature_snapshot,
            )

        label = build_outcome_label(
            decision_time=self.decision_time,
            as_of_time=self.as_of_time,
            horizon="5d",
            label_source="yfinance",
            price_source="yfinance_adjusted_close",
            return_value=0.01,
            max_drawdown_after_decision=-0.02,
            decision_feature_snapshot=self.feature_snapshot,
        )
        self.assertEqual(label.label_source, "yfinance")
        self.assertEqual(label.price_source, "yfinance_adjusted_close")
        payload = serialize_outcome_label(label)
        self.assertEqual(payload["training_authority"], "feature_scope_limited")
        self.assertIn("fallback_label_source", payload["scope_limit_reasons"])
        self.assertEqual(payload["source_metadata"]["label_source_role"], "fallback")

    def test_qc_snapshot_fallback_label_is_marked_scope_limited(self):
        label = build_outcome_label(
            decision_time=self.decision_time,
            as_of_time=self.as_of_time,
            horizon="5d",
            label_source="qc_snapshot",
            price_source="qc_market_price",
            return_value=0.01,
            max_drawdown_after_decision=-0.02,
            decision_feature_snapshot=self.feature_snapshot,
        )

        payload = serialize_outcome_label(label)

        self.assertEqual(payload["training_authority"], "feature_scope_limited")
        self.assertIn("fallback_label_source", payload["scope_limit_reasons"])
        self.assertEqual(payload["source_metadata"]["label_source_role"], "fallback")
        self.assertFalse(label_has_training_authority(payload))

    def test_mixed_feature_authority_prevents_training_authority(self):
        feature_snapshot = dict(self.feature_snapshot)
        feature_snapshot["feature_authority"] = "mixed"
        feature_snapshot["training_authority"] = "feature_scope_limited"

        label = build_outcome_label(
            decision_time=self.decision_time,
            as_of_time=self.as_of_time,
            horizon="5d",
            label_source="qc_snapshot",
            price_source="qc_market_price",
            return_value=0.01,
            max_drawdown_after_decision=-0.02,
            decision_feature_snapshot=feature_snapshot,
        )

        payload = serialize_outcome_label(label)

        self.assertEqual(payload["training_authority"], "feature_scope_limited")
        self.assertIn("mixed_feature_authority", payload["scope_limit_reasons"])
        self.assertIn("feature_snapshot_scope_limited", payload["scope_limit_reasons"])


if __name__ == "__main__":
    unittest.main()
