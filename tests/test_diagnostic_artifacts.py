from datetime import UTC, datetime
import json
import unittest

try:
    from pydantic import ValidationError
    from services.diagnostic_artifacts import (
        CandidateEvent,
        DecisionFeatureSnapshot,
        MarketRiskAssessment,
        append_diagnostic_artifacts,
        build_debate_impact,
        build_decision_funnel_observability,
        build_pipeline_diagnostic_artifacts,
        serialize_artifact,
    )
    HAS_PYDANTIC = True
except ModuleNotFoundError:  # pragma: no cover - local lightweight env
    ValidationError = Exception
    HAS_PYDANTIC = False


@unittest.skipUnless(HAS_PYDANTIC, "pydantic is not installed in this test environment")
class DiagnosticArtifactTests(unittest.TestCase):
    def test_serializer_includes_schema_version_and_no_execution_authority(self):
        artifact = MarketRiskAssessment(
            analysis_id=123,
            created_at=datetime(2026, 6, 6, 1, 0, tzinfo=UTC),
            market_regime="neutral",
        )

        payload = serialize_artifact(artifact)

        self.assertEqual(payload["schema_version"], "market_risk_assessment_v1")
        self.assertEqual(payload["execution_authority"], "none")
        self.assertEqual(payload["analysis_id"], 123)
        json.dumps(payload)

    def test_missing_required_fields_fail_validation(self):
        with self.assertRaises(ValidationError):
            MarketRiskAssessment(market_regime="neutral")

    def test_non_none_execution_authority_is_rejected(self):
        with self.assertRaises(ValidationError):
            MarketRiskAssessment(
                analysis_id=123,
                market_regime="neutral",
                execution_authority="gated",
            )

    def test_append_only_embedding_preserves_previous_observations(self):
        first = MarketRiskAssessment(analysis_id=1, market_regime="neutral")
        second = MarketRiskAssessment(analysis_id=1, market_regime="defensive")
        payload = append_diagnostic_artifacts({}, [first])
        updated = append_diagnostic_artifacts(payload, [second])

        self.assertEqual(len(updated["diagnostic_artifacts"]), 2)
        self.assertEqual(
            updated["diagnostic_artifacts"][0]["market_regime"],
            "neutral",
        )
        self.assertEqual(
            updated["diagnostic_artifacts"][1]["market_regime"],
            "defensive",
        )

    def test_mixed_feature_snapshot_is_training_limited(self):
        snapshot = DecisionFeatureSnapshot(
            analysis_id=7,
            created_at=datetime(2026, 6, 6, 1, 0, tzinfo=UTC),
            as_of_time=datetime(2026, 6, 6, 1, 0, tzinfo=UTC),
            price_source="mixed",
            feature_authority="mixed",
            feature_values={"SPY": {"mom_60d": 0.02}},
            raw_source_refs=["account_snapshot:250", "yfinance_batch:latest"],
        )

        payload = serialize_artifact(snapshot)

        self.assertEqual(payload["schema_version"], "decision_feature_snapshot_v1")
        self.assertEqual(payload["training_authority"], "feature_scope_limited")
        self.assertIn("mixed_feature_authority", payload["scope_limit_reasons"])
        self.assertIn("mixed_price_source", payload["scope_limit_reasons"])

    def test_candidate_event_links_to_decision_feature_snapshot(self):
        feature = DecisionFeatureSnapshot(
            analysis_id=7,
            as_of_time=datetime(2026, 6, 6, 1, 0, tzinfo=UTC),
            price_source="qc_snapshot",
            feature_authority="qc_live",
            feature_values={"SPY": {"weight_current": 0.1}},
            raw_source_refs=["account_snapshot:250"],
        )
        candidate = CandidateEvent(
            analysis_id=7,
            feature_snapshot_id=str(feature.artifact_id),
            ticker="SPY",
            candidate_weight=0.1,
        )

        self.assertEqual(candidate.feature_snapshot_id, feature.artifact_id)
        self.assertEqual(candidate.execution_authority, "none")

    def test_debate_impact_records_overlap_without_counterfactual_claim(self):
        artifact = build_debate_impact(
            analysis_id=9,
            as_of_time=datetime(2026, 6, 6, 1, 0, tzinfo=UTC),
            bull_output={
                "stance": "neutral",
                "confidence": "medium",
                "_token_usage": {"prompt_tokens": 100, "completion_tokens": 20},
                "rebuttal_vs_bear": {
                    "failed": False,
                    "_token_usage": {"prompt_tokens": 30, "completion_tokens": 10},
                },
            },
            bear_output={
                "stance": "defensive",
                "confidence": "medium",
                "rebuttal_vs_bull": {"failed": False},
            },
            synthesizer_out={
                "debate_summary": {
                    "disagreement_map": [
                        {"ticker": "QQQ"},
                        {"ticker": "XLK"},
                    ]
                },
                "reasoning_chain": {
                    "step3_debate_arbitration": [
                        {"ticker": "QQQ", "decision_basis": "bear_wins"}
                    ]
                },
            },
            risk_out={
                "target_weights": {"QQQ": 0.10, "CASH": 0.90},
                "target_builder_input": {
                    "per_ticker": {
                        "QQQ": {"changed_by": ["scorecard_clip"]},
                        "SPY": {"changed_by": []},
                    }
                },
            },
        )

        payload = serialize_artifact(artifact)

        self.assertEqual(payload["schema_version"], "debate_impact_v1")
        self.assertEqual(payload["execution_authority"], "none")
        self.assertEqual(payload["disagreement_count"], 2)
        self.assertEqual(payload["arbitration_count"], 1)
        self.assertEqual(payload["disagreement_tickers_in_target_builder"], ["QQQ"])
        self.assertEqual(payload["disagreement_tickers_changed_by_target_builder"], ["QQQ"])
        self.assertEqual(payload["disagreement_tickers_in_final_target"], ["QQQ"])
        self.assertFalse(payload["counterfactual_available"])
        self.assertIsNone(payload["execution_delta_from_debate"])
        self.assertIn("no_no_debate_counterfactual_shadow", payload["measurement_limitations"])

    def test_pipeline_artifacts_link_candidate_ranking_and_mix_to_feature_snapshot(self):
        artifacts = build_pipeline_diagnostic_artifacts(
            analysis_id=42,
            as_of_time=datetime(2026, 6, 6, 1, 0, tzinfo=UTC),
            pipeline_context={
                "account_state_guard": {
                    "snapshot": {
                        "id": 250,
                        "qc_snapshot_id": 1099,
                        "source_packet_type": "heartbeat",
                    }
                }
            },
            brief={
                "current_weights": {"SPY": 0.1},
                "feature_provenance": {"sources": ["QC heartbeat", "yfinance"]},
                "holdings": [
                    {
                        "ticker": "SPY",
                        "weight_current": 0.1,
                        "mom_60d": 0.02,
                        "atr_pct": 0.01,
                    }
                ],
            },
            market_scorecard={"permission": "small_overweight_only"},
            synthesizer_out={"market_judgment": {"regime": "neutral"}},
            risk_out={
                "approved": False,
                "target_weights": {"SPY": 0.1, "QQQ": 0.05, "CASH": 0.85},
            },
            base_weights={"SPY": 0.1},
            bull_output={"stance": "neutral"},
            bear_output={"stance": "defensive"},
        )

        serialized = [serialize_artifact(item) for item in artifacts]
        feature = next(
            item for item in serialized
            if item["schema_version"] == "decision_feature_snapshot_v1"
        )
        linked = [
            item for item in serialized
            if item["schema_version"] in {
                "candidate_event_v1",
                "ranking_event_v1",
                "portfolio_mix_event_v1",
            }
        ]

        self.assertTrue(feature["artifact_id"])
        self.assertEqual(feature["training_authority"], "feature_scope_limited")
        self.assertTrue(linked)
        self.assertTrue(
            all(item["feature_snapshot_id"] == feature["artifact_id"] for item in linked)
        )
        self.assertTrue(any(item["schema_version"] == "debate_impact_v1" for item in serialized))
        style_event = next(
            item for item in serialized
            if item["schema_version"] == "decision_style_event_v1"
        )
        self.assertEqual(style_event["execution_authority"], "none")
        self.assertEqual(style_event["analysis_style"], "unknown")
        self.assertIn("style_event_records_policy", style_event["measurement_limitations"][0])

    def test_decision_funnel_human_required_is_not_scorecard_no_add(self):
        artifact = build_decision_funnel_observability(
            analysis_id=42,
            as_of_time=datetime(2026, 6, 6, 1, 0, tzinfo=UTC),
            pipeline_context={"decision_style": {}},
            brief={"current_weights": {"CASH": 1.0}},
            market_scorecard={
                "investment_permission": "small_overweight_only",
                "require_human_confirmation": True,
                "triggered_rules": ["limited_data_quality"],
            },
            risk_out={"target_weights": {"SPY": 0.02, "CASH": 0.98}},
            base_weights={"SPY": 0.04, "CASH": 0.96},
        )

        payload = serialize_artifact(artifact)
        self.assertEqual(len(payload["buy_intents"]), 1)
        self.assertEqual(payload["stateless_all_blocker_distribution"]["scorecard"], 0)
        self.assertEqual(
            payload["stateless_independent_verdicts"]["scorecard"]["verdict_by_ticker"]["SPY"]["verdict"],
            "passed",
        )
        self.assertEqual(
            payload["scorecard_semantic_acceptance"]["limited_data_quality_human_required_small_add"]["status"],
            "pending_execution_truth",
        )
        trace = payload["full_chain_buy_intent_trace"][0]
        self.assertEqual(trace["ticker"], "SPY")
        self.assertEqual(trace["gates"]["scorecard"]["verdict"], "passed")
        self.assertEqual(trace["stage_buy_deltas"]["final_allowed_delta"], 0.02)
        self.assertEqual(payload["full_chain_trace_summary"]["final_allowed_buy_delta"], 0.02)
        self.assertTrue(payload["data_quality_flags"]["frozen_at_decision_time"])
        self.assertEqual(payload["cash_drift_attribution"]["schema_version"], "cash_drift_four_bucket_v1")

    def test_decision_funnel_strategy_advisory_only_remains_scorecard_blocker(self):
        artifact = build_decision_funnel_observability(
            analysis_id=43,
            as_of_time=datetime(2026, 6, 6, 1, 0, tzinfo=UTC),
            pipeline_context={"decision_style": {}},
            brief={"current_weights": {"CASH": 1.0}},
            market_scorecard={
                "investment_permission": "small_overweight_only",
                "require_human_confirmation": True,
                "triggered_rules": ["strategy_advisory_only"],
            },
            risk_out={
                "target_weights": {"SPY": 0.0, "CASH": 1.0},
                "target_builder_input": {
                    "per_ticker": {
                        "SPY": {
                            "base_weight": 0.04,
                            "current_weight": 0.0,
                            "governance_target": 0.0,
                            "pre_normalized_target": 0.0,
                            "final_target": 0.0,
                            "allowed_actions": ["hold", "trim"],
                            "reason_codes": ["scorecard_insufficient_execution_evidence"],
                            "changed_by": ["scorecard_clip"],
                        }
                    }
                },
            },
            base_weights={"SPY": 0.04, "CASH": 0.96},
        )

        payload = serialize_artifact(artifact)
        self.assertEqual(len(payload["buy_intents"]), 1)
        self.assertEqual(payload["stateless_all_blocker_distribution"]["scorecard"], 1)
        self.assertEqual(
            payload["stateless_independent_verdicts"]["scorecard"]["verdict_by_ticker"]["SPY"]["verdict"],
            "blocked",
        )
        self.assertEqual(
            payload["stateless_independent_verdicts"]["scorecard"]["verdict_by_ticker"]["SPY"]["reason"],
            "scorecard_strategy_advisory_only",
        )
        self.assertEqual(
            payload["scorecard_semantic_acceptance"]["strategy_advisory_only_scorecard_block"]["status"],
            "blocked",
        )
        trace = payload["full_chain_buy_intent_trace"][0]
        self.assertEqual(trace["gates"]["scorecard"]["verdict"], "blocked")
        self.assertEqual(trace["gates"]["position_governance"]["verdict"], "blocked")
        self.assertEqual(trace["gates"]["target_builder_scorecard_clip"]["verdict"], "clipped")
        self.assertIn("scorecard", trace["all_blockers"])
        self.assertEqual(payload["full_chain_trace_summary"]["blocker_counts"]["scorecard"], 1)

    def test_decision_funnel_insufficient_execution_evidence_is_strategy_blocker(self):
        artifact = build_decision_funnel_observability(
            analysis_id=44,
            as_of_time=datetime(2026, 6, 6, 1, 0, tzinfo=UTC),
            pipeline_context={"decision_style": {}},
            brief={"current_weights": {"CASH": 1.0}},
            market_scorecard={
                "investment_permission": "small_overweight_only",
                "require_human_confirmation": True,
                "triggered_rules": ["insufficient_execution_evidence"],
                "strategy_execution_evidence": {
                    "schema_version": "strategy_execution_evidence_summary_v1",
                    "available": True,
                    "execution_grade_strategy_count": 0,
                    "insufficient_execution_evidence_count": 1,
                    "rows": [
                        {
                            "strategy_name": "momentum_lite_v1",
                            "execution_evidence_status": "insufficient_execution_evidence",
                            "failed_checks": ["live_samples_min"],
                            "evidence_checks": {
                                "checks": {
                                    "live_samples_min": {"pass": False, "actual": 0, "threshold": 5}
                                },
                                "failed": ["live_samples_min"],
                            },
                        }
                    ],
                },
            },
            risk_out={"target_weights": {"SPY": 0.0, "CASH": 1.0}},
            base_weights={"SPY": 0.04, "CASH": 0.96},
        )

        payload = serialize_artifact(artifact)
        self.assertEqual(
            payload["stateless_independent_verdicts"]["scorecard"]["verdict_by_ticker"]["SPY"]["reason"],
            "scorecard_insufficient_execution_evidence",
        )
        self.assertEqual(
            payload["scorecard_semantic_acceptance"]["strategy_advisory_only_scorecard_block"]["status"],
            "blocked",
        )
        self.assertEqual(
            payload["data_quality_flags"]["strategy_execution_evidence"]["insufficient_execution_evidence_count"],
            1,
        )
        trace = payload["full_chain_buy_intent_trace"][0]
        self.assertEqual(trace["gates"]["position_governance"]["verdict"], "not_evaluated")
        self.assertTrue(payload["full_chain_trace_summary"]["large_desired_buy_delta_warning"] is False)
        self.assertEqual(
            payload["data_quality_flags"]["strategy_execution_evidence"]["rows"][0]["failed_checks"],
            ["live_samples_min"],
        )
        self.assertFalse(
            payload["data_quality_flags"]["strategy_execution_evidence"]["rows"][0]
            ["evidence_checks"]["checks"]["live_samples_min"]["pass"],
        )


if __name__ == "__main__":
    unittest.main()
