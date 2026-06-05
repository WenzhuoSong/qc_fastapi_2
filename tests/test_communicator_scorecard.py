import importlib
import sys
import types
import unittest
import asyncio
from unittest.mock import patch


def _load_communicator_exports():
    openai = type(sys)("openai")
    openai.AsyncOpenAI = lambda api_key=None: object()

    config = type(sys)("config")
    config.get_settings = lambda: types.SimpleNamespace(
        openai_api_key="test",
        openai_model="test-model",
        semi_auto_timeout_minutes=20,
    )

    with patch.dict("sys.modules", {"openai": openai, "config": config}):
        module = importlib.import_module("agents.communicator")
        return module._build_payload, module._fallback_template, module.run_communicator_async


_build_payload, _fallback_template, run_communicator_async = _load_communicator_exports()


class CommunicatorScorecardTest(unittest.TestCase):
    def test_payload_includes_scorecard_and_enforcement(self):
        payload = _build_payload(
            {
                "auth_mode": "SEMI_AUTO",
                "market_scorecard": {
                    "market_condition": "bullish_but_mixed",
                    "investment_permission": "small_overweight_only",
                    "confidence": "medium",
                    "data_quality": "limited",
                    "dominant_constraint": "limited_data_quality",
                    "require_human_confirmation": True,
                    "reasons": ["Only 7 snapshots"],
                },
                "news_evidence": {
                    "macro_news_score": {
                        "overall_bias": "negative",
                        "confidence": "high",
                        "market_impact": "high",
                        "data_quality": "fresh",
                    },
                    "hard_risk_events": {"XLF": ["credit_stress"]},
                },
                "decision_style": {
                    "analysis_style": "macro_defensive",
                    "trade_style": "risk_reduce_fast",
                    "style_reason": "credit stress blocks risk expansion",
                    "weighted_conviction": -0.42,
                    "style_limits": {"allow_new_positions": False},
                },
                "strategy_use_enforcement": {
                    "applied": True,
                    "violations": ["strategy_advisory_only:max_delta:SPY 60.00%->53.00%"],
                    "strategy_use_summary": {
                        "best_actionable": {
                            "strategy_name": "momentum_lite_v1",
                            "suggested_use": "advisory",
                        }
                    },
                    "evidence_summary": {
                        "historical_evidence": "strong",
                        "live_fit": "insufficient",
                        "execution_permission": "advisory",
                    },
                },
                "feature_provenance": {
                    "source_counts": {"qc_heartbeat": 12, "yfinance": 20},
                    "authority_counts": {"live_state": 8, "intraday": 4, "daily_research": 20},
                    "stale_fields": {},
                    "has_stale_fields": False,
                },
                "evidence_bundle": {
                    "knowledge": {
                        "resolution": {
                            "conflicts": [
                                {
                                    "id": "regime_strategy_conflict",
                                    "strategy": "momentum_lite_v1",
                                    "regime": "mean_reverting",
                                }
                            ],
                            "hard_constraints": [
                                {
                                    "id": "high_atr_no_add",
                                    "action": "block_add",
                                }
                            ],
                            "missing_knowledge": [],
                        },
                        "strategy_confidence_calibration": {
                            "summary": {"total": 1, "accepted": 1, "rejected": 0}
                        },
                    },
                    "strategies": {
                        "evidence_cap_diagnostics": {
                            "DRAM": {
                                "static_cap": 0.05,
                                "evidence_adjusted_cap": 0.0212,
                                "current_or_target_weight": 0.03,
                                "would_clip": True,
                                "coverage_ratio": 0.5,
                                "voted_count": 1,
                                "abstain_count": 1,
                                "mapping_error_count": 0,
                                "conviction_status": "early_signal",
                            }
                        },
                        "evidence_vote_summary": {
                            "DRAM": {
                                "voted_count": 1,
                                "abstain_count": 1,
                                "mapping_error_count": 0,
                                "abstain_reasons": [
                                    {
                                        "strategy": "momentum_lite_v1",
                                        "reason": "insufficient_history",
                                        "fields": ["mom_252d"],
                                    }
                                ],
                            }
                        },
                        "strategy_results": [
                            {
                                "strategy_name": "momentum_lite_v1",
                                "evidence_cards": [
                                    {
                                        "ticker": "DRAM",
                                        "strategy": "momentum_lite_v1",
                                        "vote_status": "mapping_error",
                                        "vote_diagnostics": {
                                            "reason_code": "missing_compatibility_mapping",
                                            "dedupe_key": "momentum_lite_v1:DRAM:missing_compatibility_mapping",
                                        },
                                    }
                                ],
                            }
                        ],
                        "execution_gateway": {
                            "final_permission": "human_required",
                            "primary_reason": "regime_consensus_mismatch",
                            "source": "strategy_layer",
                            "strategy_layer": {
                                "verdict": "watch_only",
                                "reason": "regime_consensus_mismatch",
                            },
                            "execution_intel_layer": {
                                "verdict": "acceptable",
                                "reason": "execution_intel_available",
                            },
                        },
                        "strategy_certification": {
                            "summary": {"counts": {"research_supported": 1, "advisory": 0}},
                            "items": {
                                "momentum_lite_v1": {
                                    "status": "research_supported",
                                    "approved_use": "research_only",
                                    "promotion_blockers": ["live_samples_insufficient"],
                                    "demotion_reasons": ["turnover_high"],
                                }
                            },
                        }
                    },
                },
            },
            {
                "market_judgment": {"regime": "bull_trend", "adjusted_confidence": 0.6},
                "recommended_stance": "overweight",
                "style_compliance": {
                    "analysis_style_used": "macro_defensive",
                    "trade_style_used": "risk_reduce_fast",
                    "news_bias_used": "negative high-impact news",
                    "sizing_adjustment": "reduced risk",
                    "blocked_or_clipped_actions": ["new buys blocked"],
                    "style_non_compliant": False,
                },
            },
            {
                "approved": True,
                "target_weights": {"SPY": 0.2, "CASH": 0.8},
                "rebalance_actions": [],
                "scorecard_enforcement": {
                    "applied": True,
                    "violations": ["max_delta:SPY 70.00%->53.00%"],
                    "target_weights_pre_scorecard_clip": {"SPY": 0.7, "CASH": 0.3},
                    "target_weights_post_scorecard_clip": {"SPY": 0.53, "CASH": 0.47},
                    "post_clip_compliance": {"compliant": True},
                },
                "style_enforcement": {
                    "applied": True,
                    "violations": ["style_new_position_blocked:XLF 10.00%->0.00%"],
                    "target_weights_pre_style_clip": {"XLF": 0.1, "CASH": 0.9},
                    "target_weights_post_style_clip": {"CASH": 1.0},
                    "post_clip_compliance": {"compliant": True},
                    "one_way_tightening_ok": True,
                },
            },
        )

        self.assertEqual(payload["market_scorecard"]["market_condition"], "bullish_but_mixed")
        self.assertEqual(payload["scorecard_enforcement"]["violations"][0], "max_delta:SPY 70.00%->53.00%")
        self.assertEqual(payload["news_evidence"]["overall_bias"], "negative")
        self.assertEqual(payload["decision_style"]["analysis_style"], "macro_defensive")
        self.assertEqual(payload["style_enforcement"]["violations"][0], "style_new_position_blocked:XLF 10.00%->0.00%")
        self.assertEqual(payload["strategy_use_enforcement"]["evidence_summary"]["historical_evidence"], "strong")
        self.assertEqual(payload["knowledge_resolution"]["conflicts"][0]["id"], "regime_strategy_conflict")
        self.assertEqual(payload["knowledge_resolution"]["calibration"]["summary"]["accepted"], 1)
        self.assertEqual(payload["execution_gateway"]["final_permission"], "human_required")
        self.assertEqual(payload["strategy_certification"]["items"][0]["status"], "research_supported")
        self.assertEqual(payload["data_quality_detail"]["feature_authority_counts"]["daily_research"], 20)
        self.assertEqual(payload["evidence_cap_observe"]["would_clip_count"], 1)
        self.assertEqual(payload["evidence_cap_observe"]["rows"][0]["ticker"], "DRAM")
        self.assertEqual(payload["evidence_cap_observe"]["mapping_error_rows"][0]["reason_code"], "missing_compatibility_mapping")
        self.assertEqual(
            payload["strategy_use_enforcement"]["violations"][0],
            "strategy_advisory_only:max_delta:SPY 60.00%->53.00%",
        )

    def test_fallback_template_shows_scorecard_and_clipping(self):
        text = _fallback_template(
            {
                "approved": True,
                "regime": "bull_trend",
                "stance": "overweight",
                "rebalance_actions": [],
                "estimated_cost": 0.001,
                "overlays_applied": ["scorecard_constraints"],
                "rejection_reasons": [],
                "auth_mode": "SEMI_AUTO",
                "timeout_minutes": 20,
                "debate_summary": {},
                "market_scorecard": {
                    "market_condition": "bullish_but_mixed",
                    "investment_permission": "small_overweight_only",
                    "data_quality": "limited",
                    "dominant_constraint": "limited_data_quality",
                    "require_human_confirmation": True,
                },
                "data_quality_detail": {
                    "overall": "limited",
                    "feature_source_counts": {
                        "qc_heartbeat": 12,
                        "yfinance": 20,
                        "qc_daily_snapshot": 34,
                    },
                    "feature_authority_counts": {
                        "live_state": 8,
                        "intraday": 4,
                        "daily_research": 20,
                        "qc_eod_audit": 2,
                        "legacy_debug": 1,
                    },
                    "stale_fields": {"XSD": ["return_60d"]},
                    "source_timestamps": {
                        "macro_news_cache": "2026-05-20T12:00:00Z",
                    },
                    "qc_snapshots": 7,
                    "qc_forward_samples": 3,
                    "historical_snapshots": 290,
                    "historical_forward_samples": 289,
                    "strategy_data_quality": "historical_supported",
                    "news_data_quality": "fresh",
                    "evidence_summary": {
                        "historical_evidence": "strong",
                        "execution_intel_status": "insufficient_data",
                    },
                },
                "scorecard_enforcement": {
                    "violations": ["max_delta:SPY 70.00%->53.00%"],
                },
                "news_evidence": {
                    "overall_bias": "negative",
                    "confidence": "high",
                    "market_impact": "high",
                    "data_quality": "fresh",
                    "hard_risk_events": {"XLF": ["credit_stress"]},
                },
                "decision_style": {
                    "analysis_style": "macro_defensive",
                    "trade_style": "risk_reduce_fast",
                    "style_reason": "credit stress blocks risk expansion",
                    "weighted_conviction": -0.42,
                },
                "style_enforcement": {
                    "violations": ["style_new_position_blocked:XLF 10.00%->0.00%"],
                },
                "strategy_use_enforcement": {
                    "violations": ["strategy_advisory_only:max_delta:SPY 60.00%->53.00%"],
                    "strategy_use_summary": {
                        "best_actionable": {
                            "strategy_name": "momentum_lite_v1",
                            "suggested_use": "advisory",
                        }
                    },
                    "evidence_summary": {
                        "historical_evidence": "strong",
                        "execution_intel_status": "insufficient_data",
                        "execution_permission": "advisory",
                    },
                },
                "evidence_cap_observe": {
                    "available": True,
                    "execution_effect": "diagnostic_only",
                    "degraded_ticker_count": 1,
                    "would_clip_count": 1,
                    "mapping_error_count": 1,
                    "rows": [
                        {
                            "ticker": "DRAM",
                            "static_cap": 0.05,
                            "evidence_adjusted_cap": 0.0212,
                            "voted_count": 1,
                            "abstain_count": 1,
                            "main_abstain_reason": "insufficient_history:mom_252d",
                        }
                    ],
                    "mapping_error_rows": [
                        {
                            "ticker": "DRAM",
                            "strategy": "momentum_lite_v1",
                            "reason_code": "missing_compatibility_mapping",
                        }
                    ],
                },
                "knowledge_resolution": {
                    "conflicts": [
                        {
                            "id": "regime_strategy_conflict",
                            "strategy": "momentum_lite_v1",
                            "regime": "mean_reverting",
                        }
                    ],
                    "hard_constraints": [
                        {
                            "id": "high_atr_no_add",
                            "action": "block_add",
                        }
                    ],
                    "calibration": {
                        "summary": {"total": 1, "accepted": 1, "rejected": 0}
                    },
                },
                "strategy_certification": {
                    "summary": {"counts": {"research_supported": 1}},
                    "items": [
                        {
                            "strategy_name": "momentum_lite_v1",
                            "status": "research_supported",
                            "approved_use": "research_only",
                            "promotion_blockers": ["live_samples_insufficient"],
                            "demotion_reasons": ["turnover_high"],
                        }
                    ],
                },
                "execution_gateway": {
                    "final_permission": "human_required",
                    "primary_reason": "regime_consensus_mismatch",
                    "source": "strategy_layer",
                    "strategy_layer": {
                        "verdict": "watch_only",
                        "reason": "regime_consensus_mismatch",
                    },
                    "execution_intel_layer": {
                        "verdict": "acceptable",
                        "reason": "execution_intel_available",
                    },
                },
            }
        )

        self.assertIn("Market scorecard", text)
        self.assertIn("bullish_but_mixed", text)
        self.assertIn("tightened=scorecard", text)
        self.assertNotIn("human confirm", text)
        self.assertIn("Data quality detail", text)
        self.assertIn("Feature source summary", text)
        self.assertIn("live_state=QC heartbeat", text)
        self.assertIn("research=yfinance", text)
        self.assertIn("fallback=3 fields", text)
        self.assertIn("stale=1 fields (XSD)", text)
        self.assertIn("QC heartbeat fields=12", text)
        self.assertIn("Daily snapshot fields=34", text)
        self.assertIn("yfinance fields=20", text)
        self.assertIn("QC live snapshots=7/3 forward", text)
        self.assertIn("QC execution intel=insufficient_data", text)
        self.assertIn("yfinance history=290/289 forward", text)
        self.assertIn("yfinance evidence=strong", text)
        self.assertIn("News cache=fresh", text)
        self.assertIn("overall=limited", text)
        self.assertIn("Risk clipping", text)
        self.assertIn("max_delta:SPY", text)
        self.assertIn("News evidence", text)
        self.assertIn("bias=negative", text)
        self.assertIn("Decision style", text)
        self.assertIn("macro_defensive", text)
        self.assertIn("Style clipping", text)
        self.assertIn("style_new_position_blocked:XLF", text)
        self.assertIn("Strategy-use clipping", text)
        self.assertIn("Evidence cap observe", text)
        self.assertIn("diagnostic_only", text)
        self.assertIn("DRAM cap 5.0%->2.1%", text)
        self.assertIn("reason=insufficient_history:mom_252d", text)
        self.assertIn("mapping_error: DRAM:momentum_lite_v1:missing_compatibility_mapping", text)
        self.assertIn("historical=strong", text)
        self.assertIn("execution=insufficient_data", text)
        self.assertIn("permission=advisory", text)
        self.assertIn("strategy_advisory_only:max_delta:SPY", text)
        self.assertIn("Execution gateway", text)
        self.assertIn("final=tightened", text)
        self.assertNotIn("final=human_required", text)
        self.assertIn("strategy=watch_only:regime_consensus_mismatch", text)
        self.assertIn("Knowledge resolution", text)
        self.assertIn("regime_strategy_conflict:momentum_lite_v1", text)
        self.assertIn("confidence calibration: accepted=1, rejected=0", text)
        self.assertIn("Strategy certification", text)
        self.assertIn("momentum_lite_v1=research_supported", text)
        self.assertIn("/confirm", text)

    def test_rejected_fallback_shows_scorecard(self):
        text = _fallback_template(
            {
                "approved": False,
                "regime": "high_vol",
                "stance": "underweight",
                "rebalance_actions": [],
                "estimated_cost": 0,
                "overlays_applied": [],
                "rejection_reasons": ["Evidence bundle is stale"],
                "auth_mode": "FULL_AUTO",
                "timeout_minutes": 20,
                "debate_summary": {},
                "market_scorecard": {
                    "market_condition": "high_volatility",
                    "investment_permission": "defensive_only",
                    "data_quality": "stale",
                    "dominant_constraint": "stale_evidence",
                },
                "scorecard_enforcement": {},
                "news_evidence": {
                    "overall_bias": "neutral",
                    "confidence": "low",
                    "market_impact": "low",
                    "data_quality": "stale",
                },
                "decision_style": {
                    "analysis_style": "conservative",
                    "trade_style": "hold_unless_strong",
                    "style_reason": "stale data",
                },
                "style_enforcement": {},
                "position_governance": {
                    "mode": "diagnostic_only",
                    "position_decisions": [],
                    "forced_trims": ["QQQ 12.0%->11.0%"],
                    "replacements": [{"ticker": "SPY", "added_weight": 0.01, "support": "advisory", "score": 0.7}],
                    "portfolio_summary": {
                        "position_explanations": [
                            {
                                "ticker": "QQQ",
                                "position_state": "loss_review",
                                "why_not_add": ["position is in unrealized loss review"],
                                "next_trigger": "trim if loss <= -8% and strategy support remains weak",
                            }
                        ]
                    },
                },
                "decision_ledger": {
                    "portfolio_summary": {
                        "risk_approved": False,
                        "execution_status": "not_sent",
                        "governance_available": True,
                    },
                    "top_decisions": [
                        {
                            "ticker": "QQQ",
                            "proposed_action": "trim",
                            "final_action": "none",
                            "reason_codes": ["risk_rejected", "human_required"],
                            "changed_by": ["risk_rejected_final_target_current"],
                        }
                    ],
                },
            }
        )

        self.assertIn("Market scorecard", text)
        self.assertIn("Decision ledger", text)
        self.assertIn("QQQ: trim -> none", text)
        self.assertIn("risk_rejected", text)
        self.assertIn("review_flag", text)
        self.assertNotIn("human_required", text)
        self.assertNotIn("scorecard_human_required", text)
        self.assertIn("Decision style", text)
        self.assertIn("defensive_only", text)
        self.assertIn("Evidence bundle is stale", text)
        self.assertNotIn("/confirm", text)
        self.assertIn("mode=diagnostic_only", text)
        self.assertIn("explain QQQ", text)
        self.assertNotIn("trims: QQQ", text)
        self.assertNotIn("replacements:", text)

    def test_decision_ledger_payload_compacts_top_five_decisions(self):
        ledger_rows = {
            f"T{i}": {
                "ticker": f"T{i}",
                "proposed_action": "trim",
                "final_action": "none" if i == 0 else "trim",
                "execution_status": "not_sent",
                "risk_result": "blocked",
                "reason_codes": ["risk_rejected"] if i == 0 else ["trim_review"],
                "source_effects": {"risk": ["risk_rejected"], "scorecard": ["human_required"]} if i == 0 else {},
                "trade_lifecycle": {"final_target": 0.01, "changed_by": ["risk_target"]},
                "evidence_used": {
                    "position_governance": {
                        "decision": "trim_review",
                        "risk_rank": i + 1,
                    }
                },
                "explanation": {"position_state": "risk_budget_review"},
            }
            for i in range(7)
        }
        payload = _build_payload(
            {"auth_mode": "FULL_AUTO"},
            {
                "market_judgment": {"regime": "neutral", "adjusted_confidence": 0.5},
                "recommended_stance": "maintain",
            },
            {
                "approved": False,
                "target_weights": {},
                "rebalance_actions": [],
                "rejection_reasons": [],
                "decision_ledger": {
                    "phase": "phase_3_sparse_lifecycle",
                "portfolio_summary": {
                    "risk_approved": False,
                    "execution_status": "not_sent",
                    "governance_available": True,
                    "target_construction_mode": "target_builder_gated",
                    "raw_llm_adjusted_weights_consumed": False,
                    "ticker_count": 7,
                },
                    "tickers": ledger_rows,
                    "warnings": [],
                },
            },
        )

        compact = payload["decision_ledger"]
        self.assertEqual(len(compact["top_decisions"]), 5)
        self.assertEqual(compact["top_decisions"][0]["ticker"], "T0")
        text = _fallback_template(payload)
        self.assertIn("Decision ledger", text)
        self.assertIn("T0: trim -> none", text)
        self.assertIn("target=target_builder_gated", text)
        self.assertIn("raw_llm=False", text)
        self.assertIn("final=1.0%", text)
        self.assertIn("sources=scorecard,risk", text)
        self.assertNotIn("T6:", text)

    def test_decision_ledger_line_shows_target_builder_and_advisory_lifecycle(self):
        text = _fallback_template(
            {
                "approved": False,
                "regime": "neutral",
                "stance": "maintain",
                "rebalance_actions": [],
                "estimated_cost": 0,
                "overlays_applied": [],
                "rejection_reasons": [],
                "auth_mode": "FULL_AUTO",
                "timeout_minutes": 20,
                "debate_summary": {},
                "market_scorecard": {},
                "scorecard_enforcement": {},
                "news_evidence": {},
                "decision_style": {},
                "style_enforcement": {},
                "decision_ledger": {
                    "portfolio_summary": {
                        "risk_approved": False,
                        "execution_status": "not_sent",
                        "governance_available": True,
                        "target_construction_mode": "target_builder_gated",
                        "raw_llm_adjusted_weights_consumed": False,
                    },
                    "top_decisions": [
                        {
                            "ticker": "QQQ",
                            "proposed_action": "trim",
                            "final_action": "none",
                            "reason_codes": ["risk_rejected"],
                            "final_target": 0.12,
                            "target_builder_target": 0.11,
                            "validated_advisory_delta": -0.01,
                            "advisory_validator_result": "accepted_as_trim_1.00%",
                            "changed_by": ["target_builder_target", "validated_llm_advisory"],
                            "source_effects": ["risk", "strategy"],
                        }
                    ],
                },
            }
        )

        self.assertIn("QQQ: trim -> none", text)
        self.assertIn("final=12.0%,tb=11.0%,adv=-1.0%", text)
        self.assertIn("advisory=accepted_as_trim_1.00%", text)
        self.assertIn("changed_by=target_builder_target,validated_llm_advisory", text)

    def test_decision_ledger_line_shows_policy_ack_and_hedge_path(self):
        text = _fallback_template(
            {
                "approved": True,
                "regime": "defensive",
                "stance": "reduce risk",
                "rebalance_actions": [],
                "estimated_cost": 0,
                "overlays_applied": [],
                "rejection_reasons": [],
                "auth_mode": "FULL_AUTO",
                "timeout_minutes": 20,
                "debate_summary": {},
                "market_scorecard": {},
                "scorecard_enforcement": {},
                "news_evidence": {},
                "decision_style": {},
                "decision_ledger": {
                    "portfolio_summary": {
                        "risk_approved": True,
                        "execution_status": "rejected",
                        "qc_status": "rejected",
                        "governance_available": True,
                        "target_construction_mode": "target_builder_gated",
                        "policy_version": "sprint8a",
                    },
                    "top_decisions": [
                        {
                            "ticker": "SQQQ",
                            "proposed_action": "add",
                            "final_action": "add",
                            "execution_status": "rejected",
                            "qc_status": "rejected",
                            "qc_rejection_reason": "single weight rejected",
                            "reason_codes": ["hedge_only_requires_hedge_intent"],
                            "final_target": 0.03,
                            "policy_cap_applied": True,
                            "policy_cap_original": 0.04,
                            "entered_via_hedge_path": True,
                        }
                    ],
                },
            }
        )

        self.assertIn("qc=rejected", text)
        self.assertIn("policy=sprint8a", text)
        self.assertIn("policy_cap=0.04->0.03", text)
        self.assertIn("hedge_path=true", text)
        self.assertIn("qc_reject=single weight rejected", text)

    def test_decision_ledger_line_shows_hedge_intent_summary(self):
        text = _fallback_template(
            {
                "approved": True,
                "regime": "defensive",
                "stance": "reduce risk",
                "rebalance_actions": [],
                "estimated_cost": 0,
                "overlays_applied": [],
                "rejection_reasons": [],
                "auth_mode": "FULL_AUTO",
                "timeout_minutes": 20,
                "debate_summary": {},
                "market_scorecard": {},
                "scorecard_enforcement": {},
                "news_evidence": {},
                "decision_style": {},
                "decision_ledger": {
                    "portfolio_summary": {
                        "risk_approved": True,
                        "execution_status": "unknown",
                        "governance_available": True,
                        "target_construction_mode": "target_builder_gated",
                        "hedge_intent": {
                            "triggered": True,
                            "severity": 0.52,
                            "add_hedge_etf": False,
                            "why_not_add_hedge": "severity_0.52_below_threshold_0.70",
                            "trim_targets": ["SOXX", "XLK"],
                            "cash_raise_pct": 0.05,
                        },
                    },
                    "top_decisions": [],
                },
            }
        )

        self.assertIn("Hedge intent", text)
        self.assertIn("triggered=True", text)
        self.assertIn("severity=0.52", text)
        self.assertIn("add_hedge=False", text)
        self.assertIn("severity_0.52_below_threshold_0.70", text)
        self.assertIn("trim SOXX,XLK", text)
        self.assertIn("raise_cash 5%", text)

    def test_hedge_intent_outcome_line_is_visible(self):
        text = _fallback_template(
            {
                "approved": False,
                "regime": "neutral",
                "stance": "maintain",
                "rebalance_actions": [],
                "estimated_cost": 0,
                "overlays_applied": [],
                "rejection_reasons": [],
                "auth_mode": "FULL_AUTO",
                "timeout_minutes": 20,
                "debate_summary": {},
                "market_scorecard": {},
                "scorecard_enforcement": {},
                "news_evidence": {},
                "decision_style": {},
                "style_enforcement": {},
                "decision_ledger": {},
                "hedge_intent_outcome": {
                    "outcome_status": "pending_t5",
                    "triggered": True,
                    "add_hedge_etf": False,
                    "candidate_hedge_instrument": "PSQ",
                    "why_not_add_hedge": "severity_0.52_below_threshold_0.70",
                },
            }
        )

        self.assertIn("Hedge intent outcome log", text)
        self.assertIn("status=pending_t5", text)
        self.assertIn("candidate=PSQ", text)

    def test_decision_ledger_line_warns_on_final_policy_cap(self):
        text = _fallback_template(
            {
                "approved": False,
                "regime": "neutral",
                "stance": "maintain",
                "rebalance_actions": [],
                "estimated_cost": 0,
                "overlays_applied": ["final_execution_policy_cap"],
                "rejection_reasons": ["diagnostic"],
                "auth_mode": "FULL_AUTO",
                "timeout_minutes": 20,
                "debate_summary": {},
                "market_scorecard": {},
                "scorecard_enforcement": {},
                "news_evidence": {},
                "decision_style": {},
                "decision_ledger": {
                    "portfolio_summary": {
                        "risk_approved": True,
                        "execution_status": "unknown",
                        "governance_available": True,
                        "target_construction_mode": "target_builder_gated",
                        "policy_version": "sprint8a",
                        "final_policy_cap_triggered": True,
                        "final_policy_cap_events": [
                            {"ticker": "XLK", "original": 0.1722, "capped_to": 0.15},
                            {"ticker": "XLE", "original": 0.1566, "capped_to": 0.15},
                        ],
                    },
                    "top_decisions": [],
                },
            }
        )

        self.assertIn("final_cap=true", text)
        self.assertIn("post-governance policy cap triggered", text)
        self.assertIn("XLK (17.22% -> 15.00%)", text)
        self.assertIn("XLE (15.66% -> 15.00%)", text)
        self.assertIn("out-of-policy weights", text)

    def test_decision_ledger_line_warns_on_minimum_weight_floor(self):
        text = _fallback_template(
            {
                "approved": False,
                "regime": "neutral",
                "stance": "maintain",
                "rebalance_actions": [],
                "estimated_cost": 0,
                "overlays_applied": ["final_execution_policy_cap"],
                "rejection_reasons": ["diagnostic"],
                "auth_mode": "FULL_AUTO",
                "timeout_minutes": 20,
                "debate_summary": {},
                "market_scorecard": {},
                "scorecard_enforcement": {},
                "news_evidence": {},
                "decision_style": {},
                "decision_ledger": {
                    "portfolio_summary": {
                        "risk_approved": True,
                        "execution_status": "unknown",
                        "governance_available": True,
                        "target_construction_mode": "target_builder_gated",
                        "policy_version": "sprint8a",
                        "final_policy_cap_triggered": True,
                        "minimum_weight_floor_events": [
                            {"ticker": "XLU", "original": 0.001},
                            {"ticker": "XLRE", "original": 0.0018},
                        ],
                    },
                    "top_decisions": [],
                },
            }
        )

        self.assertIn("min_floor=true", text)
        self.assertIn("Minimum position floor cleared", text)
        self.assertIn("XLU 0.10%->0", text)
        self.assertIn("XLRE 0.18%->0", text)

    def test_decision_ledger_line_shows_active_basket_summary(self):
        text = _fallback_template(
            {
                "approved": True,
                "regime": "neutral",
                "stance": "maintain",
                "rebalance_actions": [],
                "estimated_cost": 0,
                "overlays_applied": [],
                "rejection_reasons": [],
                "auth_mode": "FULL_AUTO",
                "timeout_minutes": 20,
                "debate_summary": {},
                "market_scorecard": {},
                "scorecard_enforcement": {},
                "news_evidence": {},
                "decision_style": {},
                "decision_ledger": {
                    "portfolio_summary": {
                        "risk_approved": True,
                        "execution_status": "unknown",
                        "governance_available": True,
                        "target_construction_mode": "target_builder_gated",
                        "active_basket_policy": {
                            "execution_effect": "diagnostic_only",
                            "active_count": 6,
                            "target_active_count_min": 4,
                            "target_active_count_max": 10,
                            "roles": {
                                "core": {"active_count": 2, "policy": {"max_positions": 3}},
                                "sector": {"active_count": 2, "policy": {"max_positions": 5}},
                                "thematic": {"active_count": 2, "policy": {"max_positions": 4}},
                                "hedge": {"active_count": 0, "policy": {"max_positions": 2}},
                            },
                            "subscale_positions": [{"ticker": "QQQ", "weight": 0.049}],
                            "floor_cleared_positions": [{"ticker": "XLU", "weight": 0.001}],
                        },
                    },
                    "top_decisions": [],
                },
            }
        )

        self.assertIn("Active basket: 6/4-10 diagnostic", text)
        self.assertIn("core=2/3", text)
        self.assertIn("sector=2/5", text)
        self.assertIn("subscale: QQQ 4.90%", text)
        self.assertIn("floor: XLU 0.10%", text)

    def test_portfolio_construction_evaluation_line_shows_status_and_blockers(self):
        text = _fallback_template(
            {
                "approved": False,
                "regime": "neutral",
                "stance": "maintain",
                "rebalance_actions": [],
                "estimated_cost": 0,
                "overlays_applied": [],
                "rejection_reasons": [],
                "auth_mode": "FULL_AUTO",
                "timeout_minutes": 20,
                "debate_summary": {},
                "market_scorecard": {},
                "scorecard_enforcement": {},
                "news_evidence": {},
                "decision_style": {},
                "style_enforcement": {},
                "decision_ledger": {},
                "portfolio_construction_evaluation": {
                    "status": "shadow_only",
                    "promotion_ready": False,
                    "mean_abs_weight_deviation": 0.012,
                    "turnover_delta": 0.01,
                    "shadow_policy_allowed": False,
                    "blockers": ["shadow_policy_violation"],
                    "warnings": ["shadow_reduces_turnover"],
                },
            }
        )

        self.assertIn("Portfolio construction evaluation", text)
        self.assertIn("status=shadow_only", text)
        self.assertIn("blockers=shadow_policy_violation", text)

    def test_portfolio_construction_readiness_line_shows_rolling_status(self):
        text = _fallback_template(
            {
                "approved": False,
                "regime": "neutral",
                "stance": "maintain",
                "rebalance_actions": [],
                "estimated_cost": 0,
                "overlays_applied": [],
                "rejection_reasons": [],
                "auth_mode": "FULL_AUTO",
                "timeout_minutes": 20,
                "debate_summary": {},
                "market_scorecard": {},
                "scorecard_enforcement": {},
                "news_evidence": {},
                "decision_style": {},
                "style_enforcement": {},
                "decision_ledger": {},
                "portfolio_construction_readiness": {
                    "status": "collecting_evidence",
                    "promotion_ready": False,
                    "cycles": 7,
                    "pass_rate": 0.57,
                    "blocker_counts": {"shadow_policy_violation": 2},
                },
            }
        )

        self.assertIn("Portfolio construction rolling readiness", text)
        self.assertIn("cycles=7", text)
        self.assertIn("blockers=shadow_policy_violation:2", text)

    def test_portfolio_construction_promotion_gate_line_shows_auto_status(self):
        text = _fallback_template(
            {
                "approved": False,
                "regime": "neutral",
                "stance": "maintain",
                "rebalance_actions": [],
                "estimated_cost": 0,
                "overlays_applied": [],
                "rejection_reasons": [],
                "auth_mode": "FULL_AUTO",
                "timeout_minutes": 20,
                "debate_summary": {},
                "market_scorecard": {},
                "scorecard_enforcement": {},
                "news_evidence": {},
                "decision_style": {},
                "style_enforcement": {},
                "decision_ledger": {},
                "portfolio_construction_promotion_gate": {
                    "status": "auto_approved",
                    "eligible": True,
                    "enabled": True,
                    "approval_mode": "auto",
                    "blockers": [],
                    "execution_authority": "none",
                },
            }
        )

        self.assertIn("Portfolio construction promotion gate", text)
        self.assertIn("status=auto_approved", text)
        self.assertIn("approval=auto", text)

    def test_final_validation_line_shows_mode_and_blockers(self):
        text = _fallback_template(
            {
                "approved": False,
                "regime": "neutral",
                "stance": "maintain",
                "rebalance_actions": [],
                "estimated_cost": 0,
                "overlays_applied": [],
                "rejection_reasons": ["final validation blocked"],
                "auth_mode": "FULL_AUTO",
                "timeout_minutes": 20,
                "debate_summary": {},
                "market_scorecard": {},
                "scorecard_enforcement": {},
                "news_evidence": {},
                "decision_style": {},
                "style_enforcement": {},
                "decision_ledger": {},
                "final_validation": {
                    "mode": "blocking",
                    "approved": False,
                    "policy_allowed": False,
                    "max_abs_drift": 0.02,
                    "material_drift_threshold": 0.015,
                    "blocking_violations": ["execution_policy_violation"],
                },
            }
        )

        self.assertIn("Final risk validation", text)
        self.assertIn("mode=blocking", text)
        self.assertIn("blockers=execution_policy_violation", text)

    def test_manual_trim_review_shows_advisory_as_weak_positive(self):
        text = _fallback_template(
            {
                "approved": False,
                "regime": "bull_trend",
                "stance": "maintain",
                "rebalance_actions": [],
                "estimated_cost": 0,
                "overlays_applied": [],
                "rejection_reasons": ["Market scorecard requires human confirmation"],
                "auth_mode": "FULL_AUTO",
                "timeout_minutes": 20,
                "position_governance": {
                    "mode": "diagnostic_only",
                    "position_decisions": [
                        {
                            "ticker": "FTXL",
                            "decision": "trim_review",
                            "strategy_support": "advisory",
                            "current_weight": 0.03,
                            "target_before": 0.03,
                            "target_after": 0.03,
                            "reason_codes": [
                                "scorecard_human_required",
                                "unrealized_loss_review",
                                "basket_review",
                                "advisory_basket_loss_review",
                            ],
                        }
                    ],
                    "manual_action_hints": [
                        {
                            "ticker": "FTXL",
                            "current_weight": 0.03,
                            "suggested_target": 0.02,
                            "reason_codes": [
                                "unrealized_loss_review",
                                "basket_review",
                                "advisory_basket_loss_review",
                            ],
                        }
                    ],
                    "portfolio_summary": {
                        "basket_reviews": [
                            {"group": "semiconductors", "tickers": ["FTXL", "PSI", "SOXX"]}
                        ],
                    },
                },
            }
        )

        self.assertIn("manual trim review", text)
        self.assertIn("FTXL 3.0%->2.0% (advisory=weak-positive, basket loss review)", text)

    def test_full_auto_governance_only_shows_trims_not_manual_review(self):
        text = _fallback_template(
            {
                "approved": True,
                "regime": "bull_trend",
                "stance": "maintain",
                "rebalance_actions": [
                    {"ticker": "FTXL", "action": "sell", "weight_delta": -0.01}
                ],
                "estimated_cost": 0.0002,
                "overlays_applied": ["full_auto_position_governance_risk_reduction"],
                "rejection_reasons": [],
                "auth_mode": "FULL_AUTO",
                "timeout_minutes": 20,
                "position_governance": {
                    "mode": "full_auto_governance_only",
                    "position_decisions": [],
                    "forced_trims": ["FTXL 3.00%->2.00% advisory_basket_loss_auto"],
                    "manual_action_hints": [
                        {
                            "ticker": "FTXL",
                            "current_weight": 0.03,
                            "suggested_target": 0.02,
                            "reason_codes": ["advisory_basket_loss_review"],
                        }
                    ],
                    "portfolio_summary": {},
                },
            }
        )

        self.assertIn("mode=full_auto_governance_only", text)
        self.assertIn("trims: FTXL", text)
        self.assertNotIn("manual trim review", text)

    def test_rejected_communicator_uses_deterministic_fallback(self):
        out = asyncio.run(run_communicator_async(
            {
                "auth_mode": "FULL_AUTO",
                "market_scorecard": {
                    "market_condition": "mean_reverting",
                    "investment_permission": "small_overweight_only",
                    "data_quality": "limited",
                    "dominant_constraint": "strategy_advisory_only",
                    "require_human_confirmation": True,
                },
            },
            {
                "market_judgment": {"regime": "mean_reverting", "adjusted_confidence": 0.5},
                "recommended_stance": "maintain",
            },
            {
                "approved": False,
                "rebalance_actions": [
                    {"ticker": "QQQ", "action": "sell", "weight_delta": -0.0203}
                ],
                "rejection_reasons": ["Market scorecard requires human confirmation"],
            },
        ))

        self.assertTrue(out["used_fallback"])
        self.assertIn("Rebalance rejected by risk", out["text"])
        self.assertIn("No execution this round", out["text"])
        self.assertIn("Market scorecard tightened the proposal", out["text"])
        self.assertNotIn("requires human confirmation", out["text"])
        self.assertNotIn("Action taken", out["text"])
        self.assertNotIn("/confirm", out["text"])


if __name__ == "__main__":
    unittest.main()
