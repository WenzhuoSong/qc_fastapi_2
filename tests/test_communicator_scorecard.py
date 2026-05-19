import sys
import types
import unittest
import asyncio


def _install_import_stubs() -> None:
    openai = types.ModuleType("openai")
    openai.AsyncOpenAI = lambda api_key=None: object()
    sys.modules["openai"] = openai

    config = types.ModuleType("config")
    config.get_settings = lambda: types.SimpleNamespace(
        openai_api_key="test",
        openai_model="test-model",
        semi_auto_timeout_minutes=20,
    )
    sys.modules["config"] = config


_install_import_stubs()
sys.modules.pop("agents.communicator", None)

from agents.communicator import _build_payload, _fallback_template, run_communicator_async  # noqa: E402


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
        self.assertEqual(payload["strategy_certification"]["items"][0]["status"], "research_supported")
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
                    "qc_snapshots": 7,
                    "qc_forward_samples": 3,
                    "historical_snapshots": 290,
                    "historical_forward_samples": 289,
                    "strategy_data_quality": "historical_supported",
                    "evidence_summary": {
                        "historical_evidence": "strong",
                        "live_fit": "insufficient",
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
                        "live_fit": "insufficient",
                        "execution_permission": "advisory",
                    },
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
            }
        )

        self.assertIn("Market scorecard", text)
        self.assertIn("bullish_but_mixed", text)
        self.assertIn("Data quality detail", text)
        self.assertIn("QC live snapshots=7/3 forward", text)
        self.assertIn("QC live fit=insufficient", text)
        self.assertIn("yfinance history=290/289 forward", text)
        self.assertIn("yfinance evidence=strong", text)
        self.assertIn("Risk clipping", text)
        self.assertIn("max_delta:SPY", text)
        self.assertIn("News evidence", text)
        self.assertIn("bias=negative", text)
        self.assertIn("Decision style", text)
        self.assertIn("macro_defensive", text)
        self.assertIn("Style clipping", text)
        self.assertIn("style_new_position_blocked:XLF", text)
        self.assertIn("Strategy-use clipping", text)
        self.assertIn("historical=strong", text)
        self.assertIn("live=insufficient", text)
        self.assertIn("permission=advisory", text)
        self.assertIn("strategy_advisory_only:max_delta:SPY", text)
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
        self.assertIn("Decision style", text)
        self.assertIn("defensive_only", text)
        self.assertIn("Evidence bundle is stale", text)
        self.assertNotIn("/confirm", text)
        self.assertIn("mode=diagnostic_only", text)
        self.assertIn("explain QQQ", text)
        self.assertNotIn("trims: QQQ", text)
        self.assertNotIn("replacements:", text)

    def test_decision_ledger_payload_compacts_top_five_decisions(self):
        from agents.communicator import _build_payload

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
        self.assertIn("sources=scorecard,risk", text)
        self.assertNotIn("T6:", text)

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
        self.assertNotIn("Action taken", out["text"])
        self.assertNotIn("/confirm", out["text"])


if __name__ == "__main__":
    unittest.main()
