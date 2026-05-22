import importlib
import sys
import types
import unittest
from unittest.mock import patch


def _load_prompt_builders():
    openai = type(sys)("openai")
    openai.AsyncOpenAI = object

    config = type(sys)("config")
    config.get_settings = lambda: types.SimpleNamespace(
        openai_api_key="test",
        openai_model_heavy="test-model",
    )

    with patch.dict("sys.modules", {"openai": openai, "config": config}):
        return (
            importlib.import_module("agents.bear_researcher")._build_user_message,
            importlib.import_module("agents.bull_researcher")._build_user_message,
            importlib.import_module("agents.researcher")._build_user_message,
            importlib.import_module("agents.synthesizer")._build_user_message,
        )


(
    _build_bear_message,
    _build_bull_message,
    _build_researcher_message,
    _build_synthesizer_message,
) = _load_prompt_builders()


NEWS_EVIDENCE = {
    "macro_news_score": {
        "overall_bias": "negative",
        "confidence": "high",
        "market_impact": "high",
        "data_quality": "fresh",
    },
    "ticker_news_scores": {
        "XLF": {
            "bias": "negative",
            "confidence": "high",
            "effective_credibility": 0.9,
            "action_bias": "block_new_buy",
            "supporting_items": [{"headline": "credit stress", "action_bias": "block_new_buy"}],
        }
    },
    "hard_risk_events": {"XLF": ["credit_stress"]},
    "data_gaps": [],
}

DECISION_STYLE = {
    "analysis_style": "macro_defensive",
    "trade_style": "risk_reduce_fast",
    "style_limits": {
        "max_adjustment_multiplier": 0.5,
        "max_turnover_per_cycle": 0.10,
        "max_new_buys_per_cycle": 0,
        "min_cash_floor_addition": 0.08,
        "allow_new_positions": False,
    },
    "style_reason": "credit stress blocks risk expansion",
}


class AgentPromptContextTest(unittest.TestCase):
    def test_researcher_prompt_includes_news_evidence_and_decision_style(self):
        message = _build_researcher_message(
            brief={
                "evidence_bundle": {
                    "news_evidence": NEWS_EVIDENCE,
                    "decision_style": DECISION_STYLE,
                    "knowledge": {
                        "resolution": {
                            "hard_constraints": [
                                {
                                    "id": "high_atr_no_add",
                                    "type": "position_action_constraint",
                                    "action": "block_add",
                                }
                            ],
                            "conflicts": [
                                {
                                    "id": "regime_strategy_conflict",
                                    "strategy": "momentum_lite_v1",
                                    "regime": "mean_reverting",
                                }
                            ],
                            "confidence_adjustments": {
                                "intended_consumer": "strategy_confidence_calibrator",
                                "items": [
                                    {
                                        "target_type": "strategy",
                                        "target": "momentum_lite_v1",
                                        "delta": -0.1,
                                        "reason": "regime_strategy_conflict",
                                    }
                                ],
                            },
                        }
                    },
                    "strategies": {
                        "strategy_certification": {
                            "summary": {"counts": {"research_supported": 1}},
                            "items": {
                                "momentum_lite_v1": {
                                    "status": "research_supported",
                                    "approved_use": "research_only",
                                    "promotion_blockers": ["live_samples_insufficient"],
                                    "demotion_reasons": [],
                                }
                            },
                        }
                    },
                },
                "market_scorecard": {"investment_permission": "defensive_only"},
                "news_evidence": NEWS_EVIDENCE,
                "decision_style": DECISION_STYLE,
            },
            quant_baseline={"base_weights": {"SPY": 0.8, "CASH": 0.2}},
        )

        self.assertIn("Structured news evidence", message)
        self.assertIn("Decision style", message)
        self.assertIn("block_new_buy", message)
        self.assertIn("macro_defensive", message)
        self.assertIn("knowledge_resolution", message)
        self.assertIn("regime_strategy_conflict", message)
        self.assertIn("strategy_confidence_calibrator", message)
        self.assertIn("strategy_certification", message)
        self.assertIn("research_supported", message)

    def test_bull_and_bear_prompts_include_style_context(self):
        bull = _build_bull_message(
            {"market_regime": {"regime": "neutral"}},
            {"SPY": 0.8, "CASH": 0.2},
            news_evidence=NEWS_EVIDENCE,
            decision_style=DECISION_STYLE,
        )
        bear = _build_bear_message(
            {"market_regime": {"regime": "neutral"}},
            {"SPY": 0.8, "CASH": 0.2},
            news_evidence=NEWS_EVIDENCE,
            decision_style=DECISION_STYLE,
        )

        self.assertIn("Structured News Evidence", bull)
        self.assertIn("Decision Style", bull)
        self.assertIn("risk_reduce_fast", bull)
        self.assertIn("block_new_buy", bear)
        self.assertIn("risk_reduce_fast", bear)

    def test_synthesizer_prompt_requires_style_compliance(self):
        message = _build_synthesizer_message(
            research_report={"market_regime": {}, "macro_outlook": {}, "cross_signal_insights": []},
            bull_output={},
            bear_output={},
            base_weights={"SPY": 0.8, "CASH": 0.2},
            risk_params={"max_single_position": 1.0, "min_cash_pct": 0.05},
            market_scorecard={"investment_permission": "defensive_only"},
            evidence_bundle={},
            news_evidence=NEWS_EVIDENCE,
            decision_style=DECISION_STYLE,
        )

        self.assertIn("Structured News Evidence", message)
        self.assertIn("Decision Style Contract", message)
        self.assertIn("style_compliance", message)
        self.assertIn("min_cash_floor_addition", message)


if __name__ == "__main__":
    unittest.main()
