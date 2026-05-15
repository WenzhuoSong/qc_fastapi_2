import sys
import types
import unittest


def _install_import_stubs() -> None:
    openai = types.ModuleType("openai")
    openai.AsyncOpenAI = object
    sys.modules["openai"] = openai

    config = types.ModuleType("config")
    config.get_settings = lambda: types.SimpleNamespace(
        openai_api_key="test",
        openai_model_heavy="test-model",
    )
    sys.modules["config"] = config


_install_import_stubs()
for module in (
    "agents.researcher",
    "agents.bull_researcher",
    "agents.bear_researcher",
    "agents.synthesizer",
):
    sys.modules.pop(module, None)

from agents.bear_researcher import _build_user_message as _build_bear_message  # noqa: E402
from agents.bull_researcher import _build_user_message as _build_bull_message  # noqa: E402
from agents.researcher import _build_user_message as _build_researcher_message  # noqa: E402
from agents.synthesizer import _build_user_message as _build_synthesizer_message  # noqa: E402


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
