import sys
import types
import unittest


def _install_import_stubs() -> None:
    """Allow importing agents.synthesizer without external settings."""
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
sys.modules.pop("agents.synthesizer", None)

from agents.synthesizer import _normalize, _validate  # noqa: E402


def _valid_reasoning_chain() -> dict:
    return {
        "step1_regime_acknowledgment": {
            "regime": "bull_trend",
            "constraints_accepted": True,
            "override_reason": None,
        },
        "step2_quant_baseline_assessment": {
            "baseline_quality": "reliable",
            "questionable_reason": None,
            "top3_by_score": ["SPY"],
            "bottom3_by_score": ["TLT"],
        },
        "step3_debate_arbitration": [],
        "step4_risk_sanity_check": {
            "total_equity_pct": 0.8,
            "largest_single_position": {"ticker": "SPY", "weight": 0.8},
            "hedge_allocation_pct": 0.0,
            "cash_pct": 0.2,
            "regime_constraints_satisfied": True,
        },
        "step5_final_judgment": {
            "market_view": "trend intact",
            "key_conviction": "SPY",
            "biggest_uncertainty": "macro data",
        },
    }


def _valid_scorecard_compliance() -> dict:
    return {
        "scorecard_alignment": "aligned",
        "action_permission_used": "normal_rebalance",
        "data_quality_adjustment": "data quality supports normal sizing",
        "why_this_trade_is_reasonable": "weights stay near baseline",
        "known_limitations": [],
    }


def _valid_style_compliance() -> dict:
    return {
        "analysis_style_used": "balanced",
        "trade_style_used": "normal_rebalance",
        "style_limits_respected": True,
        "news_bias_used": "positive macro news confirmed quant signal",
        "sizing_adjustment": "normal sizing within style limits",
        "blocked_or_clipped_actions": [],
        "known_limitations": [],
    }


class SynthesizerContractTest(unittest.TestCase):
    def test_validate_rejects_string_market_judgment(self):
        with self.assertRaisesRegex(ValueError, "market_judgment must be dict"):
            _validate({
                "reasoning_chain": _valid_reasoning_chain(),
                "adjusted_weights": {"SPY": 0.8, "CASH": 0.2},
                "decision_rationale": "test",
                "market_judgment": "bullish",
                "scorecard_compliance": _valid_scorecard_compliance(),
                "style_compliance": _valid_style_compliance(),
            })

    def test_validate_rejects_missing_scorecard_compliance(self):
        with self.assertRaisesRegex(ValueError, "scorecard_compliance"):
            _validate({
                "reasoning_chain": _valid_reasoning_chain(),
                "adjusted_weights": {"SPY": 0.8, "CASH": 0.2},
                "decision_rationale": "test",
                "market_judgment": {
                    "regime": "bull_trend",
                    "adjusted_confidence": 0.74,
                    "uncertainty_flag": False,
                },
                "style_compliance": _valid_style_compliance(),
            })

    def test_validate_rejects_missing_style_compliance(self):
        with self.assertRaisesRegex(ValueError, "style_compliance"):
            _validate({
                "reasoning_chain": _valid_reasoning_chain(),
                "adjusted_weights": {"SPY": 0.8, "CASH": 0.2},
                "decision_rationale": "test",
                "market_judgment": {
                    "regime": "bull_trend",
                    "adjusted_confidence": 0.74,
                    "uncertainty_flag": False,
                },
                "scorecard_compliance": _valid_scorecard_compliance(),
            })

    def test_normalize_preserves_structured_regime(self):
        out = _normalize(
            {
                "reasoning_chain": _valid_reasoning_chain(),
                "adjusted_weights": {"SPY": 0.8, "CASH": 0.2},
                "decision_rationale": "test",
                "recommended_stance": "overweight",
                "market_judgment": {
                    "regime": "bull_trend",
                    "adjusted_confidence": 0.74,
                    "uncertainty_flag": False,
                },
                "scorecard_compliance": _valid_scorecard_compliance(),
                "style_compliance": _valid_style_compliance(),
            },
            base_weights={"SPY": 0.8, "CASH": 0.2},
            allowed_tickers={"SPY", "CASH"},
            max_single_position=1.0,
            bull_output={"overall_confidence": "high"},
            bear_output={"overall_confidence": "low"},
            research_report={},
        )

        self.assertEqual(out["market_judgment"]["regime"], "bull_trend")
        self.assertEqual(out["market_judgment"]["adjusted_confidence"], 0.74)
        self.assertFalse(out["market_judgment"]["uncertainty_flag"])
        self.assertIn("scorecard_compliance", out)
        self.assertIn("style_compliance", out)
        self.assertFalse(out["scorecard_compliance"]["scorecard_non_compliant"])
        self.assertFalse(out["style_compliance"]["style_non_compliant"])

    def test_normalize_marks_scorecard_non_compliance(self):
        out = _normalize(
            {
                "reasoning_chain": _valid_reasoning_chain(),
                "adjusted_weights": {"SPY": 0.8, "CASH": 0.2},
                "decision_rationale": "test",
                "recommended_stance": "overweight",
                "market_judgment": {
                    "regime": "bull_trend",
                    "adjusted_confidence": 0.74,
                    "uncertainty_flag": False,
                },
                "scorecard_compliance": _valid_scorecard_compliance(),
                "style_compliance": _valid_style_compliance(),
            },
            base_weights={"SPY": 0.5, "CASH": 0.5},
            allowed_tickers={"SPY", "CASH"},
            max_single_position=1.0,
            bull_output={"overall_confidence": "high"},
            bear_output={"overall_confidence": "low"},
            research_report={},
            market_scorecard={
                "investment_permission": "small_overweight_only",
                "max_adjustment_from_base": 0.03,
                "max_equity_weight": 0.70,
                "min_cash_weight": 0.25,
                "allow_new_positions": True,
            },
        )

        compliance = out["scorecard_compliance"]
        self.assertTrue(compliance["scorecard_non_compliant"])
        self.assertFalse(compliance["python_validation"]["compliant"])
        self.assertTrue(compliance["python_validation"]["violations"])

    def test_normalize_marks_style_non_compliance(self):
        out = _normalize(
            {
                "reasoning_chain": _valid_reasoning_chain(),
                "adjusted_weights": {"SPY": 0.8, "QQQ": 0.05, "CASH": 0.15},
                "decision_rationale": "test",
                "recommended_stance": "overweight",
                "market_judgment": {
                    "regime": "bull_trend",
                    "adjusted_confidence": 0.74,
                    "uncertainty_flag": False,
                },
                "scorecard_compliance": _valid_scorecard_compliance(),
                "style_compliance": _valid_style_compliance(),
            },
            base_weights={"SPY": 0.7, "CASH": 0.3},
            allowed_tickers={"SPY", "QQQ", "CASH"},
            max_single_position=1.0,
            bull_output={"overall_confidence": "high"},
            bear_output={"overall_confidence": "low"},
            research_report={},
            decision_style={
                "analysis_style": "conservative",
                "trade_style": "step_in",
                "style_limits": {
                    "max_adjustment_multiplier": 0.6,
                    "max_new_buys_per_cycle": 0,
                    "allow_new_positions": False,
                },
            },
        )

        compliance = out["style_compliance"]
        self.assertTrue(compliance["style_non_compliant"])
        self.assertFalse(compliance["python_validation"]["compliant"])
        self.assertTrue(compliance["python_validation"]["violations"])

    def test_normalize_preserves_position_advisory_proposals(self):
        out = _normalize(
            {
                "reasoning_chain": _valid_reasoning_chain(),
                "adjusted_weights": {"SPY": 0.8, "CASH": 0.2},
                "decision_rationale": "test",
                "recommended_stance": "overweight",
                "market_judgment": {
                    "regime": "bull_trend",
                    "adjusted_confidence": 0.74,
                    "uncertainty_flag": False,
                },
                "scorecard_compliance": _valid_scorecard_compliance(),
                "style_compliance": _valid_style_compliance(),
                "position_advisory_proposals": [
                    {
                        "ticker": "SPY",
                        "llm_advisory": "trim_review",
                        "target_weight": 0.79,
                        "reason": "risk budget review",
                        "confidence": 0.6,
                    }
                ],
            },
            base_weights={"SPY": 0.8, "CASH": 0.2},
            allowed_tickers={"SPY", "CASH"},
            max_single_position=1.0,
            bull_output={"overall_confidence": "high"},
            bear_output={"overall_confidence": "low"},
            research_report={},
        )

        self.assertEqual(out["position_advisory_proposals"][0]["ticker"], "SPY")
        self.assertEqual(out["position_advisory_proposals"][0]["llm_advisory"], "trim_review")


if __name__ == "__main__":
    unittest.main()
