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


class SynthesizerContractTest(unittest.TestCase):
    def test_validate_rejects_string_market_judgment(self):
        with self.assertRaisesRegex(ValueError, "market_judgment must be dict"):
            _validate({
                "reasoning_chain": _valid_reasoning_chain(),
                "adjusted_weights": {"SPY": 0.8, "CASH": 0.2},
                "decision_rationale": "test",
                "market_judgment": "bullish",
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


if __name__ == "__main__":
    unittest.main()
