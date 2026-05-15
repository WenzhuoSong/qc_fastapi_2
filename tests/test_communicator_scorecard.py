import sys
import types
import unittest


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

from agents.communicator import _build_payload, _fallback_template  # noqa: E402


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
            },
            {
                "market_judgment": {"regime": "bull_trend", "adjusted_confidence": 0.6},
                "recommended_stance": "overweight",
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
            },
        )

        self.assertEqual(payload["market_scorecard"]["market_condition"], "bullish_but_mixed")
        self.assertEqual(payload["scorecard_enforcement"]["violations"][0], "max_delta:SPY 70.00%->53.00%")

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
                "scorecard_enforcement": {
                    "violations": ["max_delta:SPY 70.00%->53.00%"],
                },
            }
        )

        self.assertIn("Market scorecard", text)
        self.assertIn("bullish_but_mixed", text)
        self.assertIn("Risk clipping", text)
        self.assertIn("max_delta:SPY", text)
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
            }
        )

        self.assertIn("Market scorecard", text)
        self.assertIn("defensive_only", text)
        self.assertIn("Evidence bundle is stale", text)
        self.assertNotIn("/confirm", text)


if __name__ == "__main__":
    unittest.main()
