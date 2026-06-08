import unittest

from services.decision_degradation import build_decision_degradation_report


class DecisionDegradationTests(unittest.TestCase):
    def test_normal_decision_is_not_degraded(self):
        report = build_decision_degradation_report(
            pipeline_context={},
            brief={"current_weights": {"QQQ": 0.1}},
            base_weights={"QQQ": 0.1, "CASH": 0.9},
            news_evidence={"status": "ok", "items": [{"ticker": "QQQ"}]},
            research_report={"ticker_signals": [{"ticker": "QQQ", "action": "hold"}]},
            bull_output={"failed": False},
            bear_output={"failed": False},
            rebuttal_vs_bear={"failed": False},
            rebuttal_vs_bull={"failed": False},
            synthesizer_out={"used_degraded_fallback": False},
        )

        self.assertEqual(report["schema_version"], "decision_degradation_v1")
        self.assertFalse(report["is_degraded"])
        self.assertEqual(report["degraded_modes"], [])
        self.assertEqual(report["fallback_paths"], [])
        self.assertEqual(report["missing_inputs"], [])

    def test_fallbacks_missing_inputs_and_news_degradation_are_explicit(self):
        report = build_decision_degradation_report(
            pipeline_context={
                "news_degraded_mode": {
                    "enabled": True,
                    "reason": "news_cache_stale",
                }
            },
            brief={"current_weights": {}},
            base_weights={},
            news_evidence={},
            research_report={"used_degraded_fallback": True, "fallback_reason": "llm_error"},
            bull_output={"failed": True, "error": "timeout"},
            bear_output={"failed": False},
            rebuttal_vs_bear={"failed": True, "error": "timeout"},
            rebuttal_vs_bull={"failed": False},
            synthesizer_out={"used_degraded_fallback": True},
        )

        self.assertTrue(report["is_degraded"])
        self.assertIn("news_stale_degraded_mode", report["degraded_modes"])
        self.assertIn("researcher_degraded_fallback", report["degraded_modes"])
        self.assertIn("synthesizer_degraded_fallback", report["degraded_modes"])
        self.assertIn("bull_researcher_failed", report["degraded_modes"])
        self.assertIn("bull_cross_exam_failed", report["degraded_modes"])
        self.assertIn("base_weights_missing", report["missing_inputs"])
        self.assertIn("current_weights_missing", report["missing_inputs"])
        self.assertIn("news_evidence_missing", report["missing_inputs"])
        self.assertIn("researcher_ticker_signals_missing", report["missing_inputs"])
        self.assertEqual(report["stage_status"]["news"]["reason"], "news_cache_stale")
        self.assertEqual(report["evaluation_guidance"], "stratify_metrics_by_degraded_mode")


if __name__ == "__main__":
    unittest.main()
