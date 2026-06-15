import unittest
from datetime import date

from services.strategy_execution_evidence_sources import build_paper_live_outcome_metrics


class StrategyExecutionEvidenceSourcesTest(unittest.TestCase):
    def test_only_fastapi_live_increase_outcomes_are_trusted(self):
        outcomes = [
            {
                "signal_source": "fastapi_live_freeze",
                "strategy_id": "momentum_lite_v1",
                "ticker": "SOXX",
                "action": "increase",
                "horizon_days": 1,
                "signal_date": date(2026, 6, 9),
                "label_date": date(2026, 6, 10),
                "excess_vs_spy": 0.02,
                "hit": True,
                "data_quality": "ok",
            },
            {
                "signal_source": "fastapi_live_freeze",
                "strategy_id": "momentum_lite_v1",
                "ticker": "FTXL",
                "action": "increase",
                "horizon_days": 1,
                "signal_date": date(2026, 6, 10),
                "label_date": date(2026, 6, 11),
                "excess_vs_spy": -0.01,
                "hit": False,
                "data_quality": "ok",
            },
            {
                "signal_source": "fastapi_live_freeze",
                "strategy_id": "low_vol_factor",
                "ticker": "XLU",
                "action": "neutral",
                "horizon_days": 1,
                "signal_date": date(2026, 6, 10),
                "label_date": date(2026, 6, 11),
                "excess_vs_spy": 0.01,
                "hit": True,
                "data_quality": "ok",
            },
            {
                "signal_source": "yfinance_replay",
                "strategy_id": "momentum_lite_v1",
                "ticker": "SOXX",
                "action": "increase",
                "horizon_days": 1,
                "signal_date": date(2026, 6, 10),
                "label_date": date(2026, 6, 11),
                "excess_vs_spy": 0.50,
                "hit": True,
                "data_quality": "ok",
            },
        ]

        metrics = build_paper_live_outcome_metrics(outcomes, as_of_date=date(2026, 6, 12))

        self.assertEqual(metrics["summary"]["sample_count"], 2)
        self.assertEqual(metrics["items"]["momentum_lite_v1"]["n_forward_return_samples"], 2)
        self.assertEqual(metrics["items"]["momentum_lite_v1"]["signal_source"], "fastapi_live_freeze")
        self.assertTrue(metrics["items"]["momentum_lite_v1"]["trusted_for_execution_evidence"])
        self.assertNotIn("low_vol_factor", metrics["items"])
        self.assertEqual(metrics["summary"]["skipped"]["unsupported_signal_source"], 1)
        self.assertEqual(metrics["summary"]["skipped"]["unsupported_action"], 1)


if __name__ == "__main__":
    unittest.main()
