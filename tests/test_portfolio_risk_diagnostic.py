import unittest
from datetime import date, timedelta

from services.portfolio_risk_diagnostic import evaluate_portfolio_var_cvar


class PortfolioRiskDiagnosticTest(unittest.TestCase):
    def test_historical_var_cvar_uses_left_tail_losses(self):
        start = date(2026, 1, 1)
        rows = []
        for idx in range(100):
            rows.append({
                "trading_date": (start + timedelta(days=idx)).isoformat(),
                "ticker": "SPY",
                "return_1d": -0.02 if idx < 5 else 0.001,
            })

        out = evaluate_portfolio_var_cvar(
            target_weights={"SPY": 1.0, "CASH": 0.0},
            current_weights={"SPY": 0.5, "CASH": 0.5},
            historical_return_rows=rows,
            min_samples=60,
        )

        self.assertEqual(out["status"], "ok")
        self.assertEqual(out["mode"], "diagnostic_only")
        self.assertEqual(out["execution_authority"], "none")
        self.assertEqual(out["target_weight_mutation"], "none")
        self.assertAlmostEqual(out["target_historical"]["var_95_loss"], 0.02)
        self.assertAlmostEqual(out["target_historical"]["cvar_95_loss"], 0.02)
        self.assertAlmostEqual(out["current_historical"]["var_95_loss"], 0.01)
        self.assertEqual(out["target_historical"]["tail_count"], 5)

    def test_scenarios_capture_leveraged_etf_and_uvxy_risk(self):
        out = evaluate_portfolio_var_cvar(
            target_weights={"TQQQ": 0.10, "UVXY": 0.03, "CASH": 0.87},
            current_weights={"CASH": 1.0},
            historical_return_rows=[],
            min_samples=60,
        )

        self.assertEqual(out["status"], "insufficient_data")
        scenarios = {row["scenario"]: row for row in out["target_scenarios"]}
        self.assertAlmostEqual(
            scenarios["spy_minus_3_growth_shock"]["portfolio_return"],
            -0.009,
        )
        self.assertAlmostEqual(
            scenarios["uvxy_decay_day"]["estimated_loss"],
            0.0,
        )
        self.assertIn("historical_var_cvar_insufficient_samples", out["warnings"])

    def test_missing_coverage_marks_historical_data_insufficient(self):
        rows = [
            {"trading_date": "2026-01-01", "ticker": "SPY", "return_1d": -0.01},
            {"trading_date": "2026-01-02", "ticker": "SPY", "return_1d": 0.01},
        ]

        out = evaluate_portfolio_var_cvar(
            target_weights={"SPY": 0.50, "QQQ": 0.50},
            current_weights={},
            historical_return_rows=rows,
            min_samples=2,
            min_coverage=0.80,
        )

        self.assertEqual(out["target_historical"]["sample_count"], 0)
        self.assertEqual(out["target_historical"]["data_quality"], "missing")


if __name__ == "__main__":
    unittest.main()
