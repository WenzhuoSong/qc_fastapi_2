import unittest
from datetime import date, timedelta
from pathlib import Path

from services.portfolio_risk_diagnostic import (
    build_beta_shock_report,
    build_scenario_stress_report,
    evaluate_portfolio_var_cvar,
)


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

    def test_historical_scenario_stress_reports_planned_windows(self):
        out = evaluate_portfolio_var_cvar(
            target_weights={"QQQ": 0.10, "XLE": 0.10, "CASH": 0.80},
            current_weights={"QQQ": 0.05, "CASH": 0.95},
            historical_return_rows=[],
            min_samples=60,
        )

        stress = out["target_scenario_stress"]
        self.assertEqual(stress["report_version"], "scenario_stress_v1")
        self.assertEqual(stress["execution_authority"], "none")
        self.assertEqual(stress["target_weight_mutation"], "none")
        scenarios = {row["scenario"]: row for row in stress["scenarios"]}
        self.assertEqual(
            set(scenarios),
            {
                "covid_crash_2020_03",
                "rate_shock_2022",
                "q4_selloff_2018",
                "tech_rebound_2023",
            },
        )
        covid = scenarios["covid_crash_2020_03"]
        self.assertLess(covid["portfolio_return"], 0.0)
        self.assertGreater(covid["estimated_loss"], 0.0)
        self.assertTrue(covid["top_loss_contributors"])
        self.assertIn("XLE", covid["top_loss_summary"])
        self.assertEqual(out["summary"]["max_target_historical_scenario_loss"], stress["summary"]["max_estimated_loss"])

    def test_beta_shock_reports_spy_qqq_and_role_shocks(self):
        report = build_beta_shock_report({"QQQ": 0.10, "SOXX": 0.05, "SH": 0.02, "CASH": 0.83})

        self.assertEqual(report["report_version"], "beta_shock_v1")
        self.assertEqual(report["execution_authority"], "none")
        self.assertEqual(report["target_weight_mutation"], "none")
        self.assertEqual(len(report["spy_shocks"]), 3)
        self.assertEqual(len(report["qqq_shocks"]), 3)
        self.assertEqual(len(report["role_shocks"]), 5)
        self.assertTrue(report["spy_shocks"][0]["top_loss_contributors"])
        thematic = {
            row["shock_name"]: row
            for row in report["role_shocks"]
        }["thematic_role_minus_15pct"]
        self.assertIn("SOXX", thematic["affected_tickers"])
        self.assertIn("SOXX", thematic["top_loss_summary"])

    def test_pr8_scenario_stress_avoids_covariance_estimation(self):
        report = build_scenario_stress_report({"SPY": 0.50, "CASH": 0.50})
        self.assertEqual(report["method"], "deterministic_historical_window_proxy")

        source = Path("services/portfolio_risk_diagnostic.py").read_text()
        scenario_source = source[source.index("def build_scenario_stress_report") : source.index("def build_beta_shock_report")]
        beta_source = source[source.index("def build_beta_shock_report") : source.index("def _historical_var_cvar")]
        for forbidden in ("np.cov", ".cov(", "covariance_matrix"):
            self.assertNotIn(forbidden, scenario_source.lower())
            self.assertNotIn(forbidden, beta_source.lower())


if __name__ == "__main__":
    unittest.main()
