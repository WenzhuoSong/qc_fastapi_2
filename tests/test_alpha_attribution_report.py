import unittest
from datetime import date, timedelta

from services.alpha_attribution_report import build_monthly_alpha_attribution_report
from services.performance_attribution import FactorReturnPoint, PortfolioReturnPoint


def synthetic_series(*, alpha: float, noise_scale: float, n: int = 40):
    start = date(2026, 5, 1)
    spy_returns = [0.001 * ((idx % 7) - 3) for idx in range(n)]
    noise_pattern = [-1.0, 1.0, 0.5, -0.5, 1.2, -1.2, 0.3, -0.3]
    portfolio_returns = []
    factor_returns = []
    for idx, spy in enumerate(spy_returns):
        trading_date = start + timedelta(days=idx)
        noise = noise_scale * noise_pattern[idx % len(noise_pattern)]
        portfolio = alpha + 0.6 * spy + noise
        portfolio_returns.append(PortfolioReturnPoint(trading_date, portfolio))
        factor_returns.append(FactorReturnPoint(trading_date, "SPY", spy))
    return start, start + timedelta(days=n - 1), portfolio_returns, factor_returns


class AlphaAttributionReportTests(unittest.TestCase):
    def test_insufficient_samples_do_not_emit_alpha_claim(self):
        start, end, portfolio_returns, factor_returns = synthetic_series(alpha=0.001, noise_scale=0.001, n=2)

        report = build_monthly_alpha_attribution_report(
            portfolio_returns=portfolio_returns,
            factor_returns=factor_returns,
            period_start=start,
            period_end=end,
            min_samples=5,
        )

        self.assertEqual(report["report_version"], "alpha_attribution_report_v1")
        self.assertEqual(report["execution_authority"], "none")
        self.assertEqual(report["target_weight_mutation"], "none")
        self.assertEqual(report["status"], "insufficient_data")
        self.assertEqual(report["honest_interpretation"], "insufficient_samples")
        self.assertIsNone(report["alpha_t_stat"])
        self.assertFalse(report["meets_t2_suggestive"])
        self.assertFalse(report["meets_harvey_t3_threshold"])

    def test_suggestive_t_stat_is_not_reported_as_proven_alpha(self):
        start, end, portfolio_returns, factor_returns = synthetic_series(alpha=0.0001, noise_scale=0.0003)

        report = build_monthly_alpha_attribution_report(
            portfolio_returns=portfolio_returns,
            factor_returns=factor_returns,
            period_start=start,
            period_end=end,
            min_samples=5,
        )

        self.assertEqual(report["status"], "attributed")
        self.assertEqual(report["factor_model"], "spy_single_factor_v1")
        self.assertEqual(report["sample_status"], "monitoring_ready")
        self.assertGreaterEqual(abs(report["alpha_t_stat"]), 2.0)
        self.assertLess(abs(report["alpha_t_stat"]), 3.0)
        self.assertTrue(report["meets_t2_suggestive"])
        self.assertFalse(report["meets_harvey_t3_threshold"])
        self.assertEqual(report["honest_interpretation"], "suggestive_not_proven")

    def test_t3_threshold_keeps_multiple_testing_caution(self):
        start, end, portfolio_returns, factor_returns = synthetic_series(alpha=0.0002, noise_scale=0.0005)

        report = build_monthly_alpha_attribution_report(
            portfolio_returns=portfolio_returns,
            factor_returns=factor_returns,
            period_start=start,
            period_end=end,
            min_samples=5,
        )

        self.assertGreaterEqual(abs(report["alpha_t_stat"]), 3.0)
        self.assertTrue(report["meets_harvey_t3_threshold"])
        self.assertEqual(
            report["honest_interpretation"],
            "statistically_meaningful_with_multiple_testing_caution",
        )
        self.assertTrue(report["interpretation_contract"]["not_execution_authority"])


if __name__ == "__main__":
    unittest.main()
