import unittest
from datetime import date, timedelta

from services.performance_attribution import (
    FactorReturnPoint,
    PortfolioReturnPoint,
    build_performance_attribution,
)


class PerformanceAttributionTests(unittest.TestCase):
    def test_builds_attribution_with_momentum_proxy_fallback(self):
        start = date(2026, 5, 11)
        portfolio_returns = []
        factor_returns = []
        spy_returns = [0.004, -0.002, 0.003, 0.001, -0.001, 0.002]
        qqq_returns = [0.006, -0.004, 0.005, 0.002, -0.002, 0.003]

        for idx, (spy, qqq) in enumerate(zip(spy_returns, qqq_returns)):
            trading_date = start + timedelta(days=idx)
            momentum_proxy = qqq - spy
            portfolio = 0.0002 + 0.45 * spy + 0.35 * qqq + 0.20 * momentum_proxy
            portfolio_returns.append(PortfolioReturnPoint(trading_date, portfolio))
            factor_returns.extend([
                FactorReturnPoint(trading_date, "SPY", spy),
                FactorReturnPoint(trading_date, "QQQ", qqq),
            ])

        result = build_performance_attribution(
            portfolio_returns=portfolio_returns,
            factor_returns=factor_returns,
            period_start=start,
            period_end=start + timedelta(days=5),
        )

        self.assertEqual(result.status, "attributed")
        self.assertEqual(result.sample_count, 6)
        self.assertEqual(result.source_tickers["momentum"], "QQQ_minus_SPY")
        self.assertEqual(result.data_quality, "ok")
        self.assertIsNotNone(result.residual_alpha_candidate)
        self.assertEqual(
            result.diagnostics["residual_label"],
            "residual_alpha_candidate_not_proven_alpha",
        )

    def test_attribution_identity_uses_residual_alpha_candidate_component(self):
        start = date(2026, 5, 11)
        portfolio_returns = []
        factor_returns = []
        rows = [
            (0.004, 0.006, 0.001),
            (-0.002, -0.004, -0.001),
            (0.003, 0.005, 0.002),
            (0.001, 0.002, -0.001),
            (-0.001, -0.002, 0.000),
            (0.002, 0.003, 0.002),
        ]
        for idx, (spy, qqq, mtum) in enumerate(rows):
            trading_date = start + timedelta(days=idx)
            portfolio = 0.0001 + 0.4 * spy + 0.3 * qqq + 0.2 * mtum
            portfolio_returns.append({"trading_date": trading_date, "portfolio_return": portfolio})
            factor_returns.extend([
                {"trading_date": trading_date, "ticker": "SPY", "return_1d": spy},
                {"trading_date": trading_date, "ticker": "QQQ", "return_1d": qqq},
                {"trading_date": trading_date, "ticker": "MTUM", "return_1d": mtum},
            ])

        result = build_performance_attribution(
            portfolio_returns=portfolio_returns,
            factor_returns=factor_returns,
            period_start=start,
            period_end=start + timedelta(days=5),
        )

        additive_total = (
            result.spy_beta_contribution
            + result.qqq_beta_contribution
            + result.momentum_factor_contribution
            + result.residual_alpha_candidate
        )
        self.assertEqual(round(additive_total, 6), result.arithmetic_portfolio_return)
        self.assertTrue(result.diagnostics["intercept_folded_into_residual_alpha_candidate"])

    def test_insufficient_data_does_not_emit_alpha_like_fields(self):
        start = date(2026, 5, 11)
        result = build_performance_attribution(
            portfolio_returns=[
                PortfolioReturnPoint(start, 0.01),
                PortfolioReturnPoint(start + timedelta(days=1), -0.002),
            ],
            factor_returns=[
                FactorReturnPoint(start, "SPY", 0.003),
                FactorReturnPoint(start, "QQQ", 0.004),
                FactorReturnPoint(start + timedelta(days=1), "SPY", -0.001),
                FactorReturnPoint(start + timedelta(days=1), "QQQ", -0.002),
            ],
            period_start=start,
            period_end=start + timedelta(days=1),
            min_samples=5,
        )

        self.assertEqual(result.status, "insufficient_data")
        self.assertEqual(result.sample_count, 2)
        self.assertIsNone(result.residual_alpha_candidate)
        self.assertIsNone(result.r_squared)
        self.assertEqual(
            result.diagnostics["reason"],
            "not_enough_joined_portfolio_and_factor_returns",
        )

    def test_content_hash_is_stable_for_same_inputs(self):
        start = date(2026, 5, 11)
        portfolio_returns = [
            PortfolioReturnPoint(start + timedelta(days=idx), value)
            for idx, value in enumerate([0.001, 0.002, -0.001, 0.003, 0.0])
        ]
        factor_returns = []
        for idx in range(5):
            trading_date = start + timedelta(days=idx)
            factor_returns.extend([
                FactorReturnPoint(trading_date, "SPY", 0.001 * (idx + 1)),
                FactorReturnPoint(trading_date, "QQQ", 0.0015 * (idx + 1)),
            ])

        first = build_performance_attribution(
            portfolio_returns=portfolio_returns,
            factor_returns=factor_returns,
            period_start=start,
            period_end=start + timedelta(days=4),
        )
        second = build_performance_attribution(
            portfolio_returns=portfolio_returns,
            factor_returns=factor_returns,
            period_start=start,
            period_end=start + timedelta(days=4),
        )

        self.assertEqual(first.content_hash, second.content_hash)


if __name__ == "__main__":
    unittest.main()
