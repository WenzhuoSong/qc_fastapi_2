import unittest
from datetime import date, datetime, timezone

from services.strategy_regime_gap_analysis import build_strategy_regime_gap_analysis


def profile(
    strategy_id: str,
    regime: str,
    *,
    ticker: str = "SPY",
    action: str = "increase",
    branch: str = "default",
    horizon_days: int = 5,
    source_bucket: str = "combined",
    status: str = "calibrated",
    n: int = 120,
    hit_rate: float = 0.6,
    avg_excess_vs_spy: float = 0.01,
    ic: float = 0.1,
) -> dict:
    return {
        "strategy_id": strategy_id,
        "ticker": ticker,
        "branch": branch,
        "action": action,
        "regime_at_signal": regime,
        "horizon_days": horizon_days,
        "source_bucket": source_bucket,
        "status": status,
        "n": n,
        "hit_rate": hit_rate,
        "avg_excess_vs_spy": avg_excess_vs_spy,
        "ic": ic,
        "conviction": 0.7,
    }


class StrategyRegimeGapAnalysisTests(unittest.TestCase):
    def test_detects_momentum_overconcentration_missing_regimes_and_weak_rows(self):
        profiles = [
            profile(
                "leveraged_etf_momentum_allocator",
                "trending_bull",
                ticker="TQQQ",
                source_bucket="historical_prior",
                hit_rate=0.55,
            ),
            profile(
                "leveraged_etf_momentum_allocator",
                "trending_bull",
                ticker="TQQQ",
                source_bucket="combined",
                hit_rate=0.62,
                avg_excess_vs_spy=0.03,
            ),
            profile(
                "leveraged_etf_momentum_allocator",
                "defensive",
                ticker="UVXY",
                action="hedge",
                hit_rate=0.40,
                avg_excess_vs_spy=-0.01,
                ic=-0.05,
            ),
            profile(
                "mean_reversion_lite",
                "mean_reverting",
                status="early_estimate",
                n=12,
                hit_rate=0.58,
            ),
            profile("risk_parity_lite", "defensive", hit_rate=0.70),
        ]

        summary = build_strategy_regime_gap_analysis(
            profiles=profiles,
            alpha_validation_runs=[
                {
                    "analysis_id": 101,
                    "generated_at": datetime(2026, 5, 25, tzinfo=timezone.utc),
                    "status": "warning",
                    "independent_alpha_family_count": 1,
                    "calibrated_conviction_count": 2,
                }
            ],
            as_of_date=date(2026, 5, 25),
        )

        self.assertEqual(summary["contract_version"], "strategy_regime_gap_analysis_v1")
        self.assertEqual(summary["execution_authority"], "none")
        self.assertEqual(summary["target_weight_mutation"], "none")
        self.assertEqual(summary["profile_count"], 4)
        self.assertEqual(summary["calibrated_alpha_profile_count"], 2)
        self.assertEqual(summary["actionable_alpha_families"], ["momentum"])
        self.assertTrue(summary["momentum_overconcentration"])
        self.assertIn("momentum_only_actionable_alpha_family", summary["warnings"])
        self.assertIn("missing_calibrated_regime_coverage:mean_reverting", summary["warnings"])
        self.assertIn("family_regime_degraded:momentum:defensive", summary["warnings"])

        trending = next(row for row in summary["regime_rows"] if row["regime"] == "trending_bull")
        self.assertEqual(trending["coverage_status"], "covered")
        self.assertEqual(trending["calibrated_families"], ["momentum"])
        self.assertEqual(trending["hit_rate"], 0.62)

        defensive = next(row for row in summary["regime_rows"] if row["regime"] == "defensive")
        self.assertEqual(defensive["coverage_status"], "covered_by_non_preferred_family")
        self.assertIn("low_vol_defensive", defensive["missing_expected_families"])

        weak = summary["weak_family_regime_rows"]
        self.assertEqual(len(weak), 1)
        self.assertEqual(weak[0]["family"], "momentum")
        self.assertEqual(weak[0]["regime"], "defensive")
        self.assertIn("hit_rate_below_45pct", weak[0]["reasons"])
        self.assertIn("negative_excess_vs_spy", weak[0]["reasons"])

        suggestions = {
            (row["regime"], row["suggested_family"])
            for row in summary["research_queue"]
        }
        self.assertIn(("mean_reverting", "mean_reversion"), suggestions)
        self.assertIn(("high_vol", "volatility_hedge"), suggestions)

    def test_multiple_calibrated_alpha_families_clear_momentum_only_warning(self):
        profiles = [
            profile("leveraged_etf_momentum_allocator", "trending_bull", ticker="TQQQ"),
            profile("low_vol_factor", "defensive", ticker="BSV", action="de_risk", hit_rate=0.57),
            profile("mean_reversion_lite", "mean_reverting", ticker="SPY", hit_rate=0.56),
        ]

        summary = build_strategy_regime_gap_analysis(
            profiles=profiles,
            alpha_validation_runs=[],
            as_of_date=date(2026, 5, 25),
        )

        self.assertGreaterEqual(summary["actionable_alpha_family_count"], 3)
        self.assertFalse(summary["momentum_overconcentration"])
        self.assertNotIn("momentum_only_actionable_alpha_family", summary["warnings"])
        family_names = {row["family"] for row in summary["family_rows"]}
        self.assertEqual(
            family_names,
            {"low_vol_defensive", "mean_reversion", "momentum"},
        )

    def test_empty_profiles_are_insufficient_data(self):
        summary = build_strategy_regime_gap_analysis(
            profiles=[],
            alpha_validation_runs=[],
            as_of_date=date(2026, 5, 25),
        )

        self.assertEqual(summary["status"], "insufficient_data")
        self.assertEqual(summary["profile_count"], 0)
        self.assertEqual(summary["execution_authority"], "none")

    def test_flags_regime_where_all_calibrated_profiles_are_weak(self):
        summary = build_strategy_regime_gap_analysis(
            profiles=[
                profile(
                    "momentum_lite_v1",
                    "high_vol",
                    ticker="SPY",
                    hit_rate=0.40,
                    avg_excess_vs_spy=-0.01,
                ),
                profile(
                    "mean_reversion_lite",
                    "high_vol",
                    ticker="QQQ",
                    hit_rate=0.43,
                    ic=-0.02,
                ),
                profile(
                    "low_vol_factor",
                    "defensive",
                    ticker="BSV",
                    hit_rate=0.60,
                    avg_excess_vs_spy=0.01,
                    ic=0.05,
                ),
            ],
            alpha_validation_runs=[],
            as_of_date=date(2026, 5, 25),
        )

        rows = summary["simultaneous_failure_regime_rows"]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["regime"], "high_vol")
        self.assertEqual(rows[0]["reason"], "all_calibrated_alpha_profiles_weak_in_regime")
        self.assertIn("all_strategies_fail_simultaneously:high_vol", summary["warnings"])


if __name__ == "__main__":
    unittest.main()
