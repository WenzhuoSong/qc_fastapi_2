import unittest
from datetime import date, datetime, timezone

from services.strategy_promotion_recommendations import build_strategy_promotion_recommendations


def profile(
    strategy_id: str,
    regime: str,
    *,
    ticker: str = "SPY",
    action: str = "increase",
    status: str = "calibrated",
    source_bucket: str = "combined",
    n: int = 120,
    hit_rate: float = 0.60,
    avg_excess_vs_spy: float = 0.02,
    ic: float = 0.10,
) -> dict:
    return {
        "strategy_id": strategy_id,
        "ticker": ticker,
        "branch": "default",
        "action": action,
        "regime_at_signal": regime,
        "horizon_days": 5,
        "source_bucket": source_bucket,
        "status": status,
        "n": n,
        "hit_rate": hit_rate,
        "avg_excess_vs_spy": avg_excess_vs_spy,
        "ic": ic,
        "conviction": 0.72,
    }


class StrategyPromotionRecommendationsTests(unittest.TestCase):
    def test_promotes_watch_only_strategy_when_calibrated_positive(self):
        summary = build_strategy_promotion_recommendations(
            profiles=[
                profile(
                    "mean_reversion_lite",
                    "mean_reverting",
                    ticker="SPY",
                    hit_rate=0.61,
                    avg_excess_vs_spy=0.03,
                    ic=0.12,
                )
            ],
            strategy_evidence={
                "strategy_results": [
                    {
                        "strategy_name": "mean_reversion_lite",
                        "suggested_use": "watch_only",
                        "data_ready": True,
                        "can_influence_allocation": False,
                    }
                ]
            },
            alpha_validation_runs=[
                {
                    "analysis_id": 201,
                    "generated_at": datetime(2026, 5, 25, tzinfo=timezone.utc),
                    "status": "ok",
                    "independent_alpha_family_count": 2,
                    "calibrated_conviction_count": 1,
                }
            ],
            as_of_date=date(2026, 5, 25),
        )

        self.assertEqual(summary["contract_version"], "strategy_promotion_recommendations_v1")
        self.assertTrue(summary["recommendation_only"])
        self.assertEqual(summary["execution_authority"], "none")
        self.assertEqual(summary["target_weight_mutation"], "none")
        recommendations = summary["recommendations"]
        promote = next(
            row for row in recommendations
            if row["recommendation"] == "promote_to_advisory_review"
        )
        self.assertEqual(promote["strategy_id"], "mean_reversion_lite")
        self.assertEqual(promote["current_use"], "watch_only")
        self.assertEqual(promote["recommended_use"], "advisory")
        self.assertIn("operator_approval_required", promote["blockers"])
        self.assertEqual(summary["recommendation_counts"]["promote_to_advisory_review"], 1)
        self.assertEqual(
            promote["statistical_status_counts"],
            {"indicative": 1},
        )

    def test_demotes_actionable_strategy_and_archives_weak_family_regime(self):
        summary = build_strategy_promotion_recommendations(
            profiles=[
                profile(
                    "leveraged_etf_momentum_allocator",
                    "defensive",
                    ticker="UVXY",
                    action="hedge",
                    hit_rate=0.38,
                    avg_excess_vs_spy=-0.02,
                    ic=-0.08,
                )
            ],
            strategy_evidence={
                "strategy_results": [
                    {
                        "strategy_name": "leveraged_etf_momentum_allocator",
                        "suggested_use": "advisory",
                        "data_ready": True,
                        "can_influence_allocation": True,
                    }
                ]
            },
            alpha_validation_runs=[],
            as_of_date=date(2026, 5, 25),
        )

        demote = next(
            row for row in summary["recommendations"]
            if row["recommendation"] == "demote_to_watch_only_review"
        )
        self.assertEqual(demote["strategy_id"], "leveraged_etf_momentum_allocator")
        self.assertEqual(demote["current_use"], "advisory")
        self.assertEqual(demote["recommended_use"], "watch_only")
        self.assertIn("hit_rate_below_45pct", demote["reasons"])
        self.assertIn("negative_excess_vs_spy", demote["reasons"])

        archive = next(
            row for row in summary["recommendations"]
            if row["recommendation"] == "archive_family_regime_review"
        )
        self.assertEqual(archive["canonical_family"], "momentum")
        self.assertEqual(archive["regime"], "defensive")
        self.assertEqual(archive["recommended_use"], "archive_in_regime")
        self.assertGreaterEqual(summary["high_priority_count"], 2)
        self.assertEqual(summary["status"], "operator_review_required")

    def test_requires_more_samples_before_interpreting_non_calibrated_profile(self):
        summary = build_strategy_promotion_recommendations(
            profiles=[
                profile(
                    "low_vol_factor",
                    "defensive",
                    ticker="BSV",
                    action="de_risk",
                    status="early_estimate",
                    n=12,
                    hit_rate=0.58,
                )
            ],
            strategy_evidence={
                "strategy_results": [
                    {
                        "strategy_name": "low_vol_factor",
                        "suggested_use": "watch_only",
                        "data_ready": True,
                    }
                ]
            },
            alpha_validation_runs=[],
            as_of_date=date(2026, 5, 25),
        )

        row = next(
            item for item in summary["recommendations"]
            if item["recommendation"] == "require_more_samples"
        )
        self.assertEqual(row["strategy_id"], "low_vol_factor")
        self.assertEqual(row["sample_count"], 12)
        self.assertIn("early_estimate", row["reasons"])
        self.assertEqual(row["recommended_use"], "watch_only")

    def test_operationally_calibrated_profile_still_requires_statistical_maturity(self):
        summary = build_strategy_promotion_recommendations(
            profiles=[
                profile(
                    "mean_reversion_lite",
                    "mean_reverting",
                    ticker="SPY",
                    status="calibrated",
                    n=40,
                    hit_rate=0.62,
                    avg_excess_vs_spy=0.03,
                    ic=0.12,
                )
            ],
            strategy_evidence={
                "strategy_results": [
                    {
                        "strategy_name": "mean_reversion_lite",
                        "suggested_use": "watch_only",
                        "data_ready": True,
                    }
                ]
            },
            alpha_validation_runs=[],
            as_of_date=date(2026, 5, 25),
        )

        row = next(
            item for item in summary["recommendations"]
            if item["recommendation"] == "require_statistical_maturity"
        )
        self.assertEqual(row["strategy_id"], "mean_reversion_lite")
        self.assertEqual(row["sample_count"], 40)
        self.assertIn("operationally_calibrated_but_statistically_early", row["reasons"])
        self.assertEqual(row["statistical_status_counts"], {"early_signal": 1})
        self.assertNotIn("promote_to_advisory_review", summary["recommendation_counts"])

    def test_high_correlation_with_actionable_strategy_blocks_promotion(self):
        summary = build_strategy_promotion_recommendations(
            profiles=[
                profile(
                    "absolute_trend_following_lite",
                    "trending_bull",
                    ticker="SPY",
                    n=140,
                    hit_rate=0.61,
                    avg_excess_vs_spy=0.03,
                    ic=0.12,
                )
            ],
            strategy_evidence={
                "strategy_results": [
                    {
                        "strategy_name": "absolute_trend_following_lite",
                        "suggested_use": "watch_only",
                        "selected_tickers": ["SPY"],
                    },
                    {
                        "strategy_name": "momentum_lite_v1",
                        "suggested_use": "advisory",
                        "selected_tickers": ["SPY"],
                    },
                ],
                "strategy_independence": {
                    "status": "available",
                    "high_correlation_pairs": [
                        {
                            "left": "absolute_trend_following_lite",
                            "right": "momentum_lite_v1",
                            "correlation": 0.94,
                            "overlap": 120,
                        }
                    ],
                },
            },
            alpha_validation_runs=[],
            as_of_date=date(2026, 5, 25),
        )

        row = next(
            item for item in summary["recommendations"]
            if item["recommendation"] == "require_promotion_evidence_alignment"
        )

        self.assertEqual(row["strategy_id"], "absolute_trend_following_lite")
        self.assertIn("independence_diagnostics_not_clear", row["blockers"])
        self.assertIn(
            "high_correlation_with_actionable:momentum_lite_v1:0.94",
            row["reasons"],
        )
        self.assertNotIn("promote_to_advisory_review", summary["recommendation_counts"])

    def test_decay_liquidity_and_cost_diagnostics_block_promotion(self):
        summary = build_strategy_promotion_recommendations(
            profiles=[
                profile(
                    "leveraged_long_amplifier_lite",
                    "trending_bull",
                    ticker="TQQQ",
                    n=140,
                    hit_rate=0.62,
                    avg_excess_vs_spy=0.04,
                    ic=0.15,
                )
            ],
            strategy_evidence={
                "strategy_results": [
                    {
                        "strategy_name": "leveraged_long_amplifier_lite",
                        "suggested_use": "watch_only",
                        "selected_tickers": ["TQQQ"],
                        "estimated_cost_pct": 0.004,
                        "turnover": 0.30,
                    }
                ],
                "etf_decay_diagnostics": {
                    "status": "available",
                    "rows": [
                        {
                            "ticker": "TQQQ",
                            "severity": "high",
                            "severity_reason": "material negative drag",
                        }
                    ],
                },
                "liquidity_proxy_diagnostics": {
                    "status": "available",
                    "rows": [
                        {
                            "ticker": "TQQQ",
                            "execution_quality": "defer_weak_signals",
                            "liquidity_bucket": "usable",
                            "spread_cost_proxy_pct": 0.003,
                        }
                    ],
                },
            },
            alpha_validation_runs=[
                {
                    "analysis_id": 300,
                    "generated_at": datetime(2026, 5, 25, tzinfo=timezone.utc),
                    "status": "observe_warning",
                    "low_edge_trade_count": 1,
                    "min_edge_to_cost_ratio": 1.2,
                }
            ],
            as_of_date=date(2026, 5, 25),
        )

        row = next(
            item for item in summary["recommendations"]
            if item["recommendation"] == "require_promotion_evidence_alignment"
        )

        self.assertIn("decay_diagnostics_not_clear", row["blockers"])
        self.assertIn("liquidity_diagnostics_not_clear", row["blockers"])
        self.assertIn("cost_diagnostics_not_clear", row["blockers"])
        self.assertIn("decay_review:TQQQ:high", row["reasons"])
        self.assertIn("liquidity_review:TQQQ:defer_weak_signals", row["reasons"])
        self.assertIn("strategy_estimated_cost_high", row["reasons"])
        self.assertIn("recent_transaction_cost_gate_low_edge", row["reasons"])

    def test_mixed_regime_coverage_blocks_global_promotion(self):
        summary = build_strategy_promotion_recommendations(
            profiles=[
                profile(
                    "mean_reversion_lite",
                    "mean_reverting",
                    ticker="SPY",
                    n=140,
                    hit_rate=0.62,
                    avg_excess_vs_spy=0.03,
                    ic=0.12,
                ),
                profile(
                    "mean_reversion_lite",
                    "trending_bull",
                    ticker="SPY",
                    n=140,
                    hit_rate=0.42,
                    avg_excess_vs_spy=-0.01,
                    ic=-0.04,
                ),
            ],
            strategy_evidence={
                "strategy_results": [
                    {
                        "strategy_name": "mean_reversion_lite",
                        "suggested_use": "watch_only",
                    }
                ]
            },
            alpha_validation_runs=[],
            as_of_date=date(2026, 5, 25),
        )

        row = next(
            item for item in summary["recommendations"]
            if item["recommendation"] == "require_promotion_evidence_alignment"
        )

        self.assertIn("regime_coverage_diagnostics_not_clear", row["blockers"])
        self.assertIn("mixed_regime_coverage:weak=trending_bull", row["reasons"])

    def test_empty_profiles_are_insufficient_data(self):
        summary = build_strategy_promotion_recommendations(
            profiles=[],
            strategy_evidence={},
            alpha_validation_runs=[],
            as_of_date=date(2026, 5, 25),
        )

        self.assertEqual(summary["status"], "insufficient_data")
        self.assertEqual(summary["recommendation_count"], 0)
        self.assertEqual(summary["execution_authority"], "none")


if __name__ == "__main__":
    unittest.main()
