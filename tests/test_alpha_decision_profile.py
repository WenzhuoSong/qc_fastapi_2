import unittest
from datetime import date, datetime, timezone

from services.alpha_decision_profile import (
    build_alpha_decision_profiles,
    redundancy_multiplier,
)
from services.construction_epoch import build_construction_epoch


def profile(
    strategy_id: str,
    regime: str,
    *,
    ticker: str = "SPY",
    action: str = "increase",
    status: str = "calibrated",
    source_bucket: str = "combined",
    n: int = 320,
    hit_rate: float = 0.61,
    avg_excess_vs_spy: float = 0.02,
    ic: float = 0.10,
) -> dict:
    epoch = build_construction_epoch(
        pc_mode="gated",
        construction_objective_version="maximize_signal_weighted_effective_n_v1",
        policy_version="sprint8a",
        promotion_config_hash="test",
        source="unit_test",
    )
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
        "diagnostics": {
            "construction_epoch": epoch,
        },
    }


def attribution(residual: float = 0.003, sample_count: int = 8) -> dict:
    return {
        "period_key": "2026-W21",
        "period_start": date(2026, 5, 18),
        "period_end": date(2026, 5, 24),
        "generated_at": datetime(2026, 5, 25, tzinfo=timezone.utc),
        "status": "attributed",
        "attribution_method": "spy_qqq_momentum_v1",
        "residual_alpha_candidate": residual,
        "sample_count": sample_count,
        "r_squared": 0.62,
        "data_quality": "ok",
        "benchmark_source": "yfinance",
    }


def alpha_run(**overrides) -> dict:
    row = {
        "analysis_id": 201,
        "generated_at": datetime(2026, 5, 25, tzinfo=timezone.utc),
        "status": "ok",
        "data_quality": "ok",
        "cost_gate_status": "ok",
        "low_edge_trade_count": 0,
        "min_edge_to_cost_ratio": 3.2,
        "avg_edge_to_cost_ratio": 5.4,
        "independent_alpha_family_count": 2,
    }
    row.update(overrides)
    return row


def evidence(strategy_id: str = "mean_reversion_lite") -> dict:
    return {
        "strategy_results": [
            {
                "strategy_name": strategy_id,
                "suggested_use": "watch_only",
                "data_ready": True,
                "can_influence_allocation": False,
                "estimated_cost_pct": 0.001,
                "turnover": 0.22,
                "reason_codes": ["historical_medium"],
            }
        ]
    }


class AlphaDecisionProfileTests(unittest.TestCase):
    def test_builds_profile_with_required_read_only_fields(self):
        summary = build_alpha_decision_profiles(
            profiles=[profile("mean_reversion_lite", "mean_reverting")],
            performance_attribution_rows=[attribution()],
            alpha_validation_runs=[alpha_run()],
            strategy_independence={
                "status": "available",
                "effective_independent_alpha_count": 2,
                "pair_rows": [],
                "high_correlation_pairs": [],
            },
            strategy_evidence=evidence("mean_reversion_lite"),
            as_of_date=date(2026, 5, 25),
        )

        self.assertEqual(summary["contract_version"], "alpha_decision_profiles_v1")
        self.assertEqual(summary["execution_authority"], "none")
        self.assertEqual(summary["target_weight_mutation"], "none")
        self.assertTrue(summary["decision_input_only"])
        row = summary["rows"][0]
        self.assertEqual(row["strategy_id"], "mean_reversion_lite")
        self.assertEqual(row["strategy_family"], "mean_reversion")
        self.assertEqual(row["statistical_status"], "indicative")
        self.assertEqual(row["residual_alpha_status"], "positive")
        self.assertEqual(row["cost_status"], "ok")
        self.assertEqual(row["net_edge_status"], "ok")
        self.assertAlmostEqual(row["gross_expected_edge"], 0.003)
        self.assertAlmostEqual(row["estimated_ibkr_cost_pct"], 0.001)
        self.assertAlmostEqual(row["cost_adjusted_edge"], 0.002)
        self.assertAlmostEqual(row["edge_to_cost_ratio"], 3.0)
        self.assertEqual(row["cost_model"], "IBKR_return_drag_v1")
        self.assertEqual(row["decision_status"], "eligible")
        self.assertEqual(row["execution_authority"], "none")
        self.assertEqual(row["target_weight_mutation"], "none")
        self.assertEqual(row["independence_cluster_id"], "independent:mean_reversion_lite")
        self.assertIn("hit_rate_ci_width", row)
        self.assertEqual(row["pc_mode"], "gated")

    def test_operationally_calibrated_early_samples_need_more_samples(self):
        summary = build_alpha_decision_profiles(
            profiles=[profile("mean_reversion_lite", "mean_reverting", n=40, status="calibrated")],
            performance_attribution_rows=[attribution()],
            alpha_validation_runs=[alpha_run()],
            strategy_independence={"status": "available", "pair_rows": []},
            strategy_evidence=evidence("mean_reversion_lite"),
            as_of_date=date(2026, 5, 25),
        )

        row = summary["rows"][0]
        self.assertEqual(row["statistical_status"], "monitoring_ready")
        self.assertEqual(row["decision_status"], "needs_more_samples")
        self.assertEqual(row["statistical_credit"], 0.10)

    def test_negative_residual_alpha_degrades_profile(self):
        summary = build_alpha_decision_profiles(
            profiles=[profile("momentum_lite_v1", "trending_bull")],
            performance_attribution_rows=[attribution(residual=-0.004)],
            alpha_validation_runs=[alpha_run()],
            strategy_independence={"status": "available", "pair_rows": []},
            strategy_evidence=evidence("momentum_lite_v1"),
            as_of_date=date(2026, 5, 25),
        )

        row = summary["rows"][0]
        self.assertEqual(row["residual_alpha_status"], "negative")
        self.assertEqual(row["decision_status"], "degraded")
        self.assertIn("degraded_alpha_profile:momentum_lite_v1:trending_bull", summary["warnings"])

    def test_positive_gross_edge_can_be_negative_after_cost(self):
        summary = build_alpha_decision_profiles(
            profiles=[profile("mean_reversion_lite", "mean_reverting")],
            performance_attribution_rows=[attribution(residual=0.001)],
            alpha_validation_runs=[alpha_run()],
            strategy_independence={"status": "available", "pair_rows": []},
            strategy_evidence={
                "strategy_results": [
                    {
                        "strategy_name": "mean_reversion_lite",
                        "suggested_use": "watch_only",
                        "data_ready": True,
                        "estimated_cost_pct": 0.002,
                        "turnover": 0.24,
                    }
                ]
            },
            as_of_date=date(2026, 5, 25),
        )

        row = summary["rows"][0]
        self.assertEqual(row["residual_alpha_status"], "positive")
        self.assertEqual(row["net_edge_status"], "negative_after_cost")
        self.assertEqual(row["cost_status"], "negative_after_cost")
        self.assertAlmostEqual(row["cost_adjusted_edge"], -0.001)
        self.assertEqual(row["decision_status"], "watch_only")
        self.assertEqual(summary["net_edge_status_counts"], {"negative_after_cost": 1})

    def test_high_correlation_sets_cluster_and_redundancy_multiplier(self):
        summary = build_alpha_decision_profiles(
            profiles=[
                profile("momentum_lite_v1", "trending_bull"),
                profile("absolute_trend_following_lite", "trending_bull"),
            ],
            performance_attribution_rows=[attribution()],
            alpha_validation_runs=[alpha_run()],
            strategy_independence={
                "status": "available",
                "effective_independent_alpha_count": 1.1,
                "pair_rows": [
                    {
                        "left_strategy": "momentum_lite_v1",
                        "right_strategy": "absolute_trend_following_lite",
                        "correlation": 0.82,
                    }
                ],
                "high_correlation_pairs": [],
            },
            strategy_evidence={
                "strategy_results": [
                    {
                        "strategy_name": "momentum_lite_v1",
                        "estimated_cost_pct": 0.001,
                        "turnover": 0.20,
                    },
                    {
                        "strategy_name": "absolute_trend_following_lite",
                        "estimated_cost_pct": 0.001,
                        "turnover": 0.20,
                    },
                ]
            },
            as_of_date=date(2026, 5, 25),
        )

        rows = {row["strategy_id"]: row for row in summary["rows"]}
        self.assertEqual(rows["momentum_lite_v1"]["max_positive_correlation"], 0.82)
        self.assertEqual(rows["momentum_lite_v1"]["redundancy_multiplier"], 0.05)
        self.assertEqual(
            rows["momentum_lite_v1"]["independence_cluster_id"],
            rows["absolute_trend_following_lite"]["independence_cluster_id"],
        )
        self.assertEqual(summary["raw_alpha_strategy_count"], 2)
        self.assertEqual(summary["independence_adjusted_strategy_count"], 0.1)
        self.assertEqual(
            summary["independence_consumption"]["method"],
            "sum_alpha_profile_redundancy_multipliers",
        )
        self.assertEqual(redundancy_multiplier(0.39), 0.85)
        self.assertEqual(redundancy_multiplier(0.65), 0.20)


if __name__ == "__main__":
    unittest.main()
