import inspect
import unittest

from env_setup import ensure_test_settings

ensure_test_settings()

import services.portfolio_construction as portfolio_construction_module
from services.portfolio_construction_gate import construction_input_for_target_builder
from services.portfolio_construction import (
    PortfolioConstructionModel,
    build_construction_alpha_decision_context,
    build_construction_signal_strengths,
)


class PortfolioConstructionTests(unittest.TestCase):
    def test_weight_arithmetic_uses_weight_ops_contract(self):
        source = inspect.getsource(portfolio_construction_module)

        self.assertIn("from services.weight_ops import", source)
        self.assertIn("normalize_cash_first", source)
        self.assertIn("normalize_proportional", source)
        self.assertIn("apply_group_caps_cash_first", source)
        self.assertNotIn("def _normalize_cash_first", source)

    def test_initial_base_construction_uses_proportional_normalization(self):
        out = PortfolioConstructionModel().construct(
            base_weights={"SPY": 0.20, "CASH": 0.20},
            current_weights={"SPY": 0.50, "CASH": 0.50},
            signal_strengths={},
            basket_reviews=None,
            scorecard_permission="normal_rebalance",
            turnover_budget=None,
        ).to_dict()

        self.assertAlmostEqual(out["target_weights"]["SPY"], 0.50)
        self.assertAlmostEqual(out["target_weights"]["CASH"], 0.50)

    def test_factor_limits_scale_excess_exposure_to_cash(self):
        out = PortfolioConstructionModel().construct(
            base_weights={
                "QQQ": 0.18,
                "XLK": 0.12,
                "SOXX": 0.10,
                "PSI": 0.08,
                "CASH": 0.52,
            },
            current_weights={
                "QQQ": 0.18,
                "XLK": 0.12,
                "SOXX": 0.10,
                "PSI": 0.08,
                "CASH": 0.52,
            },
            signal_strengths={},
            basket_reviews=None,
            scorecard_permission="normal_rebalance",
            turnover_budget=None,
        ).to_dict()

        self.assertLessEqual(out["factor_exposures"]["tech_growth"], 0.350001)
        self.assertIn("factor_exposure_before", out)
        self.assertIn("factor_exposure_after", out)
        self.assertIn("policy_evaluation", out)
        self.assertEqual(out["objective"]["primary"], "maximize_effective_n_with_active_basket_policy")
        self.assertEqual(out["pc_objective_version"], "maximize_effective_n_with_active_basket_v1")
        self.assertEqual(out["execution_authority"], "none")
        self.assertEqual(out["target_weight_mutation"], "none")
        self.assertEqual(out["pc_mode"], "shadow")
        self.assertFalse(out["ready_for_gated_review"])
        self.assertIn("signal_quality_not_diluted", out["objective"]["subject_to"])
        self.assertIn("alpha_decision_quality_not_diluted", out["objective"]["subject_to"])
        self.assertIn("global_active_count_within_active_basket_policy", out["objective"]["subject_to"])
        self.assertIn("role_position_counts_within_active_basket_policy", out["objective"]["subject_to"])
        self.assertIn("sub_min_executable_positions_excluded", out["objective"]["subject_to"])
        self.assertIn("hedge_role_requires_hedge_intent", out["objective"]["subject_to"])
        self.assertIn("max_cluster_exposure_by_correlated_strategy_group", out["objective"]["subject_to"])
        self.assertIn("factor_concentration_within_group_limits", out["objective"]["subject_to"])
        self.assertEqual(out["diagnostics"]["objective"]["effective_n_target"], 8)
        self.assertIn("without diluting higher-quality alpha-decision evidence", out["objective"]["rationale"])
        self.assertEqual(out["construction_source"], "portfolio_construction")
        self.assertEqual(out["diagnostics"]["execution_effect"], "diagnostic_only")
        self.assertIn("signal_objective_metrics", out)
        self.assertIn("signal_objective_rows", out)
        self.assertIn("alpha_decision_objective_metrics", out)
        self.assertIn("alpha_decision_objective_rows", out)
        self.assertIn("strategy_cluster_exposure_rows", out)
        self.assertTrue(out["diagnostics"]["signal_weighted_objective_enabled"])
        self.assertTrue(out["diagnostics"]["alpha_decision_objective_enabled"])
        self.assertTrue(out["diagnostics"]["pc_shadow_candidate_is_not_target_builder_input"])
        self.assertGreater(out["target_weights"]["CASH"], 0.52)
        self.assertIn("candidate_weights", out)
        self.assertIn("basket_evaluation", out)
        self.assertTrue(out["basket_evaluation"]["candidate_policy_ok"])
        self.assertIn("objective_terms", out)
        for key in [
            "alpha_support_score",
            "diversification_score",
            "turnover_penalty",
            "concentration_penalty",
            "active_basket_violation_penalty",
            "subscale_position_penalty",
        ]:
            self.assertIn(key, out["objective_terms"])
        self.assertTrue(any(item.startswith("factor_limit:tech_growth") for item in out["violations"]))
        self.assertFalse(out["diagnostics"]["consumes_raw_llm_adjusted_weights"])

    def test_shadow_candidate_excludes_sub_min_executable_positions(self):
        out = PortfolioConstructionModel().construct(
            base_weights={"SPY": 0.20, "XLRE": 0.001, "CASH": 0.799},
            current_weights={"SPY": 0.20, "XLRE": 0.001, "CASH": 0.799},
            signal_strengths={},
            basket_reviews=None,
            scorecard_permission="normal_rebalance",
            turnover_budget=None,
        ).to_dict()

        self.assertNotIn("XLRE", out["candidate_weights"])
        self.assertAlmostEqual(out["candidate_weights"]["SPY"], 0.20)
        self.assertGreater(out["candidate_weights"]["CASH"], out["target_weights"]["CASH"])
        floor_events = out["basket_evaluation"]["candidate_cleanup_events"]["minimum_weight_floor_events"]
        self.assertEqual(floor_events[0]["ticker"], "XLRE")

    def test_shadow_candidate_respects_role_and_global_position_caps(self):
        out = PortfolioConstructionModel().construct(
            base_weights={
                "SPY": 0.08,
                "QQQ": 0.08,
                "IWM": 0.08,
                "RSP": 0.08,
                "XLI": 0.06,
                "XLE": 0.06,
                "XLK": 0.06,
                "XLV": 0.06,
                "XLP": 0.06,
                "XLY": 0.06,
                "SOXX": 0.04,
                "PSI": 0.04,
                "FTXL": 0.04,
                "CASH": 0.26,
            },
            current_weights={"CASH": 1.0},
            signal_strengths={},
            basket_reviews=None,
            scorecard_permission="normal_rebalance",
            turnover_budget=None,
        ).to_dict()

        self.assertLessEqual(out["basket_evaluation"]["active_count"], 10)
        self.assertLessEqual(out["basket_evaluation"]["roles"]["sector"]["active_count"], 5)
        self.assertTrue(out["basket_evaluation"]["candidate_policy_ok"])
        self.assertTrue(out["basket_evaluation"]["candidate_cleanup_events"]["role_max_trim_events"])

    def test_shadow_candidate_is_not_target_builder_input(self):
        out = PortfolioConstructionModel().construct(
            base_weights={"SPY": 0.20, "CASH": 0.80},
            current_weights={"SPY": 0.20, "CASH": 0.80},
            signal_strengths={},
            basket_reviews=None,
            scorecard_permission="normal_rebalance",
            turnover_budget=None,
        ).to_dict()

        gate = construction_input_for_target_builder(
            portfolio_construction_payload={"candidate_weights": out["candidate_weights"]},
            promotion_gate={"status": "passed", "eligible": True, "blockers": []},
            config={"portfolio_construction_mode": "gated", "enabled": True},
        )

        self.assertIsNone(gate["construction_weights"])
        self.assertEqual(gate["blocked_reason"], "construction_weights_missing")
        self.assertEqual(gate["construction_weight_source"], "pc_shadow_weights")

    def test_basket_review_tightens_group_to_multiplier_limit(self):
        out = PortfolioConstructionModel().construct(
            base_weights={
                "SOXX": 0.10,
                "PSI": 0.08,
                "FTXL": 0.07,
                "CASH": 0.75,
            },
            current_weights={
                "SOXX": 0.10,
                "PSI": 0.08,
                "FTXL": 0.07,
                "CASH": 0.75,
            },
            signal_strengths={},
            basket_reviews=[{"group": "semiconductors", "tickers": ["SOXX", "PSI", "FTXL"]}],
            scorecard_permission="normal_rebalance",
            turnover_budget=None,
        ).to_dict()

        self.assertLessEqual(out["factor_exposures"]["semiconductors"], 0.175001)
        self.assertGreater(out["basket_exposure_before"]["semiconductors"]["exposure"], out["basket_exposure_after"]["semiconductors"]["exposure"])
        self.assertIn("semiconductors", out["diagnostics"]["active_basket_reviews"])
        self.assertTrue(any(item.startswith("basket_limit:semiconductors") for item in out["violations"]))

    def test_turnover_budget_preserves_stronger_signal_adjustment(self):
        out = PortfolioConstructionModel().construct(
            base_weights={"SPY": 0.20, "QQQ": 0.20, "CASH": 0.60},
            current_weights={"SPY": 0.10, "QQQ": 0.10, "CASH": 0.80},
            signal_strengths={"SPY": 0.9, "QQQ": 0.1},
            basket_reviews=None,
            scorecard_permission="normal_rebalance",
            turnover_budget=0.10,
        ).to_dict()

        self.assertAlmostEqual(out["target_weights"]["SPY"], 0.20)
        self.assertAlmostEqual(out["target_weights"]["QQQ"], 0.10)
        self.assertLessEqual(out["turnover"]["estimated"], 0.100001)
        self.assertTrue(any(item.startswith("turnover_budget:") for item in out["violations"]))

    def test_signal_weighted_objective_penalizes_low_signal_dilution(self):
        out = PortfolioConstructionModel().construct(
            base_weights={"SPY": 0.20, "QQQ": 0.20, "CASH": 0.60},
            current_weights={"SPY": 0.20, "QQQ": 0.20, "CASH": 0.60},
            signal_strengths={"SPY": 0.9, "QQQ": 0.1},
            basket_reviews=None,
            scorecard_permission="normal_rebalance",
            turnover_budget=None,
        ).to_dict()

        self.assertAlmostEqual(out["effective_n_after"], 12.5)
        self.assertLess(out["signal_weighted_effective_n_after"], out["effective_n_after"])
        self.assertAlmostEqual(out["signal_alignment_score_after"], 0.50)
        qqq = next(row for row in out["signal_objective_rows"] if row["ticker"] == "QQQ")
        self.assertAlmostEqual(qqq["signal_weighted_after"], 0.02)

    def test_alpha_decision_objective_penalizes_correlated_duplicate_signals(self):
        evidence_bundle = {
            "strategies": {
                "strategy_results": [
                    {
                        "strategy_name": "momentum_lite_v1",
                        "suggested_use": "advisory",
                        "confidence_score": 0.9,
                        "selected_tickers": ["QQQ"],
                    },
                    {
                        "strategy_name": "absolute_trend_following_lite",
                        "suggested_use": "advisory",
                        "confidence_score": 0.9,
                        "selected_tickers": ["XLK"],
                    },
                ],
                "strategy_independence": {
                    "status": "available",
                    "pair_rows": [
                        {
                            "left_strategy": "momentum_lite_v1",
                            "right_strategy": "absolute_trend_following_lite",
                            "correlation": 0.82,
                        }
                    ],
                },
            },
            "rotation": {"signals": {"QQQ": 1.0, "XLK": 1.0}},
        }
        signals = build_construction_signal_strengths(evidence_bundle)
        alpha_context = build_construction_alpha_decision_context(evidence_bundle)
        out = PortfolioConstructionModel().construct(
            base_weights={"QQQ": 0.20, "XLK": 0.20, "CASH": 0.60},
            current_weights={"QQQ": 0.20, "XLK": 0.20, "CASH": 0.60},
            signal_strengths=signals,
            alpha_decision_context=alpha_context,
            basket_reviews=None,
            scorecard_permission="normal_rebalance",
            turnover_budget=None,
        ).to_dict()

        self.assertAlmostEqual(out["signal_weighted_effective_n_after"], 2.0)
        self.assertAlmostEqual(out["independence_adjusted_net_signal_effective_n_after"], 2.0)
        self.assertAlmostEqual(
            out["alpha_decision_objective_metrics"]["independence_adjusted_strategy_count"],
            0.1,
        )
        self.assertIn(
            "alpha_strategy_count_collapses_after_redundancy",
            out["alpha_decision_objective_metrics"]["warnings"],
        )
        cluster = out["strategy_cluster_exposure_rows"][0]
        self.assertEqual(cluster["strategy_count"], 2)
        self.assertAlmostEqual(cluster["weight_after"], 0.35)
        qqq = next(row for row in out["alpha_decision_objective_rows"] if row["ticker"] == "QQQ")
        self.assertAlmostEqual(qqq["redundancy_multiplier"], 0.05)
        self.assertAlmostEqual(qqq["decision_multiplier"], 0.05)

    def test_negative_correlation_keeps_alpha_decision_signal_credit(self):
        evidence_bundle = {
            "strategies": {
                "strategy_results": [
                    {
                        "strategy_name": "volatility_hedge_lite",
                        "suggested_use": "advisory",
                        "confidence_score": 0.8,
                        "selected_tickers": ["VIXY"],
                    },
                    {
                        "strategy_name": "momentum_lite_v1",
                        "suggested_use": "advisory",
                        "confidence_score": 0.8,
                        "selected_tickers": ["SPY"],
                    },
                ],
                "strategy_independence": {
                    "status": "available",
                    "pair_rows": [
                        {
                            "left_strategy": "volatility_hedge_lite",
                            "right_strategy": "momentum_lite_v1",
                            "correlation": -0.45,
                        }
                    ],
                },
            },
            "rotation": {"signals": {"VIXY": 1.0, "SPY": 1.0}},
        }
        alpha_context = build_construction_alpha_decision_context(evidence_bundle)

        self.assertEqual(alpha_context["independence_adjusted_strategy_count"], 2.0)
        vixy = alpha_context["ticker_adjustments"]["VIXY"]
        self.assertAlmostEqual(vixy["redundancy_multiplier"], 1.0)
        self.assertAlmostEqual(vixy["independence_adjusted_signal_strength"], 0.8)

    def test_alpha_decision_context_carries_net_edge_into_pc_rows(self):
        evidence_bundle = {
            "strategies": {
                "strategy_results": [
                    {
                        "strategy_name": "mean_reversion_lite",
                        "suggested_use": "advisory",
                        "confidence_score": 0.8,
                        "selected_tickers": ["SPY"],
                    }
                ],
            },
            "rotation": {"signals": {"SPY": 1.0}},
        }
        alpha_context = build_construction_alpha_decision_context(
            evidence_bundle,
            alpha_decision_profiles={
                "rows": [
                    {
                        "strategy_id": "mean_reversion_lite",
                        "tickers": ["SPY"],
                        "independence_cluster_id": "independent:mean_reversion_lite",
                        "redundancy_multiplier": 1.0,
                        "decision_multiplier": 0.25,
                        "net_edge_status": "low_edge_after_cost",
                        "gross_expected_edge": 0.003,
                        "estimated_ibkr_cost_pct": 0.002,
                        "cost_adjusted_edge": 0.001,
                        "edge_to_cost_ratio": 1.5,
                    }
                ]
            },
        )
        out = PortfolioConstructionModel().construct(
            base_weights={"SPY": 0.20, "CASH": 0.80},
            current_weights={"SPY": 0.20, "CASH": 0.80},
            signal_strengths=build_construction_signal_strengths(evidence_bundle),
            alpha_decision_context=alpha_context,
            basket_reviews=None,
            scorecard_permission="normal_rebalance",
            turnover_budget=None,
        ).to_dict()

        row = next(item for item in out["alpha_decision_objective_rows"] if item["ticker"] == "SPY")
        self.assertEqual(row["net_edge_status"], "low_edge_after_cost")
        self.assertAlmostEqual(row["gross_expected_edge"], 0.003)
        self.assertAlmostEqual(row["estimated_ibkr_cost_pct"], 0.002)
        self.assertAlmostEqual(row["cost_adjusted_edge"], 0.001)
        self.assertAlmostEqual(row["edge_to_cost_ratio"], 1.5)
        self.assertEqual(row["policy_effective_mode"], "observe")
        self.assertFalse(row["allocation_effect"])

    def test_alpha_decision_context_exposes_policy_mode_without_execution_authority(self):
        evidence_bundle = {
            "strategies": {
                "strategy_results": [
                    {
                        "strategy_name": "mean_reversion_lite",
                        "suggested_use": "advisory",
                        "confidence_score": 0.8,
                        "selected_tickers": ["SPY"],
                    }
                ],
            },
            "rotation": {"signals": {"SPY": 1.0}},
        }
        alpha_context = build_construction_alpha_decision_context(
            evidence_bundle,
            policy_config={
                "mode": "gated",
                "observe_cycles": 25,
                "operator_gated_approved": True,
                "raw_adjusted_diagnostics_reviewed": True,
                "dry_run_report_reviewed": True,
                "evidence_cap_calibration_fresh": True,
            },
        )

        self.assertEqual(alpha_context["policy_effective_mode"], "gated")
        self.assertTrue(alpha_context["policy_allocation_effect"])
        self.assertEqual(alpha_context["execution_authority"], "none")
        self.assertEqual(alpha_context["target_weight_mutation"], "none")

    def test_no_add_permission_clips_targets_to_current(self):
        out = PortfolioConstructionModel().construct(
            base_weights={"SPY": 0.20, "CASH": 0.80},
            current_weights={"SPY": 0.10, "CASH": 0.90},
            signal_strengths={"SPY": 1.0},
            basket_reviews=None,
            scorecard_permission="reduce_risk_only",
            turnover_budget=None,
        ).to_dict()

        self.assertAlmostEqual(out["target_weights"]["SPY"], 0.10)
        self.assertTrue(any(item.startswith("scorecard_no_add:SPY") for item in out["violations"]))

    def test_same_input_is_repeatable(self):
        payload = dict(
            base_weights={"SOXX": 0.08, "SPY": 0.20, "CASH": 0.72},
            current_weights={"SOXX": 0.04, "SPY": 0.20, "CASH": 0.76},
            signal_strengths={"SOXX": 0.7, "SPY": 0.2},
            basket_reviews={"semiconductors": {"reason": "cluster"}},
            scorecard_permission="normal_rebalance",
            turnover_budget=0.03,
        )

        first = PortfolioConstructionModel().construct(**payload).to_dict()
        second = PortfolioConstructionModel().construct(**payload).to_dict()

        self.assertEqual(first, second)

    def test_build_construction_signals_merges_strategy_and_rotation(self):
        signals = build_construction_signal_strengths(
            {
                "strategies": {
                    "strategy_results": [
                        {
                            "strategy_name": "momentum_lite_v1",
                            "suggested_use": "advisory",
                            "confidence_score": 0.80,
                            "selected_tickers": ["XLK", "XLP"],
                        },
                        {
                            "strategy_name": "watch_only",
                            "suggested_use": "watch_only",
                            "confidence_score": 1.0,
                            "selected_tickers": ["SOXX"],
                        },
                    ],
                },
                "rotation": {
                    "signals": {
                        "XLK": 1.0,
                        "XLP": -1.0,
                        "SOXX": 0.5,
                    }
                },
            }
        )

        self.assertAlmostEqual(signals["XLK"], 0.88)
        self.assertAlmostEqual(signals["XLP"], 0.08)
        self.assertAlmostEqual(signals["SOXX"], 0.20)

    def test_build_construction_signals_ignores_non_alpha_strategy_rows(self):
        signals = build_construction_signal_strengths(
            {
                "strategies": {
                    "strategy_results": [
                        {
                            "strategy_name": "equal_weight_benchmark",
                            "alpha_source": False,
                            "suggested_use": "primary",
                            "confidence_score": 1.0,
                            "selected_tickers": ["SPY"],
                        }
                    ],
                },
                "rotation": {"signals": {}},
            }
        )

        self.assertEqual(signals, {})


if __name__ == "__main__":
    unittest.main()
