import unittest

from services.position_governance import apply_position_governance


class PositionGovernanceTest(unittest.TestCase):
    def test_position_explanations_sort_by_current_weight_desc(self):
        out = apply_position_governance(
            target_weights={"AAA": 0.02, "BBB": 0.18, "CCC": 0.08, "CASH": 0.72},
            current_weights={"AAA": 0.02, "BBB": 0.18, "CCC": 0.08, "CASH": 0.72},
            holdings_meta=[
                {"ticker": "AAA", "unrealized_pnl_pct": 0.01},
                {"ticker": "BBB", "unrealized_pnl_pct": 0.01},
                {"ticker": "CCC", "unrealized_pnl_pct": 0.01},
            ],
            strategy_evidence={"strategy_results": []},
            market_scorecard={"investment_permission": "normal_rebalance"},
            news_evidence={},
        )

        rows = out.portfolio_summary["position_explanations"]

        self.assertEqual([row["ticker"] for row in rows], ["BBB", "CCC", "AAA"])

    def test_loss_with_weak_strategy_support_blocks_add_and_marks_review(self):
        out = apply_position_governance(
            target_weights={"FTXL": 0.06, "CASH": 0.94},
            current_weights={"FTXL": 0.03, "CASH": 0.97},
            holdings_meta=[
                {"ticker": "FTXL", "unrealized_pnl_pct": -0.059, "atr_pct": 0.018},
            ],
            strategy_evidence={"strategy_results": []},
            market_scorecard={"investment_permission": "small_overweight_only"},
            news_evidence={},
        )

        decision = _decision(out, "FTXL")
        self.assertEqual(decision["decision"], "hold_review")
        self.assertEqual(decision["target_after"], 0.03)
        self.assertIn("unrealized_loss_review", decision["reason_codes"])
        self.assertIn("strategy_support_weak", decision["reason_codes"])
        self.assertTrue(any(item.startswith("buy_blocked:FTXL") for item in out.blocked_actions))
        explanation = _explanation(out, "FTXL")
        self.assertEqual(explanation["position_state"], "loss_review")
        self.assertIn("loss is above hard trim threshold", explanation["why_hold"])
        self.assertIn("position is in unrealized loss review", explanation["why_not_add"])
        self.assertEqual(explanation["next_trigger"], "trim if loss <= -8% and strategy support remains weak")

    def test_deep_loss_with_weak_support_trims_position(self):
        out = apply_position_governance(
            target_weights={"PSI": 0.04, "CASH": 0.96},
            current_weights={"PSI": 0.04, "CASH": 0.96},
            holdings_meta=[
                {"ticker": "PSI", "unrealized_pnl_pct": -0.10, "atr_pct": 0.018},
            ],
            strategy_evidence={"strategy_results": []},
            market_scorecard={"investment_permission": "small_overweight_only"},
            news_evidence={},
        )

        decision = _decision(out, "PSI")
        self.assertEqual(decision["decision"], "trim")
        self.assertAlmostEqual(decision["target_after"], 0.01, places=4)
        self.assertTrue(any(item.startswith("PSI") for item in out.forced_trims))

    def test_crowded_semiconductor_group_blocks_new_adds(self):
        out = apply_position_governance(
            target_weights={"FTXL": 0.08, "SOXX": 0.07, "PSI": 0.06, "CASH": 0.79},
            current_weights={"FTXL": 0.06, "SOXX": 0.07, "PSI": 0.06, "XSD": 0.08, "CASH": 0.73},
            holdings_meta=[
                {"ticker": "FTXL", "unrealized_pnl_pct": -0.02, "atr_pct": 0.018},
                {"ticker": "SOXX", "unrealized_pnl_pct": -0.03, "atr_pct": 0.018},
                {"ticker": "PSI", "unrealized_pnl_pct": -0.04, "atr_pct": 0.018},
            ],
            strategy_evidence={
                "strategy_results": [
                    {
                        "strategy_name": "momentum_lite_v1",
                        "suggested_use": "advisory",
                        "selected_tickers": ["FTXL", "SOXX", "PSI"],
                    }
                ]
            },
            market_scorecard={"investment_permission": "small_overweight_only"},
            news_evidence={},
        )

        decision = _decision(out, "FTXL")
        self.assertEqual(decision["target_after"], 0.06)
        self.assertIn("semiconductors_concentration_high", decision["reason_codes"])
        self.assertIn("concentration_add_blocked:FTXL", out.blocked_actions)
        self.assertGreater(decision["sector_crowding_multiplier"], 1.0)
        semis = out.portfolio_summary["group_exposures"]["semiconductors"]
        self.assertEqual(semis["status"], "over_limit")
        self.assertAlmostEqual(semis["limit"], 0.25, places=4)
        explanation = _explanation(out, "FTXL")
        self.assertIn("group exposure is above limit", explanation["why_not_add"])
        self.assertIn("semiconductors exposure falls below 25%", explanation["next_trigger"])

    def test_large_winner_high_weight_gets_trimmed_for_risk_budget(self):
        out = apply_position_governance(
            target_weights={"XLK": 0.16, "CASH": 0.84},
            current_weights={"XLK": 0.16, "CASH": 0.84},
            holdings_meta=[
                {"ticker": "XLK", "unrealized_pnl_pct": 0.107, "atr_pct": 0.018},
            ],
            strategy_evidence={
                "strategy_results": [
                    {
                        "strategy_name": "momentum_lite_v1",
                        "suggested_use": "advisory",
                        "selected_tickers": ["XLK"],
                    }
                ]
            },
            market_scorecard={"investment_permission": "small_overweight_only"},
            news_evidence={},
        )

        decision = _decision(out, "XLK")
        self.assertEqual(decision["decision"], "trim")
        self.assertAlmostEqual(decision["target_after"], 0.14, places=4)
        self.assertIn("winner_risk_budget_review", decision["reason_codes"])

    def test_hard_risk_ticker_gets_trim_or_exit_permission(self):
        out = apply_position_governance(
            target_weights={"XLE": 0.09, "CASH": 0.91},
            current_weights={"XLE": 0.09, "CASH": 0.91},
            holdings_meta=[
                {"ticker": "XLE", "unrealized_pnl_pct": 0.03, "atr_pct": 0.018},
            ],
            strategy_evidence={},
            market_scorecard={"investment_permission": "small_overweight_only"},
            news_evidence={"hard_risk_events": {"XLE": ["oil_shock"]}},
        )

        decision = _decision(out, "XLE")
        self.assertEqual(decision["action_permission"], "trim_or_exit")
        self.assertIn("hard_risk", decision["reason_codes"])
        self.assertLess(decision["target_after"], decision["target_before"])
        explanation = _explanation(out, "XLE")
        self.assertEqual(explanation["position_state"], "hard_risk_review")
        self.assertEqual(explanation["why_not_exit"], ["exit is permitted for manual/hard-risk review"])
        self.assertIn("hard-risk event is active", explanation["why_hold"][0])
        self.assertNotIn("no deterministic rule requires reduction", explanation["why_hold"])
        self.assertEqual(explanation["explanation_facts"]["severity"], "hard_risk")

    def test_core_loss_uses_wider_threshold_than_satellite(self):
        out = apply_position_governance(
            target_weights={"SPY": 0.10, "PSI": 0.04, "CASH": 0.86},
            current_weights={"SPY": 0.10, "PSI": 0.04, "CASH": 0.86},
            holdings_meta=[
                {"ticker": "SPY", "universe_role": "core", "unrealized_pnl_pct": -0.045, "atr_pct": 0.012},
                {"ticker": "PSI", "universe_role": "satellite", "unrealized_pnl_pct": -0.045, "atr_pct": 0.018},
            ],
            strategy_evidence={"strategy_results": []},
            market_scorecard={"investment_permission": "small_overweight_only"},
            news_evidence={},
        )

        self.assertNotIn("unrealized_loss_review", _decision(out, "SPY")["reason_codes"])
        self.assertIn("unrealized_loss_review", _decision(out, "PSI")["reason_codes"])
        self.assertEqual(_explanation(out, "PSI")["position_state"], "loss_review")

    def test_correlated_loss_positions_create_basket_review(self):
        out = apply_position_governance(
            target_weights={"FTXL": 0.06, "SOXX": 0.06, "CASH": 0.88},
            current_weights={"FTXL": 0.06, "SOXX": 0.06, "CASH": 0.88},
            holdings_meta=[
                {"ticker": "FTXL", "universe_role": "satellite", "unrealized_pnl_pct": -0.05, "atr_pct": 0.018},
                {"ticker": "SOXX", "universe_role": "satellite", "unrealized_pnl_pct": -0.06, "atr_pct": 0.018},
            ],
            strategy_evidence={"strategy_results": []},
            market_scorecard={"investment_permission": "small_overweight_only"},
            news_evidence={},
        )

        self.assertIn("basket_review", _decision(out, "FTXL")["reason_codes"])
        self.assertEqual(out.portfolio_summary["basket_reviews"][0]["group"], "semiconductors")
        self.assertEqual(out.portfolio_summary["basket_reviews"][0]["tickers"], ["FTXL", "SOXX"])
        explanation = _explanation(out, "FTXL")
        self.assertEqual(explanation["explanation_facts"]["severity"], "basket_review")
        self.assertIn("basket has multiple correlated positions", " ".join(explanation["why_hold"]))
        self.assertIn("semiconductors basket is in correlated review", explanation["why_not_add"])
        self.assertIn("manual trim review if semiconductors basket weakness persists", explanation["next_trigger"])

    def test_loss_review_with_advisory_support_is_worded_as_limited_support(self):
        out = apply_position_governance(
            target_weights={"FTXL": 0.06, "CASH": 0.94},
            current_weights={"FTXL": 0.06, "CASH": 0.94},
            holdings_meta=[
                {"ticker": "FTXL", "universe_role": "satellite", "unrealized_pnl_pct": -0.06, "atr_pct": 0.018},
            ],
            strategy_evidence={
                "strategy_results": [
                    {"strategy_name": "momentum_lite_v1", "suggested_use": "advisory", "selected_tickers": ["FTXL"]}
                ]
            },
            market_scorecard={"investment_permission": "small_overweight_only"},
            news_evidence={},
        )

        explanation = _explanation(out, "FTXL")
        self.assertIn("only advisory strategy support remains", explanation["why_hold"])
        self.assertNotIn("strategy support remains advisory", explanation["why_hold"])
        self.assertIn("advisory support is not strong enough to justify adding", explanation["why_not_add"])

    def test_human_required_risk_reducing_trim_becomes_manual_hint(self):
        out = apply_position_governance(
            target_weights={"PSI": 0.04, "CASH": 0.96},
            current_weights={"PSI": 0.04, "CASH": 0.96},
            holdings_meta=[
                {"ticker": "PSI", "universe_role": "satellite", "unrealized_pnl_pct": -0.10, "atr_pct": 0.018},
            ],
            strategy_evidence={"strategy_results": []},
            market_scorecard={
                "investment_permission": "normal_rebalance",
                "require_human_confirmation": True,
            },
            news_evidence={},
        )

        self.assertEqual(out.manual_action_hints[0]["ticker"], "PSI")
        self.assertEqual(out.manual_action_hints[0]["suggested_action"], "manual_trim_review")

    def test_thesis_status_broken_for_hard_risk_and_has_no_execution_authority(self):
        out = apply_position_governance(
            target_weights={"XLE": 0.09, "CASH": 0.91},
            current_weights={"XLE": 0.09, "CASH": 0.91},
            holdings_meta=[
                {"ticker": "XLE", "unrealized_pnl_pct": 0.03, "atr_pct": 0.018},
            ],
            strategy_evidence={
                "evidence_summary": {"live_fit": "conflicted"},
                "strategy_results": [
                    {"strategy_name": "momentum_lite_v1", "suggested_use": "advisory", "selected_tickers": ["XLE"]}
                ],
            },
            market_scorecard={"investment_permission": "small_overweight_only"},
            news_evidence={"hard_risk_events": {"XLE": ["oil_shock"]}},
        )

        thesis = _decision(out, "XLE")["thesis_status"]
        self.assertEqual(thesis["status"], "broken")
        self.assertIn("hard_risk_event", thesis["evidence"])
        self.assertEqual(thesis["execution_authority"], "none")

    def test_llm_thesis_status_is_overridden_when_evidence_conflicts(self):
        out = apply_position_governance(
            target_weights={"SPY": 0.10, "CASH": 0.90},
            current_weights={"SPY": 0.10, "CASH": 0.90},
            holdings_meta=[
                {"ticker": "SPY", "universe_role": "core", "unrealized_pnl_pct": 0.02, "atr_pct": 0.012},
            ],
            strategy_evidence={
                "strategy_results": [
                    {"strategy_name": "momentum_lite_v1", "suggested_use": "advisory", "selected_tickers": ["SPY"]}
                ],
            },
            market_scorecard={"investment_permission": "normal_rebalance"},
            news_evidence={},
            llm_advisory_proposals=[
                {"ticker": "SPY", "llm_advisory": "hold", "thesis_status": "broken", "reason": "unsupported"}
            ],
        )

        thesis = _decision(out, "SPY")["thesis_status"]
        self.assertEqual(thesis["status"], "intact")
        self.assertEqual(thesis["llm_status"], "broken")
        self.assertTrue(thesis["llm_validator_result"].startswith("overridden_by_validator"))

    def test_llm_thesis_status_without_action_cannot_change_position(self):
        out = apply_position_governance(
            target_weights={"SPY": 0.10, "CASH": 0.90},
            current_weights={"SPY": 0.10, "CASH": 0.90},
            holdings_meta=[
                {"ticker": "SPY", "universe_role": "core", "unrealized_pnl_pct": 0.02, "atr_pct": 0.012},
            ],
            strategy_evidence={
                "strategy_results": [
                    {"strategy_name": "momentum_lite_v1", "suggested_use": "advisory", "selected_tickers": ["SPY"]}
                ],
            },
            market_scorecard={"investment_permission": "normal_rebalance"},
            news_evidence={},
            llm_advisory_proposals=[
                {"ticker": "SPY", "thesis_status": "broken", "reason": "narrative only"}
            ],
        )

        decision = _decision(out, "SPY")
        thesis = decision["thesis_status"]
        self.assertEqual(thesis["status"], "intact")
        self.assertEqual(thesis["execution_authority"], "none")
        self.assertEqual(decision["decision"], "hold")
        self.assertEqual(decision["target_after"], 0.10)
        self.assertFalse(out.forced_trims)

    def test_accepted_llm_thesis_status_still_has_no_execution_authority(self):
        out = apply_position_governance(
            target_weights={"URA": 0.005, "CASH": 0.995},
            current_weights={"URA": 0.005, "CASH": 0.995},
            holdings_meta=[
                {"ticker": "URA", "universe_role": "satellite", "unrealized_pnl_pct": 0.0, "atr_pct": 0.012},
            ],
            strategy_evidence={"strategy_results": []},
            market_scorecard={"investment_permission": "normal_rebalance"},
            news_evidence={},
            llm_advisory_proposals=[
                {"ticker": "URA", "thesis_status": "broken", "reason": "research concern without trade action"}
            ],
        )

        decision = _decision(out, "URA")
        thesis = decision["thesis_status"]
        self.assertEqual(thesis["status"], "broken")
        self.assertEqual(thesis["llm_validator_result"], "accepted")
        self.assertEqual(thesis["execution_authority"], "none")
        self.assertEqual(decision["decision"], "hold")
        self.assertEqual(decision["target_after"], 0.005)
        self.assertFalse(out.forced_trims)

    def test_thesis_summary_tracks_problem_tickers(self):
        out = apply_position_governance(
            target_weights={"FTXL": 0.06, "SOXX": 0.06, "CASH": 0.88},
            current_weights={"FTXL": 0.06, "SOXX": 0.06, "CASH": 0.88},
            holdings_meta=[
                {"ticker": "FTXL", "universe_role": "satellite", "unrealized_pnl_pct": -0.05, "atr_pct": 0.018},
                {"ticker": "SOXX", "universe_role": "satellite", "unrealized_pnl_pct": -0.06, "atr_pct": 0.018},
            ],
            strategy_evidence={"strategy_results": []},
            market_scorecard={"investment_permission": "small_overweight_only"},
            news_evidence={},
        )

        summary = out.portfolio_summary["thesis_status_summary"]
        self.assertGreaterEqual(summary["counts"]["weakening"], 2)
        self.assertEqual(summary["execution_authority"], "none")

    def test_advisory_basket_loss_escalates_to_manual_trim_review_without_auto_trim(self):
        out = apply_position_governance(
            target_weights={"FTXL": 0.06, "SOXX": 0.06, "CASH": 0.88},
            current_weights={"FTXL": 0.06, "SOXX": 0.06, "CASH": 0.88},
            holdings_meta=[
                {"ticker": "FTXL", "universe_role": "satellite", "unrealized_pnl_pct": -0.065, "atr_pct": 0.018},
                {"ticker": "SOXX", "universe_role": "satellite", "unrealized_pnl_pct": -0.068, "atr_pct": 0.018},
            ],
            strategy_evidence={
                "strategy_results": [
                    {"strategy_name": "momentum_lite_v1", "suggested_use": "advisory", "selected_tickers": ["FTXL", "SOXX"]}
                ]
            },
            market_scorecard={
                "investment_permission": "small_overweight_only",
                "require_human_confirmation": True,
            },
            news_evidence={},
        )

        decision = _decision(out, "FTXL")
        self.assertEqual(decision["decision"], "trim_review")
        self.assertAlmostEqual(decision["target_after"], 0.06, places=4)
        self.assertIn("advisory_basket_loss_review", decision["reason_codes"])
        hint = next(row for row in out.manual_action_hints if row["ticker"] == "FTXL")
        self.assertEqual(hint["suggested_action"], "manual_trim_review")
        self.assertAlmostEqual(hint["suggested_target"], 0.05, places=4)

    def test_core_etf_does_not_get_advisory_basket_loss_escalation(self):
        out = apply_position_governance(
            target_weights={"SPY": 0.06, "QQQ": 0.06, "CASH": 0.88},
            current_weights={"SPY": 0.06, "QQQ": 0.06, "CASH": 0.88},
            holdings_meta=[
                {"ticker": "SPY", "universe_role": "core", "unrealized_pnl_pct": -0.065, "atr_pct": 0.018},
                {"ticker": "QQQ", "universe_role": "core", "unrealized_pnl_pct": -0.068, "atr_pct": 0.018},
            ],
            strategy_evidence={
                "strategy_results": [
                    {"strategy_name": "momentum_lite_v1", "suggested_use": "advisory", "selected_tickers": ["SPY", "QQQ"]}
                ]
            },
            market_scorecard={
                "investment_permission": "small_overweight_only",
                "require_human_confirmation": True,
            },
            news_evidence={},
        )

        self.assertNotIn("advisory_basket_loss_review", _decision(out, "SPY")["reason_codes"])

    def test_replacement_allocates_trimmed_cash_to_supported_candidate_when_allowed(self):
        out = apply_position_governance(
            target_weights={"PSI": 0.04, "SPY": 0.10, "CASH": 0.86},
            current_weights={"PSI": 0.04, "SPY": 0.10, "CASH": 0.86},
            holdings_meta=[
                {"ticker": "PSI", "unrealized_pnl_pct": -0.10, "atr_pct": 0.018},
                {"ticker": "SPY", "unrealized_pnl_pct": 0.02, "atr_pct": 0.012},
            ],
            strategy_evidence={
                "strategy_results": [
                    {
                        "strategy_name": "momentum_lite_v1",
                        "suggested_use": "advisory",
                        "selected_tickers": ["SPY"],
                    }
                ]
            },
            market_scorecard={
                "investment_permission": "normal_rebalance",
                "require_human_confirmation": False,
            },
            news_evidence={},
            config={"replacement_max_single_pct": 0.02},
        )

        self.assertAlmostEqual(_decision(out, "PSI")["target_after"], 0.01, places=4)
        self.assertAlmostEqual(_decision(out, "SPY")["target_after"], 0.12, places=4)
        self.assertEqual(out.replacements[0]["ticker"], "SPY")
        self.assertEqual(out.trade_summary["replacements"], 1)
        self.assertIn("score", out.replacements[0])
        self.assertIn("why", out.replacements[0])

    def test_replacement_keeps_cash_when_human_confirmation_required(self):
        out = apply_position_governance(
            target_weights={"PSI": 0.04, "SPY": 0.10, "CASH": 0.86},
            current_weights={"PSI": 0.04, "SPY": 0.10, "CASH": 0.86},
            holdings_meta=[
                {"ticker": "PSI", "unrealized_pnl_pct": -0.10, "atr_pct": 0.018},
                {"ticker": "SPY", "unrealized_pnl_pct": 0.02, "atr_pct": 0.012},
            ],
            strategy_evidence={
                "strategy_results": [
                    {
                        "strategy_name": "momentum_lite_v1",
                        "suggested_use": "advisory",
                        "selected_tickers": ["SPY"],
                    }
                ]
            },
            market_scorecard={
                "investment_permission": "normal_rebalance",
                "require_human_confirmation": True,
            },
            news_evidence={},
        )

        self.assertEqual(out.replacements, [])
        self.assertAlmostEqual(_decision(out, "SPY")["target_after"], 0.10, places=4)

    def test_replacement_ranking_prefers_better_scored_candidate(self):
        out = apply_position_governance(
            target_weights={"PSI": 0.04, "SPY": 0.10, "QQQ": 0.10, "CASH": 0.76},
            current_weights={"PSI": 0.04, "SPY": 0.10, "QQQ": 0.10, "CASH": 0.76},
            holdings_meta=[
                {"ticker": "PSI", "unrealized_pnl_pct": -0.10, "atr_pct": 0.018},
                {"ticker": "SPY", "unrealized_pnl_pct": 0.01, "atr_pct": 0.012},
                {"ticker": "QQQ", "unrealized_pnl_pct": 0.01, "atr_pct": 0.025},
            ],
            strategy_evidence={
                "strategy_results": [
                    {
                        "strategy_name": "weak_first",
                        "suggested_use": "advisory",
                        "confidence_score": 0.35,
                        "selected_tickers": ["QQQ"],
                    },
                    {
                        "strategy_name": "strong_second",
                        "suggested_use": "advisory",
                        "confidence_score": 0.80,
                        "selected_tickers": ["SPY"],
                    },
                ]
            },
            market_scorecard={
                "investment_permission": "normal_rebalance",
                "require_human_confirmation": False,
            },
            news_evidence={},
            config={"replacement_max_single_pct": 0.02},
        )

        self.assertEqual(out.replacements[0]["ticker"], "SPY")
        self.assertGreater(out.replacements[0]["score"], 0.5)
        self.assertIn("high_strategy_confidence", out.replacements[0]["why"])
        candidates = out.portfolio_summary["replacement_candidates"]
        self.assertEqual(candidates[0]["ticker"], "SPY")

    def test_risk_contribution_flags_small_high_vol_position(self):
        out = apply_position_governance(
            target_weights={"SOXL": 0.04, "CASH": 0.96},
            current_weights={"SOXL": 0.04, "CASH": 0.96},
            holdings_meta=[
                {"ticker": "SOXL", "unrealized_pnl_pct": 0.01, "atr_pct": 0.09},
            ],
            strategy_evidence={
                "strategy_results": [
                    {
                        "strategy_name": "momentum_lite_v1",
                        "suggested_use": "advisory",
                        "selected_tickers": ["SOXL"],
                    }
                ]
            },
            market_scorecard={"investment_permission": "small_overweight_only"},
            news_evidence={},
        )

        decision = _decision(out, "SOXL")
        self.assertEqual(decision["risk_budget_status"], "high")
        self.assertAlmostEqual(decision["raw_risk_contribution"], 0.0036, places=5)
        self.assertEqual(out.portfolio_summary["top_risk_contributors"][0]["ticker"], "SOXL")

    def test_large_low_vol_position_does_not_become_high_risk_contribution(self):
        out = apply_position_governance(
            target_weights={"BND": 0.16, "CASH": 0.84},
            current_weights={"BND": 0.16, "CASH": 0.84},
            holdings_meta=[
                {"ticker": "BND", "unrealized_pnl_pct": 0.02, "atr_pct": 0.005},
            ],
            strategy_evidence={
                "strategy_results": [
                    {
                        "strategy_name": "low_vol_factor",
                        "suggested_use": "advisory",
                        "selected_tickers": ["BND"],
                    }
                ]
            },
            market_scorecard={"investment_permission": "small_overweight_only"},
            news_evidence={},
        )

        decision = _decision(out, "BND")
        self.assertEqual(decision["risk_budget_status"], "normal")
        self.assertNotIn("winner_risk_budget_review", decision["reason_codes"])
        self.assertAlmostEqual(decision["risk_contribution"], 0.0008, places=5)

    def test_llm_advisory_trim_is_clipped_and_logged(self):
        out = apply_position_governance(
            target_weights={"QQQ": 0.12, "CASH": 0.88},
            current_weights={"QQQ": 0.12, "CASH": 0.88},
            holdings_meta=[
                {"ticker": "QQQ", "unrealized_pnl_pct": 0.03, "atr_pct": 0.02},
            ],
            strategy_evidence={
                "strategy_results": [
                    {
                        "strategy_name": "momentum_lite_v1",
                        "suggested_use": "advisory",
                        "selected_tickers": ["QQQ"],
                    }
                ]
            },
            market_scorecard={
                "investment_permission": "normal_rebalance",
                "require_human_confirmation": False,
            },
            news_evidence={},
            llm_advisory_proposals=[
                {
                    "ticker": "QQQ",
                    "llm_advisory": "trim",
                    "target_weight": 0.05,
                    "reason": "live consensus weakened",
                }
            ],
            config={"replacement_enabled": 0},
        )

        decision = _decision(out, "QQQ")
        self.assertEqual(decision["decision"], "trim")
        self.assertAlmostEqual(decision["target_after"], 0.11, places=4)
        self.assertIn("llm_advisory_validated", decision["reason_codes"])
        self.assertEqual(out.trade_summary["advisory_overrides"], 1)
        self.assertIn("accepted_as_trim_1.00%", out.advisory_overrides[0]["validator_result"])
        quality = out.portfolio_summary["advisory_quality"]["current_run"]
        self.assertEqual(quality["accepted"], 1)
        self.assertEqual(quality["accepted_tickers"], ["QQQ"])

    def test_llm_advisory_add_rejected_when_human_required(self):
        out = apply_position_governance(
            target_weights={"SPY": 0.10, "CASH": 0.90},
            current_weights={"SPY": 0.10, "CASH": 0.90},
            holdings_meta=[
                {"ticker": "SPY", "unrealized_pnl_pct": 0.01, "atr_pct": 0.012},
            ],
            strategy_evidence={
                "strategy_results": [
                    {
                        "strategy_name": "momentum_lite_v1",
                        "suggested_use": "advisory",
                        "selected_tickers": ["SPY"],
                    }
                ]
            },
            market_scorecard={
                "investment_permission": "normal_rebalance",
                "require_human_confirmation": True,
            },
            news_evidence={},
            llm_advisory_proposals=[
                {"ticker": "SPY", "llm_advisory": "add", "target_weight": 0.12}
            ],
        )

        self.assertAlmostEqual(_decision(out, "SPY")["target_after"], 0.10, places=4)
        self.assertEqual(out.advisory_overrides[0]["validator_result"], "rejected_human_required_add")
        self.assertTrue(any(item.startswith("llm_advisory_rejected:SPY") for item in out.blocked_actions))

    def test_llm_exit_without_exit_permission_converts_to_review(self):
        out = apply_position_governance(
            target_weights={"SPY": 0.10, "CASH": 0.90},
            current_weights={"SPY": 0.10, "CASH": 0.90},
            holdings_meta=[
                {"ticker": "SPY", "unrealized_pnl_pct": 0.01, "atr_pct": 0.012},
            ],
            strategy_evidence={
                "strategy_results": [
                    {
                        "strategy_name": "momentum_lite_v1",
                        "suggested_use": "advisory",
                        "selected_tickers": ["SPY"],
                    }
                ]
            },
            market_scorecard={"investment_permission": "normal_rebalance"},
            news_evidence={},
            llm_advisory_proposals=[
                {"ticker": "SPY", "llm_advisory": "exit", "reason": "narrative concern"}
            ],
        )

        decision = _decision(out, "SPY")
        self.assertEqual(decision["decision"], "hold_review")
        self.assertAlmostEqual(decision["target_after"], 0.10, places=4)
        self.assertEqual(out.advisory_overrides[0]["validator_result"], "converted_exit_to_hold_review")


def _decision(out, ticker):
    return next(row for row in out.position_decisions if row["ticker"] == ticker)


def _explanation(out, ticker):
    return next(row for row in out.portfolio_summary["position_explanations"] if row["ticker"] == ticker)


if __name__ == "__main__":
    unittest.main()
