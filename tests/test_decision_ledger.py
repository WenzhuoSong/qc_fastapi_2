import unittest

from services.decision_ledger import (
    apply_execution_audit_to_decision_ledger,
    build_decision_ledger,
)


class DecisionLedgerTests(unittest.TestCase):
    def test_risk_rejected_sets_final_action_none_and_not_sent(self):
        ledger = build_decision_ledger(
            risk_output={
                "approved": False,
                "target_weights": {"QQQ": 0.09, "CASH": 0.91},
                "rebalance_actions": [
                    {
                        "ticker": "QQQ",
                        "action": "sell",
                        "weight_current": 0.12,
                        "weight_target": 0.09,
                        "weight_delta": -0.03,
                    }
                ],
                "failed_checks": {"human_confirmation_ok": {"pass": False}},
                "rejection_reasons": [
                    "Market scorecard requires human confirmation; FULL_AUTO execution blocked"
                ],
                "position_governance": _governance(
                    decision="trim_review",
                    reason_codes=["scorecard_human_required"],
                ),
            },
            current_holdings={"current_weights": {"QQQ": 0.12, "CASH": 0.88}},
        )

        row = ledger["tickers"]["QQQ"]
        self.assertEqual(row["proposed_action"], "trim")
        self.assertEqual(row["final_action"], "none")
        self.assertEqual(row["execution_status"], "not_sent")
        self.assertIn("risk_rejected", row["reason_codes"])
        self.assertIn("human_required", row["reason_codes"])
        self.assertIn("scorecard_human_required", row["reason_codes"])
        self.assertIn("risk_rejected", row["source_effects"]["risk"])
        self.assertIn("scorecard_human_required", row["source_effects"]["scorecard"])
        self.assertEqual(ledger["portfolio_summary"]["execution_status"], "not_sent")

    def test_missing_position_governance_warns_without_fallback_inference(self):
        ledger = build_decision_ledger(
            risk_output={
                "approved": True,
                "target_weights": {"QQQ": 0.15, "CASH": 0.85},
                "rebalance_actions": [
                    {"ticker": "QQQ", "action": "buy", "weight_delta": 0.03}
                ],
            },
            current_holdings={"QQQ": 0.12, "CASH": 0.88},
        )

        row = ledger["tickers"]["QQQ"]
        self.assertIn("position_governance_missing", ledger["warnings"])
        self.assertFalse(row["governance_available"])
        self.assertEqual(row["final_action"], "unknown")
        self.assertIn("position_governance_missing", row["reason_codes"])
        self.assertIsNone(row["evidence_used"]["position_governance"])
        self.assertIn("position_governance_missing", row["source_effects"]["risk"])

    def test_holding_without_proposed_trade_gets_hold_row_and_explanation_reference(self):
        explanation = {
            "ticker": "XLK",
            "position_state": "normal_hold",
            "why_hold": ["no deterministic rule requires reduction"],
            "why_not_add": ["add is allowed within risk limits"],
            "why_not_exit": ["no hard-risk event is active"],
            "next_trigger": "continue monitoring",
        }
        ledger = build_decision_ledger(
            risk_output={
                "approved": True,
                "target_weights": {"XLK": 0.10, "CASH": 0.90},
                "rebalance_actions": [],
                "position_governance": _governance(
                    ticker="XLK",
                    decision="hold",
                    reason_codes=[],
                    explanation=explanation,
                ),
            },
            current_holdings={
                "current_weights": {"XLK": 0.10, "CASH": 0.90},
                "holdings": [
                    {
                        "ticker": "XLK",
                        "quantity": 81,
                        "unrealized_pnl_pct": 0.107,
                        "holding_days": 24,
                    }
                ],
            },
        )

        row = ledger["tickers"]["XLK"]
        self.assertEqual(row["proposed_action"], "hold")
        self.assertEqual(row["final_action"], "hold")
        self.assertEqual(row["current"]["quantity"], 81)
        self.assertEqual(row["current"]["unrealized_pnl_pct"], 0.107)
        self.assertIs(row["explanation"], explanation)
        self.assertTrue(row["trade_lifecycle"]["is_sparse"])
        self.assertEqual(row["trade_lifecycle"]["current_weight"], 0.10)
        self.assertEqual(row["trade_lifecycle"]["risk_target"], 0.10)
        self.assertEqual(row["trade_lifecycle"]["governance_target"], 0.12)

    def test_every_proposed_trade_ticker_gets_row_even_when_not_current_holding(self):
        ledger = build_decision_ledger(
            risk_output={
                "approved": True,
                "target_weights": {"SPY": 0.05, "CASH": 0.95},
                "rebalance_actions": [
                    {"ticker": "SPY", "action": "buy", "weight_delta": 0.05}
                ],
                "position_governance": _governance(
                    ticker="SPY",
                    decision="add",
                    reason_codes=["replacement_candidate"],
                ),
            },
            current_holdings={"CASH": 1.0},
        )

        row = ledger["tickers"]["SPY"]
        self.assertEqual(row["current"]["weight"], 0.0)
        self.assertEqual(row["proposed_action"], "add")
        self.assertEqual(row["final_action"], "add")
        self.assertIn("replacement_candidate", row["reason_codes"])

    def test_preserves_placeholders_for_unwired_later_evidence(self):
        ledger = build_decision_ledger(
            risk_output={
                "approved": True,
                "target_weights": {"QQQ": 0.12, "CASH": 0.88},
                "position_governance": _governance(),
            },
            current_holdings={"QQQ": 0.12, "CASH": 0.88},
        )

        self.assertIsNone(ledger["placeholders"]["execution_audit"])
        self.assertEqual(ledger["tickers"]["QQQ"]["placeholders"]["etf_historical"], "missing")
        self.assertIsNone(ledger["tickers"]["QQQ"]["evidence_used"]["historical"])
        self.assertEqual(
            ledger["placeholders"]["full_trade_lifecycle"],
            "base_strategy_synthesizer_available_when_sources_present",
        )

    def test_hydrates_historical_evidence_from_empirical_profile(self):
        ledger = build_decision_ledger(
            evidence_bundle={
                "knowledge": {
                    "resolution": {
                        "advisory_context": [
                            {
                                "type": "asset_profile",
                                "id": "QQQ",
                                "empirical_behavior": {
                                    "source": "yfinance",
                                    "generated_at": "2026-05-18T12:00:00+00:00",
                                    "lookback_days": 290,
                                    "samples": 289,
                                    "latest_date": "2026-05-15",
                                    "avg_return": 0.001,
                                    "volatility": 0.012,
                                    "max_drawdown": -0.08,
                                    "benchmark_correlation": 0.91,
                                    "data_quality": "fresh",
                                },
                            }
                        ]
                    }
                }
            },
            risk_output={
                "approved": True,
                "target_weights": {"QQQ": 0.12, "CASH": 0.88},
                "position_governance": _governance(),
            },
            current_holdings={"QQQ": 0.12, "CASH": 0.88},
        )

        historical = ledger["tickers"]["QQQ"]["evidence_used"]["historical"]
        self.assertEqual(historical["source"], "yfinance")
        self.assertEqual(historical["samples"], 289)
        self.assertEqual(historical["source_state"], "fresh")
        self.assertEqual(historical["freshness"]["policy"], "empirical_profile_provider")
        self.assertFalse(historical["freshness"]["is_stale"])
        self.assertIsNone(ledger["tickers"]["QQQ"]["placeholders"]["etf_historical"])
        self.assertEqual(ledger["tickers"]["QQQ"]["source_effects"]["yfinance"], ["empirical_profile_available"])

    def test_hydrates_intraday_evidence_from_current_holding_row_without_freshness_rejudgment(self):
        ledger = build_decision_ledger(
            risk_output={
                "approved": True,
                "target_weights": {"XLK": 0.10, "CASH": 0.90},
                "position_governance": _governance(ticker="XLK"),
            },
            current_holdings={
                "current_weights": {"XLK": 0.10, "CASH": 0.90},
                "holdings": [
                    {
                        "ticker": "XLK",
                        "price": 175.74,
                        "atr_pct": 0.018,
                        "mom_20d": 0.04,
                        "feature_sources": [
                            {
                                "source": "qc_heartbeat",
                                "filled_fields": ["price", "weight_current"],
                                "as_of": "2026-05-18T13:31:00+00:00",
                            }
                        ],
                    }
                ],
            },
        )

        intraday = ledger["tickers"]["XLK"]["evidence_used"]["intraday"]
        self.assertEqual(intraday["source"], "current_holdings")
        self.assertEqual(intraday["fields"]["price"], 175.74)
        self.assertEqual(intraday["fields"]["atr_pct"], 0.018)
        self.assertEqual(intraday["source_state"], "available")
        self.assertEqual(intraday["freshness"][0]["policy"], "upstream_feature_provenance")
        self.assertIsNone(intraday["freshness"][0]["is_stale"])
        self.assertIsNone(ledger["tickers"]["XLK"]["placeholders"]["etf_intraday"])
        self.assertEqual(ledger["tickers"]["XLK"]["source_effects"]["qc"], ["current_holdings_available"])

    def test_source_effects_use_static_reason_code_mapping(self):
        ledger = build_decision_ledger(
            risk_output={
                "approved": False,
                "target_weights": {"FTXL": 0.02, "CASH": 0.98},
                "rebalance_actions": [
                    {"ticker": "FTXL", "action": "sell", "weight_delta": -0.01}
                ],
                "rejection_reasons": [
                    "Market scorecard requires human confirmation; FULL_AUTO execution blocked"
                ],
                "position_governance": _governance(
                    ticker="FTXL",
                    decision="trim_review",
                    reason_codes=[
                        "unrealized_loss_review",
                        "basket_review",
                        "advisory_basket_loss_review",
                        "hard_risk",
                        "scorecard_human_required",
                    ],
                    target_after=0.03,
                ),
            },
            current_holdings={
                "current_weights": {"FTXL": 0.03, "CASH": 0.97},
                "holdings": [
                    {
                        "ticker": "FTXL",
                        "unrealized_pnl_pct": -0.066,
                        "feature_sources": [
                            {"source": "qc_heartbeat", "filled_fields": ["price"]}
                        ],
                    }
                ],
            },
        )

        effects = ledger["tickers"]["FTXL"]["source_effects"]
        self.assertIn("unrealized_loss_review", effects["qc"])
        self.assertIn("current_holdings_available", effects["qc"])
        self.assertIn("correlated_basket_review", effects["knowledge"])
        self.assertIn("satellite_basket_loss_review", effects["knowledge"])
        self.assertIn("hard_risk", effects["news"])
        self.assertIn("scorecard_human_required", effects["scorecard"])
        self.assertIn("advisory_or_weaker_support", effects["strategy"])
        self.assertIn("risk_rejected", effects["risk"])

    def test_source_effects_are_repeatable_for_same_input(self):
        payload = {
            "risk_output": {
                "approved": False,
                "target_weights": {"QQQ": 0.09, "CASH": 0.91},
                "rejection_reasons": ["turnover exceeds limit"],
                "position_governance": _governance(
                    ticker="QQQ",
                    decision="trim_review",
                    reason_codes=["winner_risk_budget_review", "scorecard_human_required"],
                ),
            },
            "current_holdings": {"QQQ": 0.12, "CASH": 0.88},
        }

        first = build_decision_ledger(**payload)["tickers"]["QQQ"]["source_effects"]
        second = build_decision_ledger(**payload)["tickers"]["QQQ"]["source_effects"]

        self.assertEqual(first, second)
        self.assertEqual(first["qc"], ["winner_risk_budget_review"])
        self.assertIn("turnover_limit", first["risk"])

    def test_sparse_trade_lifecycle_uses_current_weight_as_final_when_risk_rejected(self):
        ledger = build_decision_ledger(
            risk_output={
                "approved": False,
                "target_weights": {"QQQ": 0.09, "CASH": 0.91},
                "rebalance_actions": [
                    {"ticker": "QQQ", "action": "sell", "weight_delta": -0.03}
                ],
                "position_governance": _governance(
                    ticker="QQQ",
                    decision="trim_review",
                    reason_codes=["scorecard_human_required"],
                ),
            },
            current_holdings={"QQQ": 0.12, "CASH": 0.88},
        )

        lifecycle = ledger["tickers"]["QQQ"]["trade_lifecycle"]
        self.assertEqual(lifecycle["current_weight"], 0.12)
        self.assertEqual(lifecycle["risk_target"], 0.09)
        self.assertEqual(lifecycle["final_target"], 0.12)
        self.assertIn("risk_rejected_final_target_current", lifecycle["changed_by"])

    def test_sparse_trade_lifecycle_marks_governance_change(self):
        ledger = build_decision_ledger(
            risk_output={
                "approved": True,
                "target_weights": {"QQQ": 0.15, "CASH": 0.85},
                "position_governance": _governance(
                    ticker="QQQ",
                    decision="trim_review",
                    target_after=0.12,
                ),
            },
            current_holdings={"QQQ": 0.10, "CASH": 0.90},
        )

        lifecycle = ledger["tickers"]["QQQ"]["trade_lifecycle"]
        self.assertEqual(lifecycle["current_weight"], 0.10)
        self.assertEqual(lifecycle["risk_target"], 0.15)
        self.assertEqual(lifecycle["governance_target"], 0.12)
        self.assertEqual(lifecycle["final_target"], 0.12)
        self.assertIn("risk_target", lifecycle["changed_by"])
        self.assertIn("position_governance", lifecycle["changed_by"])

    def test_trade_lifecycle_includes_base_strategy_and_synthesizer_when_available(self):
        ledger = build_decision_ledger(
            evidence_bundle={
                "strategies": {
                    "consensus_weights": {"QQQ": 0.11, "CASH": 0.89},
                }
            },
            strategy_output={
                "base_weights": {"QQQ": 0.10, "CASH": 0.90},
            },
            synthesizer_output={
                "adjusted_weights": {"QQQ": 0.13, "CASH": 0.87},
            },
            risk_output={
                "approved": True,
                "target_weights": {"QQQ": 0.12, "CASH": 0.88},
                "position_governance": _governance(
                    ticker="QQQ",
                    decision="hold",
                    target_after=0.12,
                ),
            },
            current_holdings={"QQQ": 0.09, "CASH": 0.91},
        )

        lifecycle = ledger["tickers"]["QQQ"]["trade_lifecycle"]
        self.assertEqual(lifecycle["current_weight"], 0.09)
        self.assertEqual(lifecycle["base_weight"], 0.10)
        self.assertEqual(lifecycle["strategy_target"], 0.11)
        self.assertEqual(lifecycle["synthesizer_target"], 0.13)
        self.assertEqual(lifecycle["risk_target"], 0.12)
        self.assertEqual(lifecycle["final_target"], 0.12)
        self.assertIn("base_weight", lifecycle["changed_by"])
        self.assertIn("strategy_target", lifecycle["changed_by"])
        self.assertIn("synthesizer_target", lifecycle["changed_by"])

    def test_ledger_records_validated_llm_advisory_without_reason_codes(self):
        ledger = build_decision_ledger(
            synthesizer_output={
                "adjusted_weights": {"QQQ": 0.13, "CASH": 0.87},
                "position_advisory_proposals": [
                    {
                        "ticker": "QQQ",
                        "llm_advisory": "trim",
                        "target_weight": 0.11,
                        "reason_codes": ["llm_must_not_leak"],
                    }
                ],
            },
            risk_output={
                "approved": True,
                "target_weights": {"QQQ": 0.12, "CASH": 0.88},
                "position_governance": _governance(
                    ticker="QQQ",
                    decision="trim_review",
                    target_after=0.11,
                    advisory_overrides=[
                        {
                            "ticker": "QQQ",
                            "llm_advisory": "trim",
                            "validator_result": "accepted_as_trim_1.00%",
                            "deterministic_decision": "hold_review",
                            "final_decision": "trim_review",
                            "target_before_override": 0.12,
                            "target_after_override": 0.11,
                        }
                    ],
                ),
            },
            current_holdings={"QQQ": 0.12, "CASH": 0.88},
        )

        row = ledger["tickers"]["QQQ"]
        self.assertEqual(row["llm_advisory"]["llm_advisory"], "trim")
        self.assertEqual(row["llm_advisory"]["validator_result"], "accepted_as_trim_1.00%")
        self.assertEqual(row["llm_advisory"]["validated_delta"], -0.01)
        self.assertEqual(row["llm_advisory"]["execution_authority"], "none")
        self.assertEqual(row["trade_lifecycle"]["validated_advisory_delta"], -0.01)
        self.assertIn("validated_llm_advisory", row["trade_lifecycle"]["changed_by"])
        self.assertNotIn("llm_must_not_leak", row["reason_codes"])
        self.assertEqual(ledger["position_advisory_overrides"][0]["validator_result"], "accepted_as_trim_1.00%")

    def test_trade_lifecycle_includes_target_builder_target_when_available(self):
        ledger = build_decision_ledger(
            risk_output={
                "approved": True,
                "target_construction_mode": "target_builder_gated",
                "raw_llm_adjusted_weights_consumed": False,
                "target_builder_input": {
                    "target_weights": {"QQQ": 0.11, "CASH": 0.89},
                },
                "target_weights": {"QQQ": 0.11, "CASH": 0.89},
                "position_governance": _governance(
                    ticker="QQQ",
                    decision="trim_review",
                    target_after=0.11,
                ),
            },
            strategy_output={"base_weights": {"QQQ": 0.12, "CASH": 0.88}},
            current_holdings={"QQQ": 0.12, "CASH": 0.88},
        )

        row = ledger["tickers"]["QQQ"]
        self.assertEqual(ledger["portfolio_summary"]["target_construction_mode"], "target_builder_gated")
        self.assertFalse(ledger["portfolio_summary"]["raw_llm_adjusted_weights_consumed"])
        self.assertEqual(row["trade_lifecycle"]["target_builder_target"], 0.11)
        self.assertIn("target_builder_target", row["trade_lifecycle"]["changed_by"])
        self.assertIn("target_builder_target", row["source_effects"]["risk"])

    def test_trade_lifecycle_includes_portfolio_construction_shadow_when_available(self):
        ledger = build_decision_ledger(
            risk_output={
                "approved": True,
                "target_construction_mode": "target_builder_gated",
                "raw_llm_adjusted_weights_consumed": False,
                "portfolio_construction_shadow": {
                    "target_weights": {"QQQ": 0.10, "CASH": 0.90},
                    "factor_exposures": {"tech_growth": 0.10},
                    "effective_n": 10.0,
                    "turnover": {"estimated": 0.02, "within_budget": True},
                    "violations": [],
                    "diagnostics": {
                        "mode": "portfolio_construction",
                        "active_basket_reviews": ["semiconductors"],
                    },
                },
                "target_builder_input": {
                    "target_weights": {"QQQ": 0.11, "CASH": 0.89},
                },
                "target_weights": {"QQQ": 0.11, "CASH": 0.89},
                "position_governance": _governance(
                    ticker="QQQ",
                    decision="trim_review",
                    target_after=0.11,
                ),
            },
            strategy_output={"base_weights": {"QQQ": 0.12, "CASH": 0.88}},
            current_holdings={"QQQ": 0.12, "CASH": 0.88},
        )

        row = ledger["tickers"]["QQQ"]
        portfolio_construction = ledger["portfolio_summary"]["portfolio_construction"]
        self.assertEqual(portfolio_construction["mode"], "portfolio_construction")
        self.assertEqual(portfolio_construction["execution_effect"], "diagnostic_only")
        self.assertEqual(portfolio_construction["active_basket_reviews"], ["semiconductors"])
        self.assertEqual(row["trade_lifecycle"]["portfolio_construction_target"], 0.10)
        self.assertIn("portfolio_construction_target", row["trade_lifecycle"]["changed_by"])
        self.assertIn("portfolio_construction_target", row["source_effects"]["risk"])

    def test_execution_audit_attaches_accepted_status_without_changing_final_action(self):
        ledger = build_decision_ledger(
            risk_output={
                "approved": True,
                "target_weights": {"QQQ": 0.15, "CASH": 0.85},
                "rebalance_actions": [
                    {"ticker": "QQQ", "action": "buy", "weight_delta": 0.03}
                ],
                "position_governance": _governance(
                    ticker="QQQ",
                    decision="add",
                    target_after=0.15,
                ),
            },
            current_holdings={"QQQ": 0.12, "CASH": 0.88},
        )

        updated = apply_execution_audit_to_decision_ledger(
            ledger,
            {
                "action_status": "accepted",
                "sent_weights": {"QQQ": 0.15, "CASH": 0.85},
                "rebalance_actions": [
                    {"ticker": "QQQ", "action": "buy", "weight_delta": 0.03}
                ],
                "command_id": "analysis_1",
                "recorded_at": "2026-05-18T13:30:00+00:00",
            },
        )

        row = updated["tickers"]["QQQ"]
        self.assertEqual(updated["portfolio_summary"]["execution_status"], "accepted")
        self.assertEqual(row["final_action"], "add")
        self.assertEqual(row["execution_status"], "accepted")
        self.assertEqual(row["actual_execution_action"], "add")
        self.assertEqual(row["execution_audit"]["command_id"], "analysis_1")
        self.assertEqual(updated["placeholders"]["execution_audit"], "hydrated")

    def test_execution_audit_rejected_sets_actual_execution_none(self):
        ledger = build_decision_ledger(
            risk_output={
                "approved": True,
                "target_weights": {"QQQ": 0.15, "CASH": 0.85},
                "rebalance_actions": [
                    {"ticker": "QQQ", "action": "buy", "weight_delta": 0.03}
                ],
                "position_governance": _governance(
                    ticker="QQQ",
                    decision="add",
                    target_after=0.15,
                ),
            },
            current_holdings={"QQQ": 0.12, "CASH": 0.88},
        )

        updated = apply_execution_audit_to_decision_ledger(
            ledger,
            {
                "action_status": "rejected",
                "proposed_weights": {"QQQ": 0.15, "CASH": 0.85},
                "reason": "aborted_no_token",
            },
        )

        row = updated["tickers"]["QQQ"]
        self.assertEqual(row["final_action"], "add")
        self.assertEqual(row["execution_status"], "rejected")
        self.assertEqual(row["actual_execution_action"], "none")
        self.assertEqual(row["execution_audit"]["reason"], "aborted_no_token")


def _governance(
    *,
    ticker="QQQ",
    decision="hold",
    reason_codes=None,
    explanation=None,
    target_after=0.12,
    advisory_overrides=None,
):
    reason_codes = reason_codes or []
    explanation = explanation or {
        "ticker": ticker,
        "why_hold": ["from governance"],
        "why_not_add": ["from governance"],
        "why_not_exit": ["from governance"],
        "next_trigger": "from governance",
    }
    return {
        "mode": "diagnostic_only",
        "position_decisions": [
            {
                "ticker": ticker,
                "decision": decision,
                "action_permission": "hold_or_add_or_trim",
                "allowed_actions": ["hold", "trim", "add"],
                "strategy_support": "advisory",
                "supporting_strategies": ["momentum_lite_v1"],
                "risk_rank": 1,
                "risk_budget_status": "medium",
                "current_weight": 0.12,
                "target_before": 0.12,
                "target_after": target_after,
                "reason_codes": reason_codes,
            }
        ],
        "portfolio_summary": {
            "position_explanations": [explanation],
        },
        "blocked_actions": [],
        "forced_trims": [],
        "replacements": [],
        "advisory_overrides": advisory_overrides or [],
    }


if __name__ == "__main__":
    unittest.main()
