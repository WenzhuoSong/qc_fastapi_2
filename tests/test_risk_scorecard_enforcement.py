import importlib
import sys
import unittest
from unittest.mock import patch


def _load_risk_manager_exports():
    db_tools = type(sys)("tools.db_tools")
    async def _token(_input):
        return {"approval_token": "test", "expires_at": "2099-01-01T00:00:00"}
    db_tools.tool_write_approval_token = _token

    with patch.dict(
        "sys.modules",
        {
            "tools": type(sys)("tools"),
            "tools.db_tools": db_tools,
        },
    ):
        module = importlib.import_module("agents.risk_manager")
        return (
            module.apply_scorecard_constraints,
            module.apply_style_constraints,
            module.run_risk_manager_async,
        )


apply_scorecard_constraints, apply_style_constraints, run_risk_manager_async = _load_risk_manager_exports()


class RiskScorecardEnforcementTest(unittest.TestCase):
    def test_cash_only_moves_all_equity_to_cash(self):
        out = apply_scorecard_constraints(
            target_weights={"SPY": 0.4, "QQQ": 0.2, "CASH": 0.4},
            base_weights={"SPY": 0.4, "QQQ": 0.2, "CASH": 0.4},
            market_scorecard={
                "investment_permission": "cash_only",
                "max_equity_weight": 0.0,
                "min_cash_weight": 1.0,
                "max_single_position": 0.0,
                "allow_new_positions": False,
            },
        )

        post = out["target_weights_post_scorecard_clip"]
        self.assertEqual(post, {"CASH": 1.0})
        self.assertTrue(out["post_clip_compliance"]["compliant"])
        self.assertTrue(any(v.startswith("cash_only:SPY") for v in out["violations"]))

    def test_max_equity_and_cash_floor_reduce_equity_to_cash(self):
        out = apply_scorecard_constraints(
            target_weights={"SPY": 0.5, "QQQ": 0.3, "CASH": 0.2},
            base_weights={"SPY": 0.45, "QQQ": 0.25, "CASH": 0.3},
            market_scorecard={
                "investment_permission": "defensive_only",
                "max_equity_weight": 0.6,
                "min_cash_weight": 0.4,
                "max_adjustment_from_base": 1.0,
                "max_single_position": 1.0,
                "allow_new_positions": True,
            },
        )

        post = out["target_weights_post_scorecard_clip"]
        equity = sum(v for k, v in post.items() if k != "CASH")
        self.assertLessEqual(equity, 0.6001)
        self.assertGreaterEqual(post["CASH"], 0.3999)
        self.assertTrue(any(v.startswith("max_equity:") for v in out["violations"]))
        self.assertTrue(out["post_clip_compliance"]["compliant"])

    def test_blocks_new_positions_when_not_allowed(self):
        out = apply_scorecard_constraints(
            target_weights={"SPY": 0.4, "NEW": 0.1, "CASH": 0.5},
            base_weights={"SPY": 0.4, "CASH": 0.6},
            market_scorecard={
                "investment_permission": "hold_or_trim",
                "max_equity_weight": 1.0,
                "min_cash_weight": 0.0,
                "max_adjustment_from_base": 1.0,
                "max_single_position": 1.0,
                "allow_new_positions": False,
            },
        )

        post = out["target_weights_post_scorecard_clip"]
        self.assertNotIn("NEW", post)
        self.assertAlmostEqual(post["CASH"], 0.6, places=4)
        self.assertTrue(any(v.startswith("new_position_blocked:NEW") for v in out["violations"]))
        self.assertTrue(out["post_clip_compliance"]["compliant"])

    def test_max_delta_clips_overweight_to_cash(self):
        out = apply_scorecard_constraints(
            target_weights={"SPY": 0.7, "CASH": 0.3},
            base_weights={"SPY": 0.5, "CASH": 0.5},
            market_scorecard={
                "investment_permission": "small_overweight_only",
                "max_adjustment_from_base": 0.03,
                "max_equity_weight": 1.0,
                "min_cash_weight": 0.0,
                "max_single_position": 1.0,
                "allow_new_positions": True,
            },
        )

        post = out["target_weights_post_scorecard_clip"]
        self.assertAlmostEqual(post["SPY"], 0.53, places=4)
        self.assertAlmostEqual(post["CASH"], 0.47, places=4)
        self.assertTrue(out["post_clip_compliance"]["compliant"])


class RiskStyleEnforcementTest(unittest.TestCase):
    def test_style_multiplier_tightens_scorecard_delta_and_adds_cash_floor(self):
        out = apply_style_constraints(
            target_weights={"SPY": 0.56, "CASH": 0.44},
            base_weights={"SPY": 0.50, "CASH": 0.50},
            current_weights={"SPY": 0.50, "CASH": 0.50},
            market_scorecard={
                "max_adjustment_from_base": 0.10,
                "min_cash_weight": 0.20,
            },
            decision_style={
                "analysis_style": "conservative",
                "trade_style": "step_in",
                "style_limits": {
                    "max_adjustment_multiplier": 0.5,
                    "min_cash_floor_addition": 0.10,
                },
            },
        )

        post = out["target_weights_post_style_clip"]
        self.assertAlmostEqual(post["SPY"], 0.55, places=4)
        self.assertGreaterEqual(post["CASH"], 0.30)
        self.assertTrue(any(v.startswith("style_max_delta:SPY") for v in out["violations"]))
        self.assertTrue(out["post_clip_compliance"]["compliant"])


class RiskManagerTargetBuilderGatedTest(unittest.IsolatedAsyncioTestCase):
    async def test_risk_manager_does_not_require_llm_adjusted_weights(self):
        out = await run_risk_manager_async(
            pipeline_context={
                "risk_params": {
                    "max_single_position": 1.0,
                    "max_broad_market": 1.0,
                    "min_cash_pct": 0.0,
                    "max_trade_cost_pct": 1.0,
                },
                "market_scorecard": {"investment_permission": "normal_rebalance"},
                "decision_style": {},
            },
            brief={
                "current_weights": {"SPY": 0.10, "CASH": 0.90},
                "holdings": [{"ticker": "SPY", "hist_vol_20d": 0.10}],
                "portfolio": {"current_drawdown_pct": 0.0},
            },
            quant_baseline={"base_weights": {"SPY": 0.10, "CASH": 0.90}},
            researcher_out={
                "market_judgment": {"regime": "neutral", "uncertainty_flag": False},
            },
        )

        self.assertEqual(out["target_construction_mode"], "deterministic_base_fallback")
        self.assertFalse(out["raw_llm_adjusted_weights_consumed"])
        self.assertEqual(out["target_weights"], {"SPY": 0.1, "CASH": 0.9})

    async def test_target_builder_gated_path_does_not_consume_raw_llm_weights(self):
        out = await run_risk_manager_async(
            pipeline_context={
                "risk_params": {
                    "target_builder_enabled": True,
                    "max_single_position": 1.0,
                    "max_broad_market": 1.0,
                    "min_cash_pct": 0.0,
                    "max_trade_cost_pct": 1.0,
                },
                "target_builder_gated": {
                    "target_weights": {"SPY": 0.20, "CASH": 0.80},
                    "diagnostics": {"mode": "gated"},
                },
                "market_scorecard": {"investment_permission": "normal_rebalance"},
                "decision_style": {},
            },
            brief={
                "current_weights": {"SPY": 0.10, "CASH": 0.90},
                "holdings": [{"ticker": "SPY", "hist_vol_20d": 0.10}],
                "portfolio": {"current_drawdown_pct": 0.0},
            },
            quant_baseline={"base_weights": {"SPY": 0.10, "CASH": 0.90}},
            researcher_out={
                "adjusted_weights": {"SPY": 0.80, "CASH": 0.20},
                "market_judgment": {"regime": "neutral", "uncertainty_flag": False},
            },
        )

        self.assertEqual(out["target_construction_mode"], "target_builder_gated")
        self.assertFalse(out["raw_llm_adjusted_weights_consumed"])
        self.assertEqual(out["target_weights"], {"SPY": 0.2, "CASH": 0.8})
        self.assertIn("target_builder_gated", out["overlays_applied"])
        self.assertEqual(out["scorecard_enforcement"]["mode"], "validation_only")
        self.assertEqual(out["style_enforcement"]["mode"], "validation_only")
        self.assertTrue(out["approved"])

    async def test_target_builder_gated_rejects_new_hard_risk_exposure(self):
        out = await run_risk_manager_async(
            pipeline_context={
                "risk_params": {
                    "target_builder_enabled": True,
                    "max_single_position": 1.0,
                    "max_broad_market": 1.0,
                    "min_cash_pct": 0.0,
                    "max_trade_cost_pct": 1.0,
                },
                "target_builder_gated": {
                    "target_weights": {"XLE": 0.10, "CASH": 0.90},
                },
                "market_scorecard": {"investment_permission": "normal_rebalance"},
                "decision_style": {},
            },
            brief={
                "current_weights": {"CASH": 1.0},
                "hard_risks_map": {"XLE": ["oil_shock"]},
                "holdings": [],
                "portfolio": {"current_drawdown_pct": 0.0},
            },
            quant_baseline={"base_weights": {"CASH": 1.0}},
            researcher_out={
                "adjusted_weights": {"XLE": 0.80, "CASH": 0.20},
                "market_judgment": {"regime": "neutral", "uncertainty_flag": False},
            },
        )

        self.assertFalse(out["approved"])
        self.assertFalse(out["quantitative_checks"]["target_builder_hard_risk_ok"]["pass"])
        self.assertIn("Target builder proposed new exposure to hard-risk tickers", " ".join(out["rejection_reasons"]))

    def test_style_blocks_new_positions_and_caps_new_buys(self):
        out = apply_style_constraints(
            target_weights={"AAA": 0.10, "BBB": 0.09, "CASH": 0.81},
            base_weights={"CASH": 1.0},
            current_weights={"CASH": 1.0},
            market_scorecard={"max_adjustment_from_base": 1.0, "min_cash_weight": 0.0},
            decision_style={
                "analysis_style": "macro_defensive",
                "trade_style": "risk_reduce_fast",
                "style_limits": {
                    "allow_new_positions": False,
                    "max_new_buys_per_cycle": 0,
                },
            },
        )

        post = out["target_weights_post_style_clip"]
        self.assertEqual(post, {"CASH": 1.0})
        self.assertTrue(any(v.startswith("style_new_position_blocked:AAA") for v in out["violations"]))
        self.assertTrue(out["post_clip_compliance"]["compliant"])

    def test_style_turnover_scales_toward_current(self):
        out = apply_style_constraints(
            target_weights={"AAA": 0.60, "CASH": 0.40},
            base_weights={"AAA": 0.0, "CASH": 1.0},
            current_weights={"AAA": 0.0, "CASH": 1.0},
            market_scorecard={"max_adjustment_from_base": 1.0, "min_cash_weight": 0.0},
            decision_style={
                "analysis_style": "low_turnover",
                "trade_style": "hold_unless_strong",
                "style_limits": {
                    "max_turnover_per_cycle": 0.20,
                    "max_single_trade_pct": 1.0,
                },
            },
        )

        post = out["target_weights_post_style_clip"]
        self.assertAlmostEqual(post["AAA"], 0.20, places=4)
        self.assertAlmostEqual(post["CASH"], 0.80, places=4)
        self.assertTrue(any(v.startswith("style_turnover_scaled:") for v in out["violations"]))
        self.assertTrue(out["post_clip_compliance"]["compliant"])

    def test_scorecard_turnover_limit_applies_without_style_limit(self):
        out = apply_style_constraints(
            target_weights={"AAA": 0.60, "CASH": 0.40},
            base_weights={"AAA": 0.0, "CASH": 1.0},
            current_weights={"AAA": 0.0, "CASH": 1.0},
            market_scorecard={
                "max_adjustment_from_base": 1.0,
                "min_cash_weight": 0.0,
                "max_turnover_per_cycle": 0.10,
            },
            decision_style={},
        )

        post = out["target_weights_post_style_clip"]
        self.assertAlmostEqual(post["AAA"], 0.10, places=4)
        self.assertAlmostEqual(post["CASH"], 0.90, places=4)
        self.assertTrue(any(v.startswith("style_turnover_scaled:") for v in out["violations"]))
        self.assertTrue(out["post_clip_compliance"]["compliant"])

    def test_stricter_scorecard_turnover_wins_over_style_limit(self):
        out = apply_style_constraints(
            target_weights={"AAA": 0.60, "CASH": 0.40},
            base_weights={"AAA": 0.0, "CASH": 1.0},
            current_weights={"AAA": 0.0, "CASH": 1.0},
            market_scorecard={
                "max_adjustment_from_base": 1.0,
                "min_cash_weight": 0.0,
                "max_turnover_per_cycle": 0.10,
            },
            decision_style={
                "analysis_style": "standard",
                "trade_style": "step_in",
                "style_limits": {
                    "max_turnover_per_cycle": 0.25,
                },
            },
        )

        post = out["target_weights_post_style_clip"]
        self.assertAlmostEqual(post["AAA"], 0.10, places=4)
        self.assertAlmostEqual(post["CASH"], 0.90, places=4)
        self.assertTrue(out["post_clip_compliance"]["compliant"])

    def test_cash_only_style_moves_all_equity_to_cash(self):
        out = apply_style_constraints(
            target_weights={"SPY": 0.4, "CASH": 0.6},
            base_weights={"SPY": 0.4, "CASH": 0.6},
            current_weights={"SPY": 0.4, "CASH": 0.6},
            market_scorecard={"min_cash_weight": 0.0},
            decision_style={
                "analysis_style": "macro_defensive",
                "trade_style": "cash_only",
                "style_limits": {},
            },
        )

        self.assertEqual(out["target_weights_post_style_clip"], {"CASH": 1.0})
        self.assertTrue(out["post_clip_compliance"]["compliant"])


if __name__ == "__main__":
    unittest.main()
