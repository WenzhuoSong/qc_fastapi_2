import sys
import types
import unittest


def _install_import_stubs() -> None:
    tools = types.ModuleType("tools")
    sys.modules.setdefault("tools", tools)

    db_tools = types.ModuleType("tools.db_tools")
    async def _token(_input):
        return {"approval_token": "test", "expires_at": "2099-01-01T00:00:00"}
    db_tools.tool_write_approval_token = _token
    sys.modules.setdefault("tools.db_tools", db_tools)


_install_import_stubs()

from agents.risk_manager import apply_scorecard_constraints  # noqa: E402


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


if __name__ == "__main__":
    unittest.main()
