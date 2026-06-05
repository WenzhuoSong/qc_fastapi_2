import unittest

from services.execution_policy import (
    TickerRole,
    apply_policy_caps,
    check_portfolio_exposure,
    check_weight_allowed,
    evaluate_policy,
    get_role,
    policy_snapshot,
)


class ExecutionPolicyTests(unittest.TestCase):
    def test_zero_weight_is_allowed_for_unknown_and_hedge(self):
        for ticker in ["COMPLETELY_UNKNOWN", "TQQQ", "SOXL", "SPY"]:
            allowed, reason = check_weight_allowed(ticker, 0.0)
            self.assertTrue(allowed, reason)

    def test_unknown_positive_weight_is_blocked(self):
        allowed, reason = check_weight_allowed("COMPLETELY_UNKNOWN", 0.01)
        self.assertFalse(allowed)
        self.assertIn("UNKNOWN", reason)

    def test_psi_thematic_cap(self):
        allowed, reason = check_weight_allowed("PSI", 0.075)
        self.assertTrue(allowed, reason)

        allowed, reason = check_weight_allowed("PSI", 0.076)
        self.assertFalse(allowed)
        self.assertIn("hard cap", reason)

    def test_hedge_products_are_tradable_but_tightly_capped(self):
        for ticker in [
            "TQQQ", "SQQQ", "SOXL", "SOXS", "SPXL", "SPXS", "UVXY", "VIXY",
            "SH", "PSQ", "RWM", "DOG", "MYY", "SBB", "SEF", "REK", "EUM", "EFZ", "YXI",
            "SJB", "TBF", "TBX",
        ]:
            self.assertEqual(get_role(ticker), TickerRole.HEDGE)
            allowed, reason = check_weight_allowed(ticker, 0.03)
            self.assertTrue(allowed, reason)
            allowed, reason = check_weight_allowed(ticker, 0.031)
            self.assertFalse(allowed, reason)

    def test_apply_policy_caps_releases_excess_to_cash(self):
        capped, events, cash_raised = apply_policy_caps({"PSI": 0.08, "CASH": 0.92})

        self.assertAlmostEqual(capped["PSI"], 0.075)
        self.assertAlmostEqual(cash_raised, 0.005)
        self.assertEqual(events[0]["ticker"], "PSI")
        self.assertEqual(events[0]["role"], "thematic")

    def test_role_group_cap_scales_down(self):
        capped, events, cash_raised = apply_policy_caps(
            {"TQQQ": 0.03, "SQQQ": 0.03, "SOXL": 0.03, "CASH": 0.91}
        )

        self.assertAlmostEqual(capped["TQQQ"] + capped["SQQQ"] + capped["SOXL"], 0.08)
        self.assertGreater(cash_raised, 0.0)
        self.assertTrue(any(event.get("group_role") == "hedge" for event in events))

    def test_check_portfolio_exposure_reports_role_violations(self):
        rows = check_portfolio_exposure({"PSI": 0.075, "SOXX": 0.075, "FTXL": 0.075, "SMH": 0.075})
        thematic = next(row for row in rows if row["role"] == "thematic")
        self.assertTrue(thematic["violated"])

    def test_evaluate_policy_reports_structured_violations(self):
        result = evaluate_policy(
            weights={"COMPLETELY_UNKNOWN": 0.02, "PSI": 0.08, "CASH": 0.90},
            current_weights={"CASH": 1.0},
            context={"min_cash_weight": 0.95, "max_turnover_per_cycle": 0.03},
        )

        self.assertFalse(result["allowed"])
        self.assertEqual(result["policy_version"], "sprint8a")
        self.assertFalse(result["checks"]["unknown_ticker_ok"]["pass"])
        self.assertFalse(result["checks"]["single_cap_ok"]["pass"])
        self.assertFalse(result["checks"]["cash_floor_ok"]["pass"])
        self.assertFalse(result["checks"]["turnover_ok"]["pass"])
        self.assertTrue(result["cap_violations"])

    def test_evaluate_policy_allows_valid_weights(self):
        result = evaluate_policy(
            weights={"SPY": 0.20, "PSI": 0.075, "SQQQ": 0.03, "CASH": 0.695},
            current_weights={"SPY": 0.18, "PSI": 0.07, "SQQQ": 0.02, "CASH": 0.73},
            context={"min_cash_weight": 0.05, "max_turnover_per_cycle": 0.10},
        )

        self.assertTrue(result["allowed"], result["violations"])
        self.assertTrue(result["checks"]["role_group_cap_ok"]["pass"])

    def test_turnover_ignores_cash_residual_when_current_snapshot_omits_cash(self):
        result = evaluate_policy(
            weights={"QQQ": 0.1136, "XLK": 0.1238, "CASH": 0.322019},
            current_weights={"QQQ": 0.1336, "XLK": 0.1438},
            context={"min_cash_weight": 0.05, "max_turnover_per_cycle": 0.20},
        )

        self.assertTrue(result["checks"]["turnover_ok"]["pass"], result["violations"])
        self.assertAlmostEqual(result["checks"]["turnover_ok"]["actual"], 0.02)

    def test_policy_snapshot_contains_version_and_roles(self):
        snapshot = policy_snapshot()
        self.assertEqual(snapshot["version"], "sprint8a")
        self.assertEqual(snapshot["roles"]["PSI"], "thematic")


if __name__ == "__main__":
    unittest.main()
