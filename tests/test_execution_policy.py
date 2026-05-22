import unittest

from services.execution_policy import (
    TickerRole,
    apply_policy_caps,
    check_portfolio_exposure,
    check_weight_allowed,
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
        for ticker in ["TQQQ", "SQQQ", "SOXL", "SOXS", "SPXL", "SPXS", "UVXY", "VIXY"]:
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

    def test_policy_snapshot_contains_version_and_roles(self):
        snapshot = policy_snapshot()
        self.assertEqual(snapshot["version"], "sprint8a")
        self.assertEqual(snapshot["roles"]["PSI"], "thematic")


if __name__ == "__main__":
    unittest.main()
