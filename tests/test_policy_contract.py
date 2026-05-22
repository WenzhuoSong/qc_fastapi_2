import unittest

from services.execution_policy import TICKER_ROLES, TickerRole
from services.group_contract import GROUP_DEFINITIONS, PRIMARY_GROUP


class PolicyContractTests(unittest.TestCase):
    def test_all_tradable_tickers_have_primary_group(self):
        tradable = {
            ticker
            for ticker, role in TICKER_ROLES.items()
            if role not in {TickerRole.WATCHLIST, TickerRole.UNKNOWN}
        }
        missing = tradable - set(PRIMARY_GROUP)
        self.assertFalse(missing, f"Missing primary groups: {sorted(missing)}")

    def test_every_primary_group_has_definition(self):
        missing = set(PRIMARY_GROUP.values()) - set(GROUP_DEFINITIONS)
        self.assertFalse(missing, f"Missing group definitions: {sorted(missing)}")

    def test_policy_has_no_watchlist_hedge_products(self):
        for ticker in ["TQQQ", "SQQQ", "SOXL", "SOXS", "SPXL", "SPXS", "UVXY", "VIXY"]:
            self.assertEqual(TICKER_ROLES[ticker], TickerRole.HEDGE)
            self.assertEqual(PRIMARY_GROUP[ticker], "hedges")


if __name__ == "__main__":
    unittest.main()
