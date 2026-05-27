import unittest

from constants import DEFAULT_ETF_UNIVERSE
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

    def test_default_universe_is_registered_for_execution(self):
        missing = sorted(set(DEFAULT_ETF_UNIVERSE) - set(TICKER_ROLES))
        self.assertFalse(missing, f"DEFAULT_ETF_UNIVERSE not in execution policy: {missing}")
        non_tradable = sorted(
            ticker for ticker in DEFAULT_ETF_UNIVERSE
            if TICKER_ROLES.get(ticker) in {TickerRole.WATCHLIST, TickerRole.UNKNOWN}
        )
        self.assertFalse(non_tradable, f"DEFAULT_ETF_UNIVERSE contains non-tradable tickers: {non_tradable}")


if __name__ == "__main__":
    unittest.main()
