import unittest

from services.group_contract import (
    GROUP_DEFINITIONS,
    PRIMARY_GROUP,
    calc_factor_exposure,
    get_factor_tags,
    get_primary_group,
)
from services.position_governance import _ticker_group as governance_group
from services.proposal_shaper import _ticker_group as proposal_group


class GroupContractTests(unittest.TestCase):
    def test_every_primary_group_references_definition(self):
        for ticker, group in PRIMARY_GROUP.items():
            self.assertIn(group, GROUP_DEFINITIONS, ticker)

    def test_semiconductor_basket_review_uses_primary_group(self):
        for ticker in ["SOXX", "PSI", "FTXL"]:
            self.assertEqual(get_primary_group(ticker), "semiconductors")

    def test_factor_exposure_no_double_count_within_factor(self):
        weights = {"SOXX": 0.05, "PSI": 0.03, "FTXL": 0.02}
        exposures = calc_factor_exposure(weights)

        self.assertAlmostEqual(exposures["semiconductors"], 0.10)
        self.assertAlmostEqual(exposures["tech_growth"], 0.10)

    def test_primary_group_and_factor_tags_can_differ(self):
        self.assertEqual(get_primary_group("SOXX"), "semiconductors")
        self.assertIn("tech_growth", get_factor_tags("SOXX"))
        self.assertIn("semiconductors", get_factor_tags("SOXX"))

    def test_governance_and_shaper_use_same_primary_group(self):
        for ticker in ["SOXX", "FTXL", "XLK", "XLRE", "SGOV"]:
            contract_group = get_primary_group(ticker)
            self.assertEqual(governance_group(ticker), contract_group)
            self.assertEqual(proposal_group(ticker), contract_group)


if __name__ == "__main__":
    unittest.main()
