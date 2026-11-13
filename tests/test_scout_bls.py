import unittest
from datetime import date

from scout.data import bls

DATA = {"Results": {"series": [
    {"seriesID": "WPSFD4", "data": [{"year": "2026", "period": "M10", "value": "160.254"},
                                    {"year": "2026", "period": "M09", "value": "159.699"}]},
    {"seriesID": "WPUFD4", "data": [{"year": "2026", "period": "M10", "value": "150.0"},
                                    {"year": "2025", "period": "M10", "value": "146.0"}]},
    {"seriesID": "CES0000000001", "data": [{"year": "2026", "period": "M10", "value": "159500"},
                                           {"year": "2026", "period": "M09", "value": "159381"}]},
    {"seriesID": "LNS14000000", "data": [{"year": "2026", "period": "M10", "value": "4.4"}]},
]}}


class BlsTests(unittest.TestCase):
    def test_values_formatted_like_the_forecast(self):
        d = bls.parse(DATA)
        self.assertEqual(bls.actual("PPI m/m", d, date(2026, 11, 13)), "0.3%")
        self.assertEqual(bls.actual("PPI y/y", d, date(2026, 11, 13)), "2.7%")
        self.assertEqual(bls.actual("Non-Farm Employment Change", d, date(2026, 11, 6)), "119K")
        self.assertEqual(bls.actual("Unemployment Rate", d, date(2026, 11, 6)), "4.4%")
        self.assertIsNone(bls.actual("Retail Sales m/m", d, date(2026, 11, 13)))       # not a BLS release

    def test_never_last_months_number(self):
        d = bls.parse(DATA)
        self.assertIsNone(bls.actual("PPI m/m", d, date(2026, 12, 11)))               # November not in BLS yet
        self.assertEqual(bls.reference_month(date(2027, 1, 9)), (2026, 12))

    def test_versus_the_forecast(self):
        self.assertEqual(bls.versus("0.3%", "0.4%"), "低于预期")
        self.assertEqual(bls.versus("0.3%", "0.3%"), "符合预期")
        self.assertEqual(bls.versus("150K", "120K"), "高于预期")
        self.assertIsNone(bls.versus("0.3%", None))


if __name__ == "__main__":
    unittest.main()
