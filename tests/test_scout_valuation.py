import unittest
from datetime import date

import pandas as pd

from scout import valuation as v
from scout.data.yahoo import unadjusted


def q(ticker, metric, end, start, filed, value):
    return {"ticker": ticker, "metric": metric, "period_end": end, "period_start": start, "filed": filed, "value": value}


QUARTERS = [(date(2025, 3, 31), date(2025, 1, 1)), (date(2025, 6, 30), date(2025, 4, 1)),
            (date(2025, 9, 30), date(2025, 7, 1)), (date(2025, 12, 31), date(2025, 10, 1))]


class VersionTests(unittest.TestCase):
    def test_a_value_counts_from_its_filing_date(self):
        fin = v.versions([q("X", "net_income", date(2025, 3, 31), date(2025, 1, 1), date(2025, 5, 1), 10.0)])
        self.assertEqual(v.as_of(fin[("X", "net_income")], date(2025, 4, 30)), [])
        self.assertEqual(v.as_of(fin[("X", "net_income")], date(2025, 5, 1))[0][2], 10.0)

    def test_a_restatement_counts_from_its_own_filing(self):
        fin = v.versions([q("X", "net_income", date(2025, 3, 31), date(2025, 1, 1), date(2025, 5, 1), 10.0),
                          q("X", "net_income", date(2025, 3, 31), date(2025, 1, 1), date(2025, 9, 1), 8.0)])
        self.assertEqual(v.as_of(fin[("X", "net_income")], date(2025, 8, 1))[0][2], 10.0)
        self.assertEqual(v.as_of(fin[("X", "net_income")], date(2025, 9, 1))[0][2], 8.0)

    def test_trailing_sums_four_quarters_or_takes_the_fiscal_year(self):
        inc = [(e, "quarter", 5.0, e) for e, _ in QUARTERS]
        self.assertEqual(v.trailing(inc, date(2026, 2, 20)), 20.0)
        self.assertIsNone(v.trailing(inc, date(2026, 9, 1)))                                     # stale
        self.assertIsNone(v.trailing(inc[1:], date(2026, 2, 20)))
        self.assertEqual(v.trailing([(date(2025, 12, 31), "year", 30.0, date(2026, 3, 1))], date(2026, 6, 1)), 30.0)


class SplitTests(unittest.TestCase):
    def test_shares_filed_before_a_split_move_onto_todays_basis(self):
        fin = v.versions([q("N", "shares_diluted", date(2024, 4, 28), date(2024, 1, 29), date(2024, 5, 29), 2.49e9)])
        sh = fin[("N", "shares_diluted")]
        self.assertAlmostEqual(v.shares(sh, [(date(2024, 6, 10), 10.0)], date(2024, 7, 1)), 2.49e10)
        restated = v.versions([q("N", "shares_diluted", date(2024, 4, 28), date(2024, 1, 29), date(2025, 5, 28), 2.489e10)])
        self.assertAlmostEqual(v.shares(restated[("N", "shares_diluted")], [(date(2024, 6, 10), 10.0)], date(2025, 6, 1)), 2.489e10)

    def test_one_mis_scaled_share_count_is_outvoted(self):
        rows = [q("W", "shares_diluted", e, s, date(2026, 2, 1), 59.7e6) for e, s in QUARTERS[:3]]
        rows.append(q("W", "shares_diluted", QUARTERS[3][0], QUARTERS[3][1], date(2026, 2, 1), 59.7e9))
        self.assertAlmostEqual(v.shares(v.versions(rows)[("W", "shares_diluted")], [], date(2026, 3, 1)), 59.7e6)

    def test_unadjusted_reads_closes_and_split_dates(self):
        idx = pd.to_datetime(["2024-06-07", "2024-06-10"])
        raw = pd.concat({"N": pd.DataFrame({"Close": [120.0, 121.0], "Stock Splits": [0.0, 10.0]}, index=idx)}, axis=1)
        closes, splits = unadjusted(raw, ["N", "MISSING"])
        self.assertEqual(list(closes["N"]), [120.0, 121.0])
        self.assertEqual(splits, {"N": [(date(2024, 6, 10), 10.0)]})


class BasketTests(unittest.TestCase):
    def setUp(self):
        rows = []
        for t, ni, sh in (("A", 5.0, 100.0), ("B", -1.0, 50.0)):
            rows += [q(t, "net_income", e, s, date(2026, 2, 1), ni) for e, s in QUARTERS]
            rows.append(q(t, "shares_diluted", date(2025, 12, 31), date(2025, 10, 1), date(2026, 2, 1), sh))
        self.fin = v.versions(rows)

    def test_pe_sums_market_values_over_earnings_with_losses(self):
        st = v.stocks_at(["A", "B", "C"], self.fin, {}, pd.Series({"A": 2.0, "B": 4.0, "C": 9.0}), date(2026, 3, 1))
        b = v.basket_at(["A", "B", "C"], st)
        self.assertEqual(b["n"], 2)
        self.assertAlmostEqual(b["pe"], (200 + 200) / (20 - 4))

    def test_change_splits_price_from_earnings(self):
        a = {"per": {"A": (90.0, 22.0), "B": (10.0, 1.0)}}
        b = {"per": {"A": (100.0, 20.0)}}
        c = v.change(a, b)
        self.assertAlmostEqual(c["mv"], -0.10)
        self.assertAlmostEqual(c["ni"], 0.10)

    def test_forward_and_history(self):
        f = v.forward(["A", "B", "C"], {"A": {"marketCap": 300.0, "forwardPE": 30.0}, "B": {"marketCap": 100.0, "forwardPE": 10.0}})
        self.assertEqual((round(f["pe"], 2), f["n"]), (20.0, 2))
        h = v.history_stats([float(x) for x in range(10, 30)], 25.0)
        self.assertEqual((h["months"], h["share_below"], h["median"]), (20, 0.75, 20.0))
        b = {"key": "ndx", "name": "纳指", "members": 3, "used": 2, "pe": 25.0, "history": h, "forward": f,
             "past": {"21": {"date": "2026-10-08", "pe": 28.0, "change": {"mv": -0.12, "ni": 0.03}}}}
        text = v.text(b)
        self.assertIn("21 个交易日前 28.0，期间总市值 -12.0%、滚动净利润 +3.0%", text)
        self.assertIn("现在高于其中 75% 的月份", text)


if __name__ == "__main__":
    unittest.main()
