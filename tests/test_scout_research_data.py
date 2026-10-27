import unittest
from datetime import date, datetime
from zoneinfo import ZoneInfo

from scout.data import fred, options, sec
from scout.research_data import in_options_window


def e(start, end, val, fp="Q1", form="10-Q", filed="2026-05-01"):
    return {"start": start, "end": end, "val": val, "fp": fp, "fy": 2026, "form": form, "filed": filed}


class SecQuarterTests(unittest.TestCase):
    def test_cash_flow_quarters_from_year_to_date(self):
        entries = [e("2025-01-01", "2025-03-31", 10), e("2025-01-01", "2025-06-30", 25, "Q2"),
                   e("2025-01-01", "2025-09-30", 45, "Q3"), e("2025-01-01", "2025-12-31", 70, "FY", "10-K", "2026-02-20")]
        q = sec.quarterly(entries, "flow")
        self.assertEqual({k: v["value"] for k, v in q.items()},
                         {"2025-03-31": 10, "2025-06-30": 15, "2025-09-30": 20, "2025-12-31": 25})
        self.assertTrue(q["2025-12-31"]["derived"])
        self.assertEqual(q["2025-12-31"]["fp"], "Q4")
        self.assertFalse(q["2025-03-31"]["derived"])

    def test_direct_quarter_wins_and_restatement_uses_latest_filing(self):
        entries = [e("2026-04-01", "2026-06-30", 256, "Q2"), e("2026-01-01", "2026-03-31", 165),
                   e("2026-01-01", "2026-06-30", 421, "Q2"), e("2026-01-01", "2026-03-31", 160, filed="2026-07-29")]
        q = sec.quarterly(entries, "flow")
        self.assertEqual(q["2026-06-30"]["value"], 256)            # reported 3-month value, not 421 − 160
        self.assertEqual(q["2026-03-31"]["value"], 160)            # restated value, filed later

    def test_per_share_is_never_differenced(self):
        entries = [e("2026-01-01", "2026-03-31", -0.3), e("2026-01-01", "2026-06-30", 0.09, "Q2")]
        self.assertEqual(list(sec.quarterly(entries, "per_share")), ["2026-03-31"])

    def test_instant_values_and_metric_merge(self):
        facts = {"facts": {"us-gaap": {
            "Revenues": {"units": {"USD": [e("2025-10-01", "2025-12-31", 100, "Q4")]}},
            "RevenueFromContractWithCustomerExcludingAssessedTax": {"units": {"USD": [e("2026-01-01", "2026-03-31", 120)]}},
            "StockholdersEquity": {"units": {"USD": [{"end": "2026-03-31", "val": 640, "fp": "Q1", "fy": 2026,
                                                      "form": "10-Q", "filed": "2026-05-01"}]}},
        }}}
        rows = {(r["metric"], str(r["period_end"])): r for r in sec.financial_rows(facts, "X")}
        self.assertEqual(rows[("revenue", "2026-03-31")]["concept"], "RevenueFromContractWithCustomerExcludingAssessedTax")
        self.assertEqual(rows[("revenue", "2025-12-31")]["value"], 100)     # older concept fills the gap
        self.assertEqual(rows[("equity", "2026-03-31")]["value"], 640)

    def test_filings_index(self):
        sub = {"filings": {"recent": {"accessionNumber": ["0001-26-1", "0001-26-2"], "form": ["8-K", "SD"],
                                      "filingDate": ["2026-10-23", "2026-10-20"], "reportDate": ["2026-10-23", ""],
                                      "primaryDocument": ["a.htm", "b.htm"], "items": ["2.02,9.01", ""]}}}
        f = sec.filings(sub, "CORT", "0001088856")
        self.assertEqual(len(f), 1)
        self.assertEqual(f[0]["items"], "2.02,9.01")
        self.assertEqual(f[0]["url"], "https://www.sec.gov/Archives/edgar/data/1088856/0001261/a.htm")


class OptionsTests(unittest.TestCase):
    def test_implied_vol_round_trip(self):
        for call in (True, False):
            price = options.bs_price(100, 105, 30 / 365, 0.04, 0.45, call)
            self.assertAlmostEqual(options.implied_vol(price, 100, 105, 30 / 365, 0.04, call), 0.45, places=3)
        self.assertIsNone(options.implied_vol(101, 100, 105, 30 / 365, 0.04, True))       # call above the stock: no IV

    def chain(self, spot, vol, days, r=0.04):
        t = days / 365
        def q(k, call):
            p = options.bs_price(spot, k, t, r, vol + (0.1 if (k < spot and not call) else 0), call)
            return {"strike": k, "bid": p * 0.97, "ask": p * 1.03, "lastPrice": p, "volume": 100, "openInterest": 1000}
        strikes = [spot * x for x in (0.85, 0.9, 0.95, 1.0, 1.05, 1.1, 1.15)]
        return [q(k, True) for k in strikes], [q(k, False) for k in strikes]

    def test_summary_atm_iv_move_and_skew(self):
        today = date(2026, 10, 27)
        chains = {"2026-11-20": self.chain(100, 0.5, 24), "2026-12-18": self.chain(100, 0.5, 52)}
        chains["2026-11-20"][0][3].update(volume=6000, openInterest=100)          # unusual ATM call
        s = options.summarize(chains, 100.0, today, 0.04)
        front = s["expiries"]["front"]
        self.assertAlmostEqual(front["atm_iv"], 0.5, places=2)
        self.assertAlmostEqual(front["put_skew"], 0.1, places=2)
        self.assertGreater(front["straddle_move"], 0.05)
        self.assertEqual(s["unusual"][0]["volume"], 6000)
        self.assertEqual(s["quality"], "ok")

    def test_empty_quotes_are_skipped(self):
        calls = [{"strike": 100, "bid": 0, "ask": 0, "volume": 10, "openInterest": 0}]
        s = options.expiry_summary(calls, calls, 100, 30, 0.04)
        self.assertEqual(s["valid_quotes"], 0)
        self.assertNotIn("atm_iv", s)

    def test_missing_open_interest_flags_nothing(self):
        calls, puts = self.chain(100, 0.5, 24)
        for q in calls + puts:
            q.update(openInterest=0, volume=9000)
        s = options.summarize({"2026-11-20": (calls, puts)}, 100.0, date(2026, 10, 27), 0.04)
        self.assertEqual((s["unusual"], s["quality"], s.get("oi_missing")), ([], "thin", True))

    def test_pick_expiries(self):
        p = options.pick_expiries(["2026-10-30", "2026-11-06", "2026-11-27", "2026-12-24"], date(2026, 10, 27))
        self.assertEqual(p, {"front": "2026-11-06", "m1": "2026-11-27", "m2": "2026-12-24"})

    def test_window(self):
        et = ZoneInfo("America/New_York")
        self.assertTrue(in_options_window(datetime(2026, 10, 27, 15, 40, tzinfo=et)))
        self.assertFalse(in_options_window(datetime(2026, 10, 27, 16, 5, tzinfo=et)))


class FredTests(unittest.TestCase):
    def test_parse_skips_missing(self):
        rows = fred.parse("observation_date,DGS10\n2026-10-22,4.10\n2026-10-23,.\n2026-10-26,4.12\n")
        self.assertEqual(rows, [(date(2026, 10, 22), 4.10), (date(2026, 10, 26), 4.12)])


if __name__ == "__main__":
    unittest.main()
