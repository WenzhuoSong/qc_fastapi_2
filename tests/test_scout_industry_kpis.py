import unittest
from datetime import date, timedelta

from scout import industry_kpis as k
from scout import intraday
from scout.data import fred


class CrackTests(unittest.TestCase):
    def test_321_crack_in_dollars_per_barrel(self):
        # gasoline 2.90 $/gal, diesel 4.83 $/gal, WTI 95 $/bbl → (2×121.8 + 202.86 − 285) / 3
        self.assertAlmostEqual(k.crack(2.90, 4.83, 95.0), (2 * 2.90 * 42 + 4.83 * 42 - 3 * 95.0) / 3)

    def test_crack_history_only_on_days_with_all_inputs(self):
        d = [date(2026, 10, 20) + timedelta(days=i) for i in range(3)]
        series = {"DGASNYH": [(d[0], 2.9), (d[1], 3.0), (d[2], 3.1)], "DDFUELNYH": [(d[0], 4.8), (d[2], 4.9)],
                  "DCOILWTICO": [(d[0], 90.0), (d[1], 91.0), (d[2], 92.0)]}
        h = k.history(k.METRICS["crack_321"], series)
        self.assertEqual([x for x, _ in h], [d[0], d[2]])

    def test_live_crack_and_its_change(self):
        q = {"RB=F": {"last": 3.0, "change": 0.0, "time_et": "10:00"}, "HO=F": {"last": 5.0, "change": 0.0},
             "CL=F": {"last": 90.0, "change": 0.0}}
        live = k.live_value(k.METRICS["crack_321"], q)
        self.assertAlmostEqual(live["value"], k.crack(3.0, 5.0, 90.0))
        self.assertAlmostEqual(live["pct_change"], 0.0)
        self.assertIsNone(k.live_value(k.METRICS["crack_321"], {"RB=F": q["RB=F"]}))


class RegistryTests(unittest.TestCase):
    def test_holdings_map_to_industries(self):
        self.assertEqual(k.industry_of("PBF", "Oil & Gas Refining & Marketing").key, "refining")
        self.assertEqual(k.industry_of("XLU", None).key, "utilities")
        self.assertEqual(k.industry_of("HUM", "Managed Health Care").key, "managed_care")
        self.assertTrue(k.industry_of("HUM", "Managed Health Care").missing)
        self.assertIsNone(k.industry_of("SPY", None))

    def test_every_series_is_fetched_by_the_research_job(self):
        self.assertEqual([s for s in k.all_series() if s not in fred.SERIES], [])

    def test_every_industry_names_known_metrics(self):
        for ind in k.INDUSTRIES:
            self.assertTrue(set(ind.metrics) <= set(k.METRICS), ind.key)

    def test_reading_percentile_and_change(self):
        vals = [(date(2025, 1, 1) + timedelta(days=i), float(i)) for i in range(300)]
        r = k.reading(vals)
        self.assertEqual(r["change"], 20.0)
        self.assertAlmostEqual(r["percentile"], 1.0)


class MoveVerdictTests(unittest.TestCase):
    def test_pbf_with_its_refining_peers_is_industry(self):
        self.assertEqual(intraday.move_verdict(-0.045, -0.14, 0.017, [-0.048, -0.050, -0.042, -0.035]), "行业")

    def test_high_beta_name_on_a_market_day_is_market(self):
        self.assertEqual(intraday.move_verdict(-0.05, 2.5, -0.02, [0.001, -0.002]), "大盘")

    def test_alone_is_company(self):
        self.assertEqual(intraday.move_verdict(-0.09, 1.0, 0.004, [0.01, -0.005]), "公司自身")


if __name__ == "__main__":
    unittest.main()
