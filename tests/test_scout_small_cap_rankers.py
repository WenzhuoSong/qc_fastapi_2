import unittest

import numpy as np
import pandas as pd

from scout import sharadar_research as sr
from scout import small_cap_rankers as scr


def prices(n_days: int, closes: dict[str, list[float]], opens: dict[str, list[float]] | None = None) -> sr.Prices:
    dates = pd.bdate_range("2020-01-01", periods=n_days)
    names = sorted(closes)
    c = np.array([closes[t] for t in names], dtype=float).T
    o = np.array([(opens or {}).get(t, closes[t]) for t in names], dtype=float).T
    z = np.zeros(len(names))
    return sr.Prices(dates, names, o, c, c, c, c.copy(), np.ones_like(c), z, np.zeros(len(names), dtype=bool))


class FreezeTests(unittest.TestCase):
    def test_fraction_of_pool_or_of_announcers(self):
        pool = list(range(10))
        scores = {0: 5.0, 1: 9.0, 2: 1.0, 3: 7.0}
        self.assertEqual(scr.freeze(pool, scores, 0.2, False, "pool"), [1, 3])           # 20 % of 10 = 2
        self.assertEqual(scr.freeze(pool, scores, 0.2, False, "announcers"), [1])        # 20 % of 4 → 1
        self.assertEqual(scr.freeze(pool, scores, 0.2, True, "pool"), [2, 0])
        self.assertEqual(scr.freeze(pool, scores, None, False, "pool"), [0, 1, 2, 3])
        self.assertEqual(scr.freeze(pool, {}, 0.1, False, "pool"), [])
        self.assertEqual(scr.freeze(pool, {11: 1.0}, 0.1, False, "pool"), [])           # outside the pool

    def test_ties_are_stable(self):
        self.assertEqual(scr.freeze([3, 1, 2], {1: 1.0, 2: 1.0, 3: 1.0}, 0.67, False, "pool"), [1, 2])


class FormationTests(unittest.TestCase):
    def test_leave_one_out_control_costs_and_not_filled(self):
        n = 70
        closes = {"A": [100.0] * 2 + [110.0] * (n - 2), "B": [100.0] * n, "C": [100.0] * 2 + [90.0] * (n - 2),
                  "D": [100.0] * n}
        opens = {t: list(v) for t, v in closes.items()}
        opens["D"][1] = np.nan                                   # no open the next session: not filled
        p = prices(n, closes, opens)
        cost = {j: {"main": 0.005, "spread2": 0.01, "impact2": 0.006, "gross": 0.0} for j in range(4)}
        rows, missed = scr.formation_rows(p, 0, [0, 1, 2, 3], [0, 3], cost, {0: 1, 1: 1, 2: 2, 3: 2})
        self.assertEqual(missed, 1)
        self.assertEqual(len(rows), 1)
        r = rows[0]
        self.assertAlmostEqual(r["gross"], 0.10)                  # bought at the next open (100 → 110)
        p2 = prices(n, closes, {t: [100.0] * n for t in closes})
        rows, _ = scr.formation_rows(p2, 0, [0, 1, 2], [0], cost, {})
        r = rows[0]
        self.assertAlmostEqual(r["gross"], 0.10)
        self.assertAlmostEqual(r["control"], (0.0 - 0.10) / 2)    # B and C, not A
        self.assertAlmostEqual(r["edge_main"], 0.10 - 0.01 + 0.05)
        self.assertAlmostEqual(r["edge_gross"], 0.10 + 0.05)


class SignalTests(unittest.TestCase):
    def test_momentum_skips_the_last_month(self):
        n = 260
        p = prices(n, {"A": list(np.linspace(100, 200, n)), "B": [100.0] * n})
        m = scr.momentum(p, n - 1, [0, 1])
        self.assertAlmostEqual(m[0], p.closeadj[n - 1 - 21, 0] / p.closeadj[n - 1 - 252, 0] - 1)
        self.assertEqual(m[1], 0.0)
        self.assertEqual(scr.momentum(p, 100, [0]), {})

    def test_asof_values_excludes_same_day_and_stale(self):
        t = pd.DataFrame({"ticker": ["A", "A", "B", "C"], "datekey": pd.to_datetime(["2020-01-01", "2020-03-01", "2020-03-02", "2018-01-01"]),
                          "value": [1.0, 2.0, 3.0, 4.0]})
        self.assertEqual(scr.asof_values(t, pd.Timestamp("2020-03-02")), {"A": 2.0})

    def test_insider_clusters(self):
        ins = pd.DataFrame({"ticker": ["A", "A", "A", "B", "B", "C", "C"],
                            "date": pd.to_datetime(["2020-03-01", "2020-03-05", "2020-03-05", "2020-03-01", "2020-03-02",
                                                    "2019-11-01", "2020-03-10"]),
                            "owner": ["x", "y", "y", "z", "z", "u", "v"]})
        self.assertEqual(scr.insider_clusters(ins, pd.Timestamp("2020-03-10")), {"A"})   # C's second filing is same-day

    def test_ordinary_common(self):
        self.assertTrue(scr.ordinary_common("Com"))
        self.assertTrue(scr.ordinary_common("Com;ClA"))
        self.assertFalse(scr.ordinary_common("Pref"))
        self.assertFalse(scr.ordinary_common("Com;Opt"))
        self.assertFalse(scr.ordinary_common(None))


class StatsTests(unittest.TestCase):
    def test_monthly_weights_formations_equally(self):
        rows = pd.DataFrame({"date": pd.to_datetime(["2020-01-02", "2020-01-02", "2020-01-02", "2020-01-09"]),
                             "edge_main": [0.03, 0.03, 0.03, -0.01]})
        self.assertAlmostEqual(scr.monthly(rows, "edge_main").iloc[0], 0.01)

    def test_nw_t_equals_plain_t_without_lags_on_large_n(self):
        rng = np.random.default_rng(0)
        m = pd.Series(rng.normal(0.01, 0.02, 400))
        self.assertAlmostEqual(scr.nw_t(m, 0), scr.tstat(m) * np.sqrt(400 / 399), places=6)
        ac = pd.Series(np.repeat(rng.normal(0.01, 0.02, 100), 3))                     # overlapping-like series
        self.assertLess(scr.nw_t(ac, 2), scr.tstat(ac))


if __name__ == "__main__":
    unittest.main()
