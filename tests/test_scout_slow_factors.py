import unittest

import numpy as np
import pandas as pd

from scout import sharadar_research as sr
from scout import slow_factors as sf


def prices(closes: dict[str, list[float]], opens: dict[str, list[float]] | None = None, delisted: dict[str, float] | None = None) -> sr.Prices:
    n = len(next(iter(closes.values())))
    dates = pd.bdate_range("2020-01-01", periods=n)
    names = sorted(closes)
    c = np.array([closes[t] for t in names], dtype=float).T
    o = np.array([(opens or {}).get(t, closes[t]) for t in names], dtype=float).T
    hc = np.array([(delisted or {}).get(t, 0.0) for t in names])
    dl = np.array([t in (delisted or {}) for t in names])
    return sr.Prices(dates, names, o, c, c, c, c.copy(), np.ones_like(c), hc, dl)


class ReturnTests(unittest.TestCase):
    def test_halt_earns_zero_and_delisting_haircut_then_cash(self):
        p = prices({"A": [100, 110, np.nan, 121, 121, 121], "B": [50, 50, 50, np.nan, np.nan, np.nan]}, delisted={"B": -0.3})
        r = sf.daily_returns(p)
        self.assertAlmostEqual(r[1, 0], 0.10)
        self.assertEqual(r[2, 0], 0.0)                       # halted
        self.assertAlmostEqual(r[3, 0], 0.10)                # 110 → 121 across the halt
        self.assertAlmostEqual(r[3, 1], -0.3)                # the session after B's last close
        self.assertEqual(r[4, 1], 0.0)

    def test_cohort_path_entry_day_is_open_to_close(self):
        n = sf.HOLD + 3
        p = prices({"A": [100.0] * n, "B": [100.0] * n}, opens={"A": [100.0, 95.0] + [100.0] * (n - 2), "B": [100.0] * n})
        r = sf.daily_returns(p)
        path = sf.cohort_path(p, r, 0, [0, 1])
        self.assertEqual(len(path), sf.HOLD)
        self.assertAlmostEqual(path[0], (100 / 95 - 1) / 2)
        self.assertTrue(np.allclose(path[1:], 0.0))

    def test_filled_needs_the_next_open(self):
        p = prices({"A": [100.0] * 5, "B": [100.0] * 5}, opens={"B": [100.0, np.nan, 100.0, 100.0, 100.0]})
        self.assertEqual(sf.filled(p, 0, [0, 1]), [0])


class SignalTests(unittest.TestCase):
    def test_volatility_needs_200_days(self):
        rng = np.random.default_rng(1)
        r = np.full((300, 2), np.nan)
        r[:, 0] = rng.normal(0, 0.01, 300)
        r[100:, 1] = rng.normal(0, 0.03, 200)
        v = sf.volatility(r, 299, [0, 1])
        self.assertAlmostEqual(v[0], np.std(r[48:300, 0], ddof=1))
        self.assertIn(1, v)                                   # exactly 200 valid days
        self.assertEqual(sf.volatility(r, 298, [1]), {})      # 199
        self.assertEqual(sf.volatility(r, 100, [0]), {})

    def test_value_quality_ranks_and_excludes_negative_equity(self):
        tickers = ["A", "B", "C", "D"]
        book = {"A": 50.0, "B": 10.0, "C": -5.0, "D": 30.0}
        gp = {"A": 0.1, "B": 0.5, "C": 0.9, "D": 0.4}
        cap = {"A": 100.0, "B": 100.0, "C": 100.0, "D": 100.0}
        s = sf.value_quality(book, gp, cap, tickers, [0, 1, 2, 3])
        self.assertNotIn(2, s)
        self.assertAlmostEqual(s[3], (2 / 3 + 2 / 3) / 2)     # middle on both
        self.assertAlmostEqual(s[0], (1 + 1 / 3) / 2)


class StatsTests(unittest.TestCase):
    def test_nw_alpha_recovers_intercept_and_beta(self):
        rng = np.random.default_rng(2)
        x = pd.Series(rng.normal(0.005, 0.04, 300))
        y = 0.002 + 0.7 * x + pd.Series(rng.normal(0, 0.001, 300))
        a = sf.nw_alpha(y, x)
        self.assertAlmostEqual(a["alpha"], 0.002, places=3)
        self.assertAlmostEqual(a["beta"], 0.7, places=2)
        self.assertGreater(a["t"], 10)

    def test_monthly_compounds(self):
        d = pd.Series([0.1, 0.1, -0.5], index=pd.to_datetime(["2020-01-02", "2020-01-03", "2020-02-03"]))
        m = sf.monthly(d)
        self.assertAlmostEqual(m.iloc[0], 0.21)
        self.assertAlmostEqual(m.iloc[1], -0.5)


if __name__ == "__main__":
    unittest.main()
