import unittest

import numpy as np
import pandas as pd

from scout import sharadar_recheck as rc
from scout import sharadar_research as sr


def _prices(n=200, stocks=("A", "B", "C", "D")):
    dates = pd.bdate_range("2000-01-03", periods=n)
    k = len(stocks)
    close = np.tile(np.linspace(10, 20, n)[:, None], (1, k)) * (1 + 0.1 * np.arange(k))
    return sr.Prices(dates, list(stocks), close.copy(), close * 1.01, close * 0.99, close.copy(), close.copy(),
                     np.full((n, k), 1e6), np.zeros(k))


class HoldingReturnTests(unittest.TestCase):
    def test_next_open_to_close_on_the_adjusted_scale(self):
        p = _prices()
        r = sr.holding_return(p, 0, 10, 20)
        self.assertAlmostEqual(r, p.close[30, 0] / p.open[11, 0] - 1)

    def test_a_delisted_stock_settles_at_its_last_close_with_the_haircut(self):
        p = _prices()
        p.closeadj[50:, 1] = np.nan
        p.haircut[1] = -0.3
        r = sr.holding_return(p, 1, 10, 63)
        self.assertAlmostEqual(r, (p.closeadj[49, 1] / p.open[11, 1]) * 0.7 - 1)
        p.closeadj[50:60, 2] = np.nan                       # a gap that ends: not delisted, no haircut
        p.haircut[2] = -0.3
        self.assertAlmostEqual(sr.holding_return(p, 2, 10, 63), p.closeadj[73, 2] / p.open[11, 2] - 1)


class RankRowTests(unittest.TestCase):
    def test_lowest_signal_is_picked_and_compared_with_every_other_stock(self):
        p = _prices()
        d = p.dates[10]
        rows = rc.rank_rows(p, {d: ["A", "B", "C", "D"]}, {d: {"A": 0.5, "B": -0.2, "C": 0.1, "D": 0.3}}, [10], {}, hold=20, frac=0.25)
        self.assertEqual(len(rows), 1)
        rets = [sr.holding_return(p, j, 10, 20) for j in range(4)]
        self.assertAlmostEqual(rows[0]["ret"], rets[1])
        self.assertAlmostEqual(rows[0]["select"], (sum(rets) - rets[1]) / 3)

    def test_verdict_needs_both_halves_and_t(self):
        rows = [{"date": pd.Timestamp("2000-01-31") + pd.offsets.MonthEnd(m), "ret": 0.02 + (0.003 if m % 2 else -0.003),
                 "select": 0.0, "sector": "X"} for m in range(60)]
        self.assertTrue(rc.summarize(rows, 3.0)["pass"])
        for r in rows[30:]:
            r["ret"] = -0.01
        self.assertFalse(rc.summarize(rows, 3.0)["pass"])


if __name__ == "__main__":
    unittest.main()
