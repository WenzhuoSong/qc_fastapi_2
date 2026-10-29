import unittest

import numpy as np

from scout import hold_cap as hc
from tests.test_scout_plan_exit import _panel


class HoldCapTests(unittest.TestCase):
    def setUp(self):
        from scout import exit_vs_hold as ev

        self.p = _panel(n=700)
        self.a = ev.stock_arrays(self.p, "AAA")
        self.pool = np.full(len(self.p.dates), 0.001)

    def test_after_the_sale_the_money_follows_the_pool_until_the_window_ends(self):
        s, fill = 300, float(self.a["open"][300])
        v = hc.value(self.a, s, fill, (10, fill * 1.05), self.pool)
        self.assertEqual(len(v), hc.WINDOW)
        np.testing.assert_allclose(v[:10], self.a["close"][s:s + 10] / fill)
        self.assertAlmostEqual(v[10], 1.05)
        self.assertAlmostEqual(v[-1], 1.05 * (1 - hc.COST) * 1.001 ** (hc.WINDOW - 11))

    def test_a_cap_sells_at_its_last_close_and_the_full_cap_is_holding(self):
        s, fill = 300, float(self.a["open"][300])
        got = hc.trade(self.a, s, fill, self.pool)
        self.assertEqual(set(got), {"hold", *(f"{e}|{c}" for e in hc.EXITS for c in hc.CAPS)})
        k, px = hc.sale_atr10(self.a, s, fill, 60)
        self.assertLessEqual(k, 59)
        if k == 59:
            self.assertAlmostEqual(px, self.a["close"][s + 59])

    def test_decision_needs_both_halves_t_and_drawdown(self):
        from datetime import date, timedelta

        rows = []
        for m in range(40):
            d = date(2016, 1, 1) + timedelta(days=30 * m)
            r = {"date": d, "s": 300 + m, "r_hold": 0.1, "paths": {}}
            for e in hc.EXITS:
                r[f"r_{e}|60"] = 0.05
                r[f"r_{e}|126"] = 0.07 + (0.005 if m % 2 else -0.005)
                r[f"r_{e}|252"] = 0.05 + (0.02 if m < 20 else -0.03)
            rows.append(r)
        c = hc.compare(rows, "plan|126", "plan|60")
        self.assertGreater(c["all"]["t"], 2)
        c2 = hc.compare(rows, "plan|252", "plan|60")
        self.assertLess(c2["second"]["edge"], 0)


if __name__ == "__main__":
    unittest.main()
