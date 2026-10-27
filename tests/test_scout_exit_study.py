import unittest

import numpy as np

from scout import exit_study as xs


def path(closes, lows=None, highs=None, atr=1.0, pre=0):
    c = np.array(closes, dtype=float)
    lo = np.array(lows if lows is not None else c - 0.5, dtype=float)
    hi = np.array(highs if highs is not None else c + 0.5, dtype=float)
    n = len(c)
    o = np.concatenate([[c[0]], c[:-1]])          # opens at the previous close: no gaps
    hi, lo = np.maximum(hi, o), np.minimum(lo, o)
    return xs.Path(o, hi, lo, c, np.full(n, atr), np.full(n, np.nan), np.full(n, np.nan), pre)


POL = {p.name: p for p in xs.POLICIES}


class ManageTests(unittest.TestCase):
    def test_scale_out_then_fixed_stop(self):
        # entry 100, stop 95, t1 105, t2 110: rises through both targets, then falls to the stop
        closes = [100, 103, 106, 111, 104, 96, 94]
        ret, days = xs.manage(path(closes), 100.0, 95.0, 105.0, 110.0, POL["fixed"])
        self.assertAlmostEqual(ret, (105 + 110 + 95) / 3 / 100 - 1)
        self.assertEqual(days, 7)

    def test_chandelier_locks_in_the_trend(self):
        closes = [100, 103, 106, 111, 112, 108, 104, 100]
        ret, _ = xs.manage(path(closes, atr=1.0), 100.0, 95.0, 105.0, 110.0, POL["chandelier"])
        # last third leaves at 112 − 3 = 109 (raised after the 112 close), above the fixed stop
        self.assertAlmostEqual(ret, (105 + 110 + 109) / 3 / 100 - 1)

    def test_gap_through_the_stop_fills_at_the_open(self):
        p = path([100, 90], lows=[99.5, 89.0], highs=[100.5, 91.0])
        p.open[1] = 90.0
        ret, _ = xs.manage(p, 100.0, 95.0, 105.0, 110.0, POL["fixed"])
        self.assertAlmostEqual(ret, -0.10)

    def test_close_based_exits_next_open(self):
        closes = [100, 106, 112, 115, 110, 108, 108]
        p = path(closes, atr=1.0)
        ret, days = xs.manage(p, 100.0, 95.0, 105.0, 111.0, POL["chandelier_close"])
        self.assertAlmostEqual(ret, (105 + 111 + 110) / 3 / 100 - 1)   # close 110 < 115 − 3 = 112 → out at the next open (110)
        self.assertEqual(days, 6)

    def test_swing_low_is_point_in_time(self):
        low = np.array([5, 4, 3, 2, 3, 4, 5, 6], dtype=float)
        self.assertIsNone(xs.swing_low(low, 5))                          # the low at 3 needs bars 4–6
        self.assertEqual(xs.swing_low(low, 6), 2.0)


if __name__ == "__main__":
    unittest.main()
