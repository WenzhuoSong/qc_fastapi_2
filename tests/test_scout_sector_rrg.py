import unittest

import numpy as np
import pandas as pd

from scout import sector_rrg as sr

# quadrant → (rs_ratio, rs_momentum)
Q = {"leading": (101, 101), "weakening": (101, 99), "lagging": (99, 99), "improving": (99, 101)}


def frames(seq):
    idx = pd.bdate_range("2020-01-06", periods=len(seq) * 5)
    r = pd.DataFrame({"X": np.repeat([Q[q][0] for q in seq], 5).astype(float)}, index=idx)
    m = pd.DataFrame({"X": np.repeat([Q[q][1] for q in seq], 5).astype(float)}, index=idx)
    return r, m, sr.week_end_rows(idx)


class SignalTests(unittest.TestCase):
    def test_buy_on_entering_improving_sell_on_entering_weakening(self):
        r, m, ends = frames(["lagging", "improving", "improving", "leading", "weakening", "lagging", "lagging"])
        trades, sets = sr.signals(r, m, ends, ["X"], "a")
        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0]["entry"], ends[1] + 1)          # next session after the week it entered improving
        self.assertEqual(trades[0]["exit"], ends[4] + 1)
        self.assertEqual(sets[ends[2]], {"X"})
        self.assertEqual(sets[ends[5]], set())

    def test_variant_b_also_sells_a_failed_improvement(self):
        seq = ["lagging", "improving", "lagging", "lagging", "improving", "leading", "weakening", "lagging"]
        r, m, ends = frames(seq)
        a, _ = sr.signals(r, m, ends, ["X"], "a")
        b, _ = sr.signals(r, m, ends, ["X"], "b")
        self.assertEqual(len(a), 1)                                  # a keeps holding through the relapse
        self.assertEqual(a[0]["exit"], ends[6] + 1)
        self.assertEqual([t["exit"] for t in b], [ends[2] + 1, ends[6] + 1])

    def test_the_first_week_seen_is_not_an_entry(self):
        r, m, ends = frames(["improving", "improving", "weakening"])
        self.assertEqual(sr.signals(r, m, ends, ["X"], "a")[0], [])

    def test_decisions_use_data_up_to_the_week_end_only(self):
        rng = np.random.default_rng(0)
        idx = pd.bdate_range("2019-01-01", periods=400)
        px = pd.DataFrame(np.exp(np.cumsum(rng.normal(0, 0.01, (400, 3)), axis=0)), index=idx, columns=["A", "B", "SPY"])
        ratio, mom = sr.rrg(px, "SPY")
        ends = sr.week_end_rows(idx)
        cut = ends[50]
        px2 = px.copy()
        px2.iloc[cut + 1:] *= rng.uniform(0.5, 1.5, (len(px) - cut - 1, 3))
        r2, m2 = sr.rrg(px2, "SPY")
        s1 = sr.signals(ratio, mom, ends, ["A", "B"], "a")[1]
        s2 = sr.signals(r2, m2, ends, ["A", "B"], "a")[1]
        self.assertEqual([s1[i] for i in ends[:51]], [s2[i] for i in ends[:51]])


if __name__ == "__main__":
    unittest.main()
