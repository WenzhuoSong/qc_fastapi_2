import unittest

import numpy as np
import pandas as pd

from scout import osap_check as oc


def _frame(signal="AssetGrowth", top=1.0, n=300):
    dates = pd.date_range("1963-07-31", periods=n, freq="ME")
    rng = np.random.default_rng(0)
    rows = []
    for c in oc.CONSTRUCTIONS:
        for k in range(1, 11):
            ret = rng.normal(0.8, 0.5, n) + (top if k == 10 else 0.0)
            rows += [{"construction": c, "signalname": signal, "port": f"{k:02d}", "date": d, "ret": r} for d, r in zip(dates, ret)]
    return pd.DataFrame(rows)


class OsapTests(unittest.TestCase):
    def test_edge_is_top_decile_minus_the_mean_of_ten(self):
        s = oc.edge_series(_frame(top=1.0), "deciles_vw", "AssetGrowth")
        self.assertAlmostEqual(s.mean(), 0.9, delta=0.1)              # 1.0 − 1.0 / 10
        self.assertTrue(oc.describe(s, 2008)["pass"])

    def test_no_edge_does_not_pass(self):
        s = oc.edge_series(_frame(top=0.0), "deciles_vw", "AssetGrowth")
        self.assertFalse(oc.describe(s, 2008)["pass"])

    def test_categories(self):
        self.assertEqual(oc.category(True, True, 0.1), "一致有效")
        self.assertTrue(oc.category(True, False, -0.1).endswith("近年衰减"))
        self.assertTrue(oc.category(True, False, 0.2).endswith("股票池或定义差异"))
        self.assertEqual(oc.category(False, True, 0.0), "Scout 有效、学术无效")
        self.assertEqual(oc.category(False, False, 0.0), "一致无效")
        self.assertEqual(oc.category(True, None, 0.0), "Scout 无结论")

    def test_bear_months_are_selected(self):
        s = pd.Series(1.0, index=pd.date_range("2000-01-31", "2003-12-31", freq="ME"))
        self.assertEqual(len(oc._months(s, [("2000-03", "2002-09")])), 31)


if __name__ == "__main__":
    unittest.main()
