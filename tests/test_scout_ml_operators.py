import unittest

import numpy as np
import pandas as pd

from scout.ml import operators as op


class OperatorTests(unittest.TestCase):
    def test_rank_maps_to_minus_one_one_and_keeps_missing(self):
        r = op.rank_pm1(pd.Series([3.0, 1.0, np.nan, 2.0]))
        self.assertEqual(r.tolist()[:2], [1.0, -1.0])
        self.assertTrue(np.isnan(r[2]))
        self.assertEqual(r[3], 0.0)

    def test_cross_sectional_is_within_each_date_with_flags(self):
        df = pd.DataFrame({"date": [1, 1, 1, 2, 2], "x": [1.0, 2.0, np.nan, 100.0, 50.0]})
        out = op.cross_sectional(df, ["x"])
        self.assertEqual(out["x"].tolist(), [-1.0, 1.0, 0.0, 1.0, -1.0])
        self.assertEqual(out["x_na"].tolist(), [0, 0, 1, 0, 0])

    def test_sector_neutral_ranks_within_the_group(self):
        df = pd.DataFrame({"date": [1] * 4, "sec": ["a", "a", "b", "b"], "x": [1.0, 2.0, 10.0, 20.0]})
        self.assertEqual(op.cross_sectional(df, ["x"], group="sec")["x"].tolist(), [-1.0, 1.0, -1.0, 1.0])

    def test_winsorize_limits_outliers(self):
        s = pd.Series(list(range(100)) + [10_000.0])
        self.assertLess(op.winsorize(s).max(), 10_000)

    def test_expanding_z_uses_only_earlier_dates(self):
        s = pd.Series(np.arange(40, dtype=float))
        z = op.expanding_z(s, min_history=5)
        self.assertTrue(np.isnan(z[4]))
        prev = s[:30]
        self.assertAlmostEqual(z[30], (s[30] - prev.mean()) / prev.std())
        s2 = s.copy()
        s2[35:] = 1e6                                  # changing the future leaves the past unchanged
        self.assertTrue(np.allclose(op.expanding_z(s2, 5)[:35].dropna(), z[:35].dropna()))

    def test_context_broadcasts_per_date(self):
        df = pd.DataFrame({"date": np.repeat(np.arange(30), 2), "v": np.repeat(np.arange(30, dtype=float), 2)})
        out = op.context(df, ["v"])
        self.assertEqual(out["v"].iloc[-1], out["v"].iloc[-2])

    def test_ordinal_and_one_hot(self):
        self.assertEqual(op.ordinal(pd.Series(["low", "high", "x"]), ["low", "high"]).tolist(), [0, 1, -1])
        oh = op.one_hot(pd.Series(["a", "c"]), "s", ["a", "b"])
        self.assertEqual(list(oh.columns), ["s=a", "s=b"])
        self.assertEqual(oh.values.tolist(), [[1, 0], [0, 0]])


if __name__ == "__main__":
    unittest.main()
