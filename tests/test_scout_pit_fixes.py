"""Fixes from the review of 2026-10-31: dollar volume, first-filed fundamentals, delisting, edge cases."""
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

from scout import cost_model as cm
from scout import sharadar_research as sr


class FirstFiledTests(unittest.TestCase):
    def test_the_first_filing_is_kept_whatever_the_row_order(self):
        f = pd.DataFrame({"ticker": ["A", "A", "A"], "reportperiod": pd.to_datetime(["2020-12-31"] * 2 + ["2021-03-31"]),
                          "datekey": pd.to_datetime(["2021-04-20", "2021-02-15", "2021-05-01"]), "assets": [110.0, 100.0, 120.0]})
        got = sr.first_filed(f)
        self.assertEqual(got.loc[got["reportperiod"] == "2020-12-31", "assets"].item(), 100.0)   # filed 02-15, not the restatement
        self.assertEqual(len(got), 2)


class DollarVolumeTests(unittest.TestCase):
    def test_split_adjusted_volume_pairs_with_split_adjusted_close(self):
        d = pd.bdate_range("2020-08-03", periods=20)
        # a 4:1 split mid-month: unadjusted close drops from 400 to 100, adjusted close and volume are continuous
        unadj = np.r_[np.full(10, 400.0), np.full(10, 100.0)]
        s = pd.DataFrame({"ticker": "X", "date": d, "high": 101.0, "low": 99.0, "close": 100.0, "closeunadj": unadj,
                          "volume": 1_000_000.0})
        self.assertAlmostEqual(cm.month_table(s)["adv"].item(), 100_000_000.0)

    def test_nan_inputs_are_not_tradeable(self):
        self.assertFalse(cm.tradeable(5_000_000, float("nan")))
        self.assertFalse(cm.tradeable(float("nan"), 0.01))


class DelistingTests(unittest.TestCase):
    def prices(self, delisted: bool) -> sr.Prices:
        n = 10
        c = np.r_[np.full(6, 10.0), np.full(4, np.nan)][:, None]          # stops trading after day 5
        d = pd.bdate_range("2020-01-01", periods=n)
        return sr.Prices(d, ["X"], c, c, c, c, c, np.ones((n, 1)), np.array([-0.3]), np.array([delisted]))

    def test_haircut_only_when_the_stock_was_delisted(self):
        self.assertAlmostEqual(sr.holding_return(self.prices(True), 0, 1, 7), -0.3)
        self.assertAlmostEqual(sr.holding_return(self.prices(False), 0, 1, 7), 0.0)   # a halt to the end of the data


class OperatorTests(unittest.TestCase):
    def test_infinite_values_are_missing_not_top_ranked(self):
        from scout.ml import operators as op

        df = pd.DataFrame({"date": ["d"] * 4, "x": [1.0, 2.0, np.inf, 3.0]})
        out = op.cross_sectional(df, ["x"])
        self.assertEqual(out["x"].iloc[2], 0.0)
        self.assertEqual(out["x_na"].tolist(), [0, 0, 1, 0])
        self.assertEqual(out["x"].iloc[3], out["x"].max())


class PurgeTests(unittest.TestCase):
    def test_purge_deletes_derived_results_too(self):
        from scout.data import sharadar

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "sharadar"
            for p in (root, Path(tmp) / "ml", Path(tmp) / "research_data"):
                p.mkdir()
            gone = sharadar.purge(root, ("ml", "costs"))
            self.assertEqual({p.name for p in gone}, {"sharadar", "ml"})
            self.assertTrue((Path(tmp) / "research_data").exists())     # public academic data stays


if __name__ == "__main__":
    unittest.main()
