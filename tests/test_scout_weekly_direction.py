import unittest

import numpy as np
import pandas as pd

from scout import weekly_direction as wd


class WeeklyDirectionTests(unittest.TestCase):
    def test_week_ends_drop_the_open_week(self):
        idx = pd.bdate_range("2024-01-01", "2024-01-24")
        e = wd.week_ends(idx)
        self.assertEqual(list(e.strftime("%m-%d")), ["01-05", "01-12", "01-19"])

    def test_prepare_uses_training_statistics_only(self):
        cols = list(wd.FEATURES)
        train = pd.DataFrame(np.random.default_rng(0).normal(size=(50, len(cols))), columns=cols)
        train.iloc[0, 0] = np.nan
        test = pd.DataFrame(1000.0, index=[0], columns=cols)
        test.iloc[0, 1] = np.nan
        Xtr, Xte = wd.prepare(train, test)
        self.assertEqual(Xtr.shape[1], 2 * len(cols))
        self.assertEqual(Xtr[0, len(cols)], 1.0)                  # missing flag
        self.assertGreater(Xte[0, 0], 100)                        # scaled with the training std, not refitted
        self.assertAlmostEqual(Xte[0, 1], (train.iloc[:, 1].median() - train.iloc[:, 1].mean()) / train.iloc[:, 1].std(), places=6)

    def test_economic_costs_only_on_switches(self):
        dates = pd.date_range("2020-01-03", periods=4, freq="W-FRI")
        o = pd.DataFrame({"date": dates, "asset": "SPY", "m1": [0.7, 0.7, 0.4, 0.7], "m0": 0.55,
                          "ret_next": 0.01, "rf_next": 0.0, "y": 1, "spy_next": 0.01})
        e = wd.economic(o, "m1", 0.001)
        self.assertEqual(list(np.round(e["strat"].to_numpy(), 4)), [0.009, 0.01, -0.001, 0.009])

    def test_seasonality_uses_earlier_years(self):
        idx = pd.date_range("2010-01-31", periods=120, freq="ME")
        monthly = pd.Series(0.0, index=idx)
        monthly[monthly.index.month == 3] = 0.02
        v = wd.seasonality(None, pd.Timestamp("2016-03-04"), pd.Timestamp("2016-03-11"), monthly)
        self.assertAlmostEqual(v, 0.02)
        self.assertTrue(np.isnan(wd.seasonality(None, pd.Timestamp("2013-03-04"), pd.Timestamp("2013-03-11"), monthly)))


if __name__ == "__main__":
    unittest.main()
