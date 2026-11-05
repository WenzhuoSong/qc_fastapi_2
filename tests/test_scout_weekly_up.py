import unittest
from datetime import date

import numpy as np
import pandas as pd

from scout import weekly_up as wu


class WeeklyUpTests(unittest.TestCase):
    def test_fomc_history_parser(self):
        html = "<h5>January 31-February 1 Meeting - 2006</h5><h5>March 27-28 Meeting - 2006</h5><h5>May 10 Meeting - 2006</h5>"
        self.assertEqual(wu.parse_fomc_history(html), [date(2006, 2, 1), date(2006, 3, 28), date(2006, 5, 10)])

    def test_calendar_flags(self):
        days = pd.bdate_range("2024-01-01", "2024-03-29")
        wk_tom = days[(days >= "2024-01-29") & (days <= "2024-02-02")]
        wk_mid = days[(days >= "2024-01-15") & (days <= "2024-01-19")]
        cal = wu.calendar([wk_tom, wk_mid], days, {date(2024, 1, 31)}, {2024: "ok"})
        self.assertEqual(list(cal["tom"]), [1.0, 0.0])
        self.assertEqual(list(cal["fomc"]), [1.0, 0.0])
        missing = wu.calendar([wk_tom], days, set(), {2024: "missing"})
        self.assertTrue(np.isnan(missing["fomc"].iloc[0]))

    def test_prep_winsorises_and_flags_with_training_stats(self):
        tr = pd.DataFrame({"a": list(range(100)), "b": [np.nan] + list(range(99))}, dtype=float)
        te = pd.DataFrame({"a": [1e9], "b": [np.nan]})
        prep = wu.Prep(tr, ["a", "b"])
        x = prep(te)
        self.assertEqual(x.shape, (1, 3))                       # a, b, flag for b
        self.assertLess(x[0, 0], 2)                             # clipped at the 99th percentile
        self.assertEqual(x[0, 2], 1.0)

    def test_efficiency_ratio(self):
        lc = pd.Series(np.log(np.linspace(100, 110, 30)))
        self.assertAlmostEqual(wu.efficiency(lc, 10, 1).iloc[-1], 1.0)

    def test_folds_leave_a_gap(self):
        dates = pd.date_range("2000-01-07", "2009-12-25", freq="W-FRI")
        d = pd.DataFrame({"date": np.repeat(dates, 2), "y": 1})
        d["target_end"] = d["date"] + pd.Timedelta(days=7)
        for tr, va in wu.folds(d):
            self.assertLess(pd.to_datetime(tr["target_end"]).max(), va["date"].min())
        self.assertEqual([va["date"].dt.year.iloc[0] for _, va in wu.folds(d)], [2005, 2006, 2007, 2008, 2009])


if __name__ == "__main__":
    unittest.main()
