import unittest
from datetime import date

import numpy as np
import pandas as pd

from scout import market_filter as mf


def sessions(start: str, n: int) -> list[date]:
    return [d.date() for d in pd.bdate_range(start, periods=n)]


class MarketFilterTests(unittest.TestCase):
    def test_same_rule_as_the_redeploy_study(self):
        from scout.exit_redeploy import market_off

        rng = np.random.default_rng(1)
        d = sessions("2018-01-02", 900)
        c = list(100 * np.exp(np.cumsum(rng.normal(0, 0.012, len(d)))))
        ours = [s["off"] for s in mf.states(d, c)]
        theirs = market_off(np.array(c), d)
        self.assertEqual(ours[:-1], list(theirs[:-1]))          # the study only knows a month end from the next row

    def test_off_below_the_ten_month_mean_and_the_flip_date(self):
        d = sessions("2025-01-01", 300)
        c = [100.0 + i * 0.1 for i in range(250)] + [100.0] * 50   # a long rise, then a drop
        s = mf.summary(d, c)
        self.assertTrue(s["off"])
        self.assertLess(s["gap"], 0)
        self.assertLessEqual(s["since"], s["month_end"])       # turned off at a month end and stayed off
        i = d.index(s["since"])
        self.assertFalse(mf.states(d, c)[i - 1]["off"])
        self.assertIn("避险，按计划不新买入", mf.text(s))
        self.assertIsNone(mf.summary(d[:100], c[:100]))        # fewer than ten month ends

    def test_changed_today_only_on_the_flip_session(self):
        d = sessions("2025-01-01", 300)
        c = [100.0 + i * 0.1 for i in range(250)] + [100.0] * 50
        flip = mf.summary(d, c)["since"]
        i = d.index(flip)
        self.assertTrue(mf.summary(d[:i + 1], c[:i + 1])["changed_today"])
        self.assertFalse(mf.summary(d[:i + 2], c[:i + 2])["changed_today"])


if __name__ == "__main__":
    unittest.main()
