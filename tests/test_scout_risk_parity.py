import math
import unittest

import numpy as np
import pandas as pd

from scout import risk_parity as rp


class BondTests(unittest.TestCase):
    def test_a_par_bond_prices_at_one(self):
        self.assertAlmostEqual(rp.bond_price(0.05, 0.05, 10), 1.0)

    def test_unchanged_yield_earns_about_the_coupon(self):
        y = pd.Series([0.06, 0.06], index=pd.period_range("1970-01", periods=2, freq="M"))
        self.assertAlmostEqual(float(rp.bond_returns(y).iloc[0]), 0.06 / 12, places=3)

    def test_a_yield_rise_loses_about_duration_times_the_change(self):
        y = pd.Series([0.05, 0.06], index=pd.period_range("1970-01", periods=2, freq="M"))
        r = float(rp.bond_returns(y).iloc[0])
        self.assertLess(r, -0.06)                       # duration ≈ 7.8 → about −7.4 % + coupon
        self.assertGreater(r, -0.09)

    def test_month_end_yields_take_the_last_value(self):
        from datetime import date
        s = rp.month_end_yields([(date(1970, 1, 2), 7.0), (date(1970, 1, 30), 7.5), (date(1970, 2, 27), 6.5)])
        self.assertEqual(list(s), [0.075, 0.065])


class SleeveTests(unittest.TestCase):
    def _rets(self, n=120, seed=1):
        rng = np.random.default_rng(seed)
        idx = pd.period_range("1960-01", periods=n, freq="M")
        return pd.DataFrame({"equity": rng.normal(0.008, 0.045, n), "bonds": rng.normal(0.004, 0.015, n)}, index=idx), \
            pd.Series(0.003, index=idx)

    def test_equal_risk_and_the_vol_target(self):
        rets, rf = self._rets()
        r, lev = rp.sleeve(rets, rf)
        self.assertEqual(r.index[0], rets.index[rp.WINDOW])          # 36 months before the first holding month
        self.assertGreater(float(lev.mean()), 1.0)                     # low-vol bonds need leverage to reach 12 %
        self.assertLess(abs(float(r.std() * math.sqrt(12)) - rp.VOL_TARGET), 0.04)

    def test_weights_use_only_earlier_months(self):
        rets, rf = self._rets()
        alt = rets.copy()
        alt.iloc[80:] *= 3
        a, _ = rp.sleeve(rets, rf)
        b, _ = rp.sleeve(alt, rf)
        pd.testing.assert_series_equal(a.iloc[: 80 - rp.WINDOW], b.iloc[: 80 - rp.WINDOW])

    def test_core_holds_stocks_only_above_the_10_month_mean(self):
        idx = pd.period_range("1960-01", periods=14, freq="M")
        eq = pd.Series([0.02] * 11 + [-0.30, 0.01, 0.01], index=idx)
        rf = pd.Series(0.004, index=idx)
        c = rp.core(eq, rf)
        self.assertAlmostEqual(float(c.loc["1960-12"]), -0.30)        # decided at November's close, still above
        self.assertAlmostEqual(float(c.loc["1961-01"]), 0.004 - rp.COST)   # December's close below: cash, one switch


if __name__ == "__main__":
    unittest.main()
