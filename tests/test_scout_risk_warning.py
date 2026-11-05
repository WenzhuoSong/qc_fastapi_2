import unittest

import numpy as np
import pandas as pd

from scout import risk_warning as rw


def _close(n=1600, seed=2, drift=0.0003, sigma=0.01):
    rng = np.random.default_rng(seed)
    idx = pd.bdate_range("2000-01-03", periods=n)
    return pd.Series(100 * np.exp(np.cumsum(rng.normal(drift, sigma, n))), index=idx)


class RiskWarningTests(unittest.TestCase):
    def test_conditions_use_only_past_data(self):
        c = _close()
        b = pd.Series(0.4, index=c.index)
        n = pd.Series(50, index=c.index)
        cz = pd.Series(0.0, index=c.index)
        full = rw.conditions(c, b, n, cz)
        cut = rw.conditions(c.iloc[:1200], b, n, cz)
        pd.testing.assert_frame_equal(full.iloc[:1200], cut)
        self.assertFalse(full["ready"].iloc[:rw.EXT_MIN + rw.SMA - 2].any())    # overextension needs 756 values of ext
        self.assertTrue(full["ready"].iloc[-1])

    def test_trend_break_and_warning_count(self):
        c = _close()
        c.iloc[-30:] = c.iloc[-31] * np.linspace(0.97, 0.6, 30)                  # a crash: below the average, volatile
        cz = pd.Series(3.0, index=c.index)                                       # credit stress on
        out = rw.conditions(c, pd.Series(0.6, index=c.index), pd.Series(50, index=c.index), cz)
        last = out.iloc[-1]
        self.assertTrue(last["trend_break"])
        self.assertTrue(last["credit_stress"])
        self.assertTrue(last["warning"])
        self.assertGreaterEqual(last["count"], 2)

    def test_breadth_divergence_needs_members(self):
        c = pd.Series(np.linspace(100, 200, 1600), index=pd.bdate_range("2000-01-03", periods=1600))
        cz = pd.Series(0.0, index=c.index)
        weak = rw.conditions(c, pd.Series(0.3, index=c.index), pd.Series(50, index=c.index), cz)
        few = rw.conditions(c, pd.Series(0.3, index=c.index), pd.Series(5, index=c.index), cz)
        self.assertTrue(weak["breadth_divergence"].iloc[-1])
        self.assertFalse(few["breadth_divergence"].iloc[-1])

    def test_forward_min(self):
        c = pd.Series([10, 9, 11, 8, 12], index=pd.bdate_range("2020-01-01", periods=5), dtype=float)
        f = rw.forward_min(c, 2)
        self.assertAlmostEqual(f.iloc[0], -0.1)
        self.assertAlmostEqual(f.iloc[1], 8 / 9 - 1)
        self.assertTrue(np.isnan(f.iloc[3]))

    def test_dummy_regression_is_difference_in_means(self):
        rng = np.random.default_rng(0)
        y = (rng.random(600) < 0.2).astype(float)
        w = rng.random(600) < 0.3
        y[w] = (rng.random(w.sum()) < 0.5).astype(float)
        dates = pd.bdate_range("2010-01-01", periods=600)
        r = rw.dummy_regression(y, w, dates.to_period("M").astype(str).to_numpy(), dates.to_numpy())
        self.assertAlmostEqual(r["diff"], y[w].mean() - y[~w].mean())
        self.assertGreater(r["t_month"], 3)

    def test_credit_z_uses_previous_day(self):
        idx = pd.bdate_range("2000-01-03", periods=400)
        s = pd.Series(3.0, index=idx) + np.random.default_rng(1).normal(0, 0.05, 400)
        s.iloc[-1] = 10.0
        z = rw.credit_z(s, idx)
        self.assertLess(z.iloc[-1], 2)                       # today's jump is seen tomorrow

    def test_market_schedules(self):
        c = _close(n=900)
        warn = pd.Series(False, index=c.index)
        warn.iloc[700:720] = True
        ready = pd.Series(True, index=c.index)
        s = rw.market_schedule(c, warn, ready, "warning")
        self.assertEqual(s[0], {"SPY": 1.0})
        self.assertEqual(s[700], {"SPY": 0.0})
        self.assertEqual(s[720], {"SPY": 1.0})
        self.assertEqual(list(rw.market_schedule(c, warn, ready, "hold")), [0])

    def test_sector_schedule_holds_cash_in_warning(self):
        idx = pd.bdate_range("2010-01-01", periods=60)
        px = pd.DataFrame({"A": np.linspace(1, 2, 60), "B": np.linspace(1, 1.5, 60)}, index=idx)
        flags = pd.DataFrame(False, index=idx, columns=["A", "B"])
        flags.iloc[30:, 0] = True
        ready = pd.DataFrame(True, index=idx, columns=["A", "B"])
        s = rw.sector_schedule(px, flags, ready, True)
        self.assertEqual(s[30], {"A": 0.0, "B": 0.5})
        self.assertNotIn(30, rw.sector_schedule(px, flags, ready, False))

    def test_episodes(self):
        idx = pd.bdate_range("2010-01-01", periods=10)
        f = pd.Series([0, 1, 1, 0, 0, 1, 0, 0, 0, 0], index=idx, dtype=bool)
        dd = pd.Series([-0.01, -0.2, -0.1, 0, 0, -0.05, 0, 0, np.nan, np.nan], index=idx)
        e = rw.episodes(f, dd, -0.15)
        self.assertEqual(e["episodes"], 2)
        self.assertEqual(e["episodes_with_big_dd"], 1)
        self.assertAlmostEqual(e["false_alarm_share"], 0.5)


if __name__ == "__main__":
    unittest.main()
