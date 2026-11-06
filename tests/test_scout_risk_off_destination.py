import unittest

import numpy as np
import pandas as pd

from scout import cross_asset as ca
from scout import risk_off_destination as rod


def frame(spy: list[float], bond: list[float], start="2010-01-01") -> tuple[pd.DataFrame, pd.Series]:
    idx = pd.bdate_range(start, periods=len(spy))
    return pd.DataFrame({"SPY": spy, "IEF": bond}, index=idx), pd.Series(0.0001, index=idx)


class SignalTests(unittest.TestCase):
    def test_risk_off_needs_ten_month_ends_and_uses_the_mean(self):
        spy = pd.Series([100.0] * 12 + [80.0])
        off = rod.risk_off(spy, list(range(13)))
        self.assertEqual(sorted(off), list(range(9, 13)))
        self.assertFalse(off[11])
        self.assertTrue(off[12])                # 80 < mean of nine 100s and 80

    def test_schedule_and_spells(self):
        off = {5: False, 10: True, 15: True, 20: False, 25: True}
        self.assertEqual(rod.schedule(off, "IEF", 10), {10: {"IEF": 1.0}, 15: {"IEF": 1.0}, 20: {"SPY": 1.0}, 25: {"IEF": 1.0}})
        self.assertEqual(rod.schedule(off, None, 0)[10], {})
        self.assertEqual(rod.spells(off, 0, 30), [(11, 21), (26, 29)])

    def test_spell_returns(self):
        px, rf = frame([100, 100, 90, 80, 85], [50, 50, 51, 52, 52])
        row = rod.spell_returns(px, rf, [(1, 3)], ("SPY", "IEF"))[0]
        self.assertAlmostEqual(row["SPY"], -0.2)
        self.assertAlmostEqual(row["IEF"], 0.04)
        self.assertAlmostEqual(row["cash"], 1.0001 ** 2 - 1)


class StatsTests(unittest.TestCase):
    def test_nw_t_equals_plain_t_without_autocorrelation_weights(self):
        x = pd.Series(np.random.default_rng(0).normal(0.01, 0.02, 400))
        plain = ca.tstat(x)["t"]
        self.assertAlmostEqual(rod.nw_t(x, 0), plain * np.sqrt(len(x) / (len(x) - 1)), places=6)
        self.assertIsNone(rod.nw_t(pd.Series([1.0, 2.0])))

    def test_decide(self):
        def s(sh, dd):
            return {h: {"sharpe": sh, "max_dd": dd} for h in ("all", "first", "second")}

        ev = {"series": {"cash": s(0.5, -0.20),
                         "IEF": {**s(0.6, -0.21), "vs_cash": {"all": {"t": 3.6, "nw_t": 3.5}}},
                         "TLT": {**s(0.7, -0.30), "vs_cash": {"all": {"t": 4.0, "nw_t": 3.9}}},
                         "GLD": {**s(0.6, -0.20), "vs_cash": {"all": {"t": 3.6, "nw_t": 2.0}}}}}
        d = rod.decide(ev)
        self.assertTrue(d["trials"]["IEF"]["pass"])
        self.assertFalse(d["trials"]["TLT"]["checks"]["drawdown"])
        self.assertFalse(d["trials"]["GLD"]["checks"]["t"])      # the smaller of the two t values counts
        self.assertEqual(d["pick"], "IEF")


class SimulationTests(unittest.TestCase):
    def test_destination_is_held_only_while_off(self):
        n = 260
        idx = pd.bdate_range("2003-01-01", periods=n)
        spy = np.r_[np.linspace(100, 120, 200), np.linspace(120, 90, 60)]
        px = pd.DataFrame({"SPY": spy, "IEF": np.linspace(50, 60, n)}, index=idx)
        rf = pd.Series(0.0, index=idx)
        res, sp = rod.run_sample(px, rf, "2003-11-01", ["IEF"], 0.0)
        self.assertTrue(sp)
        a, b = sp[0]
        self.assertGreater(res["IEF"]["ret"].loc[idx[a + 1]:idx[b]].sum(), 0)          # holding the rising bond
        self.assertAlmostEqual(res["cash"]["ret"].loc[idx[a + 1]:idx[b]].abs().sum(), 0.0)


if __name__ == "__main__":
    unittest.main()
