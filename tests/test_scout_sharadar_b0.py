import unittest

import numpy as np
import pandas as pd

from scout import sharadar_b0 as sb


class SimulateTests(unittest.TestCase):
    def test_equal_weight_with_costs_and_cash_when_the_gate_is_off(self):
        n, k = 60, 2
        c = np.tile(np.linspace(10, 16, n)[:, None], (1, k))
        o = c.copy()
        rf = np.zeros(n)
        gate = np.ones(n, dtype=bool)
        gate[30:] = False
        nav = sb.simulate(o, c, rf, lambda t: [0, 1], gate, 0)
        self.assertAlmostEqual(nav[0], 1.0)
        bought = 1 - 2 * 0.5 * sb.COST_SIDE * 1.0      # buy both at the open of day 1 (turnover = 1)
        self.assertAlmostEqual(nav[29], bought * c[29, 0] / o[1, 0], places=6)
        self.assertAlmostEqual(nav[-1], nav[31], places=9)   # sold at the open of day 31: flat afterwards

    def test_gate_needs_every_etf_with_history(self):
        from scout.b0 import MomentumScore

        cfg = MomentumScore(((10, 1.0),), 10, 0.0, 10, ("sp500",), ("SPY", "QQQ"), 5, 3)
        spy = np.arange(1, 21, dtype=float)
        qqq = np.full(20, np.nan)
        qqq[17:] = [5.0, 4.0, 3.0]
        self.assertTrue(sb.gate_on({"SPY": spy, "QQQ": qqq}, 16, cfg))      # QQQ not yet listed: SPY only
        qqq = np.concatenate([np.full(10, np.nan), np.linspace(10, 5, 10)])
        self.assertFalse(sb.gate_on({"SPY": spy, "QQQ": qqq}, 19, cfg))     # QQQ below its average


if __name__ == "__main__":
    unittest.main()
