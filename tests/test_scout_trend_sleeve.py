import unittest

import numpy as np
import pandas as pd

from scout import trend_sleeve as ts


def _months(n=120):
    return pd.period_range("2004-01", periods=n, freq="M")


class BlendTests(unittest.TestCase):
    def test_equal_returns_need_no_rebalancing(self):
        a = pd.Series([0.01, -0.02, 0.03])
        self.assertTrue(np.allclose(ts.blend(a, a, 0.2), a))

    def test_drift_back_to_the_weights_costs_one_way_on_both_legs(self):
        a, b = pd.Series([0.10]), pd.Series([0.0])
        gross = 0.8 * 0.10
        drift = 0.8 * 1.10 / (1 + gross) - 0.8
        self.assertAlmostEqual(float(ts.blend(a, b, 0.2).iloc[0]), gross - ts.COST * 2 * drift * (1 + gross))


class DecideTests(unittest.TestCase):
    def _p(self, sleeve_corr: float):
        rng = np.random.default_rng(7)
        idx = _months()
        core = pd.Series(rng.normal(0.008, 0.04, len(idx)), index=idx)
        noise = pd.Series(rng.normal(0.004, 0.03, len(idx)), index=idx)
        sleeve = sleeve_corr * (core - 0.008) + noise
        rf = pd.Series(0.001, index=idx)
        s = {"core_now": core, "spy": core * 1.2, "system": sleeve, "long_only": sleeve.clip(lower=-0.01), "hold": core * 0.5}
        return ts.portfolios(s, rf), rf

    def test_cash_control_and_halves_are_checked(self):
        p, rf = self._p(-0.5)
        ev = ts.evaluate(p, rf, split="2009-01")
        dec = ts.decide(ev)
        self.assertEqual(set(dec["checks"]), set(ts.PRIMARY))
        for k in ts.PRIMARY:
            self.assertEqual(set(dec["checks"][k]), {"sharpe_vs_core", "sharpe_vs_cash", "drawdown", "not_behind"})
        self.assertEqual(dec["passed"], dec["pick"] is not None)

    def test_a_sleeve_that_is_only_cash_like_fails_against_the_cash_control(self):
        p, rf = self._p(0.0)
        p["core_system"] = p["core_cash"].copy()
        p["core_long_only"] = p["core_cash"].copy()
        dec = ts.decide(ts.evaluate(p, rf, split="2009-01"))
        self.assertFalse(dec["passed"])


if __name__ == "__main__":
    unittest.main()
