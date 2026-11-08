import math
import unittest

import numpy as np
import pandas as pd

from scout import carver_trend as ct


def _px(n=900, cols=("SPY", "TLT", "GLD"), seed=3):
    rng = np.random.default_rng(seed)
    idx = pd.bdate_range("2003-01-01", periods=n)
    return pd.DataFrame(100 * np.exp(np.cumsum(rng.normal(0.0003, 0.01, (n, len(cols))), axis=0)), index=idx, columns=list(cols))


class ForecastTests(unittest.TestCase):
    def test_no_value_depends_on_later_prices(self):
        px = _px()
        alt = px.copy()
        alt.iloc[600:] *= 1.5
        f1, f2 = (ct.forecasts(p, ct.mixed_vol(p)) for p in (px, alt))
        for s in ct.RULES:
            pd.testing.assert_frame_equal(f1[s].iloc[:600], f2[s].iloc[:600])

    def test_forecasts_wait_for_256_prices_and_are_capped(self):
        px = _px()
        px.iloc[300:, 0] = np.linspace(1, 50, 600) * px.iloc[300, 0]      # a straight run up: forecasts hit the cap
        f = ct.forecasts(px, ct.mixed_vol(px))
        self.assertTrue(f["64_256"].iloc[:255].isna().all().all())
        self.assertTrue(f["64_256"].iloc[256:].notna().all().all())
        self.assertLessEqual(float(f["2_8"].abs().max().max()), ct.CAP)
        self.assertEqual(float(f["16_64"]["SPY"].iloc[-1]), ct.CAP)

    def test_turnover_counts_round_trips_per_year(self):
        idx = pd.bdate_range("2003-01-01", periods=512)
        f = pd.DataFrame({"A": [10.0 if (k // 64) % 2 == 0 else -10.0 for k in range(512)]}, index=idx)
        # a switch +10 → −10 every 64 sessions: 4 switches a year of 256, each half a round trip of 40 units / 20
        self.assertAlmostEqual(float(ct.turnover(f)["A"].iloc[-1]), 7 * 20 / 20 / 2, places=6)


class SpeedLimitTests(unittest.TestCase):
    def test_fast_rules_are_dropped_on_low_volatility_assets(self):
        idx = pd.bdate_range("2003-01-01", periods=300)
        fast = pd.DataFrame({"SPY": [10.0 * (-1) ** (k // 5) for k in range(300)], "SHY": [10.0 * (-1) ** (k // 5) for k in range(300)]}, index=idx)
        slow = pd.DataFrame(5.0, index=idx, columns=["SPY", "SHY"])
        vol = pd.DataFrame({"SPY": 0.18, "SHY": 0.015}, index=idx)
        ok = ct.allowed({"2_8": fast, "64_256": slow}, vol, [299], 0.0005)[299]
        self.assertTrue(ok.loc["SPY", "64_256"] and ok.loc["SHY", "64_256"])     # no turnover, no cost
        self.assertFalse(ok.loc["SHY", "2_8"])                                    # ~25 round trips × 0.1 % ÷ 1.5 % ≫ 0.13
        self.assertTrue(ok.loc["SPY", "2_8"] == (float(ct.turnover(fast)["SPY"].iloc[299]) * 0.001 / 0.18 <= ct.SPEED_LIMIT))


class MultiplierTests(unittest.TestCase):
    def test_diversification_multiplier(self):
        w = np.array([0.5, 0.5])
        self.assertAlmostEqual(ct.div_mult(w, np.eye(2)), math.sqrt(2))
        self.assertAlmostEqual(ct.div_mult(w, np.ones((2, 2))), 1.0)
        self.assertEqual(ct.div_mult(np.full(10, 0.1), np.eye(10)), ct.DM_MAX)

    def test_negative_and_missing_correlations(self):
        x = pd.DataFrame({"a": np.arange(60.0), "b": -np.arange(60.0), "c": [np.nan] * 60})
        h = ct.floor_corr(x, ["a", "b", "c"])
        self.assertEqual(h[0, 1], 0.0)        # floored
        self.assertEqual(h[0, 2], 1.0)        # no data: no diversification credit

    def test_handcrafted_weights_split_by_group_then_within(self):
        avail = pd.DataFrame({"SPY": [True], "IWM": [True], "TLT": [True], "GLD": [False], "UUP": [True]})
        w = ct.instrument_weights(avail).iloc[0]
        self.assertAlmostEqual(w["SPY"], 1 / 6)
        self.assertAlmostEqual(w["TLT"], 1 / 3)
        self.assertEqual(w["GLD"], 0.0)
        self.assertAlmostEqual(w.sum(), 1.0)


class SimulationTests(unittest.TestCase):
    def test_trades_a_day_late_and_only_outside_the_buffer(self):
        idx = pd.bdate_range("2010-01-01", periods=6)
        px = pd.DataFrame({"A": [100, 100, 110, 110, 110, 110.0]}, index=idx)
        rf = pd.Series(0.0, index=idx)
        target = pd.DataFrame({"A": [0.5, 0.5, 0.5, 0.52, 0.8, 0.8]}, index=idx)
        buf = pd.DataFrame({"A": [0.05] * 6}, index=idx)
        r = ct.simulate(px, rf, target, buf, 0, 0.0)["ret"]
        self.assertAlmostEqual(r.iloc[0], 0.0)                 # bought at day 1's close
        self.assertAlmostEqual(r.iloc[1], 0.45 * 0.10)         # bought to the buffer edge 0.45, then +10 %
        self.assertAlmostEqual(r.iloc[2], 0.0)

    def test_costs_and_financing_reduce_returns(self):
        idx = pd.bdate_range("2010-01-01", periods=30)
        px = pd.DataFrame({"A": np.full(30, 100.0)}, index=idx)
        rf = pd.Series(0.0, index=idx)
        target = pd.DataFrame({"A": [(-1.0) ** k for k in range(30)]}, index=idx)
        buf = pd.DataFrame({"A": np.zeros(30)}, index=idx)
        r = ct.simulate(px, rf, target, buf, 0, 0.001)["ret"]
        self.assertLess(float((1 + r).prod() - 1), -0.04)      # ~2 notional traded a day at 0.1 %


if __name__ == "__main__":
    unittest.main()
