import unittest

import numpy as np
import pandas as pd

from scout import cross_asset as ca


def frame(n: int = 600, seed: int = 0, drift: dict | None = None) -> tuple[pd.DataFrame, pd.Series]:
    rng = np.random.default_rng(seed)
    idx = pd.bdate_range("2010-01-01", periods=n)
    cols = {}
    for k, a in enumerate(("SPY", "IEF", "GLD", "DBC", "EFA", "TLT", "UUP")):
        mu = (drift or {}).get(a, 0.0003)
        cols[a] = 100 * np.exp(np.cumsum(rng.normal(mu, 0.005 + 0.002 * k, n)))
    return pd.DataFrame(cols, index=idx), pd.Series(0.0001, index=idx)


class SignalTests(unittest.TestCase):
    def test_signals_use_data_up_to_the_decision_row_only(self):
        px, rf = frame()
        ends = ca.month_end_rows(px.index)
        i = ends[-3]
        before = ca.features(px, rf, i, ends)
        later = px.copy()
        later.iloc[i + 1:] *= 3.0
        pd.testing.assert_frame_equal(before, ca.features(later, rf, i, ends))

    def test_eligibility_needs_a_year_of_history(self):
        px, rf = frame()
        px.loc[px.index[:400], "GLD"] = np.nan
        ends = ca.month_end_rows(px.index)
        early = ca.features(px, rf, [e for e in ends if e > 260][0], ends)
        self.assertFalse(early.at["GLD", "eligible"])
        self.assertTrue(early.at["SPY", "eligible"])

    def test_targets(self):
        ft = pd.DataFrame({"eligible": [True, True, True, True], "ret12": [0.2, -0.1, 0.05, 0.3],
                           "rf12": [0.01] * 4, "vol63": [0.1, 0.2, 0.4, 0.2], "above": [True, False, True, False]},
                          index=["A", "B", "C", "D"])
        iv = ca.targets("static_iv", ft)
        self.assertAlmostEqual(sum(iv.values()), 1.0)
        self.assertAlmostEqual(iv["A"], 2 * iv["B"])
        self.assertEqual(set(ca.targets("tsmom", ft)), {"A", "C", "D"})          # B's 12-month return is below cash
        self.assertEqual(ca.targets("faber_gtaa", ft), {"A": 0.25, "C": 0.25})
        self.assertEqual(ca.targets("dual_top3", ft), {"D": 1 / 3, "A": 1 / 3, "C": 1 / 3})
        ft.loc["C", "ret12"] = 0.005                                               # third best but below cash
        self.assertEqual(ca.targets("dual_top3", ft), {"D": 1 / 3, "A": 1 / 3})


class SimulationTests(unittest.TestCase):
    def test_all_cash_earns_the_bill_rate(self):
        px, rf = frame(100)
        r = ca.simulate(px, rf, {10: {}, 40: {}}, 0.001)["ret"]
        self.assertTrue(np.allclose(r.to_numpy(), 0.0001))

    def test_one_asset_held_matches_it_after_the_lag_and_cost(self):
        px, rf = frame(100)
        sim = ca.simulate(px, rf, {10: {"SPY": 1.0}}, 0.001)
        r = sim["ret"]
        spy = px["SPY"].pct_change()
        self.assertEqual(r.index[0], px.index[12])                               # decided on row 10, traded at 11's close
        self.assertAlmostEqual(r.iloc[0], (1 + spy.iloc[12]) * (1 - 0.001) - 1)
        self.assertTrue(np.allclose(r.iloc[1:].to_numpy(), spy.iloc[13:].to_numpy()))
        self.assertEqual(sim["turnover"], [1.0])

    def test_drift_and_rebalance_turnover(self):
        px, rf = frame(100)
        sim = ca.simulate(px, rf, {10: {"SPY": 0.5, "IEF": 0.5}, 40: {"SPY": 0.5, "IEF": 0.5}}, 0.0)
        self.assertEqual(len(sim["turnover"]), 2)
        self.assertGreater(sim["turnover"][1], 0)                                 # weights drifted, rebalancing trades
        self.assertLess(sim["turnover"][1], 0.2)


class DecisionTests(unittest.TestCase):
    def series(self, sharpe, dd, t=0.0, mean=0.001):
        h = {"sharpe": sharpe, "max_dd": dd}
        vs = {"all": {"t": t, "mean": mean}, "first": {"t": t, "mean": mean}, "second": {"t": t, "mean": mean}}
        return {"all": h, "first": h, "second": h, "vs": {"core_now": vs, "static_iv": vs, "static_ew": vs}}

    def test_all_conditions_are_needed(self):
        s = {"core_now": self.series(0.5, -0.34), "static_iv": self.series(0.5, -0.3), "static_ew": self.series(0.4, -0.3),
             "tsmom": self.series(0.7, -0.2), "faber_gtaa": self.series(0.6, -0.31), "dual_top3": self.series(0.45, -0.1)}
        d = ca.decide({"series": s}, 3.0)
        self.assertTrue(d["trials"]["tsmom"]["candidate"])
        self.assertFalse(d["trials"]["faber_gtaa"]["checks"]["drawdown"])           # only 3 pp better
        self.assertFalse(d["trials"]["dual_top3"]["checks"]["sharpe_vs_core"])
        self.assertEqual(d["pick"], "tsmom")
        self.assertFalse(d["trials"]["tsmom"]["edge_claim"])
        s["tsmom"] = self.series(0.7, -0.2, t=3.2)
        self.assertTrue(ca.decide({"series": s}, 3.0)["trials"]["tsmom"]["edge_claim"])

    def test_falling_behind_the_core_fails(self):
        s = {"core_now": self.series(0.5, -0.34), "static_iv": self.series(0.5, -0.3), "static_ew": self.series(0.4, -0.3),
             "tsmom": self.series(0.7, -0.2, t=-2.5), "faber_gtaa": self.series(0.3, -0.2), "dual_top3": self.series(0.3, -0.2)}
        self.assertIsNone(ca.decide({"series": s}, 3.0)["pick"])


if __name__ == "__main__":
    unittest.main()
