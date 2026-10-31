import math
import unittest

import numpy as np
import pandas as pd

from scout import cost_model as cm


def bars(spread: float, n: int = 63, seed: int = 0, ticker: str = "X") -> pd.DataFrame:
    """Daily bars from a random walk whose trades hit the bid or the ask (half-spread each side)."""
    rng = np.random.default_rng(seed)
    mid = 50 * np.exp(np.cumsum(rng.normal(0, 0.015, n * 20)))
    side = rng.choice([-1, 1], n * 20)
    trade = mid * (1 + side * spread / 2)
    t = trade.reshape(n, 20)
    return pd.DataFrame({"ticker": ticker, "date": pd.bdate_range("2021-01-01", periods=n), "high": t.max(1), "low": t.min(1),
                         "close": t[:, -1], "closeunadj": t[:, -1], "volume": 100_000.0})


class EstimatorTests(unittest.TestCase):
    def test_abdi_ranaldo_recovers_a_wide_spread(self):
        b = bars(0.02, n=252)
        est = [cm.abdi_ranaldo(g["high"].to_numpy(), g["low"].to_numpy(), g["close"].to_numpy())
               for _, g in b.groupby(b["date"].dt.to_period("M"))]
        self.assertAlmostEqual(float(np.mean([e for e in est if e is not None])), 0.02, delta=0.008)

    def test_vectorised_table_matches_the_scalar_estimators(self):
        b = pd.concat([bars(0.01, seed=1, ticker="A"), bars(0.03, seed=2, ticker="B")])
        tab = cm.month_table(b)
        for (t, m), g in b.groupby(["ticker", b["date"].dt.to_period("M")]):
            row = tab[(tab["ticker"] == t) & (tab["month"] == m)].iloc[0]
            ar = cm.abdi_ranaldo(g["high"].to_numpy(), g["low"].to_numpy(), g["close"].to_numpy())
            if ar is None:
                self.assertTrue(math.isnan(row["spread"]))
                continue
            self.assertAlmostEqual(row["spread"], max(ar, cm.TICK / g["closeunadj"].mean()), places=10)
            self.assertAlmostEqual(row["spread_cs"], cm.corwin_schultz(g["high"].to_numpy(), g["low"].to_numpy()), places=10)

    def test_short_months_are_not_estimated_and_the_tick_floor(self):
        b = bars(0.0, n=63)
        tab = cm.month_table(b)
        full = tab[tab["days"] >= cm.MIN_DAYS]
        self.assertTrue((full["spread"] >= cm.TICK / full["price"] - 1e-12).all())
        self.assertTrue(tab.loc[tab["days"] < cm.MIN_DAYS, "spread"].isna().all())


class CostTests(unittest.TestCase):
    def test_square_root_impact(self):
        self.assertAlmostEqual(cm.one_way(0.004, 0.02, 2_500_000), 0.002 + 0.5 * 0.02 * 0.1)
        self.assertAlmostEqual(cm.one_way(0.0, 0.02, 10_000_000) / cm.one_way(0.0, 0.02, 2_500_000), 0.5)

    def test_tradeable(self):
        self.assertTrue(cm.tradeable(1_000_000, 0.01))
        self.assertFalse(cm.tradeable(400_000, 0.01))            # under $1M a day (and 6 % participation)
        self.assertFalse(cm.tradeable(5_000_000, None))


if __name__ == "__main__":
    unittest.main()
