import unittest

import numpy as np
import pandas as pd

from scout.ml import risk


class P:
    def __init__(self, close, haircut=0.0):
        c = np.asarray(close, dtype=float)[:, None]
        self.dates = pd.bdate_range("2005-01-03", periods=len(c))
        self.closeadj = self.close = self.open = c
        self.haircut = np.array([haircut])


class RiskLabelTests(unittest.TestCase):
    def test_drawdown_counts_only_the_window_after_the_entry(self):
        close = [100, 100] + [95] * 10 + [79] + [100] * 70           # day 12 is 21 % below the entry (day 1 open)
        p = P(close)
        y = risk.drawdown_label(p, np.array([0, 12]), np.array([0, 0]), n=63)
        self.assertEqual(y[0], 1.0)
        self.assertEqual(y[1], 0.0)                                  # bought at day 13: never 20 % below
        self.assertTrue(np.isnan(risk.drawdown_label(p, np.array([30]), np.array([0]), n=63)[0]))   # beyond the data

    def test_a_delisting_inside_the_window_counts_its_settled_value(self):
        close = [100.0] * 10 + [np.nan] * 80
        y = risk.drawdown_label(P(close, haircut=-0.30), np.array([0]), np.array([0]), n=63)
        self.assertEqual(y[0], 1.0)                                  # settled at 70
        y = risk.drawdown_label(P(close, haircut=0.0), np.array([0]), np.array([0]), n=63)
        self.assertEqual(y[0], 0.0)                                  # acquired at the last price

    def test_losses(self):
        self.assertAlmostEqual(float(risk.qlike(np.array([np.log(0.2)]), np.array([0.04]))[0]), 0.0)
        self.assertGreater(float(risk.qlike(np.array([np.log(0.2)]), np.array([0.01]))[0]),
                           float(risk.qlike(np.array([np.log(0.2)]), np.array([0.16]))[0]))   # under-prediction costs more
        self.assertAlmostEqual(float(risk.log_loss(np.array([1.0]), np.array([0.5]))[0]), np.log(2))

    def test_calibration_of_a_perfect_forecast(self):
        rng = np.random.default_rng(0)
        p = rng.uniform(0.01, 0.3, 200_000)
        y = (rng.uniform(size=p.size) < p).astype(int)
        c = risk.calibration(y, p)
        self.assertLess(abs(c["slope"] - 1), 0.05)
        self.assertLess(c["ece"], 0.005)

    def test_design_fills_missing_with_the_date_median_and_flags_it(self):
        n = 20
        df = pd.DataFrame({"date": pd.Timestamp("2005-01-03"), "sector": ["A", "B"] * 10})
        for c in risk.STOCK:
            df[c] = np.arange(n, dtype=float)
        for c in risk.CONTEXT:
            df[c] = 0.3
        df.loc[0, "bm"] = np.nan
        X, names = risk.design(df)
        self.assertEqual(X[0, names.index("bm_na")], 1.0)
        self.assertAlmostEqual(X[0, names.index("bm")], np.median(np.arange(1, n, dtype=float).clip(None, np.quantile(np.arange(1.0, n), 0.99))), places=6)
        self.assertNotIn("vix", names)                               # constant within the sample: dropped


if __name__ == "__main__":
    unittest.main()
