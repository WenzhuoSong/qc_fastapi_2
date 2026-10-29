import unittest

import numpy as np
import pandas as pd

from scout.ml import baselines as b


class BaselineTests(unittest.TestCase):
    def test_ridge_recovers_a_linear_signal(self):
        rng = np.random.default_rng(0)
        X = rng.normal(size=(2000, 3))
        y = 2 * X[:, 0] - X[:, 2] + rng.normal(scale=0.1, size=2000)
        model = b.ridge_fit(X, y, 1.0)
        self.assertGreater(np.corrcoef(b.ridge_predict(model, X), y)[0, 1], 0.99)

    def test_top_spread_is_top_decile_minus_the_rest(self):
        df = pd.DataFrame({"date": [pd.Timestamp("2005-01-03")] * 100, "s": np.arange(100.0), "y": np.arange(100.0) / 100})
        got = b.top_spread(df, "s", "y").iloc[0]
        self.assertAlmostEqual(got, np.mean(np.arange(90, 100)) / 100 - np.mean(np.arange(90)) / 100)

    def test_ic_is_rank_correlation_per_date(self):
        df = pd.DataFrame({"date": [pd.Timestamp("2005-01-03")] * 30, "s": np.arange(30.0), "y": np.arange(30.0) ** 3})
        self.assertAlmostEqual(b.daily_ic(df, "s", "y").iloc[0], 1.0)


if __name__ == "__main__":
    unittest.main()
