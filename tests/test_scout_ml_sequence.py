import unittest

import numpy as np
import pandas as pd

from scout import sharadar_research as sr
from scout.ml import sequence as S


def prices(T=300, N=3, seed=0):
    rng = np.random.default_rng(seed)
    c = 50 * np.exp(np.cumsum(rng.normal(0, 0.02, (T, N)), axis=0))
    dates = pd.bdate_range("2000-01-03", periods=T)
    return sr.Prices(dates, [f"S{k}" for k in range(N)], c * 0.99, c * 1.01, c * 0.98, c, c,
                     rng.uniform(1e5, 1e6, (T, N)), np.zeros(N)), 100 * np.exp(np.cumsum(rng.normal(0, 0.01, T)))


class SequenceTests(unittest.TestCase):
    def test_channels_are_point_in_time(self):
        p, spy = prices()
        a = S.channels(p, spy)
        p.closeadj[200:] *= 3.0
        p.volume[200:] *= 5.0
        b = S.channels(p, spy)
        np.testing.assert_array_equal(np.nan_to_num(a[:200]), np.nan_to_num(b[:200]))
        self.assertEqual(a.shape, (300, 3, len(S.CHANNELS)))

    def test_gather_ends_at_the_sample_day_and_pads_the_start(self):
        arr = np.arange(300 * 2 * 7, dtype=np.float32).reshape(300, 2, 7)
        seq = S.gather(arr, np.array([299, 10]), np.array([1, 0]))
        self.assertEqual(seq.shape, (2, S.TOKENS, S.PATCH * 7))
        last = seq[0].reshape(S.WINDOW, 7)
        np.testing.assert_array_equal(last[-1], arr[299, 1])
        np.testing.assert_array_equal(last[0], arr[299 - S.WINDOW + 1, 1])
        early = seq[1].reshape(S.WINDOW, 7)
        self.assertTrue((early[:-11] == 0).all())
        np.testing.assert_array_equal(early[-1], arr[10, 0])

    def test_markov_patterns_and_shrinkage(self):
        states = np.zeros((30, 1), dtype=np.int16)
        states[20, 0], states[15, 0], states[10, 0] = 7, 3, 1
        code = S.patterns(states, np.array([20, 5]), np.array([0, 0]))
        self.assertEqual(code[0], 731)
        self.assertEqual(code[1], -1)                       # weeks before the data start are missing
        table = S.markov_fit(np.array([731] * 1000), np.ones(1000))
        self.assertAlmostEqual(table[731], 0.5)              # 1000 samples vs a prior of 1000
        self.assertEqual(S.markov_predict(table, np.array([-1]))[0], 0.0)

    def test_nets_map_a_batch_to_one_score_each(self):
        import torch
        torch.set_num_threads(1)          # LightGBM's OpenMP runtime may already be loaded (macOS deadlock)
        x = torch.zeros(4, S.TOKENS, S.PATCH * len(S.CHANNELS))
        for name in S.NETS:
            self.assertEqual(tuple(S.make(name)(x).shape), (4,))


if __name__ == "__main__":
    unittest.main()
