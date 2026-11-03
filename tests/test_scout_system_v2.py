import unittest

import numpy as np

from scout import scout_system_v2 as v2


class RankExitTests(unittest.TestCase):
    def test_sells_next_open_after_relative_strength_drops_below_half(self):
        rs = np.array([[0.9], [0.7], [0.55], [0.45], [0.6]])
        c = np.full((5, 1), 10.0)
        o = c + 1
        self.assertEqual(v2.rank_exit(rs, c, o, 0, 0), (4, 11.0))

    def test_leaving_the_universe_counts_as_out(self):
        rs = np.array([[0.9], [np.nan], [0.9], [0.9]])
        c = np.full((4, 1), 10.0)
        self.assertEqual(v2.rank_exit(rs, c, c, 0, 0)[0], 2)

    def test_held_while_strong(self):
        rs = np.full((5, 1), 0.8)
        c = np.full((5, 1), 10.0)
        self.assertIsNone(v2.rank_exit(rs, c, c, 0, 0))

    def test_the_trail_variant_takes_whichever_comes_first(self):
        rs = np.full((6, 1), 0.9)
        c = np.array([[100.0], [120.0], [90.0], [90.0], [90.0], [90.0]])
        self.assertEqual(v2.exit_for("rank", rs, c, c, 0, 0, None), None)
        self.assertEqual(v2.exit_for("rank_trail", rs, c, c, 0, 0, None), (3, 90.0))


if __name__ == "__main__":
    unittest.main()
