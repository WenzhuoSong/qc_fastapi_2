import unittest

import numpy as np

from scout import scout_system as ss


def data(n=30, m=8, price=100.0):
    c = np.full((n, m), price)
    return {"o": c.copy(), "c": c, "rf": np.zeros(n), "start": 1, "member": np.ones((n, m), dtype=bool)}


def sig(j, fill, rs=0.9, exit_day=None, px=100.0):
    return {"j": j, "fill": fill, "price": px, "rs": rs, "exit": {"a": (exit_day, px) if exit_day else None,
                                                                  "b": (exit_day, px) if exit_day else None}}


class TrailTests(unittest.TestCase):
    def test_sells_next_open_after_a_close_below_80_percent_of_the_high(self):
        c = np.array([[100.0], [120.0], [110.0], [95.0], [90.0], [80.0]])
        o = c + 1
        self.assertEqual(ss.trail_exit(c, o, 0, 0), (4, 91.0))      # 95 < 0.8 × 120 on day 3 → open of day 4

    def test_no_sale_while_above(self):
        c = np.array([[100.0], [101.0], [99.0]])
        self.assertIsNone(ss.trail_exit(c, c, 0, 0))


    def test_a_stock_that_stopped_trading_is_settled_the_next_day(self):
        c = np.array([[100.0], [110.0], [105.0], [105.0], [105.0]])       # stopped after day 2 (settled flat)
        self.assertEqual(ss.trail_exit(c, c, 0, 0, last=2), (3, 105.0))


class PortfolioTests(unittest.TestCase):
    def test_a_position_sold_on_its_fill_day_frees_its_slot(self):
        d = data()
        fills = {2: [sig(j, 2, exit_day=2) for j in range(5)], 3: [sig(6, 3)]}
        nav, trades, taken = ss.run_portfolio(d, fills, "b", 0.0, np.zeros(30, dtype=bool))
        self.assertEqual(len(trades), 5)
        self.assertEqual([s["j"] for s in taken[3]], [6])
        self.assertAlmostEqual(nav[-1], 1.0)

    def test_at_most_five_positions_taken_by_relative_strength(self):
        d = data()
        fills = {3: [sig(j, 3, rs=0.8 + j / 100) for j in range(7)]}
        fills[3].sort(key=lambda s: -s["rs"])
        nav, _, taken = ss.run_portfolio(d, fills, "a", 0.0, np.zeros(30, dtype=bool))
        self.assertEqual(sorted(s["j"] for s in taken[3]), [2, 3, 4, 5, 6])
        self.assertAlmostEqual(nav[-1], 1.0)

    def test_two_signals_on_one_stock_filling_the_same_day_buy_it_once(self):
        d = data()
        fills = {3: [sig(0, 3), sig(0, 3), sig(1, 3)]}
        nav, _, taken = ss.run_portfolio(d, fills, "a", 0.0, np.zeros(30, dtype=bool))
        self.assertEqual([s["j"] for s in taken[3]], [0, 1])
        self.assertAlmostEqual(nav[-1], 1.0)                          # no money lost to an overwritten position

    def test_a_slot_freed_today_is_used_tomorrow(self):
        d = data()
        fills = {2: [sig(j, 2, exit_day=5) for j in range(5)], 5: [sig(6, 5)], 6: [sig(7, 6)]}
        _, _, taken = ss.run_portfolio(d, fills, "a", 0.0, np.zeros(30, dtype=bool))
        self.assertNotIn(5, taken)
        self.assertEqual([s["j"] for s in taken[6]], [7])

    def test_no_new_buys_while_the_market_filter_is_off(self):
        d = data()
        off = np.zeros(30, dtype=bool)
        off[3] = True                                                 # off after day 3's close
        _, _, taken = ss.run_portfolio(d, {4: [sig(0, 4)], 5: [sig(1, 5)]}, "a", 0.0, off)
        self.assertEqual(list(taken), [5])

    def test_control_buys_as_many_random_members_on_the_same_days(self):
        d = data()
        fills = {3: [sig(0, 3), sig(1, 3)]}
        _, _, taken = ss.run_portfolio(d, fills, "b", 0.0, np.zeros(30, dtype=bool))
        nav, trades, _ = ss.run_portfolio(d, fills, "b", 0.0, np.zeros(30, dtype=bool),
                                          {"rng": np.random.default_rng(0), "buys": taken})
        self.assertAlmostEqual(nav[-1], 1.0)


if __name__ == "__main__":
    unittest.main()
