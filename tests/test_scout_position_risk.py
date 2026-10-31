import math
import unittest
from datetime import date

import numpy as np

from scout import portfolio, position_risk as pr


class TierAndRangeTests(unittest.TestCase):
    def test_tiers_are_quintiles_and_rise_with_volatility(self):
        self.assertEqual(pr.tier(0.0)["tier"], 1)
        self.assertEqual(pr.tier(0.39)["tier"], 2)
        self.assertEqual(pr.tier(1.0)["tier"], 5)
        self.assertIsNone(pr.tier(None))
        self.assertTrue(all(a["all"] < b["all"] for a, b in zip(pr.TIERS, pr.TIERS[1:])))

    def test_move_range_scales_with_the_square_root_of_time(self):
        r = {x["sessions"]: x for x in pr.move_range(100.0, 0.30)}
        self.assertAlmostEqual(r[63]["sigma"] / r[21]["sigma"], math.sqrt(3), places=3)
        self.assertAlmostEqual(r[21]["hi1"], round(100 * math.exp(0.30 * math.sqrt(21 / 252)), 2))
        self.assertGreater(r[21]["hi1"] - 100, 100 - r[21]["lo1"])      # log-normal: wider above
        self.assertEqual(pr.move_range(100.0, None), [])


class SizingTests(unittest.TestCase):
    S = {"account_size": 100_000, "risk_pct": 0.01, "max_position_pct": 0.5, "vol_budget_pct": 0.02}

    def test_volatility_caps_a_volatile_stock(self):
        calm = portfolio.size(100, 95, self.S, vol=0.15)
        wild = portfolio.size(100, 95, self.S, vol=0.80)
        self.assertEqual(calm["shares"], 200)                       # $1,000 risk / $5: the stop binds
        self.assertIsNone(calm["capped"])
        self.assertEqual(wild["capped"], "vol")
        self.assertEqual(wild["shares"], math.floor(2000 / (0.80 * math.sqrt(21 / 252) * 100)))
        self.assertIn("波动上限", portfolio.size_text(wild))

    def test_without_volatility_only_the_position_cap_applies(self):
        sz = portfolio.size(100, 99, {**self.S, "max_position_pct": 0.2})
        self.assertEqual((sz["shares"], sz["capped"]), (200, "position"))


class PortfolioVolTests(unittest.TestCase):
    def test_correlated_positions_do_not_diversify(self):
        rng = np.random.default_rng(0)
        a = rng.normal(0, 0.01, 63)
        same = pr.portfolio_vol({"A": 50_000, "B": 50_000}, {"A": list(a), "B": list(a)}, 100_000)
        indep = pr.portfolio_vol({"A": 50_000, "B": 50_000}, {"A": list(a), "B": list(rng.normal(0, 0.01, 63))}, 100_000)
        self.assertAlmostEqual(same["diversification"], 1.0, places=6)
        self.assertGreater(indep["diversification"], 1.2)
        self.assertAlmostEqual(sum(c["share"] for c in indep["contrib"]), 1.0)
        self.assertAlmostEqual(same["pct_invested"], np.std(a, ddof=1) * math.sqrt(252), places=6)


class AdherenceTests(unittest.TestCase):
    T = {"id": 1, "opened_on": date(2026, 10, 1), "closed_on": None,
         "plan": {"stop": 90, "stop_basis": "x", "target1": 120, "time_stop_days": 20}}

    def test_an_unsold_stop_is_outstanding(self):
        t = {**self.T, "plan": {**self.T["plan"], "stop_hit": "2026-10-05"}}
        a = pr.adherence(t, [], date(2026, 10, 9))                    # 4 sessions later
        self.assertEqual(a["signal"]["sessions"], 4)
        self.assertEqual(a["verdict"], "拖延")
        self.assertIsNone(pr.adherence(t, [], date(2026, 10, 6))["signal"])   # within the grace session

    def test_sells_are_judged_against_the_signal(self):
        t = {**self.T, "closed_on": date(2026, 10, 6), "plan": {**self.T["plan"], "stop_hit": "2026-10-05"}}
        on = pr.adherence(t, [{"sold_on": date(2026, 10, 6)}], date(2026, 10, 9))
        self.assertEqual(on["verdict"], "按计划")
        early = pr.adherence({**self.T, "closed_on": date(2026, 10, 3)}, [{"sold_on": date(2026, 10, 3)}], date(2026, 10, 9))
        self.assertEqual(early["verdict"], "提前")
        late = pr.adherence({**t, "closed_on": date(2026, 10, 15)}, [{"sold_on": date(2026, 10, 15)}], date(2026, 10, 16))
        self.assertEqual(late["verdict"], "拖延")

    def test_a_sell_after_the_time_stop_is_on_plan(self):
        t = {**self.T, "closed_on": date(2026, 11, 20)}
        self.assertEqual(pr.adherence(t, [{"sold_on": date(2026, 11, 20)}], date(2026, 11, 21))["verdict"], "按计划")

    def test_a_trailing_stop_signal_is_judged_like_a_stop(self):
        t = {**self.T, "plan": {"policy": "trail20", "stop": 80.0, "stop_hit": "2026-10-05"}}
        a = pr.adherence(t, [], date(2026, 10, 9))
        self.assertIn("回落止损触发", a["signal"]["text"])
        on = pr.adherence({**t, "closed_on": date(2026, 10, 6)}, [{"sold_on": date(2026, 10, 6)}], date(2026, 10, 9))
        self.assertEqual(on["verdict"], "按计划")                         # next open after the close signal

    def test_a_buy_against_the_market_filter_is_flagged(self):
        t = {**self.T, "closed_on": date(2026, 10, 3), "plan": {"policy": "trail20", "stop": 80.0, "market_off_at_buy": True}}
        a = pr.adherence(t, [{"sold_on": date(2026, 10, 3)}], date(2026, 10, 9))
        self.assertTrue(a["against_filter"])
        self.assertEqual(pr.adherence_summary([a, pr.adherence(self.T, [], date(2026, 10, 9))])["against_filter"], 1)

if __name__ == "__main__":
    unittest.main()
