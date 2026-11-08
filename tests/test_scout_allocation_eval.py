import unittest

import numpy as np
import pandas as pd

from scout import allocation_eval as ae


def _idx(n, start="1960-01"):
    return pd.period_range(start, periods=n, freq="M")


class EpisodeTests(unittest.TestCase):
    def test_a_fall_of_20_percent_runs_from_after_the_high_to_the_trough(self):
        idx = _idx(8)
        eq = pd.Series([0.05, -0.10, -0.10, -0.05, 0.10, 0.20, 0.01, 0.01], index=idx)
        ep = ae.bear_episodes(eq)
        self.assertEqual(len(ep), 1)
        self.assertEqual((ep[0]["start"], ep[0]["end"]), (idx[1], idx[3]))
        self.assertLess(ep[0]["dd"], -0.2)

    def test_a_smaller_fall_is_not_a_bear_market(self):
        eq = pd.Series([0.05, -0.10, -0.05, 0.20], index=_idx(4))
        self.assertEqual(ae.bear_episodes(eq), [])

    def test_an_unrecovered_fall_at_the_end_still_counts(self):
        idx = _idx(4)
        ep = ae.bear_episodes(pd.Series([0.02, -0.15, -0.15, 0.01], index=idx))
        self.assertEqual((ep[0]["start"], ep[0]["end"]), (idx[1], idx[2]))


class MeasureTests(unittest.TestCase):
    def test_underwater_counts_the_longest_run_below_the_high(self):
        r = pd.Series([0.1, -0.05, 0.01, 0.01, 0.05, -0.01, 0.02])
        self.assertEqual(ae.underwater(r), 3)

    def test_environments_compare_with_twelve_months_earlier(self):
        idx = _idx(40)
        rising = pd.Series(np.exp(np.linspace(0, 1, 40) ** 2), index=idx)     # accelerating
        env = ae.environments(rising, rising, pd.Series(np.linspace(1, 5, 40), index=idx),
                              pd.Series(np.random.default_rng(1).normal(0, 0.04, 40), index=idx),
                              pd.Series(np.random.default_rng(2).normal(0, 0.01, 40), index=idx))
        self.assertTrue(env["infl_up"].iloc[:24].isna().all())
        self.assertTrue(bool(env["infl_up"].iloc[-1]) and bool(env["rates_up"].iloc[-1]))
        self.assertTrue(env["corr_pos"].iloc[:35].isna().all())


class ChoiceTests(unittest.TestCase):
    def _prof(self, scen, dds):
        c = {k: {"max_dd": dds[k], "worst_12m": -0.2, "underwater": 30, "scenarios": scen[k]} for k in ae.DECISION}
        return {"candidates": c, "regret_scenarios": ["a", "b"]}

    def test_a_small_regret_gain_keeps_the_core(self):
        scen = {"core_now": {"a": 0.10, "b": 0.00}, "core_cash": {"a": 0.09, "b": 0.00},
                "core_bonds": {"a": 0.098, "b": 0.005}, "core_rp": {"a": 0.08, "b": 0.01}}
        ch = ae.choose(self._prof(scen, {k: -0.25 for k in ae.DECISION}), ae.DECISION)
        self.assertEqual(ch["best"], "core_bonds")                 # max regret 0.005 vs core's 0.010
        self.assertEqual(ch["pick"], "core_now")                   # gain 0.5 pp < 1 pp

    def test_a_clear_regret_gain_switches(self):
        scen = {"core_now": {"a": 0.10, "b": -0.05}, "core_cash": {"a": 0.09, "b": 0.00},
                "core_bonds": {"a": 0.095, "b": 0.01}, "core_rp": {"a": 0.08, "b": 0.0}}
        ch = ae.choose(self._prof(scen, {k: -0.25 for k in ae.DECISION}), ae.DECISION)
        self.assertEqual(ch["pick"], "core_bonds")

    def test_an_unacceptable_candidate_neither_wins_nor_sets_the_regret(self):
        scen = {"core_now": {"a": 0.10, "b": 0.00}, "core_cash": {"a": 0.09, "b": 0.00},
                "core_bonds": {"a": 0.095, "b": 0.005}, "core_rp": {"a": 0.50, "b": 0.50}}
        ch = ae.choose(self._prof(scen, {"core_now": -0.25, "core_cash": -0.20, "core_bonds": -0.24, "core_rp": -0.31}), ae.DECISION)
        self.assertNotIn("core_rp", ch["acceptable"])
        self.assertAlmostEqual(ch["regret"]["core_now"]["max"], 0.005)

    def test_final_needs_the_short_sample_too(self):
        long = {"pick": "core_rp"}
        self.assertEqual(ae.final(long, {"acceptable": ["core_now"], "pick": "core_now"})["pick"], "core_now")
        self.assertEqual(ae.final(long, {"acceptable": ["core_now", "core_rp"], "pick": ae.TREND})["pick"], "core_rp")


class ProfileTests(unittest.TestCase):
    def test_regret_is_zero_for_the_best_in_every_scenario(self):
        idx = _idx(120)
        rng = np.random.default_rng(3)
        base = pd.Series(rng.normal(0.006, 0.04, 120), index=idx)
        p = {"core_now": base, "core_cash": base - 0.001, "core_bonds": base + 0.001, "core_rp": base - 0.002}
        env = pd.DataFrame({"infl_up": [i % 2 == 0 for i in range(120)], "growth_up": [i % 3 == 0 for i in range(120)],
                            "rates_up": [True] * 120, "corr_pos": [False] * 120}, index=idx)
        prof = ae.profile(p, pd.Series(0.002, index=idx), idx, env, [], ae.DECISION)
        reg = ae.regrets(prof, list(ae.DECISION))
        self.assertAlmostEqual(reg["core_bonds"]["max"], 0.0)
        self.assertGreater(reg["core_rp"]["max"], reg["core_cash"]["max"])
        self.assertNotIn("熊市（跌 ≥ 20%）", prof["regret_scenarios"])      # no bear months here


if __name__ == "__main__":
    unittest.main()
