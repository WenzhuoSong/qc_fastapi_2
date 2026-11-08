import unittest

import numpy as np
import pandas as pd

from scout import opportunities as o


def returns() -> pd.DataFrame:
    rng = np.random.default_rng(7)
    oil, tech = rng.normal(0, 0.02, 80), rng.normal(0, 0.02, 80)
    noise = lambda: rng.normal(0, 0.005, 80)  # noqa: E731
    return pd.DataFrame({"DVN": oil + noise(), "PR": oil + noise(), "CHRD": oil + noise(), "PBF": oil + noise(),
                         "CRWD": tech + noise(), "IDXX": rng.normal(0, 0.02, 80)})


class SameBetTests(unittest.TestCase):
    def test_cards_moving_together_form_one_group_led_by_the_best_score(self):
        bets = o.same_bets([("DVN", 0.43), ("PR", 0.42), ("CHRD", 0.53), ("IDXX", 0.56)], returns(), {"PBF": "持仓"})
        self.assertEqual(bets["DVN"]["group"], ["CHRD", "DVN", "PR"])          # by score
        self.assertEqual({bets[t]["lead"] for t in ("DVN", "PR", "CHRD")}, {"CHRD"})
        self.assertNotIn("IDXX", bets)
        self.assertEqual([x["ticker"] for x in bets["PR"]["links"]], ["PBF"])

    def test_a_lone_card_tied_to_a_holding_is_kept_with_the_link(self):
        bets = o.same_bets([("CRWD", 0.7)], returns(), {"PBF": "持仓"})
        self.assertNotIn("CRWD", bets)
        bets = o.same_bets([("DVN", 0.4)], returns(), {"PBF": "持仓", "CHRD": "机会卡"})
        self.assertEqual(bets["DVN"]["lead"], "DVN")
        text = o.same_bet_text(bets["DVN"], "DVN")
        self.assertIn("和你持有的 PBF", text)
        self.assertIn("和进行中的机会卡 CHRD", text)

    def test_texts(self):
        bets = o.same_bets([("DVN", 0.43), ("PR", 0.42), ("CHRD", 0.53)], returns(), {})
        self.assertIn("以评分最高的 CHRD 为代表", o.same_bet_text(bets["PR"], "PR"))
        self.assertIn("它是其中评分最高的", o.same_bet_text(bets["CHRD"], "CHRD"))
        self.assertEqual(o.same_bet_summary(bets),
                         "今天的机会卡里有 2 张和别的卡同涨同跌，实际是 1 组同一个赌注：CHRD、DVN、PR（每组只推送评分最高的一张）")
        self.assertIsNone(o.same_bet_summary({}))

    def test_no_chains_a_card_joins_a_leader_it_moves_with_directly(self):
        rng = np.random.default_rng(3)
        a, b = rng.normal(0, 0.02, 80), rng.normal(0, 0.02, 80)
        r = pd.DataFrame({"A": a, "AB": a + b, "B": b})            # A ~ AB ~ B, but A and B unrelated
        bets = o.same_bets([("A", 0.9), ("AB", 0.8), ("B", 0.7)], r, {})
        self.assertEqual(bets["AB"]["lead"], "A")
        self.assertNotIn("B", bets)
        self.assertIsNone(o.same_bet_text(None, "X"))


if __name__ == "__main__":
    unittest.main()
