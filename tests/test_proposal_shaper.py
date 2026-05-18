import unittest

from services.proposal_shaper import shape_proposal_before_risk


class ProposalShaperTests(unittest.TestCase):
    def test_loss_review_blocks_add_before_risk(self):
        out = shape_proposal_before_risk(
            adjusted_weights={"PSI": 0.08, "CASH": 0.92},
            current_weights={"PSI": 0.03, "CASH": 0.97},
            holdings_meta=[{"ticker": "PSI", "unrealized_pnl_pct": -0.066}],
            market_scorecard={"investment_permission": "normal_rebalance", "data_quality": "fresh"},
            decision_style={},
        )

        self.assertTrue(out["applied"])
        self.assertEqual(out["adjusted_weights"]["PSI"], 0.03)
        self.assertIn("loss_review_no_add:PSI", " ".join(out["clip_log"]))

    def test_human_required_caps_delta_and_turnover(self):
        out = shape_proposal_before_risk(
            adjusted_weights={"SPY": 0.30, "QQQ": 0.30, "CASH": 0.40},
            current_weights={"SPY": 0.10, "QQQ": 0.10, "CASH": 0.80},
            holdings_meta=[],
            market_scorecard={
                "investment_permission": "small_overweight_only",
                "data_quality": "limited",
                "require_human_confirmation": True,
                "max_turnover_per_cycle": 0.10,
            },
            decision_style={"style_limits": {"max_turnover_per_cycle": 0.10}},
        )

        self.assertTrue(out["applied"])
        self.assertLessEqual(out["adjusted_weights"]["SPY"] - 0.10, 0.0151)
        self.assertLessEqual(out["adjusted_weights"]["QQQ"] - 0.10, 0.0151)
        self.assertIn("proposal_add_cap:SPY", " ".join(out["clip_log"]))

    def test_unconstrained_fresh_scorecard_allows_proposal(self):
        out = shape_proposal_before_risk(
            adjusted_weights={"SPY": 0.30, "CASH": 0.70},
            current_weights={"SPY": 0.10, "CASH": 0.90},
            holdings_meta=[],
            market_scorecard={
                "investment_permission": "normal_rebalance",
                "data_quality": "fresh",
                "require_human_confirmation": False,
            },
            decision_style={"trade_style": "normal_rebalance"},
        )

        self.assertFalse(out["applied"])
        self.assertEqual(out["adjusted_weights"]["SPY"], 0.30)


if __name__ == "__main__":
    unittest.main()
