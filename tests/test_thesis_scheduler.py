import unittest
from datetime import UTC, datetime, timedelta

from services.thesis_scheduler import build_thesis_review_queue, get_review_required


class ThesisSchedulerTests(unittest.TestCase):
    def test_loss_review_requires_daily_review(self):
        review = get_review_required(
            ticker="FTXL",
            position_state="loss_review",
            thesis_status="weakening",
            last_thesis_review_at=datetime(2026, 5, 19, tzinfo=UTC),
            now=datetime(2026, 5, 20, tzinfo=UTC),
        ).to_dict()

        self.assertTrue(review["required"])
        self.assertEqual(review["reason"], "position_state_loss_review_requires_daily_review")
        self.assertEqual(review["execution_authority"], "none")

    def test_never_reviewed_requires_review(self):
        review = get_review_required(
            ticker="SPY",
            position_state="normal_hold",
            thesis_status="intact",
            now=datetime(2026, 5, 20, tzinfo=UTC),
        ).to_dict()

        self.assertTrue(review["required"])
        self.assertEqual(review["reason"], "never_reviewed")

    def test_scheduled_review_after_five_days(self):
        now = datetime(2026, 5, 20, tzinfo=UTC)
        review = get_review_required(
            ticker="SPY",
            position_state="normal_hold",
            thesis_status="intact",
            last_thesis_review_at=now - timedelta(days=5),
            now=now,
        ).to_dict()

        self.assertTrue(review["required"])
        self.assertEqual(review["reason"], "scheduled_review_5d_elapsed")

    def test_pnl_change_triggers_review_before_schedule(self):
        now = datetime(2026, 5, 20, tzinfo=UTC)
        review = get_review_required(
            ticker="QQQ",
            position_state="normal_hold",
            thesis_status="intact",
            last_thesis_review_at=now - timedelta(days=1),
            current_pnl_pct=0.05,
            pnl_at_last_review=0.01,
            now=now,
        ).to_dict()

        self.assertTrue(review["required"])
        self.assertEqual(review["reason"], "pnl_change_4.0%_triggers_review")
        self.assertAlmostEqual(review["pnl_change_since_review"], 0.04)

    def test_recent_stable_review_does_not_trigger(self):
        now = datetime(2026, 5, 20, tzinfo=UTC)
        review = get_review_required(
            ticker="SPY",
            position_state="normal_hold",
            thesis_status="intact",
            last_thesis_review_at=now - timedelta(days=2),
            current_pnl_pct=0.02,
            pnl_at_last_review=0.01,
            now=now,
        ).to_dict()

        self.assertFalse(review["required"])
        self.assertEqual(review["reason"], "no_review_needed")

    def test_build_queue_includes_structured_review_input(self):
        queue = build_thesis_review_queue(
            [
                {
                    "ticker": "FTXL",
                    "reason_codes": ["unrealized_loss_review", "basket_review"],
                    "decision": "hold_review",
                    "unrealized_pnl_pct": -0.06,
                    "holding_days": 18,
                    "basket_review": {"group": "semiconductors"},
                    "strategy_support": "advisory",
                    "thesis_status": {"status": "weakening", "evidence": ["basket_review"]},
                }
            ],
            now=datetime(2026, 5, 20, tzinfo=UTC),
        )

        self.assertEqual(queue[0]["ticker"], "FTXL")
        self.assertTrue(queue[0]["required"])
        self.assertEqual(queue[0]["review_input"]["review_purpose"], "thesis_review")
        self.assertEqual(queue[0]["review_input"]["execution_authority"], "none")


if __name__ == "__main__":
    unittest.main()
