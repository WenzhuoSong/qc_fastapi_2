import unittest

from services.hedge_intent_outcome_log import (
    backfill_hedge_intent_outcome,
    build_hedge_intent_outcome_record,
    summarize_hedge_threshold_assessments,
)


class HedgeIntentOutcomeLogTests(unittest.TestCase):
    def test_records_non_triggered_decision(self):
        record = build_hedge_intent_outcome_record(
            hedge_intent={"triggered": False, "severity": 0.0},
            market_context={"regime": "bull", "vix": 18.0, "breadth_pct": 0.62},
            current_weights={"QQQ": 0.12, "CASH": 0.88},
            as_of="2026-06-05",
        )

        self.assertEqual(record["report_version"], "hedge_intent_outcome_v1")
        self.assertEqual(record["execution_authority"], "none")
        self.assertEqual(record["target_weight_mutation"], "none")
        self.assertEqual(record["date"], "2026-06-05")
        self.assertFalse(record["triggered"])
        self.assertEqual(record["why_not_add_hedge"], "hedge_intent_not_triggered")
        self.assertEqual(record["candidate_hedge_instrument"], "PSQ")
        self.assertEqual(record["outcome_status"], "pending_t5")

    def test_records_triggered_no_hedge_reason(self):
        record = build_hedge_intent_outcome_record(
            hedge_intent={
                "triggered": True,
                "severity": 0.52,
                "add_hedge_etf": False,
                "trim_targets": ["QQQ", "XLK"],
                "target_cash_raise_pct": 0.05,
                "trigger_reasons": ["weak_breadth"],
            },
            market_context={"regime": "defensive", "vix": 27.2, "breadth_pct": 0.38},
            current_weights={"QQQ": 0.10, "XLK": 0.09, "CASH": 0.81},
            as_of="2026-06-05",
        )

        self.assertTrue(record["triggered"])
        self.assertFalse(record["add_hedge_etf"])
        self.assertEqual(record["why_not_add_hedge"], "severity_0.52_below_threshold_0.70")
        self.assertEqual(record["trim_targets"], ["QQQ", "XLK"])
        self.assertEqual(record["cash_raise_pct"], 0.05)

    def test_backfill_not_triggered_market_drop_marks_too_conservative(self):
        record = build_hedge_intent_outcome_record(
            hedge_intent={"triggered": False},
            current_weights={"SPY": 0.40, "CASH": 0.60},
            as_of="2026-06-05",
        )

        out = backfill_hedge_intent_outcome(
            record,
            spy_return_5d=-0.031,
            hedge_instrument_return_5d=0.030,
            outcome_date="2026-06-12",
        )

        self.assertEqual(out["outcome_status"], "completed_t5")
        self.assertEqual(out["threshold_assessment"], "too_conservative")
        self.assertTrue(out["hedge_would_have_helped"])

    def test_backfill_triggered_no_hedge_deep_drop_marks_threshold_too_high(self):
        record = build_hedge_intent_outcome_record(
            hedge_intent={"triggered": True, "severity": 0.55, "add_hedge_etf": False},
            current_weights={"SPY": 0.40, "CASH": 0.60},
            as_of="2026-06-05",
        )

        out = backfill_hedge_intent_outcome(record, spy_return_5d=-0.052, hedge_instrument_return_5d=0.050)

        self.assertEqual(out["threshold_assessment"], "severity_threshold_too_high")

    def test_backfill_added_hedge_market_rally_marks_too_aggressive_and_idempotent(self):
        record = build_hedge_intent_outcome_record(
            hedge_intent={
                "triggered": True,
                "severity": 0.75,
                "add_hedge_etf": True,
                "hedge_instrument": "SH",
            },
            current_weights={"SPY": 0.40, "CASH": 0.60},
            as_of="2026-06-05",
        )

        out = backfill_hedge_intent_outcome(record, spy_return_5d=0.021, hedge_instrument_return_5d=-0.020)
        repeated = backfill_hedge_intent_outcome(out, spy_return_5d=-0.10, hedge_instrument_return_5d=0.10)

        self.assertEqual(out["threshold_assessment"], "too_aggressive")
        self.assertEqual(out, repeated)

    def test_summarizes_recent_assessments(self):
        rows = [
            {"outcome_status": "completed_t5", "threshold_assessment": "too_conservative", "date": "2026-06-01"},
            {"outcome_status": "pending_t5", "date": "2026-06-02"},
        ]

        summary = summarize_hedge_threshold_assessments(rows)

        self.assertEqual(summary["assessment_counts"]["too_conservative"], 1)
        self.assertEqual(summary["assessment_counts"]["pending"], 1)
        self.assertEqual(summary["pending_count"], 1)


if __name__ == "__main__":
    unittest.main()
