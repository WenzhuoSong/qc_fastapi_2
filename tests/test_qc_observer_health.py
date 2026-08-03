from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
import unittest

from services.qc_observer_health import (
    classify_observer_receipt,
    format_current_observer_report,
    summarize_observer_receipts,
)


NOW = datetime(2026, 7, 18, 8, 0, tzinfo=timezone.utc)


def _receipt(channel="P0", gaps=None, age=timedelta(hours=1)):
    return SimpleNamespace(
        event_stream_id="account:deployment",
        event_channel=channel,
        highest_contiguous_sequence=4,
        highest_seen_sequence=4 if not gaps else 6,
        missing_sequences=gaps or [],
        last_event_id="evt-4",
        last_event_time=NOW - age,
    )


class QCObserverHealthTests(unittest.TestCase):
    def test_p0_gap_is_audit_incomplete_even_when_fresh(self):
        row = classify_observer_receipt(_receipt(gaps=[5]), now=NOW)
        self.assertEqual(row["status"], "audit_incomplete")

    def test_channel_freshness_is_independent(self):
        self.assertEqual(
            classify_observer_receipt(_receipt("P0", age=timedelta(days=7)), now=NOW)[
                "status"
            ],
            "healthy",
        )
        self.assertEqual(
            classify_observer_receipt(_receipt("P2", age=timedelta(days=2)), now=NOW)[
                "status"
            ],
            "stale",
        )

    def test_empty_and_mixed_summaries_are_explicit(self):
        self.assertEqual(
            summarize_observer_receipts([], now=NOW)["overall_status"],
            "awaiting_first_event",
        )
        summary = summarize_observer_receipts(
            [_receipt("P0"), _receipt("P1", age=timedelta(days=2))], now=NOW
        )
        self.assertEqual(summary["overall_status"], "degraded")
        self.assertEqual(summary["execution_authority"], "none")

    def test_artifact_annotations_are_visible_without_changing_raw_receipts(self):
        summary = summarize_observer_receipts(
            [_receipt("P1"), _receipt("P2")],
            now=NOW,
            artifact_counts={
                ("account:deployment", "P1"): 260,
                ("account:deployment", "P2"): 520,
            },
        )

        self.assertEqual(summary["overall_status"], "healthy")
        self.assertEqual(summary["excluded_artifact_event_count"], 780)
        self.assertEqual(
            [row["excluded_artifact_events"] for row in summary["receipts"]],
            [260, 520],
        )

    def test_operator_report_exposes_current_channels_and_hard_fence(self):
        report = format_current_observer_report(
            {
                "available": True,
                "status": "healthy",
                "strategy_id": "stronger252_moo_band1_paper_v1",
                "deployment_id": "qc34226021-20260718-observer-v1-2",
                "effective_event_count": 31,
                "invalid_event_count": 0,
                "last_event_type": "heartbeat",
                "last_event_time": NOW.isoformat(),
                "event_type_counts": {"order_filled": 8, "decision_manifest": 1},
                "channels": [
                    summarize_observer_receipts([_receipt(channel)], now=NOW)["receipts"][0]
                    for channel in ("P0", "P1", "P2")
                ],
            }
        )

        self.assertIn("QC Observer: healthy", report)
        self.assertIn("P0=4/4 healthy", report)
        self.assertIn("31 effective, 0 invalid", report)
        self.assertIn("Execution authority: NONE (hard fence)", report)


if __name__ == "__main__":
    unittest.main()
