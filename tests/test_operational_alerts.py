import unittest
from datetime import datetime, timedelta

from services.operational_alerts import (
    evaluate_operational_alerts,
    format_operational_alert_message,
)


class OperationalAlertsTests(unittest.TestCase):
    def test_emits_stale_data_and_cron_alerts(self):
        now = datetime(2026, 5, 19, 14, 0, 0)
        snapshot = {
            "checks": {
                "qc_heartbeat": {
                    "label": "QC heartbeat",
                    "state": "stale",
                    "reason": "stale 3.0h > 2h",
                    "blocking": True,
                },
                "daily_feature_snapshot": {"label": "Daily features", "state": "ok"},
                "yfinance_backfill": {
                    "label": "YFinance backfill",
                    "state": "stale",
                    "reason": "stale 40.0h > 36h",
                },
                "pipeline_status": {"status": "success"},
            },
            "failed_crons": [
                {
                    "job_name": "yfinance_backfill",
                    "started_at": "2026-05-19T13:00:00",
                    "error_message": "fetch failed",
                }
            ],
        }

        alerts, state = evaluate_operational_alerts(snapshot, now=now)

        self.assertEqual(len(alerts), 3)
        self.assertIn("qc_heartbeat", state["alerts"])
        self.assertEqual(state["alerts"]["qc_heartbeat"]["level"], "critical")
        self.assertIn("cron_failed:yfinance_backfill", state["alerts"])

    def test_suppresses_same_alert_inside_cooldown(self):
        now = datetime(2026, 5, 19, 14, 0, 0)
        snapshot = {
            "checks": {
                "qc_heartbeat": {
                    "label": "QC heartbeat",
                    "state": "stale",
                    "reason": "stale 3.0h > 2h",
                    "blocking": True,
                },
            },
            "failed_crons": [],
        }
        previous = {
            "alerts": {
                "qc_heartbeat": {
                    "fingerprint": "qc_heartbeat:stale:stale 3.0h > 2h",
                    "last_sent_at": (now - timedelta(hours=1)).isoformat(),
                }
            }
        }

        alerts, state = evaluate_operational_alerts(snapshot, previous, now=now, cooldown_hours=6)

        self.assertEqual(alerts, [])
        self.assertEqual(
            state["alerts"]["qc_heartbeat"]["last_sent_at"],
            previous["alerts"]["qc_heartbeat"]["last_sent_at"],
        )

    def test_sends_changed_alert_inside_cooldown(self):
        now = datetime(2026, 5, 19, 14, 0, 0)
        snapshot = {
            "checks": {},
            "failed_crons": [
                {
                    "job_name": "pipeline",
                    "started_at": "2026-05-19T13:30:00",
                    "error_message": "new failure",
                }
            ],
        }
        previous = {
            "alerts": {
                "cron_failed:pipeline": {
                    "fingerprint": "cron_failed:pipeline:2026-05-19T12:00:00:old failure",
                    "last_sent_at": (now - timedelta(hours=1)).isoformat(),
                }
            }
        }

        alerts, _ = evaluate_operational_alerts(snapshot, previous, now=now, cooldown_hours=6)

        self.assertEqual(len(alerts), 1)
        self.assertIn("new failure", alerts[0]["detail"])

    def test_includes_execution_failure_when_snapshot_provides_execution(self):
        snapshot = {
            "checks": {},
            "failed_crons": [],
            "execution": {
                "available": True,
                "analysis_id": 123,
                "status": "failed",
                "executed_at": "2026-05-19T13:30:00",
            },
        }

        alerts, _ = evaluate_operational_alerts(snapshot, now=datetime(2026, 5, 19, 14, 0, 0))

        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]["key"], "execution_status")
        self.assertEqual(alerts[0]["level"], "critical")

    def test_formats_compact_telegram_message(self):
        message = format_operational_alert_message([
            {"level": "critical", "detail": "QC heartbeat: stale"},
            {"level": "warning", "detail": "Cron yfinance_backfill: failed"},
        ])

        self.assertIn("Ops alert: critical", message)
        self.assertIn("QC heartbeat", message)
        self.assertLessEqual(len(message.splitlines()), 9)


if __name__ == "__main__":
    unittest.main()
