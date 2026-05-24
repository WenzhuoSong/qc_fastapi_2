from datetime import UTC, datetime
import unittest

from services.command_lifecycle import build_command_lifecycle_event


class CommandLifecycleTests(unittest.TestCase):
    def test_build_command_lifecycle_event_normalizes_time_and_payload(self):
        event = build_command_lifecycle_event(
            command_id=" cmd_1 ",
            analysis_id=12,
            event_type="submitted_to_qc",
            event_status="submitted",
            source="fastapi",
            payload={"weights": {"SPY": 0.2}},
            event_time=datetime(2026, 5, 24, 10, 0, tzinfo=UTC),
        )

        self.assertEqual(event["command_id"], "cmd_1")
        self.assertEqual(event["analysis_id"], 12)
        self.assertEqual(event["event_type"], "submitted_to_qc")
        self.assertEqual(event["event_status"], "submitted")
        self.assertIsNone(event["event_time"].tzinfo)
        self.assertEqual(event["payload"]["weights"]["SPY"], 0.2)

    def test_build_command_lifecycle_event_rejects_unknown_event_type(self):
        with self.assertRaises(ValueError):
            build_command_lifecycle_event(command_id="cmd_1", event_type="surprise")

    def test_build_command_lifecycle_event_requires_command_id(self):
        with self.assertRaises(ValueError):
            build_command_lifecycle_event(command_id="", event_type="created")


if __name__ == "__main__":
    unittest.main()
