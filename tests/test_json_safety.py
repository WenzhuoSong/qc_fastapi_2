import json
import unittest
from dataclasses import dataclass
from datetime import datetime

from services.json_safety import json_safe
from services.command_lifecycle import build_command_lifecycle_event


@dataclass
class SamplePayload:
    tickers: set[str]


class JsonSafetyTest(unittest.TestCase):
    def test_converts_python_only_containers_to_json_safe_values(self):
        payload = {
            "set_value": {"QQQ", "SPY"},
            "frozenset_value": frozenset({"XLE", "XLK"}),
            "tuple_value": ("A", {"B"}),
            "dataclass_value": SamplePayload(tickers={"DRAM"}),
            "datetime_value": datetime(2026, 6, 3, 13, 55, 0),
        }

        safe = json_safe(payload)

        self.assertEqual(safe["set_value"], ["QQQ", "SPY"])
        self.assertEqual(safe["frozenset_value"], ["XLE", "XLK"])
        self.assertEqual(safe["tuple_value"], ["A", ["B"]])
        self.assertEqual(safe["dataclass_value"], {"tickers": ["DRAM"]})
        self.assertEqual(safe["datetime_value"], "2026-06-03T13:55:00")
        json.dumps(safe)

    def test_command_lifecycle_event_payload_is_json_safe(self):
        event = build_command_lifecycle_event(
            command_id="analysis_1",
            event_type="execution_result",
            payload={"blockers": {"daily_command_count_ok", "daily_gross_turnover_ok"}},
        )

        self.assertEqual(
            event["payload"]["blockers"],
            ["daily_command_count_ok", "daily_gross_turnover_ok"],
        )
        json.dumps(event["payload"])


if __name__ == "__main__":
    unittest.main()
