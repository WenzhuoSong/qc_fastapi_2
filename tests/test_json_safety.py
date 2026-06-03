import json
import unittest
from dataclasses import dataclass
from datetime import datetime

from services.json_safety import json_safe


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


if __name__ == "__main__":
    unittest.main()
