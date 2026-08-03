from datetime import date, datetime
import unittest

try:
    from env_setup import ensure_test_settings
except ImportError:  # pragma: no cover
    from tests.env_setup import ensure_test_settings

ensure_test_settings()

from services.execution_observer import _fill_record, _order_record


class ExecutionObserverTests(unittest.TestCase):
    def _snapshot(self, snapshot_uid: str = "snapshot-a") -> dict:
        return {
            "snapshot_uid": snapshot_uid,
            "strategy_id": "newbase",
            "algorithm_version": "test_version",
            "trading_date": date(2026, 7, 10),
            "recorded_at": datetime(2026, 7, 10, 20, 0),
            "qc_snapshot_id": 123,
        }

    def test_fill_uid_is_stable_across_repeated_snapshot_exports(self):
        fill = {
            "order_id": 42,
            "ticker": "AAPL",
            "direction": "buy",
            "quantity": 2,
            "fill_price": 100,
            "fee": 1,
            "time_utc": "2026-07-10T19:30:00",
        }

        first = _fill_record(fill, live_snapshot_id=1, snapshot_record=self._snapshot("snapshot-a"))
        second = _fill_record(fill, live_snapshot_id=2, snapshot_record=self._snapshot("snapshot-b"))

        self.assertEqual(first["fill_uid"], second["fill_uid"])
        self.assertEqual(first["notional"], 200)
        self.assertEqual(first["fee_bps"], 50)

    def test_sell_fill_has_negative_signed_notional(self):
        fill = {
            "order_id": 43,
            "ticker": "MSFT",
            "direction": "sell",
            "quantity": 3,
            "fill_price": 50,
            "fee": 0.5,
            "time_utc": "2026-07-10T19:30:00",
        }

        record = _fill_record(fill, live_snapshot_id=1, snapshot_record=self._snapshot())

        self.assertEqual(record["notional"], 150)
        self.assertEqual(record["signed_notional"], -150)
        self.assertAlmostEqual(record["fee_bps"], 33.333333333333336)

    def test_order_record_normalizes_ticker_and_direction(self):
        order = {
            "order_id": "abc",
            "ticker": "msft",
            "direction": "Buy",
            "quantity": 4,
            "status": "Filled",
            "time_utc": "2026-07-10T19:30:00",
        }

        record = _order_record(order, live_snapshot_id=1, snapshot_record=self._snapshot())

        self.assertEqual(record["ticker"], "MSFT")
        self.assertEqual(record["direction"], "buy")
        self.assertEqual(record["quantity"], 4)


if __name__ == "__main__":
    unittest.main()
