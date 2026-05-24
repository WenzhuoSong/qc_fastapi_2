import re
import unittest
from pathlib import Path


QC_ALGO_PATH = Path(__file__).resolve().parents[2] / "quantconnect_files" / "test1.py"


class QCHeartbeatSchemaTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = QC_ALGO_PATH.read_text(encoding="utf-8")

    def test_qc_heartbeat_schema_is_v16(self):
        self.assertRegex(self.source, r'SCHEMA_VERSION\s*=\s*"1\.6"')

    def test_qc_heartbeat_exports_required_intraday_fields(self):
        for field in [
            "last_price",
            "intraday_open_price",
            "intraday_high_price",
            "intraday_low_price",
            "intraday_volume",
            "intraday_return_pct",
            "last_trade_time",
        ]:
            self.assertIn(f'"{field}"', self.source)

    def test_intraday_fields_are_maintained_separately_from_daily_ohlcv(self):
        intraday_method = re.search(
            r"def _intraday_fields\(.*?\n    def _bar_value",
            self.source,
            flags=re.S,
        )
        self.assertIsNotNone(intraday_method)
        body = intraday_method.group(0)

        self.assertIn('"intraday_open_price"', body)
        self.assertNotIn('"open_price"', body)
        self.assertIn('"intraday_high_price"', body)
        self.assertNotIn('"high_price"', body)
        self.assertIn('"intraday_low_price"', body)
        self.assertNotIn('"low_price"', body)

    def test_portfolio_exports_market_session_fields(self):
        for field in ["is_market_open", "minutes_since_open"]:
            self.assertIn(f'"{field}"', self.source)

    def test_heartbeat_exports_account_state_contract(self):
        for field in [
            '"account_state"',
            '"contract_version"',
            '"buying_power"',
            '"open_order_count"',
            '"has_open_orders"',
            '"policy_version"',
            '"holdings_weights"',
            '"target_weights"',
        ]:
            self.assertIn(field, self.source)

    def test_ack_exports_account_state_contract(self):
        for field in ['"actual_holdings_weights"', '"account_state"']:
            self.assertIn(field, self.source)


if __name__ == "__main__":
    unittest.main()
