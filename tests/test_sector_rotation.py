import unittest
from datetime import date, datetime
import sys
import types
from types import SimpleNamespace

openai_stub = types.ModuleType("openai")
openai_stub.AsyncOpenAI = object
sys.modules.setdefault("openai", openai_stub)

from services.market_brief import _merge_market_snapshots, _normalize_feature_snapshot
from services.playground import _brief_from_snapshot, _dedupe_market_snapshots
from services.sector_rotation import detect_sector_rotation, format_rotation_for_prompt


class SectorRotationTests(unittest.TestCase):
    def test_detects_risk_on_rotation(self):
        holdings = [
            {"ticker": "XLK", "mom_60d": 0.08, "mom_20d": 0.04, "return_5d": 0.02, "hist_vol_20d": 0.015},
            {"ticker": "XLY", "mom_60d": 0.05, "mom_20d": 0.03, "return_5d": 0.01, "hist_vol_20d": 0.018},
            {"ticker": "XLP", "mom_60d": -0.01, "mom_20d": -0.005, "return_5d": -0.002, "hist_vol_20d": 0.010},
            {"ticker": "XLU", "mom_60d": -0.02, "mom_20d": -0.010, "return_5d": -0.004, "hist_vol_20d": 0.011},
        ]

        result = detect_sector_rotation(holdings)

        self.assertTrue(result["has_signal"])
        self.assertEqual(result["rotation_label"], "risk_on_rotation")
        self.assertEqual(result["leaders"][0]["ticker"], "XLK")
        self.assertGreater(result["risk_appetite_score"], 0.015)

    def test_detects_defensive_rotation_when_safe_havens_lead(self):
        holdings = [
            {"ticker": "TLT", "mom_60d": 0.07, "mom_20d": 0.03, "return_5d": 0.02, "hist_vol_20d": 0.012},
            {"ticker": "GLD", "mom_60d": 0.06, "mom_20d": 0.03, "return_5d": 0.01, "hist_vol_20d": 0.012},
            {"ticker": "XLK", "mom_60d": -0.04, "mom_20d": -0.02, "return_5d": -0.01, "hist_vol_20d": 0.025},
            {"ticker": "XLY", "mom_60d": -0.03, "mom_20d": -0.02, "return_5d": -0.01, "hist_vol_20d": 0.025},
        ]

        result = detect_sector_rotation(holdings)

        self.assertEqual(result["rotation_label"], "defensive_rotation")
        self.assertIn(result["leaders"][0]["ticker"], {"TLT", "GLD"})

    def test_missing_feature_data_degrades_gracefully(self):
        result = detect_sector_rotation([{"ticker": "XLK"}, {"ticker": "CASH"}])

        self.assertFalse(result["has_signal"])
        self.assertEqual(result["rotation_label"], "insufficient_data")
        self.assertIn("insufficient", format_rotation_for_prompt(result))

    def test_market_brief_enriches_heartbeat_with_daily_features(self):
        heartbeat = {
            "packet_type": "heartbeat",
            "holdings": [
                {"ticker": "XLK", "weight_current": 0.2, "mom_60d": 0.01},
            ],
        }
        feature_snapshot = {
            "packet_type": "daily_feature_snapshot",
            "timestamp_utc": "2026-05-13T20:10:00Z",
            "features": [
                {"ticker": "XLK", "volume": 123, "return_5d": 0.02, "mom_60d": 0.08},
                {"ticker": "XLP", "volume": 456, "return_5d": -0.01, "mom_60d": -0.02},
            ],
        }

        merged = _merge_market_snapshots(heartbeat, feature_snapshot)

        xlk = next(row for row in merged["holdings"] if row["ticker"] == "XLK")
        self.assertEqual(xlk["weight_current"], 0.2)
        self.assertEqual(xlk["volume"], 123)
        self.assertEqual(xlk["mom_60d"], 0.01)
        self.assertTrue(any(row["ticker"] == "XLP" for row in merged["holdings"]))

    def test_feature_snapshot_can_stand_alone_as_holdings(self):
        payload = {"features": [{"ticker": "XLK", "mom_60d": 0.08}]}

        normalized = _normalize_feature_snapshot(payload)

        self.assertEqual(normalized["holdings"], payload["features"])

    def test_playground_prefers_daily_feature_snapshot_for_same_day(self):
        heartbeat_row = SimpleNamespace(
            trading_date=date(2026, 5, 13),
            received_at=datetime(2026, 5, 13, 15, 45),
            packet_type="heartbeat",
            raw_payload={"packet_type": "heartbeat", "trading_date": "2026-05-13", "holdings": [{"ticker": "XLK"}]},
        )
        feature_row = SimpleNamespace(
            trading_date=date(2026, 5, 13),
            received_at=datetime(2026, 5, 13, 20, 10),
            packet_type="daily_feature_snapshot",
            raw_payload={
                "packet_type": "daily_feature_snapshot",
                "trading_date": "2026-05-13",
                "features": [{"ticker": "XLK", "return_5d": 0.02, "mom_60d": 0.08}],
            },
        )

        snapshots = _dedupe_market_snapshots([feature_row, heartbeat_row])
        brief = _brief_from_snapshot(snapshots[0])

        self.assertEqual(snapshots[0]["packet_type"], "daily_feature_snapshot")
        self.assertEqual(brief["holdings"][0]["return_5d"], 0.02)
        self.assertTrue(brief["sector_rotation"]["has_signal"])


if __name__ == "__main__":
    unittest.main()
