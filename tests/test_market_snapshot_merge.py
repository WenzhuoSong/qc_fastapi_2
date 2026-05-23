import unittest

from services.market_snapshot_merge import merge_market_snapshots


class MarketSnapshotMergeTests(unittest.TestCase):
    def test_audit_only_keeps_legacy_overlay_active_and_records_shadow_diff(self):
        heartbeat = {
            "packet_type": "heartbeat",
            "holdings": [{"ticker": "XLK", "weight_current": 0.2, "mom_60d": 99.0}],
        }
        qc_daily = {
            "packet_type": "daily_feature_snapshot",
            "features": [{"ticker": "XLK", "mom_60d": 0.08}],
        }
        yfinance = {
            "XLK": {"ticker": "XLK", "return_60d": 0.06, "trading_date": "2026-05-14"}
        }

        merged = merge_market_snapshots(heartbeat, qc_daily, yfinance, mode="audit_only")
        xlk = merged["holdings"][0]

        self.assertEqual(merged["feature_authority_mode"], "audit_only")
        self.assertEqual(xlk["mom_60d"], 99.0)
        self.assertNotIn("return_60d", xlk)
        self.assertGreater(merged["feature_authority_audit"]["diff_count"], 0)
        self.assertEqual(merged["feature_authority_audit"]["shadow_mode"], "yfinance_research")

    def test_legacy_overlay_ignores_yfinance_without_shadow_audit(self):
        merged = merge_market_snapshots(
            {"packet_type": "heartbeat", "holdings": [{"ticker": "SPY", "mom_60d": 99.0}]},
            {"packet_type": "daily_feature_snapshot", "features": [{"ticker": "SPY", "mom_60d": 0.07}]},
            {"SPY": {"ticker": "SPY", "return_60d": 0.05, "trading_date": "2026-05-14"}},
            mode="legacy_overlay",
        )

        spy = merged["holdings"][0]
        self.assertEqual(merged["feature_authority_mode"], "legacy_overlay")
        self.assertEqual(spy["mom_60d"], 99.0)
        self.assertNotIn("return_60d", spy)
        self.assertNotIn("feature_authority_audit", merged)

    def test_yfinance_research_overrides_qc_legacy_and_live_state_wins(self):
        heartbeat = {
            "packet_type": "heartbeat",
            "schema_version": "1.5",
            "holdings": [
                {
                    "ticker": "XLK",
                    "price": 210.0,
                    "weight_current": 0.2,
                    "mom_60d": 99.0,
                    "intraday_open_price": 209.0,
                }
            ],
        }
        qc_daily = {
            "packet_type": "daily_feature_snapshot",
            "features": [
                {
                    "ticker": "XLK",
                    "mom_60d": 0.08,
                    "open_price": 200.0,
                    "high_price": 205.0,
                    "low_price": 198.0,
                    "volume": 1000,
                }
            ],
        }
        yfinance = {
            "XLK": {
                "ticker": "XLK",
                "return_60d": 0.06,
                "open_price": 201.0,
                "high_price": 206.0,
                "low_price": 199.0,
                "volume": 2000,
                "trading_date": "2026-05-14",
            }
        }

        merged = merge_market_snapshots(heartbeat, qc_daily, yfinance)
        xlk = merged["holdings"][0]

        self.assertEqual(xlk["weight_current"], 0.2)
        self.assertEqual(xlk["price"], 210.0)
        self.assertEqual(xlk["intraday_open_price"], 209.0)
        self.assertEqual(xlk["return_60d"], 0.06)
        self.assertEqual(xlk["open_price"], 201.0)
        self.assertEqual(xlk["high_price"], 206.0)
        self.assertEqual(xlk["low_price"], 199.0)
        self.assertEqual(xlk["volume"], 2000)
        self.assertNotIn("mom_60d", xlk)
        self.assertEqual(xlk["legacy_qc_indicators"]["mom_60d"], 99.0)

    def test_missing_yfinance_uses_qc_daily_fallback_and_marks_capability(self):
        heartbeat = {"packet_type": "heartbeat", "holdings": [{"ticker": "SPY", "weight_current": 0.1}]}
        qc_daily = {
            "packet_type": "daily_feature_snapshot",
            "features": [{"ticker": "SPY", "mom_60d": 0.07, "rsi_14": 61.0}],
        }

        merged = merge_market_snapshots(heartbeat, qc_daily, {})
        spy = merged["holdings"][0]

        self.assertEqual(spy["return_60d"], 0.07)
        self.assertEqual(spy["rsi_14"], 61.0)
        self.assertEqual(spy["legacy_qc_indicators"]["mom_60d"], 0.07)
        self.assertEqual(merged["schema_capabilities"]["daily_research_authority"], "qc_daily_fallback")

    def test_stale_yfinance_does_not_override_qc_daily_fallback(self):
        heartbeat = {"packet_type": "heartbeat", "holdings": [{"ticker": "SPY", "weight_current": 0.1}]}
        qc_daily = {
            "packet_type": "daily_feature_snapshot",
            "features": [{"ticker": "SPY", "mom_60d": 0.07, "open_price": 620.0}],
        }
        yfinance = {
            "SPY": {
                "ticker": "SPY",
                "return_60d": 0.99,
                "open_price": 999.0,
                "data_quality_flag": "stale",
                "trading_date": "2026-05-01",
            }
        }

        merged = merge_market_snapshots(heartbeat, qc_daily, yfinance)
        spy = merged["holdings"][0]

        self.assertEqual(spy["return_60d"], 0.07)
        self.assertEqual(spy["open_price"], 620.0)
        self.assertEqual(merged["schema_capabilities"]["daily_research_authority"], "qc_daily_fallback")
        self.assertEqual(merged["stale_yfinance_tickers"], ["SPY"])

    def test_intraday_fields_never_overwrite_daily_ohlcv(self):
        heartbeat = {
            "packet_type": "heartbeat",
            "schema_version": "1.5",
            "holdings": [
                {
                    "ticker": "SPY",
                    "intraday_open_price": 630.0,
                    "intraday_high_price": 635.0,
                    "intraday_low_price": 628.0,
                }
            ],
        }
        yfinance = {
            "SPY": {
                "ticker": "SPY",
                "open_price": 620.0,
                "high_price": 625.0,
                "low_price": 619.0,
                "trading_date": "2026-05-14",
            }
        }

        merged = merge_market_snapshots(heartbeat, {}, yfinance)
        spy = merged["holdings"][0]

        self.assertEqual(spy["open_price"], 620.0)
        self.assertEqual(spy["high_price"], 625.0)
        self.assertEqual(spy["low_price"], 619.0)
        self.assertEqual(spy["intraday_open_price"], 630.0)
        self.assertEqual(spy["intraday_high_price"], 635.0)
        self.assertEqual(spy["intraday_low_price"], 628.0)

    def test_canonical_top_level_excludes_old_momentum_fields(self):
        merged = merge_market_snapshots(
            {"packet_type": "heartbeat", "holdings": [{"ticker": "QQQ", "mom_20d": 0.01, "mom_60d": 0.02, "mom_252d": 0.03}]},
            {},
            {},
        )

        qqq = merged["holdings"][0]
        self.assertNotIn("mom_20d", qqq)
        self.assertNotIn("mom_60d", qqq)
        self.assertNotIn("mom_252d", qqq)
        self.assertEqual(qqq["legacy_qc_indicators"]["mom_60d"], 0.02)

    def test_old_schema_heartbeat_marks_intraday_partial_or_unavailable(self):
        partial = merge_market_snapshots(
            {"packet_type": "heartbeat", "schema_version": "1.4", "holdings": [{"ticker": "SPY", "price": 630.0}]},
            {},
            {},
        )
        unavailable = merge_market_snapshots(
            {"packet_type": "heartbeat", "holdings": [{"ticker": "SPY"}]},
            {},
            {},
        )

        self.assertEqual(partial["schema_capabilities"]["intraday_live_state"], "partial")
        self.assertEqual(unavailable["schema_capabilities"]["heartbeat_schema_version"], "legacy")
        self.assertEqual(unavailable["schema_capabilities"]["intraday_live_state"], "unavailable")


if __name__ == "__main__":
    unittest.main()
