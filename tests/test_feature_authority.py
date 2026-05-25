import unittest

from services.feature_authority import (
    FeatureAuthority,
    authority_for_field,
    canonical_field_aliases,
    canonical_field_name,
    is_authoritative,
    is_canonical_top_level_field,
    legacy_debug_namespace,
)


class FeatureAuthorityTests(unittest.TestCase):
    def test_qc_heartbeat_live_state_is_authoritative(self):
        self.assertEqual(
            authority_for_field("weight_current", "qc_heartbeat"),
            FeatureAuthority.LIVE_STATE,
        )
        self.assertEqual(
            authority_for_field("intraday_open_price", "qc_heartbeat"),
            FeatureAuthority.INTRADAY,
        )
        self.assertEqual(
            authority_for_field("last_trade_time", "qc_heartbeat"),
            FeatureAuthority.LIVE_STATE,
        )
        self.assertTrue(is_authoritative("weight_current", "qc_heartbeat"))

    def test_yfinance_daily_research_is_authoritative(self):
        for field in ["return_20d", "return_60d", "rsi_14", "atr_pct", "hist_vol_20d", "beta_vs_spy"]:
            self.assertEqual(authority_for_field(field, "yfinance"), FeatureAuthority.DAILY_RESEARCH)
            self.assertTrue(is_authoritative(field, "yfinance"))

    def test_qc_momentum_fields_are_legacy_debug_not_canonical(self):
        for field in ["mom_20d", "mom_60d", "mom_252d", "daily_return_pct"]:
            self.assertEqual(authority_for_field(field, "qc_heartbeat"), FeatureAuthority.LEGACY_DEBUG)
            self.assertFalse(is_authoritative(field, "qc_heartbeat"))
            self.assertFalse(is_canonical_top_level_field(field))

    def test_canonical_aliases_freeze_return_field_names(self):
        aliases = canonical_field_aliases()

        self.assertEqual(aliases["mom_20d"], "return_20d")
        self.assertEqual(aliases["mom_60d"], "return_60d")
        self.assertEqual(aliases["mom_252d"], "return_252d")
        self.assertEqual(aliases["daily_return_pct"], "return_1d")
        self.assertEqual(canonical_field_name("mom_60d"), "return_60d")
        self.assertTrue(is_canonical_top_level_field("return_60d"))

    def test_qc_daily_snapshot_is_audit_fallback_for_research_fields(self):
        self.assertEqual(
            authority_for_field("return_60d", "qc_daily_snapshot"),
            FeatureAuthority.QC_EOD_AUDIT,
        )
        self.assertFalse(is_authoritative("return_60d", "qc_daily_snapshot"))

    def test_legacy_debug_namespace_preserves_old_qc_indicators(self):
        row = {
            "ticker": "SPY",
            "mom_60d": 0.08,
            "return_60d": 0.07,
            "weight_current": 0.12,
            "rsi_14": 60.0,
        }

        legacy = legacy_debug_namespace(row)

        self.assertEqual(legacy["mom_60d"], 0.08)
        self.assertEqual(legacy["rsi_14"], 60.0)
        self.assertNotIn("return_60d", legacy)
        self.assertNotIn("weight_current", legacy)


if __name__ == "__main__":
    unittest.main()
