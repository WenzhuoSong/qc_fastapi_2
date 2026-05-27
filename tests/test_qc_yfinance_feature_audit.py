import unittest

from tools.audit_qc_yfinance_features import (
    AUDIT_NAME,
    build_audit_sql,
    build_markdown_report,
    build_sample_sql,
    build_summary,
    detect_heartbeat_daily_feature_lag,
    detect_unit_risk,
)


class QCYFinanceFeatureAuditTests(unittest.TestCase):
    def test_build_audit_sql_compares_qc_and_yfinance_sources(self):
        sql = build_audit_sql(45)

        self.assertIn("qc_snapshots", sql)
        self.assertIn("market_daily_features", sql)
        self.assertIn("m.source = 'yfinance'", sql)
        self.assertIn("q.packet_type IN ('heartbeat', 'daily_feature_snapshot')", sql)
        self.assertIn("qc_mom_60d / 100.0", sql)
        self.assertIn("mae_mom_60d_norm", sql)
        self.assertIn("hedge_levered", sql)
        self.assertIn("schema_version", sql)
        self.assertIn("GROUP BY packet_type, schema_version, ticker_role", sql)

    def test_build_sample_sql_orders_by_raw_divergence(self):
        sql = build_sample_sql(30, 7)

        self.assertIn("LIMIT 7", sql)
        self.assertIn("e_mom20_raw", sql)
        self.assertIn("e_mom20_norm", sql)
        self.assertIn("ORDER BY", sql)

    def test_detect_unit_risk_when_normalization_substantially_improves_error(self):
        row = {
            "packet_type": "daily_feature_snapshot",
            "schema_version": "legacy",
            "ticker_role": "thematic",
            "n_mom_60d": 20,
            "mae_mom_60d": 5.8,
            "maxe_mom_60d": 184.0,
            "mae_mom_60d_norm": 0.03,
        }

        risk = detect_unit_risk(row, "mom_60d")

        self.assertIsNotNone(risk)
        self.assertEqual(risk["field"], "mom_60d")
        self.assertEqual(risk["severity"], "expected_unit_mismatch")
        self.assertEqual(risk["reason"], "qc_return_field_uses_percent_points_but_normalizes_cleanly")

    def test_detect_unit_risk_marks_non_hedge_normalized_drift_as_severe(self):
        row = {
            "packet_type": "daily_feature_snapshot",
            "schema_version": "1.6",
            "ticker_role": "core",
            "n_mom_60d": 20,
            "mae_mom_60d": 5.8,
            "maxe_mom_60d": 184.0,
            "mae_mom_60d_norm": 0.20,
        }

        risk = detect_unit_risk(row, "mom_60d")

        self.assertIsNotNone(risk)
        self.assertEqual(risk["severity"], "normalized_drift")
        self.assertEqual(risk["reason"], "qc_return_field_still_drifts_after_percent_point_normalization")

    def test_build_summary_collects_packet_totals_and_unit_risks(self):
        rows = [
            {
                "packet_type": "daily_feature_snapshot",
                "schema_version": "1.5",
                "ticker_role": "core",
                "joined_rows": 10,
                "n_mom_20d": 10,
                "mae_mom_20d": 3.0,
                "maxe_mom_20d": 120.0,
                "mae_mom_20d_norm": 0.20,
                "maxe_mom_20d_norm": 0.6,
            }
        ]

        summary = build_summary(rows, lookback_days=45)

        self.assertEqual(summary["audit_name"], AUDIT_NAME)
        self.assertEqual(summary["packet_totals"], {"daily_feature_snapshot": 10})
        self.assertEqual(summary["status"], "normalized_drift")
        self.assertEqual(summary["unit_risk_count"], 1)
        self.assertEqual(summary["severe_unit_risk_count"], 1)
        self.assertEqual(summary["daily_snapshot_max_contract_momentum_error"], 0.6)

    def test_heartbeat_daily_feature_lag_is_not_unit_risk(self):
        row = {
            "packet_type": "heartbeat",
            "schema_version": "1.6",
            "ticker_role": "core",
            "joined_rows": 20,
            "n_mom_20d": 20,
            "mae_mom_20d": 0.08,
            "maxe_mom_20d": 0.20,
            "mae_mom_20d_norm": 0.15,
            "maxe_mom_20d_norm": 0.30,
        }

        self.assertIsNone(detect_unit_risk(row, "mom_20d"))
        lag = detect_heartbeat_daily_feature_lag(row)
        self.assertIsNotNone(lag)
        self.assertEqual(lag["reason"], "heartbeat_daily_indicators_may_lag_until_eod_use_daily_feature_snapshot_for_research_parity")

        summary = build_summary([row], lookback_days=5)

        self.assertEqual(summary["status"], "ok")
        self.assertEqual(summary["unit_risk_count"], 0)
        self.assertEqual(summary["heartbeat_lag_class_count"], 1)

    def test_markdown_report_contains_operator_facing_sections(self):
        summary = build_summary(
            [
                {
                    "packet_type": "heartbeat",
                    "schema_version": "1.5",
                    "ticker_role": "core",
                    "joined_rows": 10,
                    "mae_mom_20d": 3.0,
                    "mae_mom_20d_norm": 0.02,
                    "mae_mom_60d": 4.0,
                    "mae_mom_60d_norm": 0.03,
                    "mae_rsi_14": 1.2,
                    "mae_atr_pct": 0.002,
                    "n_mom_20d": 10,
                    "maxe_mom_20d": 120.0,
                }
            ],
            lookback_days=45,
        )

        text = build_markdown_report(summary, samples=[{"ticker": "SPY", "packet_type": "heartbeat"}])

        self.assertIn("# QC vs yfinance Feature Audit", text)
        self.assertIn("## Packet Totals", text)
        self.assertIn("## Summary By Packet And Role", text)
        self.assertIn("## Largest Raw Divergence Samples", text)
        self.assertIn("Expected unit mismatches", text)
        self.assertIn("Max contract momentum error", text)

    def test_levered_etfs_are_reported_as_high_drift_class(self):
        summary = build_summary(
            [
                {
                    "packet_type": "heartbeat",
                    "schema_version": "1.6",
                    "ticker_role": "hedge_levered",
                    "joined_rows": 4,
                    "n_mom_20d": 4,
                    "mae_mom_20d": 0.10,
                    "mae_mom_20d_norm": 0.09,
                }
            ],
            lookback_days=45,
        )

        text = build_markdown_report(summary)

        self.assertEqual(summary["status"], "ok")
        self.assertEqual(summary["high_drift_classes"][0]["ticker_role"], "hedge_levered")
        self.assertIn("## High Drift Classes", text)
        self.assertIn("levered_or_inverse_etf_expected", text)


if __name__ == "__main__":
    unittest.main()
