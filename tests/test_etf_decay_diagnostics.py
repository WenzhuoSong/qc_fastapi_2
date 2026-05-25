import unittest
from datetime import date, timedelta

from services.etf_decay_diagnostics import (
    empty_etf_decay_diagnostics,
    evaluate_etf_decay_diagnostics,
    evaluate_etf_decay_diagnostics_from_snapshots,
)


class ETFDecayDiagnosticsTest(unittest.TestCase):
    def test_leveraged_etf_drag_uses_underlying_proxy(self):
        start = date(2026, 1, 1)
        rows = []
        for idx in range(80):
            day = start + timedelta(days=idx)
            qqq_ret = 0.004 if idx % 2 == 0 else -0.003
            rows.append(_row(day, "QQQ", qqq_ret))
            rows.append(_row(day, "TQQQ", 3 * qqq_ret - 0.002))

        out = evaluate_etf_decay_diagnostics(
            historical_return_rows=rows,
            tickers=["TQQQ"],
            asset_profiles={
                "TQQQ": {
                    "asset_class": "leveraged_etf",
                    "role": "leveraged_long",
                    "decay_risk": "high",
                    "max_hold_days": 10,
                    "auto_reduce_after_days": 7,
                }
            },
            min_samples=60,
        )

        self.assertEqual(out["contract_version"], "etf_decay_diagnostics_v1")
        self.assertEqual(out["execution_authority"], "none")
        self.assertEqual(out["target_weight_mutation"], "none")
        row = out["rows"][0]
        self.assertEqual(row["status"], "available")
        self.assertEqual(row["underlying_proxy"], "QQQ")
        self.assertAlmostEqual(row["avg_daily_drag"], -0.002)
        self.assertEqual(row["severity"], "high")
        self.assertTrue(out["high_decay_tickers"])

    def test_volatility_etp_decay_tracks_spy_up_market_drag(self):
        start = date(2026, 1, 1)
        rows = []
        for idx in range(80):
            day = start + timedelta(days=idx)
            spy_ret = 0.003 if idx % 3 else -0.004
            uvxy_ret = -0.012 if spy_ret >= 0 else 0.030
            rows.append(_row(day, "SPY", spy_ret))
            rows.append(_row(day, "UVXY", uvxy_ret))

        out = evaluate_etf_decay_diagnostics(
            historical_return_rows=rows,
            tickers=["UVXY"],
            asset_profiles={
                "UVXY": {
                    "asset_class": "leveraged_volatility_etp",
                    "role": "vol_hedge",
                    "decay_risk": "extreme",
                    "max_hold_days": 10,
                    "auto_reduce_after_days": 7,
                }
            },
            min_samples=60,
        )

        row = out["rows"][0]
        self.assertEqual(row["instrument_type"], "volatility_etp")
        self.assertEqual(row["status"], "available")
        self.assertLess(row["avg_return_when_spy_up"], 0)
        self.assertEqual(row["severity"], "high")
        self.assertGreater(row["spy_up_sample_count"], 0)

    def test_snapshot_adapter_and_empty_contract_are_diagnostics_only(self):
        snapshots = [
            {
                "trading_date": "2026-01-01",
                "holdings": [
                    {"ticker": "QQQ", "return_1d": 0.01},
                    {"ticker": "TQQQ", "return_1d": 0.02},
                ],
            },
            {
                "trading_date": "2026-01-02",
                "holdings": [
                    {"ticker": "QQQ", "return_1d": -0.01},
                    {"ticker": "TQQQ", "return_1d": -0.04},
                ],
            },
        ]

        out = evaluate_etf_decay_diagnostics_from_snapshots(
            snapshots,
            tickers=["TQQQ"],
            min_samples=1,
        )
        empty = empty_etf_decay_diagnostics("missing")

        self.assertEqual(out["rows"][0]["status"], "available")
        self.assertEqual(empty["execution_authority"], "none")
        self.assertEqual(empty["target_weight_mutation"], "none")
        self.assertEqual(empty["status"], "insufficient_data")


def _row(day: date, ticker: str, ret: float) -> dict:
    return {
        "trading_date": day.isoformat(),
        "ticker": ticker,
        "return_1d": ret,
    }


if __name__ == "__main__":
    unittest.main()

