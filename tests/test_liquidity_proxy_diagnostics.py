import unittest
from datetime import date, timedelta

from services.liquidity_proxy_diagnostics import (
    empty_liquidity_proxy_diagnostics,
    evaluate_liquidity_proxy_diagnostics,
    evaluate_liquidity_proxy_diagnostics_from_snapshots,
)


class LiquidityProxyDiagnosticsTest(unittest.TestCase):
    def test_high_dollar_volume_etf_is_robust(self):
        start = date(2026, 1, 1)
        rows = [
            _row(
                start + timedelta(days=idx),
                "SPY",
                close=500.0 + idx * 0.1,
                volume=15_000_000,
                high=502.0 + idx * 0.1,
                low=498.5 + idx * 0.1,
                open_price=500.2 + idx * 0.1,
                ret=0.002,
                atr_pct=0.010,
            )
            for idx in range(80)
        ]

        out = evaluate_liquidity_proxy_diagnostics(
            historical_feature_rows=rows,
            tickers=["SPY"],
            asset_profiles={"SPY": {"asset_class": "core_equity_etf", "role": "core"}},
            min_samples=60,
        )

        self.assertEqual(out["contract_version"], "liquidity_proxy_diagnostics_v1")
        self.assertEqual(out["execution_authority"], "none")
        self.assertEqual(out["target_weight_mutation"], "none")
        row = out["rows"][0]
        self.assertEqual(row["status"], "available")
        self.assertEqual(row["liquidity_bucket"], "mega_liquid")
        self.assertEqual(row["execution_quality"], "robust")
        self.assertLess(row["spread_cost_proxy_pct"], 0.0015)
        self.assertFalse(out["low_liquidity_tickers"])

    def test_thin_high_range_etf_defers_weak_signals(self):
        start = date(2026, 1, 1)
        rows = [
            _row(
                start + timedelta(days=idx),
                "THIN",
                close=20.0 + idx * 0.01,
                volume=400_000,
                high=20.8 + idx * 0.01,
                low=19.2 + idx * 0.01,
                open_price=20.6 + idx * 0.01,
                ret=0.025 if idx % 2 == 0 else -0.020,
                atr_pct=0.045,
            )
            for idx in range(80)
        ]

        out = evaluate_liquidity_proxy_diagnostics(
            historical_feature_rows=rows,
            tickers=["THIN"],
            asset_profiles={"THIN": {"asset_class": "thin_etf", "role": "thematic"}},
            min_samples=60,
        )

        row = out["rows"][0]
        self.assertEqual(row["status"], "available")
        self.assertEqual(row["liquidity_bucket"], "illiquid")
        self.assertEqual(row["execution_quality"], "no_trade_review")
        self.assertGreaterEqual(row["spread_cost_proxy_pct"], 0.003)
        self.assertTrue(out["low_liquidity_tickers"])
        self.assertTrue(out["execution_review_rows"])
        self.assertTrue(any("low_liquidity:THIN" in item for item in out["warnings"]))

    def test_snapshot_adapter_and_empty_contract_are_diagnostics_only(self):
        snapshots = [
            {
                "trading_date": "2026-01-01",
                "holdings": [
                    {
                        "ticker": "SPY",
                        "close_price": 500.0,
                        "high_price": 502.0,
                        "low_price": 499.0,
                        "open_price": 501.0,
                        "volume": 10_000_000,
                        "dollar_volume": 5_000_000_000.0,
                        "return_1d": 0.002,
                        "atr_pct": 0.01,
                    }
                ],
            }
        ]

        out = evaluate_liquidity_proxy_diagnostics_from_snapshots(
            snapshots,
            tickers=["SPY"],
            min_samples=1,
        )
        empty = empty_liquidity_proxy_diagnostics("missing")

        self.assertEqual(out["rows"][0]["status"], "available")
        self.assertEqual(empty["execution_authority"], "none")
        self.assertEqual(empty["target_weight_mutation"], "none")
        self.assertEqual(empty["status"], "insufficient_data")


def _row(
    day: date,
    ticker: str,
    *,
    close: float,
    volume: int,
    high: float,
    low: float,
    open_price: float,
    ret: float,
    atr_pct: float,
) -> dict:
    return {
        "trading_date": day.isoformat(),
        "ticker": ticker,
        "close_price": close,
        "open_price": open_price,
        "high_price": high,
        "low_price": low,
        "volume": volume,
        "dollar_volume": close * volume,
        "return_1d": ret,
        "atr_pct": atr_pct,
    }


if __name__ == "__main__":
    unittest.main()
