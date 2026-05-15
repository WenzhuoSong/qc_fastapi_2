import unittest
from datetime import date, datetime
import sys
import types
from types import SimpleNamespace

openai_stub = types.ModuleType("openai")
openai_stub.AsyncOpenAI = object
sys.modules.setdefault("openai", openai_stub)

config_stub = types.ModuleType("config")
config_stub.get_settings = lambda: SimpleNamespace(
    openai_api_key="test",
    openai_model="test-model",
)
sys.modules.setdefault("config", config_stub)

try:
    import sqlalchemy  # noqa: F401
except ImportError:
    sqlalchemy_stub = types.ModuleType("sqlalchemy")
    sqlalchemy_stub.select = lambda *args, **kwargs: None
    sqlalchemy_stub.desc = lambda *args, **kwargs: None
    sys.modules.setdefault("sqlalchemy", sqlalchemy_stub)

    sys.modules.setdefault("db", types.ModuleType("db"))

    session_stub = types.ModuleType("db.session")
    session_stub.AsyncSessionLocal = object
    session_stub.Base = object
    sys.modules.setdefault("db.session", session_stub)

    models_stub = types.ModuleType("db.models")
    for name in ("MacroNewsCache", "QCSnapshot", "TickerNewsLibrary"):
        setattr(models_stub, name, type(name, (), {}))
    sys.modules.setdefault("db.models", models_stub)

from services.market_snapshot_merge import _merge_market_snapshots, _normalize_feature_snapshot
from services.playground import (
    _brief_from_snapshot,
    _compute_replay_metrics,
    _dedupe_market_snapshots,
    _max_drawdown,
    _merge_feature_map,
    _replay_metric_reliability,
    _run_one_strategy,
)
from services.sector_rotation import detect_sector_rotation, format_rotation_for_prompt
from services.universe_policy import filter_tradable_research_rows


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
        sources = {item["source"] for item in xlk["feature_sources"]}
        self.assertEqual(sources, {"qc_heartbeat", "qc_daily_snapshot"})
        self.assertTrue(any(row["ticker"] == "XLP" for row in merged["holdings"]))

    def test_feature_snapshot_can_stand_alone_as_holdings(self):
        payload = {"features": [{"ticker": "XLK", "mom_60d": 0.08}]}

        normalized = _normalize_feature_snapshot(payload)

        self.assertEqual(normalized["holdings"][0]["ticker"], "XLK")
        self.assertEqual(normalized["holdings"][0]["mom_60d"], 0.08)
        self.assertEqual(
            normalized["holdings"][0]["feature_sources"][0]["source"],
            "qc_daily_snapshot",
        )

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

    def test_watchlist_and_leveraged_inverse_products_are_not_research_tradable(self):
        rows = [
            {"ticker": "SPY", "universe_role": "core"},
            {"ticker": "DRAM", "universe_role": "watchlist"},
            {"ticker": "SPXS", "universe_role": "watchlist"},
            {"ticker": "SQQQ", "universe_role": "watchlist"},
        ]

        filtered = filter_tradable_research_rows(rows)

        self.assertEqual([row["ticker"] for row in filtered], ["SPY"])

    def test_yfinance_feature_map_fills_strategy_required_fields(self):
        holdings = [{"ticker": "SPY", "mom_20d": None, "mom_60d": None, "mom_252d": None}]
        feature_map = {
            "SPY": {
                "source": "yfinance",
                "trading_date": "2026-05-13",
                "close_price": 100,
                "return_20d": 0.02,
                "return_60d": 0.06,
                "return_252d": 0.18,
                "hist_vol_20d": 0.01,
            }
        }

        enriched = _merge_feature_map(holdings, feature_map)

        self.assertEqual(enriched[0]["mom_20d"], 0.02)
        self.assertEqual(enriched[0]["mom_60d"], 0.06)
        self.assertEqual(enriched[0]["mom_252d"], 0.18)
        self.assertEqual(enriched[0]["hist_vol_20d"], 0.01)
        self.assertEqual(enriched[0]["feature_sources"][0]["source"], "yfinance")

    def test_replay_metrics_suppress_sharpe_until_enough_samples(self):
        snapshots = []
        for i in range(3):
            snapshots.append({
                "packet_type": "daily_feature_snapshot",
                "features": [
                    {
                        "ticker": "SPY",
                        "mom_20d": 0.02,
                        "mom_60d": 0.03,
                        "mom_252d": 0.10,
                        "hist_vol_20d": 0.01,
                        "daily_return_pct": 0.001,
                    }
                ],
                "portfolio": {},
            })

        metrics = _compute_replay_metrics(snapshots, ["momentum_lite_v1"])

        self.assertIsNone(metrics["momentum_lite_v1"]["sharpe"])
        self.assertEqual(
            metrics["momentum_lite_v1"]["metric_reliability"]["level"],
            "insufficient",
        )
        self.assertEqual(
            metrics["momentum_lite_v1"]["selection_guardrail"],
            "Do not select this strategy based on replay performance; sample size is insufficient.",
        )
        self.assertIn("suppressed", metrics["momentum_lite_v1"]["metric_notes"])

    def test_replay_metric_reliability_boundaries(self):
        insufficient = _replay_metric_reliability(
            sample_count=9,
            ic_sample_count=9,
            strategy_ready_samples=9,
        )
        medium = _replay_metric_reliability(
            sample_count=12,
            ic_sample_count=3,
            strategy_ready_samples=12,
        )
        high = _replay_metric_reliability(
            sample_count=31,
            ic_sample_count=10,
            strategy_ready_samples=31,
        )

        self.assertEqual(insufficient["level"], "insufficient")
        self.assertEqual(medium["level"], "medium")
        self.assertIn("ic_samples", medium["reasons"][0])
        self.assertEqual(high["level"], "high")

    def test_max_drawdown_tracks_peak_to_trough_loss(self):
        self.assertAlmostEqual(_max_drawdown([0.10, -0.05, -0.05, 0.02]), 0.0975)

    def test_strategy_result_includes_agent_consumable_explanation(self):
        holdings = [
            {
                "ticker": "SPY",
                "mom_20d": 0.02,
                "mom_60d": 0.05,
                "mom_252d": 0.12,
                "rsi_14": 55,
                "atr_pct": 0.011,
                "hist_vol_20d": 0.14,
            },
            {
                "ticker": "QQQ",
                "mom_20d": 0.04,
                "mom_60d": 0.08,
                "mom_252d": 0.20,
                "rsi_14": 68,
                "atr_pct": 0.018,
                "hist_vol_20d": 0.22,
            },
        ]
        context = {
            "regime": "trending_bull",
            "risk_params": {"max_single_position": 0.20, "min_cash_pct": 0.05},
        }

        result = _run_one_strategy("momentum_lite_v1", holdings, context, {})

        self.assertIn("strategy_card", result.__dict__)
        self.assertEqual(result.strategy_card["family"], "trend_following")
        self.assertTrue(result.agent_interpretation["agent_checks"])
        self.assertIn("turnover", result.risk_profile)
        self.assertTrue(result.data_quality["ready"])

    def test_playground_consensus_discounts_weak_memory_feedback(self):
        holdings = [
            {
                "ticker": "SPY",
                "mom_20d": 0.02,
                "mom_60d": 0.05,
                "mom_252d": 0.12,
                "rsi_14": 55,
                "atr_pct": 0.011,
                "hist_vol_20d": 0.14,
            },
            {
                "ticker": "QQQ",
                "mom_20d": 0.04,
                "mom_60d": 0.08,
                "mom_252d": 0.20,
                "rsi_14": 68,
                "atr_pct": 0.018,
                "hist_vol_20d": 0.22,
            },
        ]
        context = {
            "regime": "trending_bull",
            "risk_params": {"max_single_position": 0.20, "min_cash_pct": 0.05},
        }
        strong = _run_one_strategy(
            "momentum_lite_v1",
            holdings,
            context,
            {},
            memory_feedback={"discount_multiplier": 1.0, "advisory_note": "ok"},
        )
        weak = _run_one_strategy(
            "equal_weight_benchmark",
            holdings,
            context,
            {},
            memory_feedback={"discount_multiplier": 0.5, "advisory_note": "discounted"},
        )

        from services.playground import compute_consensus_weights

        consensus = compute_consensus_weights([strong, weak])

        self.assertEqual(strong.memory_feedback["discount_multiplier"], 1.0)
        self.assertEqual(weak.agent_interpretation["memory_discount_multiplier"], 0.5)
        self.assertAlmostEqual(sum(consensus.values()), 1.0, places=4)


if __name__ == "__main__":
    unittest.main()
