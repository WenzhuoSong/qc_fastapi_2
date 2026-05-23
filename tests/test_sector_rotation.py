import importlib
import unittest
from datetime import date, datetime
import sys
import types
from types import SimpleNamespace
from unittest.mock import patch


def _load_sector_rotation_exports():
    openai_stub = type(sys)("openai")
    openai_stub.AsyncOpenAI = object

    config_stub = type(sys)("config")
    config_stub.get_settings = lambda: SimpleNamespace(
        openai_api_key="test",
        openai_model="test-model",
    )

    sqlalchemy_stub = type(sys)("sqlalchemy")
    sqlalchemy_stub.select = lambda *args, **kwargs: None
    sqlalchemy_stub.desc = lambda *args, **kwargs: None

    session_stub = type(sys)("db.session")
    session_stub.AsyncSessionLocal = object
    session_stub.Base = object

    models_stub = type(sys)("db.models")
    for name in ("MacroNewsCache", "MarketDailyFeature", "QCSnapshot", "TickerNewsLibrary"):
        setattr(models_stub, name, type(name, (), {}))

    with patch.dict(
        "sys.modules",
        {
            "openai": openai_stub,
            "config": config_stub,
            "sqlalchemy": sqlalchemy_stub,
            "db": type(sys)("db"),
            "db.session": session_stub,
            "db.models": models_stub,
        },
    ):
        market_snapshot_merge = importlib.import_module("services.market_snapshot_merge")
        playground = importlib.import_module("services.playground")
        sector_rotation = importlib.import_module("services.sector_rotation")
        universe_policy = importlib.import_module("services.universe_policy")
        return {
            "_merge_market_snapshots": market_snapshot_merge._merge_market_snapshots,
            "_normalize_feature_snapshot": market_snapshot_merge._normalize_feature_snapshot,
            "_brief_from_snapshot": playground._brief_from_snapshot,
            "_compute_strategy_confidence": playground._compute_strategy_confidence,
            "_build_playground_evidence_summary": playground._build_playground_evidence_summary,
            "_compute_replay_metrics": playground._compute_replay_metrics,
            "_dedupe_market_snapshots": playground._dedupe_market_snapshots,
            "_feature_rows_to_snapshots": playground._feature_rows_to_snapshots,
            "_format_evidence_summary": playground._format_evidence_summary,
            "_format_strategy_confidence_summary": playground._format_strategy_confidence_summary,
            "_max_drawdown": playground._max_drawdown,
            "_merge_feature_map": playground._merge_feature_map,
            "_recent_snapshot_row_limit": playground._recent_snapshot_row_limit,
            "_replay_metric_reliability": playground._replay_metric_reliability,
            "_run_one_strategy": playground._run_one_strategy,
            "compute_consensus_weights": playground.compute_consensus_weights,
            "PlaygroundBundle": playground.PlaygroundBundle,
            "detect_sector_rotation": sector_rotation.detect_sector_rotation,
            "format_rotation_for_prompt": sector_rotation.format_rotation_for_prompt,
            "rotation_signal_strengths": sector_rotation.rotation_signal_strengths,
            "filter_tradable_research_rows": universe_policy.filter_tradable_research_rows,
        }


globals().update(_load_sector_rotation_exports())


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

    def test_rotation_signal_strengths_are_deterministic_and_signed(self):
        holdings = [
            {"ticker": "XLK", "mom_60d": 0.08, "mom_20d": 0.04, "return_5d": 0.02, "hist_vol_20d": 0.015},
            {"ticker": "XLP", "mom_60d": -0.02, "mom_20d": -0.01, "return_5d": -0.004, "hist_vol_20d": 0.010},
        ]

        result = detect_sector_rotation(holdings)
        first = rotation_signal_strengths(result)
        second = rotation_signal_strengths(result)

        self.assertEqual(first, second)
        self.assertGreater(first["XLK"], 0)
        self.assertLess(first["XLP"], 0)
        self.assertLessEqual(abs(first["XLK"]), 1.0)

    def test_rotation_marks_legacy_fallback_when_only_old_momentum_fields_exist(self):
        result = detect_sector_rotation([
            {"ticker": "XLK", "mom_60d": 0.08, "mom_20d": 0.04},
            {"ticker": "XLP", "return_60d": -0.02, "return_20d": -0.01},
        ])

        self.assertEqual(result["data_quality"], "legacy_fallback")
        self.assertEqual(result["legacy_fallback_tickers"], ["XLK"])
        xlk = next(row for row in result["leaders"] if row["ticker"] == "XLK")
        self.assertEqual(xlk["data_quality"], "legacy_fallback")

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
        self.assertEqual(xlk["return_60d"], 0.08)
        self.assertNotIn("mom_60d", xlk)
        self.assertEqual(xlk["legacy_qc_indicators"]["mom_60d"], 0.01)
        sources = {item["source"] for item in xlk["feature_sources"]}
        self.assertEqual(sources, {"qc_heartbeat", "qc_daily_snapshot"})
        self.assertTrue(any(row["ticker"] == "XLP" for row in merged["holdings"]))

    def test_feature_snapshot_can_stand_alone_as_holdings(self):
        payload = {"features": [{"ticker": "XLK", "mom_60d": 0.08}]}

        normalized = _normalize_feature_snapshot(payload)

        self.assertEqual(normalized["holdings"][0]["ticker"], "XLK")
        self.assertEqual(normalized["holdings"][0]["return_60d"], 0.08)
        self.assertNotIn("mom_60d", normalized["holdings"][0])
        self.assertEqual(normalized["holdings"][0]["legacy_qc_indicators"]["mom_60d"], 0.08)
        self.assertEqual(
            normalized["holdings"][0]["feature_sources"][0]["source"],
            "qc_daily_snapshot",
        )

    def test_yfinance_research_overrides_qc_daily_and_keeps_live_state(self):
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
                },
            ],
        }
        feature_snapshot = {
            "packet_type": "daily_feature_snapshot",
            "features": [{"ticker": "XLK", "mom_60d": 0.08, "open_price": 200.0}],
        }
        yfinance = {
            "XLK": {
                "ticker": "XLK",
                "return_60d": 0.06,
                "open_price": 201.0,
                "trading_date": "2026-05-14",
            }
        }

        merged = _merge_market_snapshots(heartbeat, feature_snapshot, yfinance)
        xlk = merged["holdings"][0]

        self.assertEqual(xlk["weight_current"], 0.2)
        self.assertEqual(xlk["price"], 210.0)
        self.assertEqual(xlk["intraday_open_price"], 209.0)
        self.assertEqual(xlk["open_price"], 201.0)
        self.assertEqual(xlk["return_60d"], 0.06)
        self.assertNotIn("mom_60d", xlk)
        self.assertEqual(xlk["legacy_qc_indicators"]["mom_60d"], 99.0)
        self.assertEqual(merged["schema_capabilities"]["intraday_live_state"], "available")
        self.assertEqual(merged["schema_capabilities"]["daily_research_authority"], "yfinance")

    def test_yfinance_research_still_merges_when_qc_daily_missing(self):
        heartbeat = {
            "packet_type": "heartbeat",
            "holdings": [
                {"ticker": "SPY", "weight_current": 0.1, "price": 630.0, "mom_60d": 99.0},
            ],
        }
        yfinance = {
            "SPY": {
                "ticker": "SPY",
                "return_60d": 0.05,
                "open_price": 625.0,
                "trading_date": "2026-05-14",
            }
        }

        merged = _merge_market_snapshots(heartbeat, {}, yfinance)
        spy = merged["holdings"][0]

        self.assertEqual(spy["weight_current"], 0.1)
        self.assertEqual(spy["price"], 630.0)
        self.assertEqual(spy["open_price"], 625.0)
        self.assertEqual(spy["return_60d"], 0.05)
        self.assertNotIn("mom_60d", spy)
        self.assertEqual(spy["legacy_qc_indicators"]["mom_60d"], 99.0)
        self.assertEqual(merged["schema_capabilities"]["daily_research_authority"], "yfinance")

    def test_old_schema_heartbeat_marks_intraday_partial_without_inference(self):
        heartbeat = {
            "packet_type": "heartbeat",
            "schema_version": "1.4",
            "holdings": [{"ticker": "SPY", "price": 630.0, "weight_current": 0.1}],
        }

        merged = _merge_market_snapshots(heartbeat, {}, {})
        spy = merged["holdings"][0]

        self.assertEqual(merged["schema_capabilities"]["heartbeat_schema_version"], "1.4")
        self.assertEqual(merged["schema_capabilities"]["intraday_live_state"], "partial")
        self.assertNotIn("intraday_open_price", spy)
        self.assertNotIn("open_price", spy)

    def test_missing_heartbeat_schema_marks_intraday_unavailable_without_price(self):
        heartbeat = {
            "packet_type": "heartbeat",
            "holdings": [{"ticker": "SPY", "weight_current": 0.1}],
        }

        merged = _merge_market_snapshots(heartbeat, {}, {})

        self.assertEqual(merged["schema_capabilities"]["heartbeat_schema_version"], "legacy")
        self.assertEqual(merged["schema_capabilities"]["intraday_live_state"], "unavailable")

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

    def test_recent_snapshot_limit_covers_full_lookback_before_dedupe(self):
        self.assertEqual(_recent_snapshot_row_limit(1), 180)
        self.assertEqual(_recent_snapshot_row_limit(30), 1200)

    def test_hedge_products_are_not_ordinary_research_tradable(self):
        rows = [
            {"ticker": "SPY", "universe_role": "core"},
            {"ticker": "DRAM", "universe_role": "satellite"},
            {"ticker": "SPXS", "universe_role": "hedge"},
            {"ticker": "SQQQ", "universe_role": "hedge"},
        ]

        filtered = filter_tradable_research_rows(rows)

        self.assertEqual([row["ticker"] for row in filtered], ["SPY", "DRAM"])

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
        self.assertEqual(enriched[0]["feature_sources"][0]["authority_by_field"]["mom_60d"], "daily_research")
        self.assertEqual(enriched[0]["feature_sources"][0]["canonical_aliases"]["mom_60d"], "return_60d")

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

    def test_yfinance_feature_rows_build_historical_replay_snapshots(self):
        rows = []
        for day, ret in ((date(2026, 1, 2), 0.01), (date(2026, 1, 3), -0.002)):
            rows.append(SimpleNamespace(
                trading_date=day,
                ticker="SPY",
                open_price=100,
                high_price=101,
                low_price=99,
                close_price=100,
                adj_close_price=100,
                volume=1000,
                dollar_volume=100000,
                return_1d=ret,
                return_5d=0.02,
                return_20d=0.03,
                return_60d=0.06,
                return_252d=0.12,
                sma_20=99,
                sma_50=98,
                sma_200=95,
                hist_vol_20d=0.01,
                rsi_14=55,
                atr_pct=0.012,
                bb_position=0.55,
            ))

        snapshots = _feature_rows_to_snapshots(rows)

        self.assertEqual(len(snapshots), 2)
        self.assertEqual(snapshots[0]["packet_type"], "yfinance_historical")
        self.assertEqual(snapshots[0]["holdings"][0]["mom_20d"], 0.03)
        self.assertEqual(snapshots[0]["holdings"][0]["rsi_14"], 55.0)

    def test_strategy_confidence_combines_historical_and_live_evidence(self):
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
        result = _run_one_strategy(
            "momentum_lite_v1",
            holdings,
            {"regime": "trending_bull", "risk_params": {"max_single_position": 0.2}},
            {},
        )

        confidence = _compute_strategy_confidence(
            [result],
            {"momentum_lite_v1": {"metric_reliability": {"level": "insufficient"}, "n_forward_return_samples": 3}},
            {
                "momentum_lite_v1": {
                    "metric_reliability": {"level": "high"},
                    "n_forward_return_samples": 100,
                    "sharpe": 1.2,
                    "hit_rate": 0.55,
                }
            },
            "trending_bull",
            {"SPY": 0.5, "QQQ": 0.3, "CASH": 0.2},
        )

        row = confidence["momentum_lite_v1"]
        self.assertGreater(row["historical_score"], row["live_fit_score"] - 0.3)
        self.assertIn(row["suggested_use"], {"advisory", "primary"})
        self.assertEqual(row["historical_reliability"], "high")
        self.assertIn("historical_strong", row["reason_codes"])
        self.assertIn("regime_fit_strong", row["reason_codes"])

    def test_strategy_confidence_summary_is_structured_for_telegram(self):
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
        result = _run_one_strategy(
            "momentum_lite_v1",
            holdings,
            {"regime": "trending_bull", "risk_params": {"max_single_position": 0.2}},
            {},
        )
        historical_metrics = {
            "momentum_lite_v1": {
                "metric_reliability": {"level": "high"},
                "n_forward_return_samples": 100,
                "sharpe": 1.2,
                "hit_rate": 0.55,
                "avg_turnover": 0.084,
            }
        }
        confidence = _compute_strategy_confidence(
            [result],
            {"momentum_lite_v1": {"metric_reliability": {"level": "insufficient"}, "n_forward_return_samples": 3}},
            historical_metrics,
            "trending_bull",
            {"SPY": 0.5, "QQQ": 0.3, "CASH": 0.2},
        )
        bundle = PlaygroundBundle(
            generated_at="2026-05-15T00:00:00",
            regime_label="trending_bull",
            regime_confidence="medium",
            snapshot_count=8,
            strategies=[result],
            divergence_map=[],
            consensus_weights={"SPY": 0.5, "QQQ": 0.3, "CASH": 0.2},
            replay_metrics={},
            historical_replay_metrics=historical_metrics,
            historical_snapshot_count=100,
            strategy_confidence=confidence,
            evidence_summary=_build_playground_evidence_summary(
                snapshot_count=8,
                historical_snapshot_count=100,
                replay_metrics={},
                historical_replay_metrics=historical_metrics,
                strategy_confidence=confidence,
                data_gaps=[],
            ),
            data_gaps=[],
        )

        summary = _format_strategy_confidence_summary(bundle)

        self.assertIn("<b>Strategy Confidence</b>", summary)
        self.assertIn("momentum_lite_v1", summary)
        self.assertIn("use=", summary)
        self.assertIn("hist=high/100", summary)
        self.assertIn("live_samples=3", summary)
        self.assertIn("sharpe=1.20", summary)
        self.assertIn("current_turnover=", summary)
        self.assertIn("hist_avg_turnover=8.4%", summary)
        self.assertIn("historical_strong", summary)

    def test_strategy_confidence_summary_prioritizes_risk_reason_codes(self):
        result = _run_one_strategy(
            "momentum_lite_v1",
            [
                {
                    "ticker": "SPY",
                    "mom_20d": 0.02,
                    "mom_60d": 0.05,
                    "mom_252d": 0.12,
                    "rsi_14": 55,
                    "atr_pct": 0.011,
                    "hist_vol_20d": 0.14,
                }
            ],
            {"regime": "trending_bull", "risk_params": {"max_single_position": 0.2}},
            {},
        )
        confidence = {
            "momentum_lite_v1": {
                "strategy_name": "momentum_lite_v1",
                "suggested_use": "advisory",
                "confidence_score": 0.56,
                "historical_reliability": "high",
                "historical_samples": 100,
                "live_samples": 3,
                "reason_codes": [
                    "historical_strong",
                    "historical_positive_sharpe",
                    "regime_fit_strong",
                    "live_qc_limited",
                    "high_turnover",
                    "consensus_regime_conflict",
                ],
            }
        }
        bundle = PlaygroundBundle(
            generated_at="2026-05-15T00:00:00",
            regime_label="trending_bull",
            regime_confidence="medium",
            snapshot_count=8,
            strategies=[result],
            divergence_map=[],
            consensus_weights={"SPY": 0.5, "CASH": 0.5},
            replay_metrics={},
            historical_replay_metrics={
                "momentum_lite_v1": {
                    "metric_reliability": {"level": "high"},
                    "n_forward_return_samples": 100,
                    "sharpe": 1.2,
                    "avg_turnover": 0.084,
                }
            },
            historical_snapshot_count=100,
            strategy_confidence=confidence,
            evidence_summary={},
            data_gaps=[],
        )

        summary = _format_strategy_confidence_summary(bundle)
        reason_line = next(line for line in summary.splitlines() if "reasons=" in line)

        self.assertIn("consensus_regime_conflict", reason_line)
        self.assertIn("high_turnover", reason_line)
        self.assertLess(reason_line.index("consensus_regime_conflict"), reason_line.index("historical_strong"))

    def test_bundle_consensus_conflict_does_not_leak_to_aligned_strategy_reasons(self):
        result = _run_one_strategy(
            "momentum_lite_v1",
            [
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
            ],
            {"regime": "trending_bull", "risk_params": {"max_single_position": 0.2}},
            {},
        )
        confidence = _compute_strategy_confidence(
            [result],
            {"momentum_lite_v1": {"metric_reliability": {"level": "high"}, "n_forward_return_samples": 30}},
            {
                "momentum_lite_v1": {
                    "metric_reliability": {"level": "high"},
                    "n_forward_return_samples": 100,
                    "sharpe": 1.2,
                    "hit_rate": 0.55,
                }
            },
            "trending_bull",
            {"IEF": 0.4, "BND": 0.3, "TLT": 0.2, "CASH": 0.1},
        )

        row = confidence["momentum_lite_v1"]
        self.assertTrue(row["consensus_conflict"])
        self.assertFalse(row["strategy_regime_conflict"])
        self.assertNotIn("consensus_regime_conflict", row["reason_codes"])
        self.assertNotIn("strategy_regime_conflict", row["reason_codes"])

    def test_playground_evidence_summary_labels_historical_live_and_permission(self):
        summary = _build_playground_evidence_summary(
            snapshot_count=8,
            historical_snapshot_count=100,
            replay_metrics={},
            historical_replay_metrics={
                "momentum_lite_v1": {
                    "metric_reliability": {"level": "high"},
                    "n_forward_return_samples": 100,
                }
            },
            strategy_confidence={
                "momentum_lite_v1": {
                    "strategy_name": "momentum_lite_v1",
                    "suggested_use": "advisory",
                    "confidence_score": 0.63,
                    "consensus_conflict": False,
                    "reason_codes": ["historical_strong", "live_qc_limited"],
                }
            },
            data_gaps=[],
        )

        self.assertEqual(summary["historical_evidence"], "strong")
        self.assertEqual(summary["execution_intel_status"], "insufficient_data")
        self.assertEqual(summary["execution_permission"], "advisory")
        rendered = _format_evidence_summary(summary)
        self.assertIn("Historical evidence: strong", rendered)
        self.assertIn("Status: insufficient_data", rendered)
        self.assertIn("Execution permission: advisory", rendered)

    def test_consensus_conflict_blocks_permission_without_polluting_execution_intel(self):
        summary = _build_playground_evidence_summary(
            snapshot_count=30,
            historical_snapshot_count=100,
            replay_metrics={
                "momentum_lite_v1": {
                    "metric_reliability": {"level": "high"},
                    "n_forward_return_samples": 30,
                }
            },
            historical_replay_metrics={
                "momentum_lite_v1": {
                    "metric_reliability": {"level": "high"},
                    "n_forward_return_samples": 100,
                }
            },
            strategy_confidence={
                "momentum_lite_v1": {
                    "strategy_name": "momentum_lite_v1",
                    "suggested_use": "advisory",
                    "confidence_score": 0.63,
                    "consensus_conflict": True,
                }
            },
            data_gaps=[],
        )

        self.assertEqual(summary["execution_intel_status"], "live_available")
        self.assertEqual(summary["execution_permission"], "human_required")

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

        consensus = compute_consensus_weights([strong, weak])

        self.assertEqual(strong.memory_feedback["discount_multiplier"], 1.0)
        self.assertEqual(weak.agent_interpretation["memory_discount_multiplier"], 0.5)
        self.assertAlmostEqual(sum(consensus.values()), 1.0, places=4)


if __name__ == "__main__":
    unittest.main()
