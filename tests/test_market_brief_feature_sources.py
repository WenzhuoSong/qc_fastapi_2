import importlib
import sys
import types
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from services.feature_provenance import summarize_feature_provenance
from services.market_snapshot_merge import merge_market_snapshots


def _load_compute_key_facts():
    sqlalchemy_stub = type(sys)("sqlalchemy")
    sqlalchemy_stub.select = lambda *args, **kwargs: None
    sqlalchemy_stub.desc = lambda *args, **kwargs: None

    session_stub = type(sys)("db.session")
    session_stub.AsyncSessionLocal = object

    queries_stub = type(sys)("db.queries")
    async def fake_get_system_config(*args, **kwargs):
        return None
    queries_stub.get_system_config = fake_get_system_config

    models_stub = type(sys)("db.models")
    for name in ("MacroNewsCache", "MarketDailyFeature", "QCSnapshot", "TickerNewsLibrary"):
        setattr(models_stub, name, type(name, (), {}))

    with patch.dict(
        "sys.modules",
        {
            "sqlalchemy": sqlalchemy_stub,
            "db": type(sys)("db"),
            "db.session": session_stub,
            "db.queries": queries_stub,
            "db.models": models_stub,
            "config": SimpleNamespace(get_settings=lambda: SimpleNamespace()),
        },
    ):
        module = importlib.import_module("services.market_brief")
        return module._compute_key_facts


_compute_key_facts = _load_compute_key_facts()


class MarketBriefFeatureSourcesTests(unittest.TestCase):
    def test_key_facts_use_canonical_return_60d(self):
        facts = _compute_key_facts(
            [
                {"ticker": "SPY", "return_60d": 0.07, "mom_60d": 99.0, "sma_200": 500.0, "price": 630.0},
                {"ticker": "QQQ", "return_60d": 0.09, "sma_200": 400.0, "price": 510.0},
            ],
            {"current_drawdown_pct": 0.01},
        )

        self.assertEqual(facts["spy_mom_60d"], 0.07)
        self.assertEqual(facts["top5_momentum"][0], "QQQ")

    def test_feature_source_summary_exposes_authority_counts(self):
        merged = merge_market_snapshots(
            {
                "packet_type": "heartbeat",
                "schema_version": "1.5",
                "holdings": [
                    {
                        "ticker": "SPY",
                        "price": 630.0,
                        "weight_current": 0.1,
                        "intraday_open_price": 628.0,
                        "mom_60d": 99.0,
                    }
                ],
            },
            {
                "packet_type": "daily_feature_snapshot",
                "features": [{"ticker": "SPY", "mom_60d": 0.06}],
            },
            {
                "SPY": {
                    "ticker": "SPY",
                    "return_60d": 0.07,
                    "rsi_14": 61.0,
                    "trading_date": "2026-05-14",
                }
            },
        )

        summary = summarize_feature_provenance(merged["holdings"])

        self.assertGreater(summary["authority_counts"]["live_state"], 0)
        self.assertGreater(summary["authority_counts"]["intraday"], 0)
        self.assertGreater(summary["authority_counts"]["daily_research"], 0)
        self.assertGreater(summary["authority_counts"]["legacy_debug"], 0)
        self.assertIn("SPY", summary["yfinance_filled_fields"])


if __name__ == "__main__":
    unittest.main()
