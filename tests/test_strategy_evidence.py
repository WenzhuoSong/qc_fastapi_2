import copy
import importlib
import sys
import types
import unittest
from unittest.mock import patch

from services.knowledge_base import build_knowledge_context
from services.strategy_evidence import build_evidence_cards, summarize_evidence_cards
from strategies import ScoredTicker, get_strategy


def _scored(**scores):
    return [
        ScoredTicker(
            ticker=ticker,
            score=score,
            raw_factors={"branch": f"branch_to_{ticker.lower()}"},
        )
        for ticker, score in scores.items()
    ]


def _leveraged_context(tickers=None):
    return build_knowledge_context(
        tickers=tickers or ["TQQQ", "UVXY", "BSV", "SPY", "QQQ"],
        strategy_names=["leveraged_etf_momentum_allocator"],
        regime="trending_bull",
        max_assets=12,
    )


def _sample_allocator_holdings():
    values = {
        "SPY": (500, 490, 450, 55),
        "QQQ": (450, 440, 430, 60),
        "TQQQ": (90, 88, 85, 58),
        "UVXY": (20, 25, 22, 40),
        "TECL": (80, 79, 78, 52),
        "SPXL": (130, 128, 125, 50),
        "SQQQ": (16, 18, 17, 45),
        "TECS": (10, 12, 11, 48),
        "BSV": (75, 75, 75, 50),
    }
    return [
        {
            "ticker": ticker,
            "close_price": close,
            "price": close,
            "sma_20": sma20,
            "sma_200": sma200,
            "rsi_10": rsi,
            "feature_sources": [
                {
                    "source": "yfinance",
                    "filled_fields": ["close_price", "sma_20", "sma_200", "rsi_10"],
                    "authority_by_field": {
                        "close_price": "daily_research",
                        "sma_20": "daily_research",
                        "sma_200": "daily_research",
                        "rsi_10": "daily_research",
                    },
                }
            ],
        }
        for ticker, (close, sma20, sma200, rsi) in values.items()
    ]


def _load_run_one_strategy():
    openai_stub = types.ModuleType("openai")
    openai_stub.AsyncOpenAI = object
    sqlalchemy_stub = types.ModuleType("sqlalchemy")
    sqlalchemy_stub.select = lambda *args, **kwargs: None
    sqlalchemy_stub.desc = lambda *args, **kwargs: None
    config_stub = types.ModuleType("config")
    config_stub.get_settings = lambda: types.SimpleNamespace(
        openai_api_key="test",
        openai_model="test-model",
    )
    session_stub = types.ModuleType("db.session")
    session_stub.AsyncSessionLocal = object
    models_stub = types.ModuleType("db.models")
    for name in ("MarketDailyFeature", "QCSnapshot"):
        setattr(models_stub, name, type(name, (), {}))
    sys.modules.pop("services.playground", None)
    with patch.dict(
        sys.modules,
        {
            "openai": openai_stub,
            "sqlalchemy": sqlalchemy_stub,
            "config": config_stub,
            "db.session": session_stub,
            "db.models": models_stub,
        },
    ):
        playground = importlib.import_module("services.playground")
    return playground._run_one_strategy


class StrategyEvidenceTest(unittest.TestCase):
    def test_tqqq_score_1_translates_to_increase(self):
        strategy = get_strategy("leveraged_etf_momentum_allocator")
        cards = build_evidence_cards(
            strategy=strategy,
            scored=_scored(TQQQ=1.0),
            knowledge_context=_leveraged_context(["TQQQ"]),
            mode="semi_auto",
        )

        self.assertEqual(cards[0].action, "increase")
        self.assertEqual(cards[0].vote_status, "voted")
        self.assertIsNone(cards[0].abstain_reason)
        self.assertEqual(cards[0].vote_diagnostics["alert_class"], None)
        self.assertEqual(cards[0].signal_type, "risk_on_amplifier")
        self.assertEqual(cards[0].max_reasonable_weight, 0.08)

    def test_uvxy_score_1_translates_to_hedge(self):
        strategy = get_strategy("leveraged_etf_momentum_allocator")
        cards = build_evidence_cards(
            strategy=strategy,
            scored=_scored(UVXY=1.0),
            knowledge_context=_leveraged_context(["UVXY"]),
            mode="semi_auto",
        )

        self.assertEqual(cards[0].action, "hedge")
        self.assertEqual(cards[0].signal_type, "tail_risk_hedge")
        self.assertEqual(cards[0].max_reasonable_weight, 0.03)

    def test_momentum_core_market_mapping_votes_after_mapping_cleanup(self):
        strategy = get_strategy("momentum_lite_v1")
        ctx = build_knowledge_context(
            tickers=["SPY"],
            strategy_names=["momentum_lite_v1"],
            regime="trending_bull",
        )

        cards = build_evidence_cards(
            strategy=strategy,
            scored=_scored(SPY=0.8),
            knowledge_context=ctx,
        )

        self.assertEqual(cards[0].action, "increase")
        self.assertEqual(cards[0].vote_status, "voted")
        self.assertIsNone(cards[0].vote_diagnostics["alert_class"])
        self.assertGreater(cards[0].max_reasonable_weight, 0.0)
        self.assertEqual(cards[0].signal_type, "broad_market_momentum")
        self.assertEqual(cards[0].reason, "mapped_by_compatibility_threshold")

    def test_tqqq_and_uvxy_same_score_have_different_weight_caps(self):
        strategy = get_strategy("leveraged_etf_momentum_allocator")
        cards = build_evidence_cards(
            strategy=strategy,
            scored=_scored(TQQQ=0.8, UVXY=0.8),
            knowledge_context=_leveraged_context(["TQQQ", "UVXY"]),
            mode="semi_auto",
        )

        by_ticker = {card.ticker: card for card in cards}
        self.assertGreater(
            by_ticker["TQQQ"].max_reasonable_weight,
            by_ticker["UVXY"].max_reasonable_weight,
        )
        self.assertEqual(by_ticker["TQQQ"].action, "increase")
        self.assertEqual(by_ticker["UVXY"].action, "hedge")

    def test_score_equals_threshold_uses_gte_match(self):
        strategy = get_strategy("leveraged_etf_momentum_allocator")
        cards = build_evidence_cards(
            strategy=strategy,
            scored=_scored(TQQQ=0.70),
            knowledge_context=_leveraged_context(["TQQQ"]),
            mode="semi_auto",
        )

        self.assertEqual(cards[0].action, "increase")

    def test_conviction_profile_adds_shadow_effective_confidence_without_changing_weight(self):
        strategy = get_strategy("leveraged_etf_momentum_allocator")
        ctx = _leveraged_context(["TQQQ"])
        scored = _scored(TQQQ=0.8)

        baseline = build_evidence_cards(
            strategy=strategy,
            scored=scored,
            knowledge_context=ctx,
            mode="semi_auto",
        )[0]
        with_conviction = build_evidence_cards(
            strategy=strategy,
            scored=scored,
            knowledge_context=ctx,
            mode="semi_auto",
            conviction_profiles=[
                {
                    "strategy_id": "leveraged_etf_momentum_allocator",
                    "ticker": "TQQQ",
                    "branch": "branch_to_tqqq",
                    "action": "increase",
                    "horizon_days": 5,
                    "source_bucket": "combined",
                    "conviction": 0.5,
                    "status": "early_estimate",
                    "n": 14,
                    "source_counts": {"historical_prior": 14, "live_paper": 0},
                    "data_lag_filtered": 1,
                }
            ],
        )[0]

        self.assertEqual(with_conviction.max_reasonable_weight, baseline.max_reasonable_weight)
        self.assertEqual(with_conviction.conviction, 0.5)
        self.assertEqual(with_conviction.conviction_status, "early_estimate")
        self.assertEqual(with_conviction.conviction_statistical_status, "insufficient")
        self.assertEqual(with_conviction.conviction_source_bucket, "combined")
        self.assertEqual(with_conviction.conviction_n, 14)
        self.assertAlmostEqual(with_conviction.effective_confidence, 0.0)
        self.assertTrue(with_conviction.diagnostics["conviction"]["shadow_only"])

    def test_insufficient_conviction_samples_keep_effective_confidence_zero(self):
        strategy = get_strategy("leveraged_etf_momentum_allocator")
        cards = build_evidence_cards(
            strategy=strategy,
            scored=_scored(TQQQ=0.8),
            knowledge_context=_leveraged_context(["TQQQ"]),
            mode="semi_auto",
            conviction_profiles=[
                {
                    "strategy_id": "leveraged_etf_momentum_allocator",
                    "ticker": "TQQQ",
                    "branch": "branch_to_tqqq",
                    "action": "increase",
                    "horizon_days": 5,
                    "source_bucket": "combined",
                    "conviction": None,
                    "status": "insufficient_samples",
                    "n": 6,
                }
            ],
        )

        self.assertIsNone(cards[0].conviction)
        self.assertEqual(cards[0].conviction_status, "insufficient_samples")
        self.assertEqual(cards[0].effective_confidence, 0.0)
        self.assertIn("insufficient_conviction_samples", cards[0].reason)

    def test_historical_prior_requires_live_confirmation_discounts_confidence(self):
        strategy = get_strategy("leveraged_etf_momentum_allocator")
        cards = build_evidence_cards(
            strategy=strategy,
            scored=_scored(TQQQ=0.8),
            knowledge_context=_leveraged_context(["TQQQ"]),
            mode="semi_auto",
            conviction_profiles=[
                {
                    "strategy_id": "leveraged_etf_momentum_allocator",
                    "ticker": "TQQQ",
                    "branch": "branch_to_tqqq",
                    "action": "increase",
                    "horizon_days": 5,
                    "source_bucket": "combined",
                    "conviction": 0.9,
                    "status": "historical_prior_requires_live_confirmation",
                    "n": 30,
                }
            ],
        )

        self.assertEqual(cards[0].conviction, 0.9)
        self.assertEqual(cards[0].conviction_statistical_status, "monitoring_ready")
        self.assertAlmostEqual(cards[0].effective_confidence, 0.072)
        self.assertIn("historical_prior_requires_live_confirmation", cards[0].reason)

    def test_action_not_allowed_by_asset_profile_falls_back_to_watch(self):
        strategy = get_strategy("leveraged_etf_momentum_allocator")
        ctx = _leveraged_context(["UVXY"])
        bad_ctx = copy.deepcopy(ctx)
        bad_ctx["assets"][0]["allowed_actions"] = ["watch", "avoid", "neutral"]

        cards = build_evidence_cards(
            strategy=strategy,
            scored=_scored(UVXY=1.0),
            knowledge_context=bad_ctx,
            mode="semi_auto",
        )

        self.assertEqual(cards[0].action, "watch")
        self.assertEqual(cards[0].vote_status, "watch")
        self.assertIsNone(cards[0].vote_diagnostics["alert_class"])
        self.assertEqual(cards[0].max_reasonable_weight, 0.0)
        self.assertIn("action_not_allowed_by_asset_profile", cards[0].reason)

    def test_watch_threshold_is_watch_vote_status_without_replacing_action(self):
        strategy = get_strategy("leveraged_etf_momentum_allocator")
        cards = build_evidence_cards(
            strategy=strategy,
            scored=_scored(TQQQ=0.20),
            knowledge_context=_leveraged_context(["TQQQ"]),
            mode="semi_auto",
        )

        self.assertEqual(cards[0].action, "neutral")
        self.assertEqual(cards[0].vote_status, "watch")
        self.assertEqual(cards[0].signal_type, "no_signal")
        self.assertIsNone(cards[0].vote_diagnostics["dedupe_key"])

    def test_summary_counts_vote_statuses(self):
        strategy = get_strategy("leveraged_etf_momentum_allocator")
        voted = build_evidence_cards(
            strategy=strategy,
            scored=_scored(TQQQ=0.80),
            knowledge_context=_leveraged_context(["TQQQ"]),
            mode="semi_auto",
        )[0]
        watch = build_evidence_cards(
            strategy=strategy,
            scored=_scored(TQQQ=0.20),
            knowledge_context=_leveraged_context(["TQQQ"]),
            mode="semi_auto",
        )[0]
        mapping_error_strategy = get_strategy("momentum_lite_v1")
        missing_profile_context = build_knowledge_context(
            tickers=["SPY"],
            strategy_names=["momentum_lite_v1"],
            regime="trending_bull",
        )
        missing_profile_context["strategies"] = []
        mapping_error = build_evidence_cards(
            strategy=mapping_error_strategy,
            scored=_scored(SPY=0.8),
            knowledge_context=missing_profile_context,
        )[0]

        summary = summarize_evidence_cards([voted, watch, mapping_error])

        self.assertEqual(summary["vote_statuses"], {
            "mapping_error": 1,
            "voted": 1,
            "watch": 1,
        })
        self.assertEqual(summary["mapping_error_count"], 1)
        self.assertEqual(summary["watch_vote_count"], 1)
        self.assertEqual(summary["abstain_count"], 0)
        self.assertEqual(summary["cards_generated"], 3)

    def test_full_auto_uvxy_cap_is_not_above_semi_auto(self):
        strategy = get_strategy("leveraged_etf_momentum_allocator")
        ctx = _leveraged_context(["UVXY"])

        semi = build_evidence_cards(
            strategy=strategy,
            scored=_scored(UVXY=1.0),
            knowledge_context=ctx,
            mode="semi_auto",
        )[0]
        full = build_evidence_cards(
            strategy=strategy,
            scored=_scored(UVXY=1.0),
            knowledge_context=ctx,
            mode="full_auto",
        )[0]

        self.assertLessEqual(full.max_reasonable_weight, semi.max_reasonable_weight)
        self.assertEqual(full.max_reasonable_weight, 0.0)

    def test_safety_fields_missing_raises_in_strict_mode(self):
        strategy = get_strategy("leveraged_etf_momentum_allocator")
        bad_ctx = {
            "selection": {"regime": "trending_bull"},
            "assets": [{"id": "NEWETF", "role": "leveraged_long"}],
            "strategies": [
                {
                    "id": "leveraged_etf_momentum_allocator",
                    "compatibility_mappings": [
                        {
                            "role": "leveraged_long",
                            "score_thresholds": [{"gte": 0.7, "action": "increase"}],
                        }
                    ],
                }
            ],
        }

        with self.assertRaisesRegex(ValueError, "missing required safety field"):
            build_evidence_cards(
                strategy=strategy,
                scored=_scored(NEWETF=1.0),
                knowledge_context=bad_ctx,
                strict=True,
            )

    def test_playground_emits_evidence_cards_beside_existing_scores(self):
        run_one_strategy = _load_run_one_strategy()
        context = {
            "regime": "trending_bull",
            "risk_params": {"max_single_position": 0.20, "min_cash_pct": 0.05},
        }

        result = run_one_strategy(
            "leveraged_etf_momentum_allocator",
            _sample_allocator_holdings(),
            context,
            {},
        )

        self.assertEqual(result.evidence_contract_version, "v1")
        self.assertTrue(result.score_breakdown)
        self.assertEqual(result.scored_tickers, result.score_breakdown)
        self.assertTrue(result.evidence_cards)
        top_card = next(card for card in result.evidence_cards if card["ticker"] == "TQQQ")
        self.assertEqual(top_card["action"], "increase")
        self.assertEqual(top_card["branch"], "bull_trend_to_tqqq")
        self.assertEqual(result.evidence_summary["cards_generated"], len(result.evidence_cards))


if __name__ == "__main__":
    unittest.main()
