import unittest
from datetime import date

try:
    from env_setup import ensure_test_settings
except ModuleNotFoundError:
    from tests.env_setup import ensure_test_settings

ensure_test_settings()

from services.playground import _rows_with_strategy_universe
from services.strategy_input_builder import ExclusionReason, build_strategy_input
from services.strategy_feature_contract import build_strategy_feature_contract
from services.universe_policy import default_strategy_research_universe
from strategies import get_strategy


def _valid_momentum_feature(ticker: str, mom20: float, mom60: float, mom252: float) -> dict:
    return {
        "ticker": ticker,
        "source": "yfinance",
        "trading_date": "2026-05-27",
        "close_price": 100.0,
        "return_20d": mom20,
        "return_60d": mom60,
        "return_252d": mom252,
        "rsi_14": 55.0,
        "atr_pct": 0.012,
    }


class PlaygroundFeatureMergeTests(unittest.TestCase):
    def test_generic_strategy_gets_default_research_universe(self):
        rows = _rows_with_strategy_universe(
            holdings=[{"ticker": "QQQ", "universe_role": "core"}],
            strategy_names=["momentum_lite_v1"],
        )

        tickers = {row["ticker"] for row in rows}
        self.assertTrue(set(default_strategy_research_universe()).issubset(tickers))
        self.assertIn("SPY", tickers)
        self.assertIn("SOXX", tickers)

    def test_default_research_universe_excludes_hedges_for_generic_strategy(self):
        rows = _rows_with_strategy_universe(
            holdings=[
                {"ticker": "QQQ", "universe_role": "core"},
                {"ticker": "PSQ", "universe_role": "hedge"},
            ],
            strategy_names=["momentum_lite_v1"],
        )

        tickers = {row["ticker"] for row in rows}
        self.assertNotIn("PSQ", tickers)
        self.assertNotIn("TQQQ", tickers)
        self.assertNotIn("UVXY", tickers)

    def test_yfinance_replaces_non_authoritative_qc_research_fields(self):
        strategy = get_strategy("momentum_lite_v1")
        result = build_strategy_input(
            strategy=strategy,
            live_rows=[
                {
                    "ticker": "SPY",
                    "mom_20d": 0.01,
                    "mom_60d": 0.02,
                    "mom_252d": 0.03,
                    "rsi_14": 70.0,
                    "atr_pct": 0.02,
                }
            ],
            feature_matrix={
                "SPY": {
                    "ticker": "SPY",
                    "source": "yfinance",
                    "trading_date": "2026-05-27",
                    "return_20d": 0.11,
                    "return_60d": 0.12,
                    "return_252d": 0.13,
                    "rsi_14": 55.0,
                    "atr_pct": 0.01,
                }
            },
            as_of=date(2026, 5, 28),
        )

        self.assertEqual(result.status, "scored")
        row = result.scorable_rows[0]
        self.assertEqual(row["mom_20d"], 0.11)
        self.assertEqual(row["mom_60d"], 0.12)
        self.assertEqual(row["mom_252d"], 0.13)
        self.assertEqual(row["rsi_14"], 55.0)
        source = row["feature_sources"][0]
        self.assertEqual(source["source"], "yfinance")
        self.assertIn("mom_60d", source["filled_fields"])
        self.assertEqual(source["authority_by_field"]["mom_60d"], "daily_research")

        contract = build_strategy_feature_contract(
            strategy,
            result.scorable_rows,
            as_of=date(2026, 5, 28),
        )
        self.assertEqual(contract["verdict"], "ready")
        self.assertEqual(contract["non_authoritative_required_fields"], [])

    def test_young_etf_missing_long_history_is_isolated(self):
        strategy = get_strategy("momentum_lite_v1")
        result = build_strategy_input(
            strategy=strategy,
            live_rows=[{"ticker": "DRAM"}],
            feature_matrix={
                "DRAM": {
                    "ticker": "DRAM",
                    "source": "yfinance",
                    "trading_date": "2026-05-27",
                    "close_price": 52.0,
                    "return_20d": 0.62,
                    "return_60d": None,
                    "return_252d": None,
                    "rsi_14": 74.0,
                    "atr_pct": 0.057,
                    "hist_vol_20d": 0.057,
                }
            },
            as_of=date(2026, 5, 28),
        )

        self.assertEqual(result.status, "not_scored")
        self.assertEqual(result.scorable_rows, [])
        reasons = result.excluded_tickers["DRAM"]
        self.assertTrue(any(
            reason["type"] == ExclusionReason.INSUFFICIENT_HISTORY.value
            and reason["field"] == "mom_60d"
            for reason in reasons
        ))
        self.assertTrue(any(
            reason["type"] == ExclusionReason.INSUFFICIENT_HISTORY.value
            and reason["field"] == "mom_252d"
            for reason in reasons
        ))

    def test_one_bad_ticker_does_not_block_scorable_universe(self):
        strategy = get_strategy("momentum_lite_v1")
        feature_matrix = {
            "SPY": _valid_momentum_feature("SPY", 0.02, 0.06, 0.18),
            "QQQ": _valid_momentum_feature("QQQ", 0.04, 0.08, 0.22),
            "IWM": _valid_momentum_feature("IWM", 0.01, 0.03, 0.10),
            "DRAM": {
                "ticker": "DRAM",
                "source": "yfinance",
                "trading_date": "2026-05-27",
                "close_price": 52.0,
                "return_20d": 0.62,
                "return_60d": None,
                "return_252d": None,
                "rsi_14": 74.0,
                "atr_pct": 0.057,
            },
        }

        result = build_strategy_input(
            strategy=strategy,
            live_rows=[{"ticker": ticker} for ticker in feature_matrix],
            feature_matrix=feature_matrix,
            as_of=date(2026, 5, 28),
        )

        self.assertEqual(result.status, "partially_scored")
        self.assertEqual({row["ticker"] for row in result.scorable_rows}, {"SPY", "QQQ", "IWM"})
        self.assertIn("DRAM", result.excluded_tickers)
        self.assertEqual(result.readiness_summary["exclusion_counts"]["insufficient_history"], 2)

    def test_low_coverage_still_partial_scores_scorable_rows(self):
        strategy = get_strategy("momentum_lite_v1")
        feature_matrix = {
            "SPY": _valid_momentum_feature("SPY", 0.02, 0.06, 0.18),
            "DRAM": {
                "ticker": "DRAM",
                "source": "yfinance",
                "trading_date": "2026-05-27",
                "close_price": 52.0,
                "return_20d": 0.62,
                "return_60d": None,
                "return_252d": None,
                "rsi_14": 74.0,
                "atr_pct": 0.057,
            },
            "NEW1": {
                "ticker": "NEW1",
                "source": "yfinance",
                "trading_date": "2026-05-27",
                "close_price": 20.0,
                "return_20d": 0.10,
                "return_60d": None,
                "return_252d": None,
                "rsi_14": 55.0,
                "atr_pct": 0.030,
            },
            "NEW2": {
                "ticker": "NEW2",
                "source": "yfinance",
                "trading_date": "2026-05-27",
                "close_price": 30.0,
                "return_20d": 0.08,
                "return_60d": None,
                "return_252d": None,
                "rsi_14": 52.0,
                "atr_pct": 0.028,
            },
        }

        result = build_strategy_input(
            strategy=strategy,
            live_rows=[{"ticker": ticker} for ticker in feature_matrix],
            feature_matrix=feature_matrix,
            as_of=date(2026, 5, 28),
        )

        self.assertEqual(result.status, "partially_scored")
        self.assertTrue(result.can_score)
        self.assertEqual([row["ticker"] for row in result.scorable_rows], ["SPY"])
        self.assertIsNone(result.not_scored_reason)
        self.assertTrue(result.readiness_summary["ready"])
        self.assertTrue(result.readiness_summary["coverage_below_min_required"])
        self.assertEqual(
            result.readiness_summary["partial_scoring_reason"],
            "scorable_coverage_below_min_required",
        )
        self.assertGreater(result.readiness_summary["coverage_shortfall"], 0.0)
        self.assertEqual(result.readiness_summary["selection_policy"], "partial_scoring_with_ticker_isolation")


if __name__ == "__main__":
    unittest.main()
