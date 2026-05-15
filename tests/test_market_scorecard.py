import unittest
from datetime import datetime, timedelta, timezone

from services.market_scorecard import (
    build_market_scorecard,
    is_evidence_stale,
    resolve_conflicts,
)


def fresh_evidence(**overrides):
    base = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "max_age_seconds": 1800,
        "market": {
            "regime": "trending_bull",
            "regime_confidence": "medium",
            "spy_mom_20d": 0.03,
            "spy_mom_60d": 0.07,
            "spy_rsi": 61.0,
            "vix": 18.0,
            "drawdown_pct": 0.02,
            "breadth_pct": 0.68,
            "avg_atr_pct": 0.014,
            "risk_on_score": 0.08,
        },
        "rotation": {
            "rotation_label": "risk_on_rotation",
            "risk_appetite_score": 0.03,
            "leaders": [{"ticker": "XLK"}, {"ticker": "XLY"}, {"ticker": "QQQ"}],
            "laggards": [],
            "notes": [],
        },
        "news": {
            "macro_signals": [],
            "ticker_signals": {},
            "data_quality": "fresh",
            "warnings": [],
        },
        "strategies": {
            "playground_available": True,
            "snapshot_count": 30,
            "forward_return_samples": 12,
            "strategy_results": [{"name": "momentum_lite_v1", "turnover": 0.25}],
            "data_quality": "fresh",
        },
        "data_quality": {
            "overall": "fresh",
            "warnings": [],
        },
    }
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            merged = dict(base[key])
            merged.update(value)
            base[key] = merged
        else:
            base[key] = value
    return base


class MarketScorecardTest(unittest.TestCase):
    def test_clean_bullish_market_allows_normal_rebalance(self):
        scorecard = build_market_scorecard(fresh_evidence())

        self.assertEqual(scorecard["market_condition"], "bullish")
        self.assertEqual(scorecard["investment_permission"], "normal_rebalance")
        self.assertFalse(scorecard["require_human_confirmation"])
        self.assertEqual(scorecard["max_adjustment_from_base"], 0.05)
        self.assertEqual(scorecard["min_cash_weight"], 0.05)

    def test_limited_data_caps_adjustment_and_requires_human_confirmation(self):
        evidence = fresh_evidence(
            strategies={
                "playground_available": True,
                "snapshot_count": 7,
                "forward_return_samples": 2,
                "strategy_results": [],
                "data_quality": "limited",
            },
            data_quality={"overall": "limited", "warnings": []},
        )

        scorecard = build_market_scorecard(evidence)

        self.assertEqual(scorecard["investment_permission"], "small_overweight_only")
        self.assertEqual(scorecard["max_adjustment_from_base"], 0.03)
        self.assertTrue(scorecard["require_human_confirmation"])
        self.assertIn("limited_data_quality", scorecard["triggered_rules"])

    def test_missing_playground_has_explicit_fallback(self):
        evidence = fresh_evidence(
            strategies={
                "playground_available": False,
                "snapshot_count": 0,
                "forward_return_samples": 0,
                "strategy_results": [],
                "data_quality": "missing",
            },
            data_quality={"overall": "missing", "warnings": []},
        )

        scorecard = build_market_scorecard(evidence)

        self.assertIn("playground_missing", scorecard["triggered_rules"])
        self.assertIn("limited_data_quality", scorecard["triggered_rules"])
        self.assertTrue(scorecard["require_human_confirmation"])
        self.assertLessEqual(scorecard["max_adjustment_from_base"], 0.03)

    def test_bullish_but_bond_heavy_rotation_is_mixed(self):
        evidence = fresh_evidence(
            rotation={
                "rotation_label": "mixed_rotation",
                "risk_appetite_score": 0.0,
                "leaders": [{"ticker": "IEF"}, {"ticker": "TLT"}, {"ticker": "BND"}],
            }
        )

        scorecard = build_market_scorecard(evidence)

        self.assertEqual(scorecard["market_condition"], "bullish_but_mixed")
        self.assertIn("bullish_but_mixed_rotation", scorecard["triggered_rules"])
        self.assertEqual(scorecard["investment_permission"], "small_overweight_only")

    def test_advisory_only_strategy_confidence_caps_action(self):
        evidence = fresh_evidence(
            strategies={
                "playground_available": True,
                "snapshot_count": 8,
                "forward_return_samples": 3,
                "historical_forward_return_samples": 289,
                "strategy_results": [
                    {"strategy_name": "momentum_lite_v1", "turnover": 0.66}
                ],
                "strategy_confidence": {
                    "momentum_lite_v1": {
                        "confidence_score": 0.64,
                        "suggested_use": "advisory",
                        "consensus_conflict": True,
                    }
                },
                "data_quality": "historical_supported",
            },
            data_quality={"overall": "historical_supported", "warnings": []},
        )

        scorecard = build_market_scorecard(evidence)

        self.assertIn("strategy_consensus_regime_conflict", scorecard["triggered_rules"])
        self.assertIn("strategy_advisory_only", scorecard["triggered_rules"])
        self.assertEqual(scorecard["investment_permission"], "small_overweight_only")
        self.assertLessEqual(scorecard["max_turnover_per_cycle"], 0.20)
        self.assertTrue(scorecard["require_human_confirmation"])

    def test_high_volatility_sets_defensive_limits(self):
        evidence = fresh_evidence(market={"vix": 34.0, "avg_atr_pct": 0.014})

        scorecard = build_market_scorecard(evidence)

        self.assertEqual(scorecard["volatility"], "high")
        self.assertEqual(scorecard["investment_permission"], "defensive_only")
        self.assertEqual(scorecard["min_cash_weight"], 0.15)
        self.assertEqual(scorecard["max_single_position"], 0.15)
        self.assertTrue(scorecard["prefer_hedges"])

    def test_extreme_volatility_produces_cash_only(self):
        evidence = fresh_evidence(market={"vix": 55.0})

        scorecard = build_market_scorecard(evidence)

        self.assertEqual(scorecard["investment_permission"], "cash_only")
        self.assertEqual(scorecard["max_equity_weight"], 0.0)
        self.assertEqual(scorecard["min_cash_weight"], 1.0)
        self.assertFalse(scorecard["allow_new_positions"])

    def test_defensive_drawdown_blocks_new_positions(self):
        evidence = fresh_evidence(market={"drawdown_pct": 0.12})

        scorecard = build_market_scorecard(evidence)

        self.assertEqual(scorecard["investment_permission"], "reduce_risk_only")
        self.assertEqual(scorecard["max_equity_weight"], 0.50)
        self.assertEqual(scorecard["min_cash_weight"], 0.20)
        self.assertFalse(scorecard["allow_new_positions"])

    def test_stale_evidence_limits_action(self):
        evidence = fresh_evidence(
            generated_at=(datetime.now(timezone.utc) - timedelta(hours=2)).isoformat(),
            max_age_seconds=1800,
        )

        self.assertTrue(is_evidence_stale(evidence))
        scorecard = build_market_scorecard(evidence)

        self.assertIn("stale_evidence", scorecard["triggered_rules"])
        self.assertTrue(scorecard["require_human_confirmation"])
        self.assertLessEqual(scorecard["max_adjustment_from_base"], 0.01)

    def test_resolve_conflicts_takes_most_conservative_intersection(self):
        resolved = resolve_conflicts(
            [
                {
                    "name": "limited_data_quality",
                    "investment_permission": "small_overweight_only",
                    "max_adjustment_from_base": 0.03,
                    "max_equity_weight": 0.85,
                    "min_cash_weight": 0.10,
                    "max_turnover_per_cycle": 0.20,
                    "allow_new_positions": True,
                    "require_human_confirmation": True,
                },
                {
                    "name": "high_volatility",
                    "investment_permission": "defensive_only",
                    "max_adjustment_from_base": 0.03,
                    "max_equity_weight": 0.65,
                    "min_cash_weight": 0.15,
                    "max_turnover_per_cycle": 0.30,
                    "max_single_position": 0.15,
                    "allow_new_positions": False,
                    "prefer_hedges": True,
                },
            ]
        )

        self.assertEqual(resolved["investment_permission"], "defensive_only")
        self.assertEqual(resolved["dominant_constraint"], "high_volatility")
        self.assertEqual(resolved["max_equity_weight"], 0.65)
        self.assertEqual(resolved["min_cash_weight"], 0.15)
        self.assertEqual(resolved["max_turnover_per_cycle"], 0.20)
        self.assertFalse(resolved["allow_new_positions"])
        self.assertTrue(resolved["require_human_confirmation"])
        self.assertTrue(resolved["prefer_hedges"])


if __name__ == "__main__":
    unittest.main()
