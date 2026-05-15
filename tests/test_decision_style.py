import unittest

from services.decision_style import (
    apply_style_limits,
    compute_weighted_conviction,
    conviction_to_style,
    resolve_decision_style,
    resolve_style_conflicts,
)


def _scorecard(**overrides):
    base = {
        "investment_permission": "normal_rebalance",
        "regime": "trending_bull",
        "breadth": "broad",
        "risk_appetite": "risk_on",
        "volatility": "normal",
        "data_quality": "fresh",
        "require_human_confirmation": False,
    }
    base.update(overrides)
    return base


def _news(**overrides):
    base = {
        "macro_news_score": {
            "overall_bias": "positive",
            "confidence": "high",
            "market_impact": "medium",
            "data_quality": "fresh",
        },
        "ticker_news_scores": {
            "XSD": {
                "action_bias": "allow_overweight",
                "effective_credibility": 0.9,
            }
        },
        "hard_risk_events": {},
    }
    base.update(overrides)
    return base


def _strategies(**overrides):
    base = {
        "data_quality": "fresh",
        "snapshot_count": 80,
        "forward_return_samples": 40,
        "strategy_results": [{"name": "momentum", "turnover": 0.20}],
    }
    base.update(overrides)
    return base


class DecisionStyleTest(unittest.TestCase):
    def test_weighted_conviction_uses_operational_weights(self):
        value = compute_weighted_conviction(
            {
                "quant_score": 0.4,
                "news_score": 0.2,
                "macro_score": 0.1,
                "risk_penalty": 0.3,
            },
            weights={
                "quant_weight": 1.0,
                "news_weight": 2.0,
                "macro_weight": 1.0,
                "risk_weight": 1.0,
            },
        )

        self.assertAlmostEqual(value, 0.6)

    def test_conviction_to_style_thresholds_and_scorecard_cap(self):
        strong = conviction_to_style(0.75, _scorecard())
        self.assertEqual(strong["raw_trade_style"], "normal_rebalance")
        self.assertEqual(strong["trade_style"], "normal_rebalance")

        capped = conviction_to_style(0.75, _scorecard(investment_permission="small_overweight_only"))
        self.assertEqual(capped["raw_trade_style"], "normal_rebalance")
        self.assertEqual(capped["trade_style"], "step_in")
        self.assertTrue(capped["capped_by_scorecard"])

        cash = conviction_to_style(0.75, _scorecard(investment_permission="cash_only"))
        self.assertEqual(cash["trade_style"], "cash_only")

        weak = conviction_to_style(0.05, _scorecard())
        self.assertEqual(weak["trade_style"], "hold_unless_strong")

        negative = conviction_to_style(-0.30, _scorecard())
        self.assertEqual(negative["trade_style"], "risk_reduce_fast")

    def test_resolve_style_conflicts_uses_conservative_intersection(self):
        resolved = resolve_style_conflicts(
            [
                {
                    "name": "limited_data_quality",
                    "analysis_style": "conservative",
                    "trade_style": "step_in",
                    "style_limits": {
                        "max_adjustment_multiplier": 0.6,
                        "max_turnover_per_cycle": 0.15,
                        "max_single_trade_pct": 0.04,
                        "max_new_buys_per_cycle": 2,
                        "min_cash_floor_addition": 0.05,
                    },
                },
                {
                    "name": "high_turnover_strategy",
                    "analysis_style": "low_turnover",
                    "trade_style": "hold_unless_strong",
                    "style_limits": {
                        "max_adjustment_multiplier": 0.7,
                        "max_turnover_per_cycle": 0.10,
                        "rebalance_threshold_boost": 0.02,
                    },
                },
            ]
        )

        self.assertEqual(resolved["analysis_style"], "conservative")
        self.assertEqual(resolved["trade_style"], "hold_unless_strong")
        self.assertEqual(resolved["dominant_style_constraint"], "high_turnover_strategy")
        self.assertEqual(resolved["style_limits"]["max_adjustment_multiplier"], 0.6)
        self.assertEqual(resolved["style_limits"]["max_turnover_per_cycle"], 0.10)
        self.assertEqual(resolved["style_limits"]["max_single_trade_pct"], 0.04)
        self.assertEqual(resolved["style_limits"]["max_new_buys_per_cycle"], 2)
        self.assertEqual(resolved["style_limits"]["min_cash_floor_addition"], 0.05)
        self.assertEqual(resolved["style_limits"]["rebalance_threshold_boost"], 0.02)

    def test_resolve_decision_style_momentum_confirmed(self):
        resolved = resolve_decision_style(
            market_scorecard=_scorecard(),
            news_evidence=_news(),
            strategy_evidence=_strategies(),
        )

        self.assertEqual(resolved["analysis_style"], "momentum_confirmed")
        self.assertEqual(resolved["trade_style"], "normal_rebalance")
        self.assertIn("momentum_confirmed", resolved["triggered_style_rules"])
        self.assertGreaterEqual(resolved["weighted_conviction"], 0.6)

    def test_resolve_decision_style_limited_data_and_high_turnover(self):
        resolved = resolve_decision_style(
            market_scorecard=_scorecard(data_quality="limited", require_human_confirmation=True),
            news_evidence=_news(),
            strategy_evidence=_strategies(
                data_quality="limited",
                snapshot_count=7,
                forward_return_samples=2,
                strategy_results=[{"name": "momentum", "turnover": 0.67}],
            ),
        )

        self.assertEqual(resolved["analysis_style"], "conservative")
        self.assertEqual(resolved["trade_style"], "hold_unless_strong")
        self.assertIn("strategy_data_quality", resolved["triggered_style_rules"])
        self.assertIn("high_turnover_strategy", resolved["triggered_style_rules"])
        self.assertEqual(resolved["style_limits"]["max_turnover_per_cycle"], 0.10)
        self.assertLessEqual(resolved["style_limits"]["max_adjustment_multiplier"], 0.6)
        self.assertEqual(resolved["style_limits"]["min_cash_floor_addition"], 0.05)

    def test_historical_supported_strategy_quality_does_not_trigger_limited_data_rule(self):
        resolved = resolve_decision_style(
            market_scorecard=_scorecard(data_quality="historical_supported"),
            news_evidence=_news(),
            strategy_evidence=_strategies(
                data_quality="historical_supported",
                snapshot_count=8,
                forward_return_samples=3,
                historical_forward_return_samples=289,
                strategy_results=[{"name": "momentum", "turnover": 0.20}],
            ),
        )

        self.assertNotIn("strategy_data_quality", resolved["triggered_style_rules"])

    def test_macro_negative_high_impact_triggers_macro_defensive(self):
        resolved = resolve_decision_style(
            market_scorecard=_scorecard(regime="range_bound", breadth="weak", risk_appetite="risk_off"),
            news_evidence=_news(
                macro_news_score={
                    "overall_bias": "negative",
                    "confidence": "high",
                    "market_impact": "high",
                    "data_quality": "fresh",
                },
                ticker_news_scores={},
                hard_risk_events={"XLF": ["credit_stress"]},
            ),
            strategy_evidence=_strategies(),
        )

        self.assertEqual(resolved["analysis_style"], "macro_defensive")
        self.assertEqual(resolved["trade_style"], "risk_reduce_fast")
        self.assertFalse(resolved["style_limits"]["allow_new_positions"])
        self.assertTrue(resolved["style_limits"]["prefer_hedges"])
        self.assertTrue(resolved["style_limits"]["sell_priority"])

    def test_forced_style_override_is_still_resolved_conservatively(self):
        resolved = resolve_decision_style(
            market_scorecard=_scorecard(regime="range_bound", breadth="moderate", risk_appetite="mixed"),
            news_evidence=_news(ticker_news_scores={}, macro_news_score={"overall_bias": "neutral", "confidence": "medium", "market_impact": "low", "data_quality": "fresh"}),
            strategy_evidence=_strategies(),
            config={"force_analysis_style": "conservative", "force_trade_style": "step_in"},
        )

        self.assertEqual(resolved["analysis_style"], "conservative")
        self.assertEqual(resolved["trade_style"], "step_in")
        self.assertIn("forced_style_config", resolved["triggered_style_rules"])
        self.assertEqual(resolved["style_limits"]["min_cash_floor_addition"], 0.05)

    def test_apply_style_limits_cash_floor_is_additive(self):
        merged = apply_style_limits(
            {
                "max_adjustment_from_base": 0.05,
                "min_cash_weight": 0.22,
                "max_turnover_per_cycle": 0.20,
                "max_single_position": 0.12,
                "allow_new_positions": True,
            },
            {
                "style_limits": {
                    "max_adjustment_multiplier": 0.6,
                    "min_cash_floor_addition": 0.05,
                    "max_turnover_per_cycle": 0.10,
                    "max_single_trade_pct": 0.04,
                    "allow_new_positions": False,
                    "prefer_hedges": True,
                }
            },
        )

        self.assertAlmostEqual(merged["max_adjustment_from_base"], 0.03)
        self.assertAlmostEqual(merged["min_cash_weight"], 0.27)
        self.assertAlmostEqual(merged["max_turnover_per_cycle"], 0.10)
        self.assertAlmostEqual(merged["max_single_position"], 0.04)
        self.assertFalse(merged["allow_new_positions"])
        self.assertTrue(merged["prefer_hedges"])


if __name__ == "__main__":
    unittest.main()
