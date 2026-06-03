import unittest

from services.strategy_mapping_audit import (
    build_current_strategy_mapping_audit,
    build_strategy_mapping_audit,
)


def _asset(
    *,
    role: str = "core_market",
    allowed_actions: list[str] | None = None,
    include_safety_fields: bool = True,
) -> dict:
    asset = {
        "id": "TEST",
        "role": role,
        "asset_class": "equity_etf",
    }
    if include_safety_fields:
        asset.update({
            "allowed_actions": allowed_actions or ["increase", "reduce", "hold", "watch"],
            "max_reasonable_weight": {"full_auto": 0.2},
            "risk_budget_cost": 0.4,
            "decay_risk": "low",
        })
    return asset


def _strategy_profile(*, role: str = "core_market", action: str = "increase") -> dict:
    return {
        "id": "momentum_lite_v1",
        "compatibility_mappings": [
            {
                "role": role,
                "score_thresholds": [
                    {"gte": 0.0, "action": action, "signal_type": "test_signal"},
                ],
                "weight_formula": "confidence_cap_multiplier",
            }
        ],
    }


class StrategyMappingAuditTest(unittest.TestCase):
    def test_current_policy_universe_has_no_hard_mapping_errors(self):
        audit = build_current_strategy_mapping_audit()

        self.assertEqual(audit["hard_mapping_error_count"], 0)
        self.assertEqual(audit["missing_strategy_profiles"], [])
        self.assertEqual(audit["missing_asset_profiles"], [])
        self.assertNotIn("missing_asset_profile", audit["by_reason"])
        self.assertNotIn("missing_strategy_profile", audit["by_reason"])
        self.assertNotIn("missing_compatibility_mapping", audit["by_reason"])

    def test_missing_strategy_profile_is_hard_mapping_error(self):
        audit = build_strategy_mapping_audit(
            strategy_ids=["momentum_lite_v1"],
            tickers=["SPY"],
            strategy_profiles={},
            asset_profiles={"SPY": _asset()},
        )

        self.assertEqual(audit["by_reason"], {"missing_strategy_profile": 1})
        self.assertEqual(audit["hard_mapping_error_count"], 1)
        self.assertEqual(audit["normal_watch_count"], 0)
        self.assertEqual(audit["missing_strategy_profiles"], ["momentum_lite_v1"])

    def test_missing_asset_profile_is_hard_mapping_error(self):
        audit = build_strategy_mapping_audit(
            strategy_ids=["momentum_lite_v1"],
            tickers=["SPY"],
            strategy_profiles={"momentum_lite_v1": _strategy_profile()},
            asset_profiles={},
        )

        self.assertEqual(audit["by_reason"], {"missing_asset_profile": 1})
        self.assertEqual(audit["hard_mapping_error_count"], 1)
        self.assertEqual(audit["missing_asset_profiles"], ["SPY"])

    def test_missing_compatibility_mapping_is_hard_mapping_error(self):
        audit = build_strategy_mapping_audit(
            strategy_ids=["momentum_lite_v1"],
            tickers=["SPY"],
            strategy_profiles={
                "momentum_lite_v1": _strategy_profile(role="satellite_theme")
            },
            asset_profiles={"SPY": _asset(role="core_market")},
        )

        self.assertEqual(audit["by_reason"], {"missing_compatibility_mapping": 1})
        self.assertEqual(audit["hard_mapping_error_count"], 1)

    def test_missing_required_safety_field_is_hard_mapping_error(self):
        audit = build_strategy_mapping_audit(
            strategy_ids=["momentum_lite_v1"],
            tickers=["SPY"],
            strategy_profiles={"momentum_lite_v1": _strategy_profile()},
            asset_profiles={"SPY": _asset(include_safety_fields=False)},
        )

        self.assertEqual(audit["by_reason"], {"missing_required_safety_field": 1})
        self.assertEqual(audit["hard_mapping_error_count"], 1)
        self.assertEqual(
            audit["hard_mapping_errors"][0]["missing_fields"],
            ["allowed_actions", "max_reasonable_weight", "risk_budget_cost", "decay_risk"],
        )

    def test_action_disallowed_by_asset_profile_is_normal_watch(self):
        audit = build_strategy_mapping_audit(
            strategy_ids=["momentum_lite_v1"],
            tickers=["SPY"],
            strategy_profiles={
                "momentum_lite_v1": _strategy_profile(action="hedge")
            },
            asset_profiles={
                "SPY": _asset(allowed_actions=["increase", "reduce", "hold", "watch"])
            },
        )

        self.assertEqual(audit["by_reason"], {"action_not_allowed_by_asset_profile": 1})
        self.assertEqual(audit["hard_mapping_error_count"], 0)
        self.assertEqual(audit["normal_watch_count"], 1)
        self.assertEqual(audit["normal_watch_rows"][0]["status"], "watch")

    def test_watch_only_mapping_counts_as_covered_watch(self):
        audit = build_strategy_mapping_audit(
            strategy_ids=["momentum_lite_v1"],
            tickers=["SPY"],
            strategy_profiles={
                "momentum_lite_v1": _strategy_profile(action="watch")
            },
            asset_profiles={"SPY": _asset()},
        )

        self.assertEqual(audit["by_reason"], {"watch_only_mapping": 1})
        self.assertEqual(audit["hard_mapping_error_count"], 0)
        self.assertEqual(audit["normal_watch_count"], 1)
        coverage = audit["strategy_coverage"]["momentum_lite_v1"]
        self.assertEqual(coverage["voted_or_watch_rows"], 1)
        self.assertEqual(coverage["coverage_pct"], 1.0)

    def test_coverage_denominator_is_eligible_ticker_count(self):
        audit = build_strategy_mapping_audit(
            strategy_ids=["momentum_lite_v1"],
            tickers=["SPY", "QQQ", "XLE"],
            strategy_profiles={"momentum_lite_v1": _strategy_profile()},
            asset_profiles={
                "SPY": _asset(role="core_market"),
                "QQQ": _asset(role="core_market", allowed_actions=["watch"]),
                # XLE intentionally missing to create one hard mapping error.
            },
        )

        coverage = audit["strategy_coverage"]["momentum_lite_v1"]
        self.assertEqual(coverage["eligible_ticker_count"], 3)
        self.assertEqual(coverage["voted_or_watch_rows"], 2)
        self.assertEqual(coverage["mapping_error_rows"], 1)
        self.assertEqual(coverage["coverage_pct"], round(2 / 3, 6))
        self.assertEqual(audit["ticker_coverage"]["QQQ"]["watch_count"], 1)
        self.assertEqual(audit["ticker_coverage"]["XLE"]["mapping_error_count"], 1)


if __name__ == "__main__":
    unittest.main()
