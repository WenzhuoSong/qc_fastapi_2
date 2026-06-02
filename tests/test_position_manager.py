import importlib
import sys
import types
import unittest
from unittest.mock import patch


def _load_position_manager_exports():
    """Allow importing services.position_manager without DB dependencies."""
    sqlalchemy = type(sys)("sqlalchemy")
    sqlalchemy.select = lambda *args, **kwargs: None
    sqlalchemy.desc = lambda *args, **kwargs: None

    session = type(sys)("db.session")
    session.AsyncSessionLocal = object

    models = type(sys)("db.models")
    for name in (
        "HoldingsFactor",
        "QCSnapshot",
        "AlertLog",
        "MacroNewsCache",
        "MarketDailyFeature",
        "TickerNewsLibrary",
    ):
        setattr(models, name, type(name, (), {}))

    queries = type(sys)("db.queries")
    queries.upsert_alert = lambda *args, **kwargs: None

    config = type(sys)("config")
    config.get_settings = lambda: object()

    with patch.dict(
        "sys.modules",
        {
            "sqlalchemy": sqlalchemy,
            "db": type(sys)("db"),
            "db.session": session,
            "db.models": models,
            "db.queries": queries,
            "config": config,
        },
    ):
        module = importlib.import_module("services.position_manager")
        return (
            module.apply_position_constraints,
            module._build_position_monitor_diagnostics,
            module._holding_days_are_trusted,
        )


apply_position_constraints, _build_position_monitor_diagnostics, _holding_days_are_trusted = _load_position_manager_exports()


class PositionManagerTest(unittest.TestCase):
    def test_holding_days_schema_gate_rejects_warmup_polluted_version(self):
        snapshot = types.SimpleNamespace(schema_version="1.3")

        self.assertFalse(_holding_days_are_trusted(snapshot))

    def test_holding_days_schema_gate_accepts_fixed_version(self):
        snapshot = types.SimpleNamespace(schema_version="1.4")

        self.assertTrue(_holding_days_are_trusted(snapshot))

    def test_position_monitor_diagnostics_reports_schema_and_filters_unheld_atr(self):
        snapshot = types.SimpleNamespace(id=42, schema_version="1.4")
        rows = [
            types.SimpleNamespace(
                ticker="SPY",
                weight_current=0.12,
                holding_days=7,
                atr_pct=0.012,
            ),
            types.SimpleNamespace(
                ticker="SOXS",
                weight_current=0.0,
                holding_days=0,
                atr_pct=0.19,
            ),
        ]

        diagnostics = _build_position_monitor_diagnostics(
            snapshot=snapshot,
            rows=rows,
            max_holding_days=60,
            atr_threshold=2.0,
        )

        self.assertEqual(diagnostics["heartbeat_schema_version"], "1.4")
        self.assertTrue(diagnostics["holding_days_trusted"])
        self.assertTrue(diagnostics["holding_period_alerts_enabled"])
        self.assertEqual(diagnostics["held_positions"], 1)
        self.assertEqual(diagnostics["unheld_rows_filtered"], 1)
        self.assertEqual(diagnostics["unheld_high_atr_filtered"], 1)
        self.assertEqual(diagnostics["max_observed_holding_days"], 7)

    def test_position_monitor_diagnostics_marks_old_schema_untrusted(self):
        diagnostics = _build_position_monitor_diagnostics(
            snapshot=types.SimpleNamespace(id=1, schema_version="1.3"),
            rows=[],
            max_holding_days=60,
            atr_threshold=2.0,
        )

        self.assertFalse(diagnostics["holding_days_trusted"])
        self.assertFalse(diagnostics["holding_period_alerts_enabled"])
        self.assertEqual(diagnostics["holding_period_skip_reason"], "heartbeat schema_version < 1.4")

    def test_min_hold_days_preserves_young_sell(self):
        out = apply_position_constraints(
            target_weights={"AAA": 0.0, "CASH": 1.0},
            current_holdings={"AAA": 0.20, "CASH": 0.80},
            config={"min_hold_days": 2},
            holdings_meta=[{"ticker": "AAA", "holding_days": 1}],
        )

        self.assertAlmostEqual(out.adjusted_weights["AAA"], 0.20, places=4)
        self.assertTrue(any(v.startswith("min_hold_days:AAA") for v in out.violations))
        self.assertIn("defer_sell_due_to_min_hold_days", out.mutation_types)
        self.assertAlmostEqual(sum(out.adjusted_weights.values()), 1.0, places=4)

    def test_min_hold_days_does_not_defer_exempt_risk_trim(self):
        out = apply_position_constraints(
            target_weights={"XLE": 0.09, "CASH": 0.91},
            current_holdings={"XLE": 0.12, "CASH": 0.88},
            config={
                "min_hold_days": 2,
                "min_hold_exempt_tickers": ["XLE"],
                "max_single_trade_pct": 1.0,
                "max_turnover_per_cycle": 1.0,
            },
            holdings_meta=[{"ticker": "XLE", "holding_days": 1}],
        )

        self.assertAlmostEqual(out.adjusted_weights["XLE"], 0.09, places=4)
        self.assertFalse(any(v.startswith("min_hold_days:XLE") for v in out.violations))
        self.assertNotIn("defer_sell_due_to_min_hold_days", out.mutation_types)

    def test_normalization_missing_cash_does_not_amplify_risk_weight(self):
        out = apply_position_constraints(
            target_weights={"AAA": 0.20},
            current_holdings={"CASH": 1.0},
            config={
                "max_new_buys_per_cycle": 10,
                "max_single_trade_pct": 1.0,
                "max_turnover_per_cycle": 1.0,
                "max_daily_trades": 10,
            },
        )

        self.assertAlmostEqual(out.adjusted_weights["AAA"], 0.20, places=4)
        self.assertAlmostEqual(out.adjusted_weights["CASH"], 0.80, places=4)

    def test_turnover_cap_scales_toward_current_weights(self):
        out = apply_position_constraints(
            target_weights={"AAA": 0.60, "CASH": 0.40},
            current_holdings={"AAA": 0.0, "CASH": 1.0},
            config={
                "max_single_trade_pct": 1.0,
                "max_turnover_per_cycle": 0.20,
                "max_daily_trades": 10,
            },
        )

        self.assertLessEqual(out.trade_summary["total_turnover"], 0.2001)
        self.assertAlmostEqual(out.adjusted_weights["AAA"], 0.20, places=4)
        self.assertTrue(any(v.startswith("turnover_scaled:") for v in out.violations))
        self.assertIn("turnover_scale_toward_current", out.mutation_types)

    def test_max_daily_trades_caps_extra_buys(self):
        out = apply_position_constraints(
            target_weights={"AAA": 0.10, "BBB": 0.09, "CCC": 0.08, "CASH": 0.73},
            current_holdings={"CASH": 1.0},
            config={
                "max_new_buys_per_cycle": 10,
                "max_single_trade_pct": 1.0,
                "max_turnover_per_cycle": 1.0,
                "max_daily_trades": 2,
            },
        )

        held = [t for t, w in out.adjusted_weights.items() if t != "CASH" and w > 0.01]
        self.assertEqual(held, ["AAA", "BBB"])
        self.assertEqual(out.trade_summary["total_trades"], 2)
        self.assertTrue(any(v.startswith("daily_trade_count_capped:") for v in out.violations))
        self.assertIn("cap_trade_count_buys", out.mutation_types)

    def test_actual_daily_trades_reduce_remaining_trade_slots(self):
        out = apply_position_constraints(
            target_weights={"AAA": 0.10, "BBB": 0.09, "CASH": 0.81},
            current_holdings={"CASH": 1.0},
            config={
                "max_new_buys_per_cycle": 10,
                "max_single_trade_pct": 1.0,
                "max_turnover_per_cycle": 1.0,
                "max_daily_trades": 3,
            },
            actual_daily_trades=2,
        )

        held = [t for t, w in out.adjusted_weights.items() if t != "CASH" and w > 0.01]
        self.assertEqual(held, ["AAA"])
        self.assertEqual(out.trade_summary["actual_daily_trades_before_cycle"], 2)
        self.assertTrue(any(v.startswith("daily_trade_count_capped:") for v in out.violations))

    def test_decay_risk_auto_reduce_uses_asset_profile_holding_policy(self):
        out = apply_position_constraints(
            target_weights={"UVXY": 0.03, "CASH": 0.97},
            current_holdings={"UVXY": 0.03, "CASH": 0.97},
            config={
                "max_single_trade_pct": 1.0,
                "max_turnover_per_cycle": 1.0,
                "decay_auto_reduce_pct": 0.25,
            },
            holdings_meta=[{"ticker": "UVXY", "holding_days": 8}],
            asset_profiles={
                "UVXY": {
                    "decay_risk": "extreme",
                    "holding_policy": {
                        "auto_reduce_after_days": 7,
                        "max_hold_days": 10,
                    },
                }
            },
        )

        self.assertLess(out.adjusted_weights["UVXY"], 0.03)
        self.assertTrue(any(v.startswith("decay_auto_reduce:UVXY") for v in out.violations))
        self.assertIn("decay_risk_auto_reduce", out.mutation_types)
        self.assertEqual(out.trade_summary["decay_holding_reviews"], 1)

    def test_decay_max_hold_review_forces_larger_trim(self):
        out = apply_position_constraints(
            target_weights={"TQQQ": 0.04, "CASH": 0.96},
            current_holdings={"TQQQ": 0.04, "CASH": 0.96},
            config={
                "max_single_trade_pct": 1.0,
                "max_turnover_per_cycle": 1.0,
                "decay_auto_reduce_pct": 0.25,
            },
            holdings_meta=[{"ticker": "TQQQ", "holding_days": 12}],
            asset_profiles={
                "TQQQ": {
                    "decay_risk": "high",
                    "holding_policy": {"max_hold_days": 10},
                }
            },
        )

        self.assertAlmostEqual(out.adjusted_weights["TQQQ"], 0.02, places=4)
        self.assertTrue(any(v.startswith("decay_max_hold_review:TQQQ") for v in out.violations))
        self.assertIn("decay_risk_auto_reduce", out.mutation_types)


if __name__ == "__main__":
    unittest.main()
