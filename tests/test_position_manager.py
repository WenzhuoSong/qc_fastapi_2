import sys
import types
import unittest


def _install_import_stubs() -> None:
    """Allow importing services.position_manager without DB dependencies."""
    sqlalchemy = types.ModuleType("sqlalchemy")
    sqlalchemy.select = lambda *args, **kwargs: None
    sqlalchemy.desc = lambda *args, **kwargs: None
    sys.modules.setdefault("sqlalchemy", sqlalchemy)

    sys.modules.setdefault("db", types.ModuleType("db"))

    session = types.ModuleType("db.session")
    session.AsyncSessionLocal = object
    sys.modules.setdefault("db.session", session)

    models = types.ModuleType("db.models")
    for name in ("HoldingsFactor", "QCSnapshot", "AlertLog", "MacroNewsCache", "TickerNewsLibrary"):
        setattr(models, name, type(name, (), {}))
    sys.modules.setdefault("db.models", models)

    queries = types.ModuleType("db.queries")
    queries.upsert_alert = lambda *args, **kwargs: None
    sys.modules.setdefault("db.queries", queries)

    config = types.ModuleType("config")
    config.get_settings = lambda: object()
    sys.modules.setdefault("config", config)


_install_import_stubs()

from services.position_manager import apply_position_constraints  # noqa: E402


class PositionManagerTest(unittest.TestCase):
    def test_min_hold_days_preserves_young_sell(self):
        out = apply_position_constraints(
            target_weights={"AAA": 0.0, "CASH": 1.0},
            current_holdings={"AAA": 0.20, "CASH": 0.80},
            config={"min_hold_days": 2},
            holdings_meta=[{"ticker": "AAA", "holding_days": 1}],
        )

        self.assertAlmostEqual(out.adjusted_weights["AAA"], 0.20, places=4)
        self.assertTrue(any(v.startswith("min_hold_days:AAA") for v in out.violations))
        self.assertAlmostEqual(sum(out.adjusted_weights.values()), 1.0, places=4)

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


if __name__ == "__main__":
    unittest.main()
