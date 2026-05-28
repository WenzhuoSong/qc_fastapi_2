import re
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATHS = [
    REPO_ROOT / "db" / "migrations",
    REPO_ROOT / "db" / "session.py",
    REPO_ROOT / "services" / "market_feature_store.py",
]

PROTECTED_HOLDINGS_FACTOR_FIELDS = {
    "daily_return_pct",
    "return_5d",
    "mom_20d",
    "mom_60d",
    "mom_252d",
    "sma_20",
    "sma_50",
    "sma_200",
    "rsi_14",
    "atr_pct",
    "bb_position",
    "hist_vol_20d",
}


class MigrationSafetyTests(unittest.TestCase):
    def test_no_destructive_schema_operations_in_migrations(self):
        patterns = [
            r"\bDROP\s+TABLE\b",
            r"\bDROP\s+COLUMN\b",
            r"\bTRUNCATE\b",
            r"\bDELETE\s+FROM\s+qc_snapshots\b",
            r"\bDELETE\s+FROM\s+holdings_factors\b",
            r"\bDELETE\s+FROM\s+market_daily_features\b",
            r"drop_column\s*\(",
            r"op\.drop_table\s*\(",
        ]
        offenders = []
        for path in _iter_schema_files():
            text = _strip_sql_comments(path.read_text(encoding="utf-8"))
            for pattern in patterns:
                if re.search(pattern, text, flags=re.IGNORECASE):
                    offenders.append(f"{path.relative_to(REPO_ROOT)}:{pattern}")

        self.assertEqual(offenders, [])

    def test_protected_raw_payload_and_market_daily_features_schema_remain_modeled(self):
        models = (REPO_ROOT / "db" / "models.py").read_text(encoding="utf-8")

        self.assertIn('class QCSnapshot', models)
        self.assertRegex(models, r"raw_payload\s*=\s*Column\(JSONB,\s*nullable=False\)")
        self.assertIn('class MarketDailyFeature', models)
        self.assertIn('__tablename__ = "market_daily_features"', models)
        self.assertRegex(models, r"\brsi_10\s*=\s*Column\(")
        self.assertRegex(models, r"\bbeta_vs_spy\s*=\s*Column\(")
        self.assertRegex(models, r"raw_payload\s*=\s*Column\(JSONB\)")
        self.assertIn('class StrategyFrozenSignal', models)
        self.assertIn('__tablename__ = "strategy_frozen_signals"', models)
        self.assertRegex(models, r"\bcontent_hash\s*=\s*Column\(")
        self.assertIn('class StrategySignalOutcome', models)
        self.assertIn('__tablename__ = "strategy_signal_outcomes"', models)
        self.assertRegex(models, r"\bexcess_calculation_method\s*=\s*Column\(")
        self.assertIn('class StrategyConvictionProfile', models)
        self.assertIn('__tablename__ = "strategy_conviction_profiles"', models)
        self.assertRegex(models, r"\brequires_live_confirmation\s*=\s*Column\(")
        self.assertIn('class AccountStateSnapshot', models)
        self.assertIn('__tablename__ = "account_state_snapshots"', models)
        self.assertRegex(models, r"\braw_snapshot\s*=\s*Column\(JSONB,\s*nullable=False\)")
        self.assertIn('class CommandLifecycleEvent', models)
        self.assertIn('__tablename__ = "command_lifecycle_events"', models)
        self.assertRegex(models, r"\bevent_type\s*=\s*Column\(")
        self.assertIn('class DeferredExecutionLedger', models)
        self.assertIn('__tablename__ = "deferred_execution_ledger"', models)
        self.assertRegex(models, r"\bremaining_delta\s*=\s*Column\(")
        self.assertIn('class PerformanceAttribution', models)
        self.assertIn('__tablename__ = "performance_attribution"', models)
        self.assertRegex(models, r"\bresidual_alpha_candidate\s*=\s*Column\(")
        self.assertIn('class AlphaValidationRun', models)
        self.assertIn('__tablename__ = "alpha_validation_runs"', models)
        self.assertRegex(models, r"\bsignal_alignment_score\s*=\s*Column\(")

    def test_holdings_factors_legacy_fields_remain_modeled(self):
        models = (REPO_ROOT / "db" / "models.py").read_text(encoding="utf-8")
        missing = [
            field
            for field in sorted(PROTECTED_HOLDINGS_FACTOR_FIELDS)
            if not re.search(rf"\b{re.escape(field)}\s*=\s*Column\(", models)
        ]

        self.assertEqual(missing, [])

    def test_feature_sources_are_preserved_and_augmented_not_replaced(self):
        provenance = (REPO_ROOT / "services" / "feature_provenance.py").read_text(encoding="utf-8")
        merge = (REPO_ROOT / "services" / "market_snapshot_merge.py").read_text(encoding="utf-8")

        self.assertIn('sources = list(out.get("feature_sources") or [])', provenance)
        self.assertIn('entries.extend(row.get("feature_sources") or [])', provenance)
        self.assertIn('"authority_by_field"', provenance)
        self.assertIn('"canonical_aliases"', provenance)
        self.assertIn('merge_feature_sources(qc_daily_row, yfinance_row, live_row)', merge)


def _iter_schema_files():
    for path in MIGRATION_PATHS:
        if path.is_dir():
            yield from sorted(item for item in path.rglob("*") if item.suffix in {".sql", ".py", ".md"})
        elif path.exists():
            yield path


def _strip_sql_comments(text: str) -> str:
    lines = []
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("--") or stripped.startswith("#"):
            continue
        lines.append(line)
    return "\n".join(lines)


if __name__ == "__main__":
    unittest.main()
