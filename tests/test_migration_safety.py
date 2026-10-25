import re
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATHS = [
    REPO_ROOT / "db" / "migrations",
    REPO_ROOT / "db" / "session.py",
    REPO_ROOT / "scout" / "migrations",
]



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

    def test_newbase_live_monitoring_migration_is_append_only(self):
        migration = (
            REPO_ROOT
            / "db"
            / "migrations"
            / "20260622_create_strategy_live_monitoring.sql"
        ).read_text(encoding="utf-8")

        self.assertIn("CREATE TABLE IF NOT EXISTS strategy_registry_entries", migration)
        self.assertIn("CREATE TABLE IF NOT EXISTS strategy_live_snapshots", migration)
        self.assertIn("CREATE INDEX IF NOT EXISTS idx_strategy_live_snapshots_strategy_recorded", migration)
        self.assertIn("CONSTRAINT uq_strategy_live_snapshot_uid UNIQUE", migration)
        self.assertIn("observer-only", migration)
        self.assertNotRegex(migration, r"\bDROP\b|\bTRUNCATE\b|\bDELETE\s+FROM\b")

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
