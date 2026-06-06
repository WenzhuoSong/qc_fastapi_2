from pathlib import Path
import unittest


class ReconciliationGuardPipelineContractTests(unittest.TestCase):
    def test_pipeline_runs_reconciliation_guard_after_account_guard_before_stage1(self):
        text = Path("services/pipeline.py").read_text()

        account_block_pos = text.index('tracker.end_run("skipped_account_state_guard")')
        guard_pos = text.index("reconciliation_guard = await load_reconciliation_guard")
        stage1_pos = text.index("# Stage 1: market_brief")

        self.assertLess(account_block_pos, guard_pos)
        self.assertLess(guard_pos, stage1_pos)
        self.assertIn('"status": "skipped_reconciliation_guard"', text)
        self.assertIn("format_reconciliation_guard_alert(reconciliation_guard)", text)

    def test_pipeline_loads_reconciliation_guard_config(self):
        text = Path("services/pipeline.py").read_text()

        self.assertIn('get_system_config(db, "reconciliation_guard_config")', text)
        self.assertIn("default_reconciliation_guard_config", text)
        self.assertIn('"reconciliation_guard_config": reconciliation_guard_config', text)

    def test_seed_has_reconciliation_guard_default(self):
        text = Path("db/seed.py").read_text()

        self.assertIn('"reconciliation_guard_config"', text)
        self.assertIn('"mode": "blocking"', text)
        self.assertIn('"ignore_cash": True', text)


if __name__ == "__main__":
    unittest.main()
