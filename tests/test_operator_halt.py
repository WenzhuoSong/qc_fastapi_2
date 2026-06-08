import unittest
from datetime import datetime
from pathlib import Path

from services.operator_halt import (
    build_operator_halt_state,
    normalize_operator_halt_state,
)


class OperatorHaltTests(unittest.TestCase):
    def test_missing_state_fails_safe_halted(self):
        state = normalize_operator_halt_state(None)

        self.assertTrue(state["halted"])
        self.assertTrue(state["fail_safe"])
        self.assertEqual(state["source"], "fail_safe")
        self.assertEqual(state["reason"], "operator_halt_state_missing")

    def test_malformed_state_fails_safe_halted(self):
        state = normalize_operator_halt_state({"reason": "missing halted"})

        self.assertTrue(state["halted"])
        self.assertTrue(state["fail_safe"])
        self.assertEqual(state["reason"], "operator_halt_state_missing_or_invalid_halted")

    def test_build_state_is_normalizable(self):
        now = datetime(2026, 6, 6, 12, 0, 0)
        raw = build_operator_halt_state(
            halted=True,
            reason="operator review",
            updated_by="test",
            now=now,
        )
        state = normalize_operator_halt_state(raw)

        self.assertTrue(state["halted"])
        self.assertFalse(state["fail_safe"])
        self.assertEqual(state["reason"], "operator review")
        self.assertEqual(state["updated_by"], "test")
        self.assertEqual(state["updated_at"], "2026-06-06T12:00:00")

    def test_pipeline_reads_operator_halt_before_auth_mode_execution(self):
        text = Path("services/pipeline.py").read_text()

        halt_read = text.index('get_system_config(db, "operator_halt_state")')
        halt_check = text.index("operator_halt_state halted")
        auth_read = text.index('auth_mode = (auth_cfg.value')

        self.assertLess(halt_read, halt_check)
        self.assertLess(halt_check, auth_read)
        self.assertIn('"operator_halt_state": operator_halt_state', text)

    def test_telegram_has_halt_resume_status_commands(self):
        text = Path("services/telegram_commands.py").read_text()

        self.assertIn('cmd == "/halt"', text)
        self.assertIn('cmd == "/resume"', text)
        self.assertIn("async def _cmd_halt", text)
        self.assertIn("async def _cmd_resume", text)
        self.assertIn("Operator halt", text)
        self.assertIn("Circuit and reconciliation halt, if active, are not cleared", text)

    def test_reset_circuit_does_not_clear_operator_halt(self):
        text = Path("services/telegram_commands.py").read_text()
        reset_body = text[text.index("async def _cmd_reset_circuit") : text.index("async def _cmd_force_reconcile")]

        self.assertIn('"circuit_state"', reset_body)
        self.assertNotIn("operator_halt_state", reset_body)

    def test_seed_has_default_operator_halt_state(self):
        text = Path("db/seed.py").read_text()

        self.assertIn('"operator_halt_state"', text)
        self.assertIn('"halted": False', text)
