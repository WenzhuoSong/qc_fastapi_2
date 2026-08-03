import os
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

# qc_tools has an existing import-time Settings dependency. These inert values
# allow the fence tests to import the module; every test proves HTTP is blocked.
for _name, _value in {
    "OPENAI_API_KEY": "test-only",
    "WEBHOOK_SECRET": "test-only",
    "QC_USER_ID": "1",
    "QC_API_TOKEN": "test-only",
    "QC_PROJECT_ID": "1",
    "TG_BOT_TOKEN": "test-only",
    "TG_CHAT_ID": "1",
}.items():
    os.environ.setdefault(_name, _value)

from services.execution_authority import (
    EXECUTION_AUTHORITY_ENV,
    evaluate_execution_authority_fence,
    execution_authority_status,
)
from services.qc_command_sender import send_setweights_command
from tools.qc_tools import (
    tool_send_cancel_orders_command,
    tool_send_policy_sync,
    tool_send_weight_command,
)


class ExecutionAuthorityFenceTests(unittest.IsolatedAsyncioTestCase):
    def test_missing_environment_is_fail_closed(self):
        decision = evaluate_execution_authority_fence("SetWeights", environ={})

        self.assertFalse(decision["allowed"])
        self.assertEqual(decision["execution_authority"], "none")
        self.assertEqual(decision["reason"], "fastapi_execution_authority_none")

    def test_legacy_full_auto_value_cannot_enable_execution(self):
        decision = evaluate_execution_authority_fence(
            "SetWeights",
            environ={EXECUTION_AUTHORITY_ENV: "FULL_AUTO"},
        )

        self.assertFalse(decision["allowed"])
        self.assertEqual(decision["execution_authority"], "none")
        self.assertEqual(decision["configured_execution_authority"], "full_auto")
        self.assertEqual(decision["reason"], "unsupported_fastapi_execution_authority")

    def test_public_status_exposes_fail_closed_state_without_secrets(self):
        status = execution_authority_status(
            environ={EXECUTION_AUTHORITY_ENV: "FULL_AUTO", "QC_API_TOKEN": "secret"}
        )

        self.assertEqual(status["execution_authority"], "none")
        self.assertEqual(status["configured_execution_authority"], "full_auto")
        self.assertTrue(status["fence_enforced"])
        self.assertFalse(status["executable_commands_enabled"])
        self.assertNotIn("QC_API_TOKEN", status)
        self.assertNotIn("secret", str(status))

    async def test_poisoned_config_and_valid_upstream_path_still_cannot_send(self):
        poison = {EXECUTION_AUTHORITY_ENV: "FULL_AUTO"}
        with (
            patch.dict(os.environ, poison, clear=False),
            patch("tools.qc_tools.is_active_newbase_observer", new=AsyncMock(return_value=False)) as observer,
            patch("tools.qc_tools.httpx.AsyncClient") as client,
        ):
            result = await send_setweights_command(
                weights={"SPY": 1.0},
                command_id="valid-confirmed-proposal",
                analysis_id=42,
                policy_version="poisoned-db-full-auto",
                target_fingerprint="valid-token-path",
            )

        self.assertFalse(result["success"])
        self.assertTrue(result["blocked"])
        self.assertEqual(result["error"], "unsupported_fastapi_execution_authority")
        observer.assert_not_awaited()
        client.assert_not_called()

    async def test_all_low_level_qc_commands_share_the_same_fence(self):
        with (
            patch.dict(os.environ, {EXECUTION_AUTHORITY_ENV: "none"}, clear=False),
            patch("tools.qc_tools.is_active_newbase_observer", new=AsyncMock(return_value=False)) as observer,
            patch("tools.qc_tools.httpx.AsyncClient") as client,
        ):
            results = [
                await tool_send_weight_command({"weights": {"SPY": 1.0}}),
                await tool_send_cancel_orders_command({"target_command_id": "cmd-1"}),
                await tool_send_policy_sync({"payload": {"version": "v1"}}),
            ]

        self.assertTrue(all(result.get("blocked") for result in results))
        self.assertTrue(all(result.get("execution_authority") == "none" for result in results))
        observer.assert_not_awaited()
        client.assert_not_called()

    def test_qc_live_command_http_endpoint_has_one_source_module(self):
        allowed = Path("tools/qc_tools.py")
        offenders = []
        for root in ("agents", "api", "cron", "services", "tools"):
            for path in Path(root).rglob("*.py"):
                if path == allowed:
                    continue
                if "/live/commands/create" in path.read_text():
                    offenders.append(str(path))

        self.assertEqual([], offenders)


if __name__ == "__main__":
    unittest.main()
