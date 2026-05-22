import unittest
from unittest.mock import AsyncMock, patch

from services.execution_ack_tracker import wait_for_qc_ack


class ExecutionAckTrackerTests(unittest.IsolatedAsyncioTestCase):
    async def test_returns_accepted_when_ack_arrives(self):
        with (
            patch("services.execution_ack_tracker.asyncio.sleep", new=AsyncMock()),
            patch("services.execution_ack_tracker.get_qc_status", new=AsyncMock(side_effect=[None, "accepted"])),
            patch("services.execution_ack_tracker.mark_timeout", new=AsyncMock()) as mark_timeout,
        ):
            status = await wait_for_qc_ack("analysis_1", timeout_seconds=2)

        self.assertEqual(status, "accepted")
        mark_timeout.assert_not_awaited()

    async def test_returns_rejected_when_ack_rejects(self):
        with (
            patch("services.execution_ack_tracker.asyncio.sleep", new=AsyncMock()),
            patch("services.execution_ack_tracker.get_qc_status", new=AsyncMock(return_value="rejected")),
            patch("services.execution_ack_tracker.mark_timeout", new=AsyncMock()) as mark_timeout,
        ):
            status = await wait_for_qc_ack("analysis_1", timeout_seconds=2)

        self.assertEqual(status, "rejected")
        mark_timeout.assert_not_awaited()

    async def test_marks_timeout_when_no_ack_arrives(self):
        with (
            patch("services.execution_ack_tracker.asyncio.sleep", new=AsyncMock()),
            patch("services.execution_ack_tracker.get_qc_status", new=AsyncMock(return_value="submitted")),
            patch("services.execution_ack_tracker.mark_timeout", new=AsyncMock()) as mark_timeout,
        ):
            status = await wait_for_qc_ack("analysis_1", timeout_seconds=2)

        self.assertEqual(status, "timeout_no_ack")
        mark_timeout.assert_awaited_once_with("analysis_1")


if __name__ == "__main__":
    unittest.main()
