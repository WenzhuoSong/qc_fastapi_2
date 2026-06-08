"""Single low-level sender for QC market-impacting commands.

Higher layers must perform proposal relevance, active execution, preflight,
dedupe, lifecycle, and approval checks before calling this module. Keeping the
actual SetWeights send behind one service makes direct-send bypasses visible
and testable.
"""
from __future__ import annotations

from typing import Any

from tools.qc_tools import tool_send_weight_command


async def send_setweights_command(
    *,
    weights: dict[str, Any],
    command_id: str | None,
    analysis_id: int | str | None,
    policy_version: str | None,
    target_fingerprint: str | None = None,
) -> dict[str, Any]:
    """Send a SetWeights command to QC after upstream controls have passed."""
    return await tool_send_weight_command(
        {
            "weights": weights,
            "command_id": command_id,
            "analysis_id": analysis_id,
            "policy_version": policy_version,
            "target_fingerprint": target_fingerprint,
        }
    )
