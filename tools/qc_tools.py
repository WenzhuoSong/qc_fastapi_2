# tools/qc_tools.py
import asyncio
import base64
import hashlib
import json
import logging
import time

import httpx

from config import get_settings
from services.execution_policy import policy_snapshot

logger   = logging.getLogger("qc_fastapi_2.qc")
settings = get_settings()


def _qc_auth_headers() -> dict:
    """QC API 使用 HMAC-SHA256 签名。"""
    timestamp = str(int(time.time()))
    hash_bytes = hashlib.sha256(
        (settings.qc_api_token + ":" + timestamp).encode()
    ).hexdigest()
    return {
        "Timestamp": timestamp,
        "Authorization": "Basic " + base64.b64encode(
            f"{settings.qc_user_id}:{hash_bytes}".encode()
        ).decode(),
    }


async def tool_send_weight_command(inp: dict) -> dict:
    """
    向 QC 下发目标权重。
    inp: {"weights": {"SPY": 0.5, "QQQ": 0.3, "CASH": 0.2}}

    QC Live Command API:
      POST {qc_api_url}/live/commands/create
      Body: {"projectId": <int>, "command": <json-string>}
    QC 算法 on_command(data) 会收到 command 字符串解析后的对象。
    """
    weights = inp.get("weights", {})
    command_id = inp.get("command_id") or inp.get("analysis_id") or f"weights_{int(time.time())}"
    policy = inp.get("policy") or policy_snapshot()
    policy_version = policy.get("version")
    if not policy_version:
        return {"success": False, "error": "policy_version missing from SetWeights command"}
    url = f"{settings.qc_api_url}/live/commands/create"

    command_payload = {
        "target":     "SetWeights",
        "command_id": command_id,
        "analysis_id": inp.get("analysis_id"),
        "weights":    {k: v for k, v in weights.items() if k != "CASH"},
        "policy_version": policy_version,
        "policy": policy,
    }
    body = {
        "projectId": int(settings.qc_project_id),
        "command":   command_payload,
    }

    async with httpx.AsyncClient(timeout=15) as client:
        for attempt in range(3):
            try:
                resp = await client.post(url, json=body, headers=_qc_auth_headers())
                resp_json = resp.json() if resp.status_code == 200 else {}
                if resp.status_code == 200 and resp_json.get("success", False):
                    logger.info(
                        f"SetWeights sent: command_id={command_id} "
                        f"weights={weights} | qc_response={resp_json}"
                    )
                    return {
                        "success": True,
                        "response": resp_json,
                        "command_id": command_id,
                        "policy_version": policy_version,
                        "command_payload": command_payload,
                    }
                logger.warning(
                    f"QC API {resp.status_code}: {resp.text} "
                    f"(attempt {attempt})"
                )
            except Exception as e:
                logger.error(f"QC API attempt {attempt}: {e}")
                await asyncio.sleep(2 ** attempt)

    return {"success": False, "error": "QC API unreachable after 3 attempts"}


async def tool_send_policy_sync(inp: dict | None = None) -> dict:
    """Sync FastAPI execution policy to QC while QC keeps its fallback policy."""
    inp = inp or {}
    command_id = inp.get("command_id") or f"policy_sync_{int(time.time())}"
    payload = inp.get("payload") or policy_snapshot()
    url = f"{settings.qc_api_url}/live/commands/create"

    command_payload = {
        "target": "PolicySync",
        "command_id": command_id,
        "payload": payload,
    }
    body = {
        "projectId": int(settings.qc_project_id),
        "command": command_payload,
    }

    async with httpx.AsyncClient(timeout=15) as client:
        for attempt in range(3):
            try:
                resp = await client.post(url, json=body, headers=_qc_auth_headers())
                resp_json = resp.json() if resp.status_code == 200 else {}
                if resp.status_code == 200 and resp_json.get("success", False):
                    logger.info(
                        "PolicySync sent: command_id=%s version=%s | qc_response=%s",
                        command_id,
                        payload.get("version"),
                        resp_json,
                    )
                    return {
                        "success": True,
                        "response": resp_json,
                        "command_id": command_id,
                        "policy_version": payload.get("version"),
                    }
                logger.warning("QC API %s: %s (attempt %s)", resp.status_code, resp.text, attempt)
            except Exception as e:
                logger.error("QC policy sync attempt %s: %s", attempt, e)
                await asyncio.sleep(2 ** attempt)

    return {"success": False, "error": "QC API unreachable after 3 attempts"}


async def tool_emergency_liquidate(_input: dict) -> dict:
    """QC 紧急清仓指令。"""
    url = f"{settings.qc_api_url}/live/commands/create"
    command_payload = {"target": "EmergencyLiquidate"}
    body = {
        "projectId": int(settings.qc_project_id),
        "command":   command_payload,
    }
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(url, json=body, headers=_qc_auth_headers())
        resp_json = resp.json() if resp.status_code == 200 else {}
        success = resp.status_code == 200 and resp_json.get("success", False)
        logger.critical(f"EmergencyLiquidate sent | status={resp.status_code} | success={success}")
        return {"success": success}
    except Exception as e:
        logger.critical(f"EmergencyLiquidate FAILED: {e}")
        return {"success": False, "error": str(e)}
