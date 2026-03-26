# tools/qc_tools.py
import hashlib
import hmac
import httpx
import logging
import time

from config import get_settings

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
        "Authorization": "Basic " + __import__("base64").b64encode(
            f"{settings.qc_user_id}:{hash_bytes}".encode()
        ).decode(),
    }


def tool_send_weight_command(inp: dict) -> dict:
    """
    向 QC 下发目标权重。
    inp: {"weights": {"SPY": 0.5, "QQQ": 0.3, "CASH": 0.2}}
    """
    weights = inp.get("weights", {})
    url = (
        f"{settings.qc_api_url}/projects/"
        f"{settings.qc_project_id}/live/commands"
    )
    body = {
        "target":  "SetWeights",
        "weights": {k: v for k, v in weights.items() if k != "CASH"},
    }

    for attempt in range(3):
        try:
            resp = httpx.post(
                url,
                json=body,
                headers=_qc_auth_headers(),
                timeout=15,
            )
            if resp.status_code == 200:
                logger.info(f"SetWeights sent: {weights}")
                return {"success": True, "response": resp.json()}
            logger.warning(f"QC API {resp.status_code}: {resp.text}")
        except Exception as e:
            logger.error(f"QC API attempt {attempt}: {e}")
            time.sleep(2 ** attempt)

    return {"success": False, "error": "QC API unreachable after 3 attempts"}


def tool_emergency_liquidate(_input: dict) -> dict:
    """QC 紧急清仓指令。"""
    url = (
        f"{settings.qc_api_url}/projects/"
        f"{settings.qc_project_id}/live/commands"
    )
    try:
        resp = httpx.post(
            url,
            json={"target": "EmergencyLiquidate"},
            headers=_qc_auth_headers(),
            timeout=10,
        )
        logger.critical(f"EmergencyLiquidate sent | status={resp.status_code}")
        return {"success": resp.status_code == 200}
    except Exception as e:
        logger.critical(f"EmergencyLiquidate FAILED: {e}")
        return {"success": False, "error": str(e)}
