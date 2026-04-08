# tools/notify_tools.py
import logging
import httpx
from config import get_settings

logger   = logging.getLogger("qc_fastapi_2.telegram")
settings = get_settings()


async def tool_send_telegram(inp: dict) -> dict:
    """
    发送 Telegram 消息。
    inp: {"text": "...", "parse_mode": "HTML"}
    parse_mode 传空字符串表示纯文本。
    遇到 parse 错误（400）会自动降级为纯文本重试。
    """
    text       = inp.get("text", "")
    parse_mode = inp.get("parse_mode", "HTML")
    url = f"https://api.telegram.org/bot{settings.tg_bot_token}/sendMessage"

    async def _post(pm: str) -> httpx.Response:
        payload: dict = {"chat_id": settings.tg_chat_id, "text": text}
        if pm:
            payload["parse_mode"] = pm
        async with httpx.AsyncClient(timeout=10) as client:
            return await client.post(url, json=payload)

    try:
        resp = await _post(parse_mode)
        if resp.status_code == 200:
            return {"sent": True}
        # 降级：因 parse 错误 400，改纯文本重试
        if resp.status_code == 400 and parse_mode:
            logger.warning(f"Telegram parse error, retrying as plain text: {resp.text}")
            resp = await _post("")
            if resp.status_code == 200:
                return {"sent": True}
        logger.warning(f"Telegram {resp.status_code}: {resp.text}")
        return {"sent": False, "error": resp.text}
    except Exception as e:
        logger.error(f"Telegram send failed: {e}")
        return {"sent": False, "error": str(e)}
