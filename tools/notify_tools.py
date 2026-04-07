# tools/notify_tools.py
import logging
import httpx
from config import get_settings

logger   = logging.getLogger("qc_fastapi_2.telegram")
settings = get_settings()


def tool_send_telegram(inp: dict) -> dict:
    """
    发送 Telegram 消息。
    inp: {"text": "...", "parse_mode": "HTML"}
    """
    text       = inp.get("text", "")
    parse_mode = inp.get("parse_mode", "HTML")
    url = f"https://api.telegram.org/bot{settings.tg_bot_token}/sendMessage"
    try:
        resp = httpx.post(
            url,
            json={
                "chat_id":    settings.tg_chat_id,
                "text":       text,
                "parse_mode": parse_mode,
            },
            timeout=10,
        )
        if resp.status_code == 200:
            return {"sent": True}
        logger.warning(f"Telegram {resp.status_code}: {resp.text}")
        return {"sent": False, "error": resp.text}
    except Exception as e:
        logger.error(f"Telegram send failed: {e}")
        return {"sent": False, "error": str(e)}
