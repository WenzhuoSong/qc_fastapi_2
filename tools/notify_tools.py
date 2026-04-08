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
    parse_mode 传空字符串表示纯文本。
    """
    text       = inp.get("text", "")
    parse_mode = inp.get("parse_mode", "HTML")
    url = f"https://api.telegram.org/bot{settings.tg_bot_token}/sendMessage"

    def _post(pm: str) -> httpx.Response:
        payload: dict = {"chat_id": settings.tg_chat_id, "text": text}
        if pm:
            payload["parse_mode"] = pm
        return httpx.post(url, json=payload, timeout=10)

    try:
        resp = _post(parse_mode)
        if resp.status_code == 200:
            return {"sent": True}
        # 降级：若因 parse 错误 400，改纯文本重试
        if resp.status_code == 400 and parse_mode:
            logger.warning(f"Telegram parse error, retrying as plain text: {resp.text}")
            resp = _post("")
            if resp.status_code == 200:
                return {"sent": True}
        logger.warning(f"Telegram {resp.status_code}: {resp.text}")
        return {"sent": False, "error": resp.text}
    except Exception as e:
        logger.error(f"Telegram send failed: {e}")
        return {"sent": False, "error": str(e)}
