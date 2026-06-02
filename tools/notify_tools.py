# tools/notify_tools.py
import logging
import httpx
from config import get_settings

logger   = logging.getLogger("qc_fastapi_2.telegram")
settings = get_settings()

TELEGRAM_MAX_MESSAGE_CHARS = 4096
TELEGRAM_SAFE_MESSAGE_CHARS = 3900


async def tool_send_telegram(inp: dict) -> dict:
    """
    发送 Telegram 消息。
    inp: {"text": "...", "parse_mode": "HTML"}
    parse_mode 传空字符串表示纯文本。
    遇到 parse 错误（400）会自动降级为纯文本重试。
    """
    text       = str(inp.get("text", "") or "")
    parse_mode = inp.get("parse_mode", "HTML")
    url = f"https://api.telegram.org/bot{settings.tg_bot_token}/sendMessage"

    async def _post(chunk: str, pm: str) -> httpx.Response:
        payload: dict = {"chat_id": settings.tg_chat_id, "text": chunk}
        if pm:
            payload["parse_mode"] = pm
        async with httpx.AsyncClient(timeout=10) as client:
            return await client.post(url, json=payload)

    chunks = _split_telegram_text(text)
    errors: list[str] = []
    try:
        for idx, chunk in enumerate(chunks, start=1):
            part = chunk
            if len(chunks) > 1:
                part = f"[{idx}/{len(chunks)}]\n{chunk}"
            resp = await _post(part, parse_mode)
            if resp.status_code == 200:
                continue
            # 降级：因 parse 错误 400，改纯文本重试
            if resp.status_code == 400 and parse_mode:
                logger.warning(f"Telegram parse/size error, retrying as plain text: {resp.text}")
                resp = await _post(part, "")
                if resp.status_code == 200:
                    continue
            logger.warning(f"Telegram {resp.status_code}: {resp.text}")
            errors.append(resp.text)
        if errors:
            return {"sent": False, "error": "; ".join(errors), "parts": len(chunks)}
        return {"sent": True, "parts": len(chunks)}
    except Exception as e:
        logger.error(f"Telegram send failed: {e}")
        return {"sent": False, "error": str(e), "parts": len(chunks)}


def _split_telegram_text(text: str, limit: int = TELEGRAM_SAFE_MESSAGE_CHARS) -> list[str]:
    """Split Telegram text under the API limit while preserving line boundaries."""
    if len(text) <= limit:
        return [text]

    chunks: list[str] = []
    current = ""
    for line in text.splitlines(keepends=True):
        if len(line) > limit:
            if current:
                chunks.append(current.rstrip())
                current = ""
            for start in range(0, len(line), limit):
                chunks.append(line[start:start + limit].rstrip())
            continue
        if len(current) + len(line) > limit:
            chunks.append(current.rstrip())
            current = line
        else:
            current += line
    if current:
        chunks.append(current.rstrip())
    return chunks or [""]
