# api/telegram_webhook.py
import logging
from fastapi import APIRouter, Request
from config import get_settings
from scheduler.jobs import handle_telegram_command
from tools.notify_tools import tool_send_telegram

logger = logging.getLogger("qc_fastapi_2.telegram_webhook")

router   = APIRouter()
settings = get_settings()


@router.post("/telegram")
async def telegram_webhook(request: Request):
    """
    Telegram 接收 webhook。
    在 BotFather 设置：
      https://api.telegram.org/bot{TOKEN}/setWebhook?url=https://your-server/api/telegram
    """
    body = await request.json()
    msg  = body.get("message", {})
    text = msg.get("text", "")
    chat = str(msg.get("chat", {}).get("id", ""))

    if not text or not chat:
        return {"ok": True}

    try:
        reply = handle_telegram_command(text, chat)
    except Exception as e:
        logger.error(f"Telegram command handler error: {e}", exc_info=True)
        reply = "⚠️ 指令处理异常，请重试"

    if reply:
        tool_send_telegram({"text": reply})

    return {"ok": True}
