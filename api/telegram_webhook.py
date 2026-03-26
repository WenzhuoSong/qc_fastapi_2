# api/telegram_webhook.py
from fastapi import APIRouter, Request
from config import get_settings
from scheduler.jobs import handle_telegram_command
from tools.notify_tools import tool_send_telegram

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

    reply = handle_telegram_command(text, chat)
    if reply:
        tool_send_telegram({"text": reply})

    return {"ok": True}
