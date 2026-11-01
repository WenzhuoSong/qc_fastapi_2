"""LLM usage: one row per call in `scout.llm_usage` (job, purpose, model, tokens, estimated cost).

Every call site records after a successful call; recording never fails the call. The job comes from a
context variable set by `jobs._run.run` (the cron or local job's name), the web app ("web") and the Telegram
handler ("telegram"). Costs use list prices per million tokens; the cache rates of the gpt-6 models are
assumptions, so the page labels the totals as estimates. The news pipeline writes the same table itself.
"""
from __future__ import annotations

import contextvars
import logging

from scout import db

logger = logging.getLogger("scout.llm_usage")

JOB: contextvars.ContextVar[str] = contextvars.ContextVar("llm_job", default="web")

# model prefix → (input, cached input, output) USD per million tokens; longest prefix wins
PRICES = {
    "gpt-4o-mini": (0.15, 0.075, 0.60),
    "gpt-4o": (2.50, 1.25, 10.0),
    "gpt-6-astra": (10.0, 1.0, 50.0),
    "gpt-6.1-sol": (2.0, 0.2, 10.0),
    "gpt-6.1-terra": (1.0, 0.1, 5.0),
    "gpt-6-luna": (0.10, 0.01, 0.50),
}
ALERT_USD = 3.0                 # a day above this is pushed once by the after-close job
PURPOSE_ZH = {"insight": "个股 AI 解读", "thesis": "机会卡论点", "agent": "问答助手", "trade_parse": "交易解析",
              "company_profile": "公司画像", "company_research": "联网检索公司", "event_cause": "异动归因",
              "event_search": "异动联网检索", "market_focus": "市场关注点", "premarket": "盘前简报", "outlook": "行业展望",
              "gate_critique": "检验关批判", "news_summary": "新闻摘要"}


def price(model: str) -> tuple[float, float, float] | None:
    key = max((k for k in PRICES if model.startswith(k)), key=len, default=None)
    return PRICES.get(key) if key else None


def cost(model: str, inp: int, cached: int, out: int) -> float | None:
    p = price(model)
    if not p:
        return None
    return ((inp - cached) * p[0] + cached * p[1] + out * p[2]) / 1e6


async def record(purpose: str, model: str, inp: int, cached: int, out: int) -> None:
    try:
        await db.execute(
            "INSERT INTO scout.llm_usage (job, purpose, model, input_tokens, cached_tokens, output_tokens, est_usd) "
            "VALUES (:j, :p, :m, :i, :c, :o, :u)",
            {"j": JOB.get()[:48], "p": purpose[:48], "m": (model or "?")[:64], "i": int(inp or 0), "c": int(cached or 0),
             "o": int(out or 0), "u": cost(model or "", int(inp or 0), int(cached or 0), int(out or 0))})
    except Exception as exc:                 # usage logging must never fail the call it measures
        logger.warning("llm usage not recorded: %s", exc)


async def record_sdk(resp, purpose: str, model: str) -> None:
    """From an OpenAI SDK chat completion (or parse) result."""
    u = getattr(resp, "usage", None)
    if u is None:
        return
    details = getattr(u, "prompt_tokens_details", None)
    await record(purpose, getattr(resp, "model", None) or model, getattr(u, "prompt_tokens", 0),
                 getattr(details, "cached_tokens", 0) or 0, getattr(u, "completion_tokens", 0))


async def record_responses(data: dict, purpose: str, model: str) -> None:
    """From a /v1/responses JSON body."""
    u = data.get("usage") or {}
    await record(purpose, data.get("model") or model, u.get("input_tokens", 0),
                 (u.get("input_tokens_details") or {}).get("cached_tokens", 0) or 0, u.get("output_tokens", 0))


async def by_day(days: int = 14) -> list[dict]:
    return await db.fetch_all(
        "SELECT at::date AS day, purpose, model, count(*) AS calls, sum(input_tokens) AS input_tokens, "
        "sum(output_tokens) AS output_tokens, sum(est_usd) AS usd FROM scout.llm_usage "
        "WHERE at > now() - make_interval(days => :d) GROUP BY 1, 2, 3 ORDER BY 1 DESC, usd DESC NULLS LAST", {"d": days})


async def totals(days: int = 30) -> dict:
    rows = await db.fetch_all("SELECT at::date AS day, sum(est_usd) AS usd, count(*) AS calls FROM scout.llm_usage "
                              "WHERE at > now() - make_interval(days => :d) GROUP BY 1 ORDER BY 1", {"d": days})
    return {"days": [{"day": str(r["day"]), "usd": round(r["usd"] or 0, 2), "calls": r["calls"]} for r in rows],
            "total_usd": round(sum(r["usd"] or 0 for r in rows), 2)}


async def check_alert(push: bool) -> dict | None:
    """The last 24 hours' estimated spend; pushed once a day when above ALERT_USD."""
    row = await db.fetch_one("SELECT sum(est_usd) AS usd, count(*) AS calls FROM scout.llm_usage WHERE at > now() - interval '24 hours'")
    usd = float((row or {}).get("usd") or 0)
    if usd <= ALERT_USD or not push:
        return {"usd": round(usd, 2), "alert": usd > ALERT_USD}
    top = await db.fetch_all("SELECT purpose, sum(est_usd) AS usd FROM scout.llm_usage WHERE at > now() - interval '24 hours' "
                             "GROUP BY 1 ORDER BY 2 DESC NULLS LAST LIMIT 3")
    from datetime import date

    from scout import intraday, telegram

    if await intraday._claim([{"key": f"llm_usage:{date.today()}", "kind": "llm_usage", "ticker": None, "payload": None}]):
        await telegram.send(f"💸 <b>AI 用量提醒</b>：过去 24 小时估计 ${usd:.2f}（{row['calls']} 次调用），超过 ${ALERT_USD:.0f}。"
                            "主要是：" + "、".join(f"{PURPOSE_ZH.get(t['purpose'], t['purpose'])} ${(t['usd'] or 0):.2f}" for t in top)
                            + "。详情见 Dashboard「数据」页。")
    return {"usd": round(usd, 2), "alert": True}
