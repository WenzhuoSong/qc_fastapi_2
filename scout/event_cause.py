"""Why did it move? Research one abnormal day, the way a desk analyst would.

1. Python gathers the window's evidence: company news dated around the day
   (not just the last 72 hours), SEC filings, open-market insider trades,
   earnings proximity, and the sector / SPY move that day.
2. A web search (OpenAI Responses `web_search_preview`) asks why the stock
   moved on that date. This is what finds read-through from a competitor,
   customer or supplier, which no per-ticker feed tags to this stock.
3. One structured LLM call names the cause from a fixed taxonomy, cites the
   evidence keys and says which part of the business is affected.
4. Deterministic checks: citations exist, numbers come from the evidence, a
   cause other than "no clear news" has a source, "market/sector" requires
   the sector or market to explain most of the move. One retry, then a rule
   fallback. Results are cached per (ticker, date) in scout.stock_events.

Web results are external and unverified; the page labels them with sources.
"""
from __future__ import annotations

import json
import logging
from datetime import date, datetime, time, timedelta, timezone
from typing import Literal
from zoneinfo import ZoneInfo

import httpx
from pydantic import BaseModel, Field

from scout import context, db, knowledge
from scout.config import get_scout_settings
from scout.data import finnhub
from scout.market_calendar import previous_us_equity_trading_day
from scout.thesis import allowed_numbers, unsupported_numbers

logger = logging.getLogger("scout.event_cause")
ET = ZoneInfo("America/New_York")
SEARCH_MODEL = "gpt-4o-mini"
MAX_NEWS = 10
MATERIAL_FORMS = {"8-K", "8-K/A", "S-1", "S-3", "424B5", "424B3", "SC 13D", "SC 13G", "DEF 14A", "10-Q", "10-K", "4"}

CauseType = Literal["财报/指引", "监管/临床", "竞争对手/同业传导", "分析师评级", "并购/交易", "融资/增发",
                    "法律/调查", "管理层变动", "大盘/板块", "无明确消息"]


class EventCause(BaseModel):
    cause_type: CauseType
    summary: str = Field(description="2–3 句中文：发生了什么、市场为什么这样反应")
    related_company: str | None = Field(description="引发异动的其他公司（竞争对手/合作方等），没有则为 null")
    business_impact: str = Field(description="1–2 句：影响的是公司哪部分业务或预期，是核心业务还是局部")
    confidence: Literal["low", "medium", "high"]
    sources: list[str] = Field(description="引用的证据 key（news_N、filing_N、web、insiders、earnings、market）")


SYSTEM_PROMPT = """你是卖方研究部负责"个股异动归因"的分析师。输入是 Python 整理的证据包：异动当天的量价数据、
当天前后的公司新闻、SEC 文件、内部人交易、财报日期、板块和大盘表现，以及一段网络检索结果（外部来源，未经核实）。

判断顺序：
1. 公司自身消息：财报/指引、监管或临床结果、并购、融资增发、诉讼调查、管理层变动（8-K 文件是重要线索）。
2. 传导：竞争对手、客户或供应商的消息（例如竞品临床数据、大客户砍单）。这类消息通常挂在别的公司名下，网络检索最有用。
3. 分析师评级变化。
4. 大盘/板块：只有当板块或大盘当天的涨跌能解释大部分异动时才能选这一类。
5. 以上都找不到可靠依据时，选"无明确消息"，不要猜。

硬性规则：
- 数字只能原样引用证据包中出现的数字。
- sources 只能填证据包里出现的 key；除"无明确消息"外至少引用一个。
- 网络检索是外部来源：可以采信，但要与日期、方向、幅度对得上；对不上就不要用。
- business_impact 要说清楚受影响的是哪项业务或哪部分预期；证据包里没有营收构成时，不要断言它是不是"核心业务"，
  只写受影响的产品/预期，并注明"占公司收入比重证据不足"。
- 中文、简洁、具体；不给买卖建议。"""


# ── evidence ──

def window(ev_date: date) -> tuple[int, int]:
    """Epoch range: previous session's close (16:00 ET) to the event day's 20:00 ET."""
    prev = previous_us_equity_trading_day(ev_date)
    start = datetime.combine(prev, time(16, 0), ET)
    end = datetime.combine(ev_date, time(20, 0), ET)
    return int(start.timestamp()), int(end.timestamp())


async def web_search(name: str, ticker: str, ev_date: date, ret: float) -> dict:
    """One web search for the cause; {"text", "sources": [{"title","url"}]} or {}."""
    key = get_scout_settings().openai_api_key
    if not key:
        return {}
    move = f"{'fell' if ret < 0 else 'rose'} about {abs(ret) * 100:.0f}%"
    prompt = (f"Why did {name} ({ticker}) stock {move} on {ev_date.isoformat()}? Identify the specific catalyst: company news, "
              "a competitor/customer/supplier event (read-through), an analyst action, or a sector/macro move. "
              "Answer in 2-4 sentences with dates, and say if no specific reason was reported.")
    try:
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post("https://api.openai.com/v1/responses", headers={"Authorization": f"Bearer {key}"},
                                     json={"model": SEARCH_MODEL, "tools": [{"type": "web_search_preview"}], "input": prompt})
        if resp.status_code != 200:
            logger.warning("web search HTTP %s for %s", resp.status_code, ticker)
            return {}
        data = resp.json()
    except Exception as exc:
        logger.warning("web search failed for %s: %s", ticker, exc)
        return {}
    text, sources = "", []
    for item in data.get("output") or []:
        if item.get("type") != "message":
            continue
        for part in item.get("content") or []:
            text += part.get("text", "")
            for ann in part.get("annotations") or []:
                if ann.get("type") == "url_citation" and ann.get("url"):
                    sources.append({"title": ann.get("title") or ann["url"], "url": ann["url"].split("?utm_source")[0]})
    seen, unique = set(), []
    for s in sources:
        if s["url"] not in seen:
            seen.add(s["url"])
            unique.append(s)
    return {"text": text.strip(), "sources": unique[:6]} if text.strip() else {}


async def gather(ticker: str, name: str, ev: dict) -> dict:
    d = date.fromisoformat(ev["date"])
    start, end = window(d)
    news = [n for n in await finnhub.company_news(ticker, d - timedelta(days=2), d + timedelta(days=1))
            if start - 12 * 3600 <= (n.get("datetime") or 0) <= end]
    news.sort(key=lambda n: n.get("datetime") or 0)
    filings = [f for f in await finnhub.filings(ticker, d - timedelta(days=3), d + timedelta(days=2))
               if f.get("form") in MATERIAL_FORMS]
    insiders = await finnhub.insider_transactions(ticker, d - timedelta(days=30))
    from scout.fundamentals import parse_insiders

    ins = parse_insiders([i for i in insiders if (i.get("transactionDate") or "") <= d.isoformat()], d - timedelta(days=30))
    earn = await db.fetch_all(
        "SELECT event_date, payload FROM scout.market_events WHERE ticker = :t AND event_type = 'earnings' "
        "AND event_date BETWEEN :a AND :b", {"t": ticker, "a": d - timedelta(days=3), "b": d + timedelta(days=1)})
    web = await web_search(name, ticker, d, ev["ret"])
    return {"news": news[:MAX_NEWS], "filings": filings[:8], "insiders": ins, "earnings": earn, "web": web}


def build_pack(ticker: str, name: str, ev: dict, facts: dict) -> dict:
    fmt = knowledge.fmt
    pack = {
        "ticker": ticker, "name": name, "date": ev["date"],
        "move": {"key": "market", "当日涨跌": fmt("event_ret", ev["ret"]), "个股自身": fmt("event_idio", ev["idio"]),
                 "标准差倍数": fmt("event_sigma", ev.get("sigma_mult")), "开盘跳空": fmt("event_gap", ev["gap"]),
                 "盘中涨跌": fmt("event_intraday", ev["intraday"]), "发生时段": ev["timing"],
                 "量比": fmt("event_vol_ratio", ev.get("vol_ratio")),
                 "所属板块当日": fmt("event_sector_ret", ev.get("sector_ret")), "SPY 当日": fmt("ret_1d", ev.get("spy_ret"))},
        "news": [{"key": f"news_{i + 1}",
                  "时间（美东）": datetime.fromtimestamp(n["datetime"], timezone.utc).astimezone(ET).strftime("%m-%d %H:%M"),
                  "来源": n.get("source"), "标题": (n.get("headline") or "")[:200], "摘要": (n.get("summary") or "")[:300]}
                 for i, n in enumerate(facts["news"])],
        "filings": [{"key": f"filing_{i + 1}", "类型": f.get("form"), "提交日": (f.get("filedDate") or "")[:10]}
                    for i, f in enumerate(facts["filings"])],
        "insiders": {"key": "insiders", "近 30 日公开市场净买卖额": fmt("insider_net_value_90d", facts["insiders"].get("insider_net_value_90d")),
                     "卖出人数": facts["insiders"].get("insider_sellers_90d"), "买入人数": facts["insiders"].get("insider_buyers_90d")},
        "earnings": [{"key": "earnings", "财报日": str(e["event_date"]), "时段": (e["payload"] or {}).get("hour")}
                     for e in facts["earnings"]],
    }
    if facts["web"]:
        pack["web"] = {"key": "web", "说明": "网络检索结果（外部来源，未经核实）", "内容": facts["web"]["text"],
                       "来源": [s["title"] for s in facts["web"]["sources"]]}
    return pack


def evidence_keys(pack: dict) -> set[str]:
    keys = {"market", "insiders"} | {n["key"] for n in pack["news"]} | {f["key"] for f in pack["filings"]}
    if pack["earnings"]:
        keys.add("earnings")
    if pack.get("web"):
        keys.add("web")
    return keys


def validate(c: EventCause, pack: dict, ev: dict) -> list[str]:
    issues = []
    bad = unsupported_numbers("\n".join([c.summary, c.business_impact, c.related_company or ""]), allowed_numbers(pack))
    if bad:
        issues.append(f"以下数字不在证据包中：{', '.join(sorted(set(bad)))}")
    unknown = sorted(set(c.sources) - evidence_keys(pack))
    if unknown:
        issues.append(f"引用了不存在的证据 key：{', '.join(unknown)}")
    if c.cause_type != "无明确消息" and not [s for s in c.sources if s != "market"]:
        issues.append("除「无明确消息」外，必须引用至少一条新闻、文件、财报或网络检索")
    if c.cause_type == "大盘/板块":
        sector = ev.get("sector_ret") or 0.0
        market = ev.get("spy_ret") or 0.0
        if max(abs(sector), abs(market)) < 0.5 * abs(ev["ret"]):
            issues.append("板块和大盘当天的涨跌不足以解释这次异动，不能归为「大盘/板块」")
    return issues


def fallback(pack: dict, ev: dict) -> EventCause:
    """Rule reading when the LLM is unavailable or fails its checks."""
    if pack["earnings"]:
        return EventCause(cause_type="财报/指引", summary="异动发生在财报日前后。", related_company=None,
                          business_impact="证据不足以判断", confidence="low", sources=["earnings"])
    flags = context.event_flags([{"headline": n["标题"], "summary": n["摘要"]} for n in pack["news"]])
    mapping = {"并购/出售": "并购/交易", "药监/临床": "监管/临床", "增发/融资": "融资/增发", "监管/诉讼": "法律/调查"}
    if flags:
        key = next(n["key"] for n in pack["news"] if flags[0]["headline"][:60] in n["标题"])
        return EventCause(cause_type=mapping[flags[0]["type"]], summary=f"当天新闻：{flags[0]['headline']}",
                          related_company=None, business_impact="证据不足以判断", confidence="low", sources=[key])
    sector = ev.get("sector_ret") or 0.0
    if abs(sector) >= 0.5 * abs(ev["ret"]):
        return EventCause(cause_type="大盘/板块", summary="所属板块当天同向大幅波动。", related_company=None,
                          business_impact="非公司特有事件", confidence="low", sources=["market"])
    return EventCause(cause_type="无明确消息", summary="没有找到能解释这次异动的公司新闻或文件。", related_company=None,
                      business_impact="证据不足以判断", confidence="low", sources=[])


async def classify(pack: dict, ev: dict) -> tuple[EventCause, str, list]:
    settings = get_scout_settings()
    history: list = []
    if settings.openai_api_key:
        from openai import AsyncOpenAI

        client = AsyncOpenAI(api_key=settings.openai_api_key, timeout=90)
        messages = [{"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": json.dumps(pack, ensure_ascii=False, default=str)}]
        for attempt in (1, 2):
            try:
                resp = await client.beta.chat.completions.parse(
                    model=settings.scout_insight_model, messages=messages, response_format=EventCause, temperature=0.1)
                parsed = resp.choices[0].message.parsed
            except Exception as exc:
                logger.warning("event cause LLM failed: %s", exc)
                history.append({"attempt": attempt, "issues": [f"LLM 调用失败：{type(exc).__name__}"]})
                break
            issues = validate(parsed, pack, ev)
            history.append({"attempt": attempt, "issues": issues})
            if not issues:
                return EventCause(**knowledge.humanize_obj(parsed.model_dump())), "llm", history
            messages += [{"role": "assistant", "content": parsed.model_dump_json()},
                         {"role": "user", "content": "校验未通过，请修正后完整重写：\n" + "\n".join(issues)}]
    return fallback(pack, ev), "fallback", history


async def explain(ticker: str, name: str, ev: dict, refresh: bool = False) -> dict:
    """Cause of one abnormal day, cached in scout.stock_events."""
    d = date.fromisoformat(ev["date"])
    if not refresh:
        row = await db.fetch_one("SELECT cause FROM scout.stock_events WHERE ticker = :t AND event_date = :d AND cause IS NOT NULL",
                                 {"t": ticker, "d": d})
        if row:
            return row["cause"]
    facts = await gather(ticker, name, ev)
    pack = build_pack(ticker, name, ev, facts)
    cause, source, history = await classify(pack, ev)
    links = {n["key"]: {"title": n["标题"], "url": f.get("url")} for n, f in zip(pack["news"], facts["news"])}
    links.update({f["key"]: {"title": f"SEC {f['类型']}（{f['提交日']}）", "url": raw.get("reportUrl") or raw.get("filingUrl")}
                  for f, raw in zip(pack["filings"], facts["filings"])})
    record = {**cause.model_dump(), "source": source, "model": get_scout_settings().scout_insight_model if source == "llm" else None,
              "web_sources": (facts["web"] or {}).get("sources", []), "links": links,
              "validation": history, "pack": pack}
    await db.execute(
        "INSERT INTO scout.stock_events (ticker, event_date, metrics, cause) VALUES (:t, :d, CAST(:m AS JSONB), CAST(:c AS JSONB)) "
        "ON CONFLICT (ticker, event_date) DO UPDATE SET metrics = EXCLUDED.metrics, cause = EXCLUDED.cause, created_at = now()",
        {"t": ticker, "d": d, "m": db.jsonb(ev), "c": db.jsonb(record)})
    return record


async def cached(ticker: str, dates: list[str]) -> dict[str, dict]:
    if not dates:
        return {}
    rows = await db.fetch_all(
        "SELECT event_date, cause FROM scout.stock_events WHERE ticker = :t AND event_date = ANY(:d) AND cause IS NOT NULL",
        {"t": ticker, "d": [date.fromisoformat(x) for x in dates]})
    return {str(r["event_date"]): r["cause"] for r in rows}
