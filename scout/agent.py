"""Q&A agent: read-only tool calling over Scout data (Telegram + dashboard)."""
from __future__ import annotations

import json
import logging
import re
from datetime import date
from typing import Any, Awaitable, Callable

from scout import context, db, universe
from scout.config import get_scout_settings
from scout.features import SNAPSHOT_FEATURES

logger = logging.getLogger("scout.agent")

MAX_ROUNDS = 6

SYSTEM_PROMPT = f"""你是 Scout，一个美股机会观察助手，服务于一位有经验的个人投资者。今天是 {{today}}。
你只能通过工具读取 Scout 数据库中的数据（日线行情、特征、新闻、财报日历、规则触发的机会卡、记分板）。

规则：
- 回答中的数字必须来自工具结果；不知道就说不知道，不要编造价格、新闻或财报数据。
- 特征字段中的收益率和比例是小数（0.05 = 5%）。rs_pct 是全市场相对强度分位（0–1）。
- 解释涨跌时先回答"是个股自身还是大盘"：market_ret_1d = 前一日 60 日 beta × SPY 当日涨跌，
  idio_ret_1d / idio_ret_5d / idio_ret_20d 是扣掉大盘贡献后的个股自身部分；beta_60、corr_60 衡量它跟大盘绑得多紧。
- 行情是日线收盘数据（免费数据源），不是实时报价，回答时注明数据日期。
- 用中文，结构清晰，简洁具体；可以给出"机会条件 / 失效条件"，不要下达买卖指令。
- 纯文本输出（不要 Markdown 表格），适合 Telegram 阅读。
可用特征：{", ".join(SNAPSHOT_FEATURES)}。"""

TOOLS = [
    {"name": "get_snapshot", "description": "某只股票/ETF 最新一日的特征快照、所属行业、是否在 watchlist、最近的机会卡",
     "parameters": {"type": "object", "properties": {"ticker": {"type": "string"}}, "required": ["ticker"]}},
    {"name": "get_price_history", "description": "最近 N 个交易日的日线（日期、开高低收、成交量）",
     "parameters": {"type": "object", "properties": {"ticker": {"type": "string"},
                    "days": {"type": "integer", "minimum": 5, "maximum": 120}}, "required": ["ticker"]}},
    {"name": "get_news", "description": "与该股票相关的新闻（标题、来源、摘要、时间），默认最近 3 天，最长可查 90 天历史",
     "parameters": {"type": "object", "properties": {"ticker": {"type": "string"},
                    "days": {"type": "integer", "minimum": 1, "maximum": 90}}, "required": ["ticker"]}},
    {"name": "get_earnings", "description": "该股票最近一次和下一次财报（日期、EPS 预期/实际）",
     "parameters": {"type": "object", "properties": {"ticker": {"type": "string"}}, "required": ["ticker"]}},
    {"name": "list_opportunities", "description": "规则触发的机会卡列表",
     "parameters": {"type": "object", "properties": {
         "status": {"type": "string", "enum": ["active", "closed", "all"]},
         "ticker": {"type": "string"}, "limit": {"type": "integer", "minimum": 1, "maximum": 30}}}},
    {"name": "get_scoreboard", "description": "各规则的实盘表现与历史基准对比", "parameters": {"type": "object", "properties": {}}},
    {"name": "get_market_overview", "description": "最新收盘的大盘状态、主要指数与行业/主题 ETF 表现",
     "parameters": {"type": "object", "properties": {}}},
    {"name": "screen", "description": "按某个特征对最新一日的股票排序筛选（例如 rs_pct 最高、ret_5d 最低）",
     "parameters": {"type": "object", "properties": {
         "sort_by": {"type": "string", "enum": list(SNAPSHOT_FEATURES)},
         "descending": {"type": "boolean"},
         "sector": {"type": "string", "description": "GICS 行业英文名，可选"},
         "watchlist_only": {"type": "boolean"},
         "limit": {"type": "integer", "minimum": 1, "maximum": 30}}, "required": ["sort_by"]}},
]


async def _latest_feature_date() -> date | None:
    row = await db.fetch_one("SELECT max(date) AS d FROM scout.features_daily")
    return row["d"] if row else None


async def get_snapshot(ticker: str) -> dict:
    t = universe.yahoo_symbol(ticker)
    feat = await db.fetch_one(
        "SELECT date, payload FROM scout.features_daily WHERE ticker = :t ORDER BY date DESC LIMIT 1", {"t": t}
    )
    inst = await db.fetch_one("SELECT name, sector, industry, kind FROM scout.instruments WHERE ticker = :t", {"t": t})
    watch = await db.fetch_one("SELECT theme FROM scout.watchlist WHERE ticker = :t AND active", {"t": t})
    opp = await db.fetch_one(
        "SELECT id, asof, primary_detector, status, score, entry_lo, entry_hi, stop, target1 "
        "FROM scout.opportunities WHERE ticker = :t ORDER BY asof DESC LIMIT 1", {"t": t}
    )
    if not feat and not inst:
        return {"error": f"{t} 不在 Scout 的标的池中，可用 /watch add {t} 加入"}
    return {"ticker": t, "instrument": inst, "watchlist_theme": (watch or {}).get("theme"),
            "features_date": feat and feat["date"], "features": feat and feat["payload"], "latest_opportunity": opp}


async def get_price_history(ticker: str, days: int = 30) -> dict:
    t = universe.yahoo_symbol(ticker)
    rows = await db.fetch_all(
        "SELECT date, open, high, low, close, volume FROM scout.bars_daily WHERE ticker = :t "
        "ORDER BY date DESC LIMIT :n", {"t": t, "n": min(max(days, 5), 120)}
    )
    return {"ticker": t, "bars": [
        [r["date"].isoformat(), round(r["open"], 2), round(r["high"], 2), round(r["low"], 2), round(r["close"], 2), int(r["volume"])]
        for r in reversed(rows)
    ], "columns": ["date", "open", "high", "low", "close", "volume"]}


async def get_news(ticker: str, days: int = 3) -> dict:
    t = universe.yahoo_symbol(ticker)
    days = min(max(int(days), 1), 90)
    limit = context.MAX_HEADLINES if days <= 3 else 20
    return {"ticker": t, "days": days, "news": await context.load_news(t, hours=days * 24, limit=limit)}


async def get_earnings(ticker: str) -> dict:
    t = universe.yahoo_symbol(ticker)
    today = date.today()
    events = (await context.earnings_map(today, days_back=120, days_ahead=90)).get(t, [])
    return {"ticker": t, **context.earnings_summary(events, today)} if events else {"ticker": t, "note": "无财报日历数据"}


async def list_opportunities(status: str = "active", ticker: str | None = None, limit: int = 10) -> dict:
    where = {"active": "status IN ('new', 'triggered')", "closed": "status NOT IN ('new', 'triggered')", "all": "true"}[status]
    params: dict[str, Any] = {"n": limit}
    if ticker:
        where += " AND ticker = :t"
        params["t"] = universe.yahoo_symbol(ticker)
    rows = await db.fetch_all(
        f"SELECT o.id, o.ticker, o.asof, o.primary_detector, o.detectors, o.status, o.score, o.entry_lo, o.entry_hi, "
        f"o.stop, o.target1, o.horizon_days, o.thesis->>'headline' AS headline, oc.return_pct, oc.excess_pct "
        f"FROM scout.opportunities o LEFT JOIN scout.opportunity_outcomes oc ON oc.opportunity_id = o.id "
        f"WHERE {where} ORDER BY o.asof DESC, o.score DESC LIMIT :n", params,
    )
    return {"opportunities": rows}


async def get_scoreboard() -> dict:
    from scout.tracker import scoreboard

    return {"scoreboard": await scoreboard()}


async def get_market_overview() -> dict:
    brief = await db.fetch_one("SELECT asof, payload FROM scout.briefs WHERE kind = 'eod' ORDER BY asof DESC, id DESC LIMIT 1")
    d = await _latest_feature_date()
    etfs = await db.fetch_all(
        "SELECT f.ticker, i.name, f.payload->'ret_1d' AS ret_1d, f.payload->'ret_5d' AS ret_5d, "
        "f.payload->'ret_20d' AS ret_20d FROM scout.features_daily f JOIN scout.instruments i USING (ticker) "
        "WHERE i.kind = 'etf' AND f.date = :d ORDER BY f.ticker", {"d": d},
    ) if d else []
    return {"date": d, "market": (brief or {}).get("payload", {}).get("market"), "etfs": etfs}


async def screen(sort_by: str, descending: bool = True, sector: str | None = None,
                 watchlist_only: bool = False, limit: int = 10) -> dict:
    if sort_by not in SNAPSHOT_FEATURES:
        return {"error": "unknown feature"}
    d = await _latest_feature_date()
    if not d:
        return {"error": "no features yet"}
    where = ["f.date = :d", "i.kind = 'stock'", f"f.payload->>'{sort_by}' IS NOT NULL"]
    params: dict[str, Any] = {"d": d, "n": limit}
    if sector:
        where.append("i.sector = :s")
        params["s"] = sector
    if watchlist_only:
        where.append("f.ticker IN (SELECT ticker FROM scout.watchlist WHERE active)")
    order = "DESC" if descending else "ASC"
    rows = await db.fetch_all(
        f"SELECT f.ticker, i.name, i.sector, f.payload FROM scout.features_daily f JOIN scout.instruments i USING (ticker) "
        f"WHERE {' AND '.join(where)} ORDER BY (f.payload->>'{sort_by}')::float {order} LIMIT :n", params,
    )
    keep = ("close", "ret_1d", "ret_5d", "ret_20d", "ret_63d", "rs_pct", "vol_ratio", "dist_52w_high", sort_by)
    return {"date": d, "rows": [
        {"ticker": r["ticker"], "name": r["name"], "sector": r["sector"],
         **{k: r["payload"].get(k) for k in keep}} for r in rows
    ]}


HANDLERS: dict[str, Callable[..., Awaitable[dict]]] = {
    "get_snapshot": get_snapshot, "get_price_history": get_price_history, "get_news": get_news,
    "get_earnings": get_earnings, "list_opportunities": list_opportunities, "get_scoreboard": get_scoreboard,
    "get_market_overview": get_market_overview, "screen": screen,
}


def _round(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 4)
    if isinstance(value, dict):
        return {k: _round(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_round(v) for v in value]
    return value


async def call_tool(name: str, args: dict) -> str:
    handler = HANDLERS.get(name)
    if handler is None:
        return json.dumps({"error": f"unknown tool {name}"})
    try:
        result = await handler(**args)
    except Exception as exc:
        logger.warning("tool %s failed: %s", name, exc)
        result = {"error": repr(exc)[:300]}
    return db.jsonb(_round(result))[:12000]


async def answer(question: str, channel: str = "telegram") -> str:
    settings = get_scout_settings()
    if not settings.openai_api_key:
        return "问答功能需要配置 OPENAI_API_KEY。"
    from openai import AsyncOpenAI

    client = AsyncOpenAI(api_key=settings.openai_api_key, timeout=settings.scout_llm_timeout_s)
    history = await db.fetch_all(
        "SELECT role, content FROM scout.chat_messages WHERE channel = :c AND created_at > now() - interval '2 hours' "
        "ORDER BY id DESC LIMIT 6", {"c": channel},
    )
    messages: list[dict] = [{"role": "system", "content": SYSTEM_PROMPT.replace("{today}", date.today().isoformat())}]
    messages += [{"role": r["role"], "content": r["content"]} for r in reversed(history)]
    messages.append({"role": "user", "content": question})
    tools = [{"type": "function", "function": t} for t in TOOLS]
    text = "抱歉，这个问题没能在工具调用轮次内完成，请换个更具体的问法。"
    for _ in range(MAX_ROUNDS):
        resp = await client.chat.completions.create(
            model=settings.scout_llm_model, messages=messages, tools=tools, temperature=0.2,
        )
        msg = resp.choices[0].message
        if not msg.tool_calls:
            text = (msg.content or "").strip() or text
            break
        messages.append(msg.model_dump(exclude_none=True))
        for tc in msg.tool_calls:
            try:
                args = json.loads(tc.function.arguments or "{}")
            except json.JSONDecodeError:
                args = {}
            messages.append({"role": "tool", "tool_call_id": tc.id, "content": await call_tool(tc.function.name, args)})
    await db.execute_many(
        "INSERT INTO scout.chat_messages (channel, role, content) VALUES (:c, :r, :t)",
        [{"c": channel, "r": "user", "t": question}, {"c": channel, "r": "assistant", "t": text}],
    )
    if channel != "telegram":
        return text
    from scout.telegram import esc

    # Models often emit **bold** despite the plain-text instruction; render it.
    return re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", esc(text))
