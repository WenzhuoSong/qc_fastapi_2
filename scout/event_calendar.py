"""Event calendar: scheduled events that can move the market, a sector or a holding, explained before and
reviewed after (user's request 2026-11-04, after the midterm-election night: Scout listed the election as one
line and said nothing about what it could mean).

Sources, most reliable first:
- rule: dates fixed by rule — US general elections, quarterly triple witching (S&P index changes take effect
  that close), market holidays;
- fed: FOMC decision days parsed from the Fed's own calendar page; past meetings feed the history;
- ff: this week's US data releases from Forex Factory (`context.economic_calendar`);
- web: a weekly web search for political, policy, industry and geopolitical events with a set date
  (external, unverified; each kept only with a cited source).

Nothing is predicted. A preview (importance 高, ≤ PREVIEW_DAYS away) says what the event is, why it matters,
what the market expects (external), two or three scenarios with the channel each works through, how the market
moved around past occurrences (Python, from Scout's bars) and which holdings are exposed. A review after the
reaction day compares the day's moves with those past occurrences. Numbers in prose come from the pack.
"""
from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime, timedelta
from typing import Literal

import pandas as pd
from pydantic import BaseModel, Field

from scout import db, llm_usage
from scout.config import get_scout_settings
from scout.market_calendar import is_us_equity_trading_day, previous_us_equity_trading_day, us_equity_holiday_name
from scout.thesis import allowed_numbers, unsupported_numbers
from scout.universe import SECTOR_ETFS, SECTOR_ZH

logger = logging.getLogger("scout.event_calendar")

HORIZON_DAYS = 45            # how far ahead rule / web events are kept fresh
WEB_DAYS = 35                # the web search looks this far ahead
WEB_EVERY_DAYS = 7
PREVIEW_DAYS = 5             # a 高 event this close gets a preview (pushed once)
SEARCH_MODEL = "gpt-4o-mini"
FED_URL = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"
REACTION_TICKERS = ("SPY", "QQQ", "IWM", "TLT")
CATEGORIES = ("宏观数据", "美联储", "政治与政策", "行业与公司", "市场结构", "地缘")
EXTERNAL = "据外部报道（未经核实）"

# When the market reacts: 0 = the event day's session, 1 = the next session (results come overnight).
REACTION_LAG = {"election": 1}

HOLIDAY_ZH = {"New Year's Day": "元旦", "Martin Luther King Jr. Day": "马丁·路德·金纪念日", "Washington's Birthday": "总统日",
              "Good Friday": "耶稣受难日", "Memorial Day": "阵亡将士纪念日", "Juneteenth National Independence Day": "六月节",
              "Independence Day": "独立日", "Labor Day": "劳动节", "Thanksgiving Day": "感恩节", "Christmas Day": "圣诞节"}


# ── Rule dates ──

def election_day(year: int) -> date:
    """US federal general election: the Tuesday after the first Monday in November."""
    d = date(year, 11, 1)
    first_monday = d + timedelta(days=(0 - d.weekday()) % 7)
    return first_monday + timedelta(days=1)


def triple_witching(year: int, month: int) -> date:
    """Third Friday of the month (the trading day before when it is a holiday)."""
    d = date(year, month, 1)
    d += timedelta(days=(4 - d.weekday()) % 7 + 14)
    return d if is_us_equity_trading_day(d) else previous_us_equity_trading_day(d)


def rule_events(start: date, end: date) -> list[dict]:
    out = []
    for y in range(start.year, end.year + 1):
        d = election_day(y)
        if y % 2 == 0 and start <= d <= end:
            title = "美国大选（总统与国会选举）" if y % 4 == 0 else "美国中期选举（国会选举）"
            out.append(_event(f"rule:election:{d}", d, None, "政治与政策", "election", title, "高", "rule",
                              {"why": "选举结果会改变财政、税收、监管和贸易政策的预期；计票在美东晚间进行，市场反应通常在第二个交易日"}))
        for m in (3, 6, 9, 12):
            d = triple_witching(y, m)
            if start <= d <= end:
                out.append(_event(f"rule:triple_witching:{d}", d, "16:00", "市场结构", "triple_witching",
                                  "季度期权与期货同时到期（三巫日），标普指数季度调整当天收盘生效", "中", "rule",
                                  {"why": "大量合约到期和指数基金调仓集中在收盘，成交量放大，个股收盘价可能被扭曲"}))
    d = start
    while d <= end:
        name = us_equity_holiday_name(d)
        if name:
            out.append(_event(f"rule:holiday:{d}", d, None, "市场结构", "holiday", f"美股休市：{HOLIDAY_ZH.get(name, name)}", "中", "rule",
                              {"why": "当天不交易；止损、提醒和简报顺延到下一个交易日"}))
        d += timedelta(days=1)
    return out


def _event(key: str, d: date, time_et: str | None, category: str, kind: str, title: str, importance: str, source: str,
           detail: dict | None = None) -> dict:
    return {"key": key[:160], "event_date": d, "time_et": time_et, "category": category, "kind": kind, "title": title,
            "importance": importance, "source": source, "detail": detail or {}}


# ── The Fed's schedule ──

_MONTHS = {m: i for i, m in enumerate(["January", "February", "March", "April", "May", "June", "July", "August", "September",
                                       "October", "November", "December"], 1)}
_MONTHS.update({m[:3]: i for m, i in list(_MONTHS.items())})


def parse_fomc(html: str) -> list[dict]:
    """Scheduled FOMC meetings on the Fed's calendar page: [{date (decision day), sep}] ("*" = projections)."""
    out = []
    parts = re.split(r"(\d{4}) FOMC Meetings", html)
    for i in range(1, len(parts), 2):
        year = int(parts[i])
        for month, days in re.findall(r'fomc-meeting__month[^>]*>\s*<strong>([^<]+)</strong>.*?fomc-meeting__date[^>]*>([^<]+)<',
                                      parts[i + 1], re.S):
            days = days.strip()
            if "(" in days:                                   # notation votes, unscheduled calls
                continue
            last_month = month.strip().split("/")[-1]
            last_day = re.findall(r"\d+", days)
            if last_month not in _MONTHS or not last_day:
                continue
            m = _MONTHS[last_month]
            y = year + 1 if month.strip().startswith("Dec") and m == 1 else year
            try:
                out.append({"date": date(y, m, int(last_day[-1])), "sep": "*" in days})
            except ValueError:
                continue
    return sorted({o["date"]: o for o in out}.values(), key=lambda o: o["date"])


async def fed_events() -> list[dict]:
    import httpx

    try:
        async with httpx.AsyncClient(timeout=30, headers={"User-Agent": "Mozilla/5.0"}, follow_redirects=True) as client:
            resp = await client.get(FED_URL)
        resp.raise_for_status()
        meetings = parse_fomc(resp.text)
    except Exception as exc:
        logger.warning("Fed calendar unavailable: %s", exc)
        return []
    return [_event(f"fed:fomc:{m['date']}", m["date"], "14:00", "美联储", "fomc",
                   "美联储利率决议（FOMC）" + ("，附经济预测与点阵图" if m["sep"] else ""), "高", "fed",
                   {"why": "决定联邦基金利率；声明措辞和主席发布会会改变市场对未来利率路径的预期，影响估值和美元", "sep": m["sep"]})
            for m in meetings]


# ── Forex Factory's week ──

_FED_TITLES = ("Federal Funds Rate", "FOMC Statement", "FOMC Press Conference")


def ff_events(items: list[dict], taken: set[tuple[date, str]]) -> list[dict]:
    """This week's US releases; the FOMC decision and elections come from their own sources (`taken`)."""
    out = []
    for e in items:
        d, title = date.fromisoformat(e["date"]), e["title"]
        if title in _FED_TITLES and (d, "fomc") in taken:
            continue
        if "Election" in title and (d, "election") in taken:
            continue
        category = ("美联储" if any(k in title for k in ("FOMC", "Fed ", "Federal Funds", "Powell")) else
                    "政治与政策" if "Election" in title else "宏观数据")
        out.append(_event(f"ff:{d}:{_slug(title)}", d, e.get("time_et"), category, "ff", e.get("title_zh") or title, e["impact"], "ff",
                          {"title_en": title, "forecast": e.get("forecast"), "previous": e.get("previous")}))
    return out


def _slug(text: str) -> str:
    return re.sub(r"[^0-9a-z一-鿿]+", "-", text.lower()).strip("-")[:60]


# ── Web search for political / policy / industry / geopolitical events ──

WEB_PROMPT = """List scheduled events between {start} and {end} that could move US stocks, a sector or large US companies.
Include only events whose date is announced or fixed: US politics and policy (elections, government funding or debt-limit
deadlines, tariff decisions and deadlines, major court rulings, key legislation votes, Treasury refunding), geopolitics with a
set date (summits, trade talks, OPEC+ meetings, foreign elections that matter to markets), and industry events (major
conferences and product launches of large US companies, FDA decisions for large caps, index reconstitutions).
Exclude routine US economic data releases, FOMC meetings and company earnings dates (covered elsewhere).
For each event give: the date (YYYY-MM-DD), a short title, why it matters for US stocks in one sentence, the sectors and
large-cap tickers most exposed, and the URL of the source that states the date. Say plainly when nothing is scheduled."""

WEB_SCHEMA = {
    "type": "object", "additionalProperties": False, "required": ["events"],
    "properties": {"events": {"type": "array", "items": {
        "type": "object", "additionalProperties": False,
        "required": ["date", "title_zh", "category", "importance", "why_zh", "sectors", "tickers", "url"],
        "properties": {
            "date": {"type": "string", "description": "YYYY-MM-DD；原文没有确定日期就不要列"},
            "title_zh": {"type": "string", "description": "中文标题，≤ 30 字"},
            "category": {"type": "string", "enum": ["政治与政策", "行业与公司", "地缘", "宏观数据"]},
            "importance": {"type": "string", "enum": ["高", "中"],
                           "description": "高 = 可能让整个美股或一个板块明显波动；中 = 影响个别板块或公司"},
            "why_zh": {"type": "string", "description": "一句中文：为什么可能影响美股，不写数字预测"},
            "sectors": {"type": "array", "items": {"type": "string", "enum": list(SECTOR_ETFS)}},
            "tickers": {"type": "array", "items": {"type": "string"}},
            "url": {"type": "string", "description": "写明日期的来源网址（必须来自检索结果）"},
        }}}},
}


async def _search(prompt: str, purpose: str, model: str = SEARCH_MODEL) -> dict:
    """One web search; {"text", "sources": [{"title","url"}]} or {}."""
    from scout import responses

    if not get_scout_settings().openai_api_key:
        return {}
    try:
        tool = "web_search_preview" if model == SEARCH_MODEL else "web_search"      # the reasoning models take the newer tool
        body = {"model": model, "tools": [{"type": tool}], "input": prompt}
        if model != SEARCH_MODEL:
            body["reasoning"] = {"effort": "low"}
        data = await responses.create(body, timeout=240, purpose=purpose)
    except Exception as exc:
        logger.warning("web search (%s) failed: %s %s", purpose, type(exc).__name__, exc)
        return {}
    sources, seen = [], set()
    for item in data.get("output") or []:
        for part in item.get("content") or [] if item.get("type") == "message" else []:
            for ann in part.get("annotations") or []:
                url = (ann.get("url") or "").split("?utm_source")[0]
                if ann.get("type") == "url_citation" and url and url not in seen:
                    seen.add(url)
                    sources.append({"title": ann.get("title") or url, "url": url})
    text = responses.output_text(data).strip()
    return {"text": text, "sources": sources} if text else {}


def web_events(parsed: list[dict], cited: set[str], text: str, start: date, end: date) -> list[dict]:
    """Keep events with a date in the window and a source the search actually cited."""
    out = []
    for e in parsed:
        try:
            d = date.fromisoformat(e["date"])
        except (KeyError, ValueError):
            continue
        url = (e.get("url") or "").split("?utm_source")[0]
        if not (start <= d <= end) or not url or (url not in cited and url not in text):
            continue
        title = (e.get("title_zh") or "").strip()[:60]
        if not title:
            continue
        out.append(_event(f"web:{d}:{_slug(title)}", d, None, e["category"], "web", title, e["importance"], "web",
                          {"why": e.get("why_zh"), "sectors": [s for s in e.get("sectors") or [] if s in SECTOR_ETFS],
                           "tickers": [t.upper() for t in e.get("tickers") or []][:8], "url": url, "label": EXTERNAL,
                           "collected": date.today().isoformat()}))
    return out


def same_event(a: dict, b: dict) -> bool:
    """Weekly searches retitle the same event: same day and most title characters shared."""
    if a["event_date"] != b["event_date"]:
        return False
    x, y = set(re.sub(r"\W", "", a["title"])), set(re.sub(r"\W", "", b["title"]))
    return bool(x and y) and len(x & y) / min(len(x), len(y)) >= 0.6


async def collect_web(today: date) -> dict:
    """The weekly search (two steps: search with citations, then structured extraction checked against them)."""
    from scout import responses

    start, end = today, today + timedelta(days=WEB_DAYS)
    settings = get_scout_settings()
    found = await _search(WEB_PROMPT.format(start=start, end=end), "calendar_search", model=settings.scout_agent_model)
    if not found:
        return {"web": 0, "status": "no search result"}
    data = await responses.create({
        "model": settings.scout_parse_model,
        "instructions": f"把检索结果里的事件整理成列表（今天 {today}）。只收录原文给出确定日期、且附带来源网址的事件；不要补充检索结果里没有的事件。",
        "input": [{"role": "user", "content": found["text"] + "\n\n来源：\n" + "\n".join(s["url"] for s in found["sources"])}],
        "text": {"format": {"type": "json_schema", "name": "events", "schema": WEB_SCHEMA, "strict": True}}}, purpose="calendar_parse")
    parsed = json.loads(responses.output_text(data) or "{}").get("events", [])
    events = web_events(parsed, {s["url"] for s in found["sources"]}, found["text"], start, end)
    existing = await load(start, end, sources=("web",))
    fresh = []
    for e in events:
        old = next((o for o in existing if same_event(o, e)), None)
        fresh.append({**e, "key": old["key"]} if old else e)
    await upsert(fresh)
    await db.execute("INSERT INTO scout.briefs (kind, asof, content, payload) VALUES ('calendar_collect', :d, :c, CAST(:p AS JSONB))",
                     {"d": today, "c": f"{len(fresh)} 个事件", "p": db.jsonb({"parsed": len(parsed), "kept": len(fresh),
                                                                              "sources": found["sources"][:20]})})
    return {"web": len(fresh), "parsed": len(parsed)}


# ── Storage ──

async def upsert(events: list[dict]) -> None:
    for e in events:
        await db.execute(
            "INSERT INTO scout.calendar_events (key, event_date, time_et, category, kind, title, importance, source, detail) "
            "VALUES (:key, :event_date, :time_et, :category, :kind, :title, :importance, :source, CAST(:detail AS JSONB)) "
            "ON CONFLICT (key) DO UPDATE SET event_date = EXCLUDED.event_date, time_et = EXCLUDED.time_et, title = EXCLUDED.title, "
            "importance = EXCLUDED.importance, detail = scout.calendar_events.detail || EXCLUDED.detail, last_seen = now()",
            {**e, "detail": db.jsonb(e["detail"])})


async def load(start: date, end: date, sources: tuple[str, ...] | None = None) -> list[dict]:
    rows = await db.fetch_all(
        "SELECT * FROM scout.calendar_events WHERE event_date BETWEEN :a AND :b ORDER BY event_date, time_et NULLS FIRST, key",
        {"a": start, "b": end})
    return [r for r in rows if sources is None or r["source"] in sources]


async def refresh(today: date, web: bool | None = None) -> dict:
    """Rule dates, the Fed's schedule (past meetings too: they feed the history), this week's releases; the web
    search when `web` is True, or when the last one is older than WEB_EVERY_DAYS (`web` None)."""
    from scout import context

    end = today + timedelta(days=HORIZON_DAYS)
    rules = rule_events(today - timedelta(days=7), end)
    fed = await fed_events()
    taken = {(e["event_date"], e["kind"]) for e in rules + fed}
    ff = ff_events(await context.economic_calendar(), taken)
    await upsert(rules + fed + ff)
    out = {"rule": len(rules), "fed": len(fed), "ff": len(ff)}
    if web is None:
        last = await db.fetch_one("SELECT max(asof) AS d FROM scout.briefs WHERE kind = 'calendar_collect'")
        web = not (last and last["d"] and (today - last["d"]).days < WEB_EVERY_DAYS)
    if web:
        try:
            out.update(await collect_web(today))
        except Exception as exc:
            logger.warning("calendar web collection failed: %s", exc)
            out["web"] = f"failed: {type(exc).__name__}"
    return out


# ── How the market moved around past occurrences ──

def reaction_day(d: date, kind: str, sessions: pd.DatetimeIndex) -> pd.Timestamp | None:
    """The session the market reacts on: the event day, or the next session for overnight results."""
    later = sessions[sessions >= pd.Timestamp(d)] if REACTION_LAG.get(kind, 0) == 0 else sessions[sessions > pd.Timestamp(d)]
    return later[0] if len(later) else None


def reaction_stats(dates: list[date], kind: str, closes: pd.DataFrame) -> dict | None:
    """Close-to-close moves on the reaction sessions of past events vs SPY's ordinary day (median |move| over the
    252 sessions before each); per-event rows kept when there are few."""
    if "SPY" not in closes or closes.empty:
        return None
    rets = closes.pct_change()
    spy_abs = rets["SPY"].abs()
    rows, normal = [], []
    for d in dates:
        rd = reaction_day(d, kind, closes.index)
        if rd is None or pd.isna(rets.at[rd, "SPY"]):
            continue
        i = closes.index.get_loc(rd)
        if i < 60:
            continue
        normal.append(float(spy_abs.iloc[max(0, i - 252):i].median()))
        rows.append({"日期": rd.date().isoformat(), **{t: float(rets.at[rd, t]) for t in closes.columns if not pd.isna(rets.at[rd, t])}})
    if not rows:
        return None
    df = pd.DataFrame(rows)
    spy_abs_ev = float(df["SPY"].abs().median())
    out = {"次数": len(rows), "最早": rows[0]["日期"], "SPY 当天涨跌幅中位数（绝对值）": spy_abs_ev,
           "SPY 平常一天涨跌幅中位数（绝对值）": float(pd.Series(normal).median()),
           "SPY 上涨次数占比": float((df["SPY"] > 0).mean()),
           "平均涨跌": {t: float(df[t].mean()) for t in df.columns if t != "日期" and df[t].notna().sum() >= max(3, len(df) // 2)}}
    out["波动倍数"] = spy_abs_ev / out["SPY 平常一天涨跌幅中位数（绝对值）"] if out["SPY 平常一天涨跌幅中位数（绝对值）"] else None
    if len(rows) <= 12:
        out["每次"] = [{"日期": r["日期"], **{t: r[t] for t in ("SPY", "QQQ", "IWM", "TLT") if t in r}} for r in rows]
    else:
        out["最近 6 次"] = [{"日期": r["日期"], **{t: r[t] for t in ("SPY", "QQQ", "IWM", "TLT") if t in r}} for r in rows[-6:]]
    return out


async def _closes(tickers: list[str], since: date) -> pd.DataFrame:
    rows = await db.fetch_all("SELECT ticker, date, close FROM scout.bars_daily WHERE ticker = ANY(:t) AND date >= :d",
                              {"t": tickers, "d": since})
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows).pivot(index="date", columns="ticker", values="close").sort_index()
    df.index = pd.to_datetime(df.index)
    return df


async def past_dates(kind: str, before: date) -> list[date]:
    if kind == "election":
        return [election_day(y) for y in range(2014, before.year + 1, 2) if election_day(y) < before]
    if kind == "triple_witching":
        return [triple_witching(y, m) for y in range(2015, before.year + 1) for m in (3, 6, 9, 12) if triple_witching(y, m) < before]
    if kind != "fomc":
        return []                     # releases and web events differ from each other; no comparable past yet
    rows = await db.fetch_all("SELECT DISTINCT event_date FROM scout.calendar_events WHERE kind = 'fomc' AND event_date < :d "
                              "ORDER BY event_date", {"d": before})
    return [r["event_date"] for r in rows]


async def history(kind: str, before: date, extra: list[str] = ()) -> dict | None:
    dates = await past_dates(kind, before)
    if not dates:
        return None
    closes = await _closes(list(REACTION_TICKERS) + list(extra), dates[0] - timedelta(days=400))
    return describe(reaction_stats(dates, kind, closes), kind)


def _p(x: float | None, digits: int = 1, sign: bool = True) -> str | None:
    return None if x is None else f"{x * 100:{'+' if sign else ''}.{digits}f}%"


def describe(st: dict | None, kind: str) -> dict | None:
    """The stats as the LLM and the user read them: percentages, which session, which kind of election."""
    if not st:
        return None
    when = "选举次日（计票在美东晚间，市场次日反应）" if REACTION_LAG.get(kind) else "事件当天"
    out = {"次数": st["次数"], "最早": st["最早"], "反应日": when,
           "SPY 反应日涨跌幅中位数（不分涨跌）": _p(st["SPY 当天涨跌幅中位数（绝对值）"], 2, False),
           "SPY 平常一天涨跌幅中位数（不分涨跌）": _p(st["SPY 平常一天涨跌幅中位数（绝对值）"], 2, False),
           "波动是平常的几倍": f"{st['波动倍数']:.1f} 倍" if st.get("波动倍数") else None,
           "SPY 上涨次数": f"{round(st['SPY 上涨次数占比'] * st['次数'])} / {st['次数']}",
           "平均涨跌": {t: _p(v, 2) for t, v in st["平均涨跌"].items()}}
    rows = st.get("每次") or st.get("最近 6 次") or []
    label = "每次" if "每次" in st else "最近 6 次"
    out[label] = [{"日期": r["日期"], **({"类型": "大选" if int(r["日期"][:4]) % 4 == 0 else "中期选举"} if kind == "election" else {}),
                   **{t: _p(r[t], 1) for t in ("SPY", "QQQ", "IWM", "TLT") if t in r}} for r in rows]
    return out


# ── Exposure of the holdings ──

async def exposure() -> list[dict]:
    from scout import research

    held = await db.fetch_all("SELECT DISTINCT t.ticker, i.sector, i.name FROM scout.trades t LEFT JOIN scout.instruments i USING (ticker) "
                              "WHERE t.closed_on IS NULL ORDER BY t.ticker")
    if not held:
        return []
    since = date.today() - timedelta(days=420)
    macro = await research._macro(since)
    spy = await research._closes("SPY", since)
    feats = {r["ticker"]: r["payload"] or {} for r in await db.fetch_all(
        "SELECT DISTINCT ON (ticker) ticker, payload FROM scout.features_daily WHERE ticker = ANY(:t) ORDER BY ticker, date DESC",
        {"t": [h["ticker"] for h in held]})}
    out = []
    for h in held:
        row = {"代码": h["ticker"], "板块": SECTOR_ZH.get(h["sector"], h["sector"] or "—"), "β": feats.get(h["ticker"], {}).get("beta_60")}
        sens = research.macro_sensitivity(await research._closes(h["ticker"], since), spy, macro) if len(spy) else {}
        for key, t, label, up in (("macro_rate_10bp", "macro_rate_t", "10 年期利率每升 0.10 个百分点", "利率上升"),
                                  ("macro_oil_10pct", "macro_oil_t", "油价每涨 10%", "油价上涨"),
                                  ("macro_usd_1pct", "macro_usd_t", "美元每涨 1%", "美元走强")):
            if sens.get(t) is not None and abs(sens[t]) >= 2:          # only the ones that are statistically clear
                v = sens[key]
                row[label] = f"它平均 {_p(v, 1)}（{up}时它{'上涨' if v > 0 else '下跌'}，过去一年统计）"
        out.append(row)
    return out


# ── Preview (before) ──

class Scenario(BaseModel):
    case: str = Field(description="一种可能的结果，白话，不写概率")
    channel: str = Field(description="它通过什么渠道影响股市：利率、企业盈利、风险偏好、政策或资金流动性，一两句白话")
    sectors: str = Field(description="通常受益或受压的板块；说不清就写'不确定'")


class HoldingImpact(BaseModel):
    ticker: str
    channel: str = Field(description="这个事件通过什么渠道影响它；没有直接关联就写'没有直接关联，主要随大盘'")


class Preview(BaseModel):
    what: str = Field(description="这是什么事件，一两句白话")
    why: str = Field(description="为什么可能影响股市")
    expectations: str = Field(description="市场现在预期什么（来自外部检索，写明'据外部报道（未经核实）'）；没有检索结果写'没有检索到市场预期'")
    scenarios: list[Scenario]
    history: str = Field(description="过去同类事件当天市场怎么动（只用 history 里的数字），样本少要说明")
    holdings: list[HoldingImpact]
    watch: str = Field(description="事后看什么来判断影响大小")


PREVIEW_PROMPT = """你在给没有金融背景的投资者解释一个即将发生的事件。证据包：event（事件）、history（Python 按 Scout 行情算的过去同类事件
当天的涨跌，没有就是 null）、market（当前大盘状态）、holdings（用户持仓：板块、β、对利率 / 油价 / 美元的历史敏感度，只有统计上显著的才列出）、
web（外部检索，未经核实）。

写法：
- 白话，专业词第一次出现时用半句话解释（例如"点阵图：每位美联储官员对未来利率的预测"）。
- 不预测结果，不给买卖建议；scenarios 写 2–3 种可能结果，每种说清通过什么渠道影响股市、通常哪些板块受益或受压。
- history 只用 history 里的数字；次数少于 10 次要写"样本少，只能参考"；没有 history 就写"Scout 还没有这类事件的历史统计"。
- expectations 来自 web，写明"据外部报道（未经核实）"和报道日期；web 说没有近期报道或为空，就写"没有检索到市场预期"。
- holdings 每只持仓一条：有利率 / 油价 / 美元敏感度时按证据里写的方向说明（不要自己推断方向）、或板块直接相关时说明渠道；
  否则写"没有直接关联，主要随大盘"。
- 数字只能原样引用证据包里出现的数值，不要自己计算。中文。"""


def validate_preview(p: Preview, pack: dict) -> list[str]:
    issues = []
    texts = [p.what, p.why, p.expectations, p.history, p.watch, *[f"{s.case} {s.channel} {s.sectors}" for s in p.scenarios],
             *[h.channel for h in p.holdings]]
    bad = unsupported_numbers("\n".join(texts), allowed_numbers(pack))
    if bad:
        issues.append(f"以下数字不在证据包中：{', '.join(sorted(set(bad)))}")
    if not 2 <= len(p.scenarios) <= 3:
        issues.append("scenarios 要写 2–3 种")
    held = {h["代码"] for h in pack["holdings"]}
    if {h.ticker for h in p.holdings} != held:
        issues.append(f"holdings 要且只要覆盖这些持仓各一条：{', '.join(sorted(held)) or '（无持仓，留空）'}")
    if pack.get("web") and "没有检索到" not in p.expectations and "据外部报道" not in p.expectations:
        issues.append("expectations 来自外部检索，要写明「据外部报道（未经核实）」")
    n = (pack.get("history") or {}).get("次数") or 0
    if n and n < 10 and "样本" not in p.history:
        issues.append(f"历史只有 {n} 次，history 要说明样本少")
    return issues


def fallback_preview(pack: dict) -> Preview:
    e, h = pack["event"], pack.get("history")
    hist = (f"过去 {h['次数']} 次，SPY {h['反应日']}涨跌幅中位数（不分涨跌）{h['SPY 反应日涨跌幅中位数（不分涨跌）']}，"
            f"平常一天 {h['SPY 平常一天涨跌幅中位数（不分涨跌）']}" + ("（样本少，只能参考）" if h["次数"] < 10 else "")) if h else \
        "Scout 还没有这类事件的历史统计"
    return Preview(what=e["标题"], why=e.get("为什么重要") or "—", expectations="没有检索到市场预期", history=hist,
                   scenarios=[Scenario(case="结果符合预期", channel="影响有限", sectors="不确定"),
                              Scenario(case="结果偏离预期", channel="利率预期或风险偏好变化", sectors="不确定")],
                   holdings=[HoldingImpact(ticker=x["代码"], channel="没有直接关联，主要随大盘") for x in pack["holdings"]],
                   watch="事后看大盘和持仓当天的涨跌是否明显大于平常")


def event_row(e: dict) -> dict:
    d = e["detail"] or {}
    return {"标题": e["title"], "日期": str(e["event_date"]), "美东时间": e.get("time_et"), "类别": e["category"],
            "重要性": e["importance"], "来源": {"rule": "按规则确定的日期", "fed": "美联储官网日程", "ff": "Forex Factory 经济日历",
                                         "web": EXTERNAL}[e["source"]],
            "为什么重要": d.get("why"), **({"预期": d["forecast"]} if d.get("forecast") else {}),
            **({"前值": d["previous"]} if d.get("previous") else {}),
            **({"相关板块": [SECTOR_ZH.get(s, s) for s in d["sectors"]]} if d.get("sectors") else {}),
            **({"相关股票": d["tickers"]} if d.get("tickers") else {}), **({"来源网址": d["url"]} if d.get("url") else {})}


async def preview(e: dict) -> dict:
    """Write, validate and store one preview; returns the stored payload."""
    from scout import market_filter

    hist = await history(e["kind"], e["event_date"])
    market = await db.fetch_one("SELECT payload FROM scout.briefs WHERE kind = 'eod' ORDER BY id DESC LIMIT 1")
    mf = await market_filter.current(date.today())
    today = date.today()
    web = await _search(f"Today is {today}. What do markets expect from this event on {e['event_date']}: {e['title']} "
                        f"({(e['detail'] or {}).get('title_en') or e['category']})? Consensus, the scenarios analysts discuss, "
                        f"and what each would mean for US stocks and sectors. Use only reports published between {today - timedelta(days=14)} "
                        f"and {today}, and give each fact's publication date. If there are no recent reports, say so plainly. "
                        "4-6 sentences.", "calendar_preview_search")
    pack = {"event": event_row(e), "history": hist, "holdings": await exposure(),
            "market": {"大盘过滤": market_filter.text(mf) if mf else None,
                       "SPY 20 日": ((market or {}).get("payload") or {}).get("market", {}).get("spy_ret_20d")},
            "web": {"内容": web["text"][:3000], "说明": EXTERNAL} if web else None}
    settings = get_scout_settings()
    result, source, tries = None, "fallback", []
    if settings.openai_api_key:
        from openai import AsyncOpenAI

        client = AsyncOpenAI(api_key=settings.openai_api_key, timeout=120)
        messages = [{"role": "system", "content": PREVIEW_PROMPT},
                    {"role": "user", "content": json.dumps(pack, ensure_ascii=False, default=str)}]
        for attempt in (1, 2):
            try:
                resp = await client.beta.chat.completions.parse(model=settings.scout_insight_model, messages=messages,
                                                                response_format=Preview, temperature=0.2)
                await llm_usage.record_sdk(resp, "calendar_preview", settings.scout_insight_model)
                parsed = resp.choices[0].message.parsed
            except Exception as exc:
                tries.append([f"LLM 调用失败：{type(exc).__name__}"])
                break
            issues = validate_preview(parsed, pack)
            tries.append(issues)
            if not issues:
                result, source = parsed, "llm"
                break
            messages += [{"role": "assistant", "content": parsed.model_dump_json()},
                         {"role": "user", "content": "校验未通过，请修正后完整重写：\n" + "\n".join(issues)}]
    if result is None:
        result = fallback_preview(pack)
    payload = {"preview": result.model_dump(), "source": source, "validation": tries, "pack": pack,
               "web_sources": (web or {}).get("sources", [])[:6]}
    await db.execute("INSERT INTO scout.event_notes (event_key, note, payload) VALUES (:k, 'preview', CAST(:p AS JSONB))",
                     {"k": e["key"], "p": db.jsonb(payload)})
    return payload


def render_preview(e: dict, payload: dict) -> str:
    p, pack = payload["preview"], payload["pack"]
    when = f"{str(e['event_date'])[5:]}" + (f" 美东 {e['time_et']}" if e.get("time_et") else "")
    lines = [f"📅 事件前瞻：{e['title']}（{when}，{e['category']}）", f"是什么：{p['what']}", f"为什么重要：{p['why']}",
             f"市场预期：{p['expectations']}", "可能的情形："]
    lines += [f"{i}. {s['case']}：{s['channel']}（板块：{s['sectors']}）" for i, s in enumerate(p["scenarios"], 1)]
    lines.append(f"历史上：{p['history']}")
    if p["holdings"]:
        lines.append("你的持仓：")
        lines += [f"· {h['ticker']}：{h['channel']}" for h in p["holdings"]]
    lines.append(f"事后看：{p['watch']}")
    if e["source"] == "web" or payload.get("web_sources"):
        lines.append(f"（事件或预期部分{EXTERNAL}" + ("；规则整理，AI 未通过校验" if payload["source"] != "llm" else "") + "）")
    elif payload["source"] != "llm":
        lines.append("（AI 未通过校验，以上为规则整理）")
    return "\n".join(lines)


async def due_previews(today: date) -> list[dict]:
    """高 events within PREVIEW_DAYS without a preview yet."""
    rows = await db.fetch_all(
        "SELECT e.* FROM scout.calendar_events e WHERE e.importance = '高' AND e.event_date BETWEEN :a AND :b "
        "AND NOT EXISTS (SELECT 1 FROM scout.event_notes n WHERE n.event_key = e.key AND n.note = 'preview') "
        "ORDER BY e.event_date", {"a": today, "b": today + timedelta(days=PREVIEW_DAYS)})
    return rows


async def send_previews(today: date, push: bool = True) -> int:
    """Previews of the 高 events within PREVIEW_DAYS (after `refresh`), each written and pushed once."""
    sent = 0
    for e in await due_previews(today):
        try:
            payload = await preview(e)
        except Exception as exc:
            logger.warning("preview of %s failed: %s", e["key"], exc)
            continue
        if push:
            from scout import telegram

            await telegram.send(telegram.esc(render_preview(e, payload)))
            await db.execute("UPDATE scout.event_notes SET pushed = true WHERE event_key = :k AND note = 'preview'", {"k": e["key"]})
            sent += 1
    return sent


def upcoming_lines(rows: list[dict], today: date, skip_ff: bool = True) -> list[str]:
    """One line per event for the briefs; Forex Factory's releases are already listed by the pre-market brief."""
    wd = "一二三四五六日"
    out = []
    for e in rows:
        if skip_ff and e["source"] == "ff":
            continue
        d = e["event_date"]
        tag = "（外部报道）" if e["source"] == "web" else ""
        out.append(f"· {str(d)[5:]} 周{wd[d.weekday()]}" + (f" {e['time_et']}" if e.get("time_et") else "")
                   + f" [{e['importance']}] {e['title']}（{e['category']}）{tag}")
    return out


# ── Review (after) ──

class Review(BaseModel):
    outcome: str = Field(description="结果是什么（来自外部检索，写明'据外部报道（未经核实）'）；没有检索结果写'没有检索到结果报道'")
    reaction: str = Field(description="市场当天怎么动，和过去同类事件相比大还是小（只用证据包数字）")
    holdings: str = Field(description="对你的持仓：哪些动得比平常明显，一两句；没有持仓写'无持仓'")


REVIEW_PROMPT = """你在给没有金融背景的投资者复盘一个刚发生的事件。证据包：event、today（Python 算的反应日涨跌：大盘、板块 ETF、持仓）、
history（过去同类事件当天的涨跌）、web（外部检索的结果报道，未经核实）。
- outcome 只来自 web，写明"据外部报道（未经核实）"；web 为空写"没有检索到结果报道"。
- reaction 用 today 和 history 的数字比较：比过去同类事件和平常一天大还是小；不要把当天涨跌全部归因于这个事件，同一天可能还有其他消息。
- 数字只能原样引用证据包里的数值；白话、中文；不给买卖建议。"""


def validate_review(r: Review, pack: dict) -> list[str]:
    issues = []
    bad = unsupported_numbers("\n".join([r.outcome, r.reaction, r.holdings]), allowed_numbers(pack))
    if bad:
        issues.append(f"以下数字不在证据包中：{', '.join(sorted(set(bad)))}")
    if pack.get("web") and "没有检索到" not in r.outcome and "据外部报道" not in r.outcome:
        issues.append("outcome 来自外部检索，要写明「据外部报道（未经核实）」")
    return issues


async def due_reviews(session: date) -> list[dict]:
    """高 events whose reaction session is `session` and that have no review yet."""
    rows = await db.fetch_all(
        "SELECT e.* FROM scout.calendar_events e WHERE e.importance = '高' AND e.event_date BETWEEN :a AND :b "
        "AND NOT EXISTS (SELECT 1 FROM scout.event_notes n WHERE n.event_key = e.key AND n.note = 'review')",
        {"a": session - timedelta(days=5), "b": session})
    idx = pd.DatetimeIndex([pd.Timestamp(session - timedelta(days=i)) for i in range(7, -1, -1)
                            if is_us_equity_trading_day(session - timedelta(days=i))])
    return [e for e in rows if (rd := reaction_day(e["event_date"], e["kind"], idx)) is not None and rd.date() == session]


async def review(e: dict, session: date) -> dict:
    held = [h["代码"] for h in await exposure()]
    tickers = list(REACTION_TICKERS) + list(SECTOR_ETFS.values()) + held
    closes = await _closes(tickers, session - timedelta(days=10))
    closes = closes[closes.index <= pd.Timestamp(session)]
    today = {}
    if len(closes) >= 2 and closes.index[-1] == pd.Timestamp(session):
        r = closes.iloc[-1] / closes.iloc[-2] - 1
        etf_zh = {v: SECTOR_ZH.get(k, k) for k, v in SECTOR_ETFS.items()}
        today = {"日期": session.isoformat(),
                 "大盘": {t: float(r[t]) for t in REACTION_TICKERS if t in r and not pd.isna(r[t])},
                 "板块": {f"{etf_zh[t]}（{t}）": float(r[t]) for t in SECTOR_ETFS.values() if t in r and not pd.isna(r[t])},
                 "持仓": {t: float(r[t]) for t in held if t in r and not pd.isna(r[t])}}
    hist = await history(e["kind"], e["event_date"])
    web = await _search(f"Today is {session}. What was the outcome of this event on {e['event_date']}: {e['title']} "
                        f"({(e['detail'] or {}).get('title_en') or e['category']}), and how did US stocks react? Use only reports "
                        f"published on or after {e['event_date']}, with their dates. 3-5 sentences.", "calendar_review_search")
    pack = {"event": event_row(e), "today": today, "history": hist, "web": {"内容": web["text"][:3000], "说明": EXTERNAL} if web else None}
    settings = get_scout_settings()
    result, source, tries = None, "fallback", []
    if settings.openai_api_key and today:
        from openai import AsyncOpenAI

        client = AsyncOpenAI(api_key=settings.openai_api_key, timeout=120)
        messages = [{"role": "system", "content": REVIEW_PROMPT},
                    {"role": "user", "content": json.dumps(pack, ensure_ascii=False, default=str)}]
        for attempt in (1, 2):
            try:
                resp = await client.beta.chat.completions.parse(model=settings.scout_insight_model, messages=messages,
                                                                response_format=Review, temperature=0.2)
                await llm_usage.record_sdk(resp, "calendar_review", settings.scout_insight_model)
                parsed = resp.choices[0].message.parsed
            except Exception as exc:
                tries.append([f"LLM 调用失败：{type(exc).__name__}"])
                break
            issues = validate_review(parsed, pack)
            tries.append(issues)
            if not issues:
                result, source = parsed, "llm"
                break
            messages += [{"role": "assistant", "content": parsed.model_dump_json()},
                         {"role": "user", "content": "校验未通过，请修正后完整重写：\n" + "\n".join(issues)}]
    if result is None:
        spy = today.get("大盘", {}).get("SPY") if today else None
        result = Review(outcome="没有检索到结果报道" if not web else f"{EXTERNAL}：见来源",
                        reaction=f"SPY 当天 {spy * 100:+.1f}%" if spy is not None else "行情未更新",
                        holdings="见收盘汇总")
    payload = {"review": result.model_dump(), "source": source, "validation": tries, "pack": pack,
               "web_sources": (web or {}).get("sources", [])[:6]}
    await db.execute("INSERT INTO scout.event_notes (event_key, note, payload) VALUES (:k, 'review', CAST(:p AS JSONB))",
                     {"k": e["key"], "p": db.jsonb(payload)})
    return payload


async def run_eod(session: date) -> dict:
    """Reviews of today's 高 events and tomorrow's lines, for the digest."""
    reviews = []
    for e in await due_reviews(session):
        try:
            reviews.append({"event": e, **await review(e, session)})
        except Exception as exc:
            logger.warning("review of %s failed: %s", e["key"], exc)
    nxt = session + timedelta(days=1)
    while not is_us_equity_trading_day(nxt):
        nxt += timedelta(days=1)
    ahead = [e for e in await load(session + timedelta(days=1), nxt) if e["importance"] == "高" or e["source"] != "ff"]
    return {"reviews": reviews, "next": ahead}


def digest_lines(ev: dict) -> list[str]:
    lines = []
    for r in ev.get("reviews", []):
        e, rv = r["event"], r["review"]
        lines += [f"🔎 事件回顾：{e['title']}", f"  结果：{rv['outcome']}", f"  市场：{rv['reaction']}", f"  持仓：{rv['holdings']}"]
    if ev.get("next"):
        lines.append("📅 下一个交易日前的事件：")
        lines += ["  " + x for x in upcoming_lines(ev["next"], date.today(), skip_ff=False)]
    return lines
