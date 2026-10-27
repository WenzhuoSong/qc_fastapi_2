"""Pre-market brief ('盘前简报'), weekdays before the US open.

Python gathers and labels everything: overnight futures and macro quotes,
pre-market prices of the watchlist and open opportunities (and where each
opportunity now sits against its entry / stop / targets), earnings reported
since the last close and due today, today's economic calendar, and overnight
news. One LLM call writes the headline, key points and per-ticker notes,
citing evidence keys; the same number / citation / label checks as
scout.market_focus apply, with a deterministic fallback. Stored in
scout.briefs (kind 'premarket').
"""
from __future__ import annotations

import json
import logging
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from pydantic import BaseModel, Field

from scout import context, db, universe
from scout.config import get_scout_settings
from scout.data.yahoo import latest_quotes
from scout.market_calendar import (is_us_equity_trading_day, previous_us_equity_trading_day,
                                   us_equity_market_status)
from scout.thesis import allowed_numbers, unsupported_numbers

logger = logging.getLogger("scout.premarket")

ET = ZoneInfo("America/New_York")
OVERNIGHT = (("ES=F", "标普期货"), ("NQ=F", "纳指期货"), ("RTY=F", "罗素期货"), ("^VIX", "VIX"),
             ("^TNX", "10 年美债收益率"), ("DX-Y.NYB", "美元指数"), ("CL=F", "原油"), ("GC=F", "黄金"),
             ("BTC-USD", "比特币"))
MAJOR_STOCKS = 150        # earnings of the most traded stocks are market events
MOVE_THRESHOLD = 0.02     # pre-market move worth listing for watchlist names
MAX_EARNINGS = 12
MAX_NEWS = 8
WEEKDAYS = "一二三四五六日"
HOUR_ZH = {"bmo": "盘前", "amc": "盘后", "dmh": "盘中"}


# ── Python labels ──

def position_label(price: float, o: dict) -> str:
    """Where a pre-market price sits against an opportunity's plan."""
    stop = o.get("current_stop") or o["stop"]
    lo, hi, t1 = o["entry_lo"], o["entry_hi"], o["target1"]
    if o["status"] == "new":
        if price <= stop:
            return f"盘前已跌破止损位 {stop:.2f}，入场前提可能失效"
        if price < lo:
            return f"低于入场区间（{lo:.2f}–{hi:.2f}）"
        if price <= hi:
            return f"在入场区间内（{lo:.2f}–{hi:.2f}）"
        if price >= t1:
            return f"已高于目标 1（{t1:.2f}），不宜追高"
        return f"高于入场上沿 {hi:.2f} 约 {(price / hi - 1) * 100:.1f}%，追高风险"
    if price <= stop:
        return f"盘前已跌破止损 {stop:.2f}"
    if price >= t1:
        return f"已到目标 1（{t1:.2f}）附近或以上"
    return f"距止损 {(price / stop - 1) * 100:.1f}%，距目标 1 {(t1 / price - 1) * 100:.1f}%"


def holding_label(price: float, t: dict, session: date | None = None) -> str:
    """Where a pre-market price sits against the user's own trade (and its locked plan's time stop)."""
    pnl = price / t["entry_price"] - 1
    text = f"较你的成本 {pnl * 100:+.1f}%"
    if t.get("stop"):
        text += f"，盘前已跌破止损（持仓止损 {t['stop']:.2f}）" if price <= t["stop"] else f"，距持仓止损 {(price / t['stop'] - 1) * 100:.1f}%"
    if t.get("target") and price >= t["target"]:
        text += f"，已到止盈位附近（{t['target']:.2f}），按计划分批卖出（建议股数见 /trades）"
    plan = t.get("plan") or {}
    if session and plan.get("time_stop_days") and not (plan.get("target1") and price >= plan["target1"]):
        from scout.intraday import sessions_since

        held = sessions_since(t["opened_on"], session)
        if held >= plan["time_stop_days"]:
            text += f"，持有 {held} 个交易日仍未到目标 1，已到计划的时间止损"
    return text


def _pct(x: float | None, digits: int = 1) -> str:
    return "—" if x is None else f"{x * 100:+.{digits}f}%"


def quote_row(sym: str, name: str, q: dict | None) -> dict:
    if not q:
        return {"key": f"q.{sym}", "名称": name, "最新": "无数据"}
    row = {"key": f"q.{sym}", "名称": name, "最新": f"{q['last']:.2f}" + ("%" if sym == "^TNX" else ""), "时间（美东）": q["time_et"]}
    if q["stale"]:
        row["说明"] = "今天尚未交易，为上一交易日报价"
    elif sym == "^TNX" and q["prev_close"]:
        row["较前收"] = f"{(q['last'] - q['prev_close']) * 100:+.1f}bp"
    else:
        row["较前收"] = _pct(q["change"], 2)
    return row


def eps_surprise(actual: float | None, estimate: float | None) -> float | None:
    if actual is None or estimate in (None, 0):
        return None
    return (actual - estimate) / abs(estimate)


def _eps(x) -> str:
    return "—" if x is None else f"{x:.2f}"


def _revenue(x) -> str | None:
    return None if not x else f"{x / 1e8:.1f} 亿美元"


# ── Evidence pack ──

async def _tickers_ranked_by_liquidity() -> dict[str, int]:
    rows = await db.fetch_all(
        "SELECT f.ticker FROM scout.features_daily f JOIN scout.instruments i USING (ticker) "
        "WHERE f.date = (SELECT max(date) FROM scout.features_daily) AND i.kind = 'stock' "
        "ORDER BY (f.payload->>'dollar_vol20')::float DESC NULLS LAST LIMIT :n", {"n": MAJOR_STOCKS},
    )
    return {r["ticker"]: i for i, r in enumerate(rows)}


async def _news_since(tickers: list[str], since: int) -> list[dict]:
    try:
        rows = await db.fetch_all(
            "SELECT ticker, url, headline, source, relevance, llm_summary, sentiment, credibility, datetime_utc "
            "FROM public.ticker_news_library WHERE ticker = ANY(:t) AND datetime_utc >= :s "
            "AND coalesce(relevance, '') <> 'not_relevant'", {"t": tickers, "s": since},
        )
    except Exception:
        return []
    by_url: dict[str, dict] = {}
    for r in rows:
        item = by_url.setdefault(r["url"] or r["headline"], {**r, "tickers": set()})
        item["tickers"].add(r["ticker"])
    rank = {"direct": 2, "indirect": 1}
    items = sorted(by_url.values(), key=lambda i: (rank.get(i["relevance"], 0), i["credibility"] or 0, i["datetime_utc"] or 0),
                   reverse=True)
    return items[:MAX_NEWS]


async def _macro_since(since: int, limit: int = 6) -> list[dict]:
    try:
        row = await db.fetch_one("SELECT macro_news FROM public.macro_news_cache ORDER BY updated_at DESC LIMIT 1")
    except Exception:
        return []
    items = [i for i in (row or {}).get("macro_news") or [] if (i.get("datetime") or 0) >= since]
    return sorted(items, key=lambda i: i.get("datetime") or 0, reverse=True)[:limit]


def _stamp(ts: int | None) -> str | None:
    return datetime.fromtimestamp(ts, ET).strftime("%m-%d %H:%M") if ts else None


async def build_pack(session: date) -> dict:
    prev = previous_us_equity_trading_day(session)
    since = int(datetime.combine(prev, time(16, 0), ET).timestamp())
    watch = {r["ticker"]: r.get("theme") for r in await universe.load_watchlist()}
    opps = await db.fetch_all(
        "SELECT ticker, status, primary_detector, entry_lo, entry_hi, stop, current_stop, target1, target2, asof "
        "FROM scout.opportunities WHERE status IN ('new', 'triggered') ORDER BY score DESC")
    major = await _tickers_ranked_by_liquidity()
    earnings = await db.fetch_all(
        "SELECT ticker, event_date, payload FROM scout.market_events WHERE event_type = 'earnings' "
        "AND event_date BETWEEN :a AND :b", {"a": prev, "b": session + timedelta(days=6)},
    )
    held = await db.fetch_all("SELECT ticker, entry_price, shares, stop, target, opened_on, plan FROM scout.trades WHERE closed_on IS NULL")
    focus = set(watch) | {o["ticker"] for o in opps} | {t["ticker"] for t in held}

    def relevant(t: str) -> bool:
        return t in focus or t in major

    def order(t: str) -> tuple:
        return (t not in focus, major.get(t, MAJOR_STOCKS))

    reported, today, week = [], [], []
    for e in earnings:
        t, d, p = e["ticker"], e["event_date"], e["payload"] or {}
        if not relevant(t):
            continue
        hour = p.get("hour") or ""
        if (d == prev and hour == "amc") or (d == session and hour == "bmo"):
            reported.append(e)
        elif d == session:
            today.append(e)
        elif d > session:
            week.append(e)
    reported.sort(key=lambda e: order(e["ticker"]))
    today.sort(key=lambda e: order(e["ticker"]))
    week.sort(key=lambda e: (e["event_date"], order(e["ticker"])))
    reported, today, week = reported[:MAX_EARNINGS], today[:MAX_EARNINGS], week[:MAX_EARNINGS]

    shown = focus | {e["ticker"] for e in reported + today}
    quotes = await latest_quotes([s for s, _ in OVERNIGHT] + sorted(shown), session)
    names = {r["ticker"]: r["name"] for r in await db.fetch_all(
        "SELECT ticker, name FROM scout.instruments WHERE ticker = ANY(:t)", {"t": sorted(shown | {e['ticker'] for e in week})})}

    def ident(t: str) -> str:
        tags = [lbl for cond, lbl in ((t in {h["ticker"] for h in held}, "持仓"), (t in {o["ticker"] for o in opps}, "活跃机会"),
                                      (t in watch, "自选")) if cond]
        return "、".join(tags) or "大市值"

    def pm(t: str) -> dict:
        q = quotes.get(t)
        if not q or q["stale"]:
            return {}
        return {"盘前价": f"{q['last']:.2f}", "盘前较前收": _pct(q["change"])}

    def earn_row(prefix: str, e: dict, when: str) -> dict:
        p = e["payload"] or {}
        row = {"key": f"{prefix}.{e['ticker']}", "代码": e["ticker"], "名称": names.get(e["ticker"]), "身份": ident(e["ticker"]),
               "时间": when, "EPS 预期": _eps(p.get("eps_estimate"))}
        if prefix == "er":
            surprise = eps_surprise(p.get("eps_actual"), p.get("eps_estimate"))
            row.update({"EPS 实际": _eps(p.get("eps_actual")) if p.get("eps_actual") is not None else "数据待更新",
                        **({"EPS 超预期幅度": _pct(surprise)} if surprise is not None else {}),
                        **({"营收实际": _revenue(p.get("revenue_actual"))} if p.get("revenue_actual") else {}), **pm(e["ticker"])})
        if p.get("revenue_estimate"):
            row["营收预期"] = _revenue(p["revenue_estimate"])
        return row

    opp_rows = []
    for o in opps:
        q = quotes.get(o["ticker"])
        row = {"key": f"opp.{o['ticker']}", "代码": o["ticker"], "规则": o["primary_detector"],
               "状态": "待触发" if o["status"] == "new" else "已触发", "入场区间": f"{o['entry_lo']:.2f}–{o['entry_hi']:.2f}",
               "止损": f"{(o.get('current_stop') or o['stop']):.2f}", "目标 1": f"{o['target1']:.2f}"}
        if q and not q["stale"]:
            row.update({"盘前价": f"{q['last']:.2f}", "盘前较前收": _pct(q["change"]), "位置判断": position_label(q["last"], o)})
        else:
            row["位置判断"] = "盘前暂无成交"
        opp_rows.append(row)

    holdings = []
    for t in held:
        q = quotes.get(t["ticker"])
        row = {"key": f"pos.{t['ticker']}", "代码": t["ticker"], "成本": f"{t['entry_price']:.2f}", "股数": f"{t['shares']:g}",
               **({"止损": f"{t['stop']:.2f}"} if t.get("stop") else {})}
        if q and not q["stale"]:
            row.update({"盘前价": f"{q['last']:.2f}", "盘前较前收": _pct(q["change"]), "位置判断": holding_label(q["last"], t, session)})
        else:
            row["位置判断"] = "盘前暂无成交"
        holdings.append(row)
    moves = sorted(((t, quotes[t]) for t in watch if t in quotes and not quotes[t]["stale"]
                    and abs(quotes[t]["change"] or 0) >= MOVE_THRESHOLD), key=lambda x: -abs(x[1]["change"]))
    econ = await context.economic_calendar()
    econ_today = [e for e in econ if e["date"] == session.isoformat()]
    econ_later = [e for e in econ if e["date"] > session.isoformat() and e["impact"] == "高"]
    news = await _news_since(sorted(shown), since)
    macro = await _macro_since(since)
    focus_rec = await db.fetch_one(
        "SELECT asof, payload->'focus' AS f FROM scout.briefs WHERE kind = 'market_focus' ORDER BY id DESC LIMIT 1")

    def econ_row(key: str, e: dict) -> dict:
        return {"key": key, "事件": e["title_zh"], "日期": e["date"], "美东时间": e["time_et"], "北京时间": e["time_bj"],
                "重要性": e["impact"], **({"预期": e["forecast"]} if e["forecast"] else {}),
                **({"前值": e["previous"]} if e["previous"] else {})}

    return {
        "session": session.isoformat(), "weekday": f"周{WEEKDAYS[session.weekday()]}", "prev_session": prev.isoformat(),
        "overnight": [quote_row(s, n, quotes.get(s)) for s, n in OVERNIGHT],
        "positions": holdings,
        "opportunities": opp_rows,
        "watchlist_moves": [{"key": f"pm.{t}", "代码": t, "名称": names.get(t), "主题": watch.get(t),
                             "盘前价": f"{q['last']:.2f}", "盘前较前收": _pct(q["change"])} for t, q in moves],
        "earnings_reported": [earn_row("er", e, "昨日盘后" if e["event_date"] == prev else "今日盘前") for e in reported],
        "earnings_today": [earn_row("et", e, HOUR_ZH.get((e["payload"] or {}).get("hour"), "时间未定")) for e in today],
        "earnings_week": [earn_row("ew", e, f"{e['event_date']:%m-%d} 周{WEEKDAYS[e['event_date'].weekday()]} "
                                            f"{HOUR_ZH.get((e['payload'] or {}).get('hour'), '')}".strip()) for e in week],
        "economic_today": [econ_row(f"econ_{i + 1}", e) for i, e in enumerate(econ_today)],
        "economic_week": [econ_row(f"econw_{i + 1}", e) for i, e in enumerate(econ_later[:6])],
        "news": [{"key": f"news_{i + 1}", "标题": n["headline"][:200], "来源": n["source"], "时间（美东）": _stamp(n["datetime_utc"]),
                  "相关股票": sorted(n["tickers"])[:4], "情绪": n["sentiment"],
                  **({"摘要": n["llm_summary"][:200]} if n.get("llm_summary") else {})} for i, n in enumerate(news)],
        "macro_news": [{"key": f"macro_{i + 1}", "标题": (m.get("headline") or "")[:200], "来源": m.get("source"),
                        "时间（美东）": _stamp(m.get("datetime"))} for i, m in enumerate(macro)],
        "last_focus": ({"key": "focus", "日期": str(focus_rec["asof"]), "结论": (focus_rec["f"] or {}).get("headline"),
                        "主题": [t["title"] for t in (focus_rec["f"] or {}).get("themes", [])]} if focus_rec else None),
    }


def evidence_keys(pack: dict) -> set[str]:
    keys = {"focus"} if pack.get("last_focus") else set()
    for part in ("overnight", "positions", "opportunities", "watchlist_moves", "earnings_reported", "earnings_today", "earnings_week",
                 "economic_today", "economic_week", "news", "macro_news"):
        keys |= {row["key"] for row in pack.get(part, [])}
    return keys


def pack_tickers(pack: dict) -> set[str]:
    return {row["代码"] for part in ("positions", "opportunities", "watchlist_moves", "earnings_reported", "earnings_today", "earnings_week")
            for row in pack.get(part, [])} | {t for n in pack["news"] for t in n["相关股票"]}


# ── LLM brief ──

class Point(BaseModel):
    text: str = Field(description="1–2 句")
    evidence: list[str] = Field(description="引用的证据 key")


class TickerNote(BaseModel):
    ticker: str
    note: str = Field(description="1 句：今天该看什么")
    evidence: list[str]


class PremarketBrief(BaseModel):
    headline: str = Field(description="一句话概括今天开盘前的局面，≤ 40 字")
    overnight: str = Field(description="隔夜期货与宏观资产的变化，1–2 句")
    points: list[Point] = Field(description="2–5 个今天最值得注意的点，按重要性排序")
    tickers: list[TickerNote] = Field(description="0–6 只今天值得盯的股票（活跃机会、自选、重要财报）")
    risks: list[str] = Field(description="今天的主要风险，1–3 条")


SYSTEM_PROMPT = """你是一名美股交易日开盘前的策略助理，基于 Python 整理好的证据包写一份盘前简报，帮助投资者在开盘前快速知道"今天要看什么"。

证据包：
- overnight：股指期货、VIX、10 年美债收益率、美元、原油、黄金、比特币相对上一交易日收盘的变化（"说明"表示今天尚未交易）。
- positions：用户自己记录的持仓（成本、止损、盘前价和"位置判断"），最重要，跌破止损的必须提到。
- opportunities：Scout 正在跟踪的机会，含入场区间、止损、目标和"位置判断"（Python 根据盘前价计算）。
- watchlist_moves：自选股中盘前涨跌幅较大的。
- earnings_reported / earnings_today / earnings_week：昨日盘后与今日盘前已公布、今日待公布、本周后续的重要财报。
- economic_today / economic_week：今天和本周后续的美国经济数据与美联储事件（美东时间）。
- news / macro_news：上一交易日收盘以来的个股新闻和宏观新闻。
- last_focus：昨晚收盘后生成的市场关注点。

工作方法：
1. 先判断开盘前的整体风险偏好：期货方向、VIX、利率和美元是否一致。
2. 找出今天的事件驱动：重要数据的公布时间、财报（已公布的超预期/不及预期与盘前反应、今天盘后待公布的）。
3. 对活跃机会，按"位置判断"说明开盘时要注意什么：高于入场上沿提示不要追高；低于入场区间提示等价格回到区间再看，不要猜反弹；
   跌破止损提示计划可能失效；"盘前暂无成交"的不必提。
4. 把新闻和价格对上：哪些盘前异动有新闻解释，哪些没有。

硬性规则：
- 数字只能原样引用证据包中出现的数值，不要自行计算。
- evidence 只能填证据包里出现的 key（如 q.ES=F、opp.NVDA、er.MSFT、econ_1、news_2、focus）。
- 机会的位置只能按"位置判断"表述，不要自己推断是否跌破止损或追高。
- 期货和盘前价格不代表开盘价；不编造新闻或数据；没有数据的项不要提。
- 中文、简洁；不给买卖指令，非投资建议。"""

_POSITION_CLAIMS = ("跌破止损", "追高", "入场区间内", "目标 1")


def validate(b: PremarketBrief, pack: dict) -> list[str]:
    issues = []
    texts = [b.headline, b.overnight, *b.risks, *(p.text for p in b.points), *(n.note for n in b.tickers)]
    bad = unsupported_numbers("\n".join(texts), allowed_numbers(pack))
    if bad:
        issues.append(f"以下数字不在证据包中：{', '.join(sorted(set(bad)))}")
    cited = {k for p in b.points for k in p.evidence} | {k for n in b.tickers for k in n.evidence}
    unknown = sorted(cited - evidence_keys(pack))
    if unknown:
        issues.append(f"引用了不存在的证据 key：{', '.join(unknown[:10])}")
    stray = sorted({n.ticker for n in b.tickers} - pack_tickers(pack))
    if stray:
        issues.append(f"个股关注只能来自证据包里的股票：{', '.join(stray)} 不在其中")
    labels = {o["代码"]: o["位置判断"] for o in pack["opportunities"] + pack.get("positions", [])}
    for n in b.tickers:
        if n.ticker in labels:
            wrong = [c for c in _POSITION_CLAIMS if c in n.note and c not in labels[n.ticker]]
            if wrong:
                issues.append(f"{n.ticker} 的位置判断是「{labels[n.ticker]}」，文中却写「{'、'.join(wrong)}」")
    if not 2 <= len(b.points) <= 5:
        issues.append("要点数量必须在 2–5 个之间")
    return issues


def fallback(pack: dict) -> PremarketBrief:
    """Deterministic version: the Python sections speak for themselves."""
    es = next((r for r in pack["overnight"] if r["key"] == "q.ES=F"), {})
    points = []
    if pack["economic_today"]:
        points.append(Point(text="今日数据：" + "、".join(e["事件"] for e in pack["economic_today"]),
                            evidence=[e["key"] for e in pack["economic_today"]]))
    if pack["earnings_reported"] or pack["earnings_today"]:
        points.append(Point(text="财报：见下方列表", evidence=[e["key"] for e in pack["earnings_reported"] + pack["earnings_today"]]))
    while len(points) < 2:
        points.append(Point(text="见下方各项", evidence=[]))
    return PremarketBrief(headline=f"盘前：标普期货 {es.get('较前收', es.get('最新', '—'))}", overnight="见隔夜行情",
                          points=points, tickers=[], risks=["AI 简报未通过校验，以上为规则整理"])


async def generate(session: date) -> dict:
    pack = await build_pack(session)
    settings = get_scout_settings()
    brief, source, history = None, "fallback", []
    if settings.openai_api_key:
        from openai import AsyncOpenAI

        client = AsyncOpenAI(api_key=settings.openai_api_key, timeout=120)
        messages = [{"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": json.dumps(pack, ensure_ascii=False, default=str)}]
        for attempt in (1, 2):
            try:
                resp = await client.beta.chat.completions.parse(
                    model=settings.scout_insight_model, messages=messages, response_format=PremarketBrief, temperature=0.2)
                parsed = resp.choices[0].message.parsed
            except Exception as exc:
                logger.warning("premarket LLM call failed: %s", exc)
                history.append({"attempt": attempt, "issues": [f"LLM 调用失败：{type(exc).__name__}"]})
                break
            issues = validate(parsed, pack)
            history.append({"attempt": attempt, "issues": issues})
            if not issues:
                brief, source = parsed, "llm"
                break
            messages += [{"role": "assistant", "content": parsed.model_dump_json()},
                         {"role": "user", "content": "校验未通过，请修正后完整重写：\n" + "\n".join(issues)}]
    if brief is None:
        brief = fallback(pack)
    record = {"source": source, "model": settings.scout_insight_model if source == "llm" else None,
              "brief": brief.model_dump(), "pack": pack, "validation": {"passed": source == "llm", "history": history}}
    await db.execute(
        "INSERT INTO scout.briefs (kind, asof, content, payload) VALUES ('premarket', :d, :c, CAST(:p AS JSONB))",
        {"d": session, "c": render_text(brief, pack), "p": db.jsonb(record)},
    )
    return record


async def latest() -> dict | None:
    row = await db.fetch_one(
        "SELECT asof, content, payload, created_at FROM scout.briefs WHERE kind = 'premarket' ORDER BY id DESC LIMIT 1")
    return {**row["payload"], "content": row["content"], "created_at": row["created_at"], "asof": row["asof"]} if row else None


def overnight_line(pack: dict) -> str:
    parts = []
    for r in pack["overnight"]:
        if "较前收" in r:
            parts.append(f"{r['名称']} {r['较前收']}" if r["key"] != "q.^TNX" else f"{r['名称']} {r['最新']}（{r['较前收']}）")
        elif r.get("最新") not in (None, "无数据"):
            parts.append(f"{r['名称']} {r['最新']}（上一交易日）")
    return " · ".join(parts)


def render_text(b: PremarketBrief, pack: dict) -> str:
    lines = [f"【盘前简报 · {pack['session']} {pack['weekday']}】{b.headline}", f"隔夜：{overnight_line(pack)}", b.overnight, ""]
    lines += [f"{i}. {p.text}" for i, p in enumerate(b.points, 1)]
    if pack["economic_today"] or pack["economic_week"]:
        lines += ["", "今日数据（美东 / 北京）："]
        lines += [f"· {e['美东时间']} / {e['北京时间'][-5:]} [{e['重要性']}] {e['事件']}"
                  + (f" 预期 {e['预期']}" if e.get("预期") else "") + (f" 前值 {e['前值']}" if e.get("前值") else "")
                  for e in pack["economic_today"]] or ["· 今天没有重要数据"]
        if pack["economic_week"]:
            lines.append("本周后续：" + "；".join(f"{e['日期'][5:]} {e['美东时间']} {e['事件']}" for e in pack["economic_week"]))
    if pack["earnings_reported"]:
        lines += ["", "已公布财报："]
        for e in pack["earnings_reported"]:
            line = f"· {e['代码']} {e['时间']} EPS {e['EPS 实际']} vs 预期 {e['EPS 预期']}"
            line += f"（{e['EPS 超预期幅度']}）" if e.get("EPS 超预期幅度") else ""
            line += f"，盘前 {e['盘前较前收']}" if e.get("盘前较前收") else ""
            lines.append(line)
    if pack["earnings_today"]:
        lines.append("今日待公布：" + "、".join(f"{e['代码']}（{e['时间']}）" for e in pack["earnings_today"]))
    if pack["earnings_week"]:
        lines.append("本周后续财报：" + "、".join(f"{e['代码']} {e['时间']}" for e in pack["earnings_week"]))
    if pack.get("positions"):
        lines += ["", "你的持仓："]
        lines += [f"· {o['代码']} {o.get('盘前价', '')} {o['位置判断']}".replace("  ", " ") for o in pack["positions"]]
    if pack["opportunities"]:
        lines += ["", "活跃机会盘前位置："]
        lines += [f"· {o['代码']}（{o['状态']}）{o.get('盘前价', '')} {o['位置判断']}".replace("  ", " ") for o in pack["opportunities"]]
    if pack["watchlist_moves"]:
        lines.append("自选股盘前异动：" + "、".join(f"{m['代码']} {m['盘前较前收']}" for m in pack["watchlist_moves"]))
    if b.tickers:
        lines += ["", "个股关注："] + [f"· {n.ticker}：{n.note}" for n in b.tickers]
    lines += ["", "风险：" + "；".join(b.risks), "（盘前价格不代表开盘价；非投资建议）"]
    return "\n".join(lines)


# ── Job ──

async def run(push: bool = True, force: bool = False) -> dict:
    status = us_equity_market_status()
    session = date.fromisoformat(status.market_date)
    if not is_us_equity_trading_day(session) and not force:
        return {"skipped": status.reason}
    done = await db.fetch_one("SELECT 1 AS x FROM scout.briefs WHERE kind = 'premarket' AND asof = :d", {"d": session})
    if done and not force:
        return {"skipped": "already generated", "session": session.isoformat()}
    summary: dict = {"session": session.isoformat()}
    try:
        summary["earnings_rows"] = await context.refresh_earnings(days_back=3, days_ahead=7)
    except Exception as exc:            # stale earnings are better than no brief
        summary["earnings_rows"] = f"failed: {type(exc).__name__}"
    record = await generate(session)
    summary.update(source=record["source"], opportunities=len(record["pack"]["opportunities"]),
                   economic=len(record["pack"]["economic_today"]), earnings=len(record["pack"]["earnings_reported"]))
    if push:
        from scout import telegram

        content = render_text(PremarketBrief(**record["brief"]), record["pack"])
        note = "" if record["source"] == "llm" else "\n（AI 简报未通过校验，要点为规则整理）"
        await telegram.send(telegram.esc(content) + note)
        summary["pushed"] = True
    return summary
