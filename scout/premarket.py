"""Pre-market brief ('盘前简报'), weekdays before the US open, written like the after-close digest: an analyst's
note to the account owner (user's request 2026-11-06, after the digest).

Python gathers, labels and ranks everything:
- `key_items`: at most five things for the open, a sell due first, then a holding below its stop, a holding that
  reported, today's high-importance data, a holding reporting today, a large futures move, a holding's unusual
  move, a related company's large reaction;
- each holding at the pre-market price: P&L, stop distance, the move split by beta on the S&P futures, the plan's
  step from the last close, its own earnings (the result against the pre-report consensus and the options-implied
  move, or a preview), related companies' reports, direct news since the last close;
- overnight quotes with the futures move against the normal daily move, today's data (Beijing time, released
  ones marked), high-importance events, prediction markets, the overnight reports, pending opportunities and
  watchlist names moving more than 2 daily sigmas.

The model (`SCOUT_AGENT_MODEL`, one structured call) writes the headline, an overnight sentence, a note per holding
with its news in Chinese against the buy reason (`holding_notes` checks) and up to four other names. Numbers must
be in the pack, beats / misses follow Python's verdicts, no forecasts, no actions beyond the plan; one correction
round, then each part falls back on its own. Stored in scout.briefs (kind 'premarket', payload version 2).
"""
from __future__ import annotations

import json
import logging
import re
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from pydantic import BaseModel, Field

from scout import responses
from scout import context, db, holding_notes, universe
from scout.config import get_scout_settings
from scout.data import bls
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
PEERS = 5                  # per holding: the most traded stocks of its sector, whose earnings move the sector
MAX_NEWS = 8
NEWS_PER_HOLDING = holding_notes.NEWS_PER_HOLDING
KEY_MAX = 5
FUTURES_SIGMAS = 1.5       # a futures move this many daily sigmas of SPY is a key item
HOLDING_SIGMAS = 2.0
NEAR_STOP_SIGMAS = 2.0     # as in the digest: a stop closer than this many daily sigmas
WATCH_SIGMAS = 2.0         # watchlist names moving at least this many of their own daily sigmas (and MOVE_THRESHOLD)
WATCH_MAX = 8
OPP_MAX = 6
RELATED_MOVE = 0.05        # a holding's peer / supply-chain neighbour reacting this much to its report is a key item
REPORTED_SHOWN = 8
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
    return f"距止损 {(1 - stop / price) * 100:.1f}%，距目标 1 {(t1 / price - 1) * 100:.1f}%"


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


REVENUE_INLINE = 0.005         # within ±0.5 % of the estimate counts as in line


def revenue_verdict(actual: float | None, estimate: float | None, source: str) -> str | None:
    """Revenue against the pre-report consensus, decided in Python (the model only repeats it)."""
    if not actual or not estimate:
        return None
    gap = actual / estimate - 1
    if abs(gap) < REVENUE_INLINE:
        return f"基本持平（{source} {_pct(gap)}）"
    return f"{'高于' if gap > 0 else '低于'}预期（{source} {_pct(gap)}）"


async def yahoo_consensus(ticker: str, report_day: date) -> dict:
    """Yahoo's current-quarter EPS and revenue consensus from the research snapshot before the report day (the
    research job runs after the close, so a same-day snapshot of an after-close report has already rolled to the
    next quarter). Only research-pool stocks have snapshots."""
    r = await db.fetch_one("SELECT payload FROM scout.research_snapshots WHERE ticker = :t AND kind = 'estimates' "
                           "AND asof < :d ORDER BY asof DESC LIMIT 1", {"t": ticker, "d": report_day})
    pl = (r or {}).get("payload") or {}
    eps = ((pl.get("earnings_estimate") or {}).get("0q") or {}).get("avg")
    rev = ((pl.get("revenue_estimate") or {}).get("0q") or {}).get("avg")
    return {"eps": float(eps) if eps is not None else None, "revenue": float(rev) if rev else None}


def consensus(payload: dict, yahoo: dict) -> tuple[str, float | None, float | None] | None:
    """(source, EPS estimate, revenue estimate) before the report: Yahoo's snapshot first; Finnhub's calendar only
    when its estimates were kept from before the report day (`context.refresh_earnings`): afterwards they drift
    toward the actuals (2026-11, 23 reports: median 0.25 % off the actual revenue, Yahoo 2.1 %; Finnhub 1.8 % higher)."""
    if yahoo.get("eps") is not None or yahoo.get("revenue"):
        return "Yahoo", yahoo.get("eps"), yahoo.get("revenue")
    if payload.get("estimates_before_report"):
        return "Finnhub", payload.get("eps_estimate"), payload.get("revenue_estimate")
    return None


_REVENUE_BEAT = ("营收超预期", "营收高于预期", "营收好于预期", "收入超预期", "收入高于预期")
_REVENUE_MISS = ("营收不及预期", "营收低于预期", "营收逊于预期", "营收未达预期", "收入不及预期", "收入低于预期")


def revenue_claim_issues(texts: list[str], rows: list[dict]) -> list[str]:
    """A sentence naming a reported company may call its revenue a beat / miss only when `营收对比预期` says so."""
    issues = []
    for row in rows:
        names = {row["代码"]} | ({row["名称"].split()[0]} if row.get("名称") else set())
        verdict = row.get("营收对比预期") or ""
        for text in texts:
            for sent in re.split(r"[。；;\n]", text):
                if not any(n in sent for n in names):
                    continue
                beat, miss = any(c in sent for c in _REVENUE_BEAT), any(c in sent for c in _REVENUE_MISS)
                if (beat and not verdict.startswith("高于")) or (miss and not verdict.startswith("低于")):
                    issues.append(f"{row['代码']} 的营收对比预期是「{verdict or '没有数据'}」，文中却写「{sent.strip()[:40]}」")
    return issues


_EPS_BEAT = ("财报超预期", "业绩超预期", "盈利超预期", "利润超预期", "EPS 超预期", "EPS超预期", "每股收益超预期")
_EPS_MISS = ("财报不及预期", "业绩不及预期", "盈利不及预期", "利润不及预期", "EPS 不及预期", "EPS不及预期", "每股收益不及预期",
             "财报低于预期", "业绩低于预期", "盈利低于预期")


def eps_claim_issues(texts: list[str], rows: list[dict]) -> list[str]:
    """Calling results a beat / miss follows the sign of `EPS 超预期幅度`; such a sentence must name the company by its code
    (2026-10-30: "亚马逊和英特尔财报不及预期" while INTC beat by 30 %; the Chinese names escaped the check)."""
    issues = []
    for text in texts:
        for sent in re.split(r"[。；;\n]", text):
            beat, miss = any(c in sent for c in _EPS_BEAT), any(c in sent for c in _EPS_MISS)
            if not (beat or miss):
                continue
            named = [r for r in rows if r["代码"] in sent or (r.get("名称") and r["名称"].split()[0] in sent)]
            if not named:
                issues.append(f"说财报超预期 / 不及预期时要写明公司代码（如 英特尔（INTC））：「{sent.strip()[:40]}」")
            for r in named:
                s = r.get("EPS 超预期幅度") or ""
                if (beat and not s.startswith("+")) or (miss and not s.startswith("-")):
                    issues.append(f"{r['代码']} 的 EPS 超预期幅度是「{s or '没有财报前预期'}」，文中却写「{sent.strip()[:40]}」"
                                  "（股价下跌常因指引，不等于财报不及预期）")
    return issues


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


LIBRARY_HOURS = 48             # the shared news library keeps 48 h; older articles live in the archive


def time_now() -> float:
    import time

    return time.time()


def _news_source(since: int, cols: str) -> str:
    """The library, plus the archive when `since` reaches past the library's 48 h (after a weekend or holiday)."""
    if since < time_now() - LIBRARY_HOURS * 3600:
        return f"(SELECT {cols} FROM public.ticker_news_library UNION SELECT {cols} FROM public.ticker_news_archive) n"
    return "public.ticker_news_library"


async def _news_since(tickers: list[str], since: int) -> list[dict]:
    cols = "ticker, url, headline, source, relevance, llm_summary, sentiment, credibility, datetime_utc"
    try:
        rows = await db.fetch_all(
            f"SELECT {cols} FROM {_news_source(since, cols)} WHERE ticker = ANY(:t) AND datetime_utc >= :s "
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


async def _direct_news(tickers: list[str], since: int) -> dict[str, list[dict]]:
    """Per ticker: direct articles since `since` that are not neutral or are hard events (as in the digest), hard
    events first, then the newest; at most NEWS_PER_HOLDING."""
    if not tickers:
        return {}
    cols = "ticker, url, headline, llm_summary, sentiment, is_hard_event, datetime_utc, relevance"
    try:
        rows = await db.fetch_all(
            f"SELECT {cols} FROM {_news_source(since, cols)} WHERE ticker = ANY(:t) AND datetime_utc >= :s "
            "AND relevance = 'direct' AND (coalesce(sentiment, 'neutral') <> 'neutral' OR is_hard_event)",
            {"t": tickers, "s": since})
    except Exception:
        return {}
    out: dict[str, dict[str, dict]] = {}
    for r in rows:
        out.setdefault(r["ticker"], {}).setdefault(r["url"] or r["headline"], r)
    return {t: sorted(v.values(), key=lambda r: (not r.get("is_hard_event"), -(r.get("datetime_utc") or 0)))[:NEWS_PER_HOLDING]
            for t, v in out.items()}


async def _macro_since(since: int, limit: int = 6) -> list[dict]:
    try:
        row = await db.fetch_one("SELECT macro_news FROM public.macro_news_cache ORDER BY updated_at DESC LIMIT 1")
    except Exception:
        return []
    items = [i for i in (row or {}).get("macro_news") or [] if (i.get("datetime") or 0) >= since]
    return sorted(items, key=lambda i: i.get("datetime") or 0, reverse=True)[:limit]


def _stamp(ts: int | None) -> str | None:
    return datetime.fromtimestamp(ts, ET).strftime("%m-%d %H:%M") if ts else None


def sigma_day(f: dict | None) -> float | None:
    """Normal daily move from the 63-day volatility."""
    v = (f or {}).get("vol_63")
    return v / 252 ** 0.5 if v else None


def split_text(chg: float | None, beta: float | None, es: float | None) -> str | None:
    """The pre-market move split by the adjusted beta on the S&P futures move."""
    if chg is None or beta is None or es is None:
        return None
    mkt = beta * es
    return f"按 β {beta:.2f} × 标普期货 {_pct(es)} ≈ {_pct(mkt)}，其余 {_pct(chg - mkt)} 是它自己的"


def holding_record(t: dict, q: dict | None, f: dict, es: float | None) -> dict:
    """One open trade at the pre-market price (P&L and stop distance), with the plan's step decided at the last close."""
    from scout import levels, portfolio
    from scout.telegram import next_step

    tk = t["ticker"]
    m0 = portfolio.metrics(t)
    live = bool(q and not q["stale"])
    price = q["last"] if live else m0.get("price")
    m = portfolio.metrics({**t, "last_close": price}) if price else m0
    chg = q["change"] if live else None
    step = next_step(t, m0) if "pnl" in m0 else ""
    sd, beta, core = sigma_day(f), f.get("beta"), levels.is_core(tk)
    th = (t.get("plan") or {}).get("thesis") or {}
    stop = None
    if m.get("to_stop") is not None:
        stop = f"{t['stop']:.2f}（距{'盘前价' if live else '上一收盘'} {_pct(m['to_stop'])}）"
    elif core:
        stop = "没有止损（核心仓位，按大盘过滤管理）"
    row = {"key": f"pos.{tk}", "代码": tk, "名称": t.get("name"), "股数": f"{m.get('remaining', 0):g}", "成本": f"{t['entry_price']:.2f}",
           "上一收盘": f"{m0['price']:.2f}" if m0.get("price") else None,
           **({"盘前价": f"{price:.2f}", "盘前较前收": _pct(chg)} if live else {"盘前": "暂无成交"}),
           "盘前拆分": split_text(chg, beta, es) if live else None,
           "日常波动": f"±{sd * 100:.1f}%（按 63 日波动）" if sd else None,
           "持仓盈亏": f"{_pct(m['pnl_pct'])}（{m['pnl']:+,.0f} 美元）" if m.get("pnl") is not None else None,
           "止损": stop, "计划提示": step or "按计划持有",
           "买入理由": th.get("reason") or t.get("note") or holding_notes.NO_REASON,
           "买入理由的证伪条件": th.get("falsification")}
    below = bool(live and t.get("stop") and not core and price <= t["stop"] and not step.startswith("⚠️"))
    return {"ticker": tk, "row": {k: v for k, v in row.items() if v not in (None, "")}, "m": m, "price": price, "chg": chg,
            "step": step, "sd": sd, "core": core, "stop": t.get("stop"), "below": below,
            "value": (price or 0) * m.get("remaining", 0)}


def earn_summary(e: dict) -> str:
    """One line for a reported earnings row (the verdicts are Python's)."""
    if e.get("EPS 实际") == "数据待更新":
        s = f"{e['代码']} {e['时间']} 已发布，结果数据还没更新" + (f"（EPS 预期 {e['EPS 预期']}）" if e.get("EPS 预期") else "")
        s += f"；盘前 {e['盘前较前收']}" if e.get("盘前较前收") else ""
        return s + (f"，财报前期权定价 {e['财报前期权定价']}" if e.get("财报前期权定价") else "")
    s = f"{e['代码']} {e['时间']} EPS {e['EPS 实际']}"
    s += f" vs 预期 {e['EPS 预期']}" if e.get("EPS 预期") else "（无财报前预期）"
    s += f"（{e['EPS 超预期幅度']}）" if e.get("EPS 超预期幅度") else ""
    s += f"，营收{e['营收对比预期']}" if e.get("营收对比预期") else ""
    s += f"；盘前 {e['盘前较前收']}" if e.get("盘前较前收") else ""
    s += f"，财报前期权定价 {e['财报前期权定价']}" if e.get("财报前期权定价") else ""
    return s


async def implied_move(ticker: str, report_day: date, hour: str, prev: date) -> dict | None:
    """The straddle-implied move from the last options snapshot before the report (an after-close report's
    same-day expiry has already expired)."""
    from scout import research_data

    snap = await research_data.latest(ticker, "options", prev)
    if not snap or snap.get("stale"):
        return None
    return holding_notes.options_move(snap.get("payload"), report_day + timedelta(days=1) if hour == "amc" else report_day)


def econ_text(e: dict) -> str:
    """One data release: the official value against the forecast once out, else the forecast."""
    head = f"北京时间 {e['北京时间'][-5:]} {e['事件']}"
    if e.get("实际"):
        vs = "，".join(x for x in (f"预期 {e['预期']}" if e.get("预期") else None, e.get("对比预期"),
                                  f"前值 {e['前值']}" if e.get("前值") else None) if x)
        return f"{head} 公布 {e['实际']}" + (f"（{vs}）" if vs else "")
    return head + (f"（预期 {e['预期']}）" if e.get("预期") else "") + ("，已公布，数值未取到" if e.get("已公布") else "")


def key_items(hs: list[dict], es: dict | None, econ_today: list[dict], related: list[dict]) -> list[str]:
    """At most KEY_MAX lines, the most consequential first: a sell due at the open, a holding below its stop, a
    holding that reported, today's high-importance data, a holding reporting today, stops within two daily sigmas,
    a large futures move, a holding's unusual move, a related company's large reaction."""
    out: list[tuple[int, str]] = []
    near = []
    for h in hs:
        tk, m = h["ticker"], h["m"]
        if h["step"].startswith("⚠️"):
            out.append((0, f"{tk}：{h['step'][2:].strip()}"))
        if h["below"]:
            out.append((1, f"{tk} 盘前 {h['price']:.2f}，低于回落止损 {h['stop']:.2f}：止损按收盘判断，今天收盘仍低于它，"
                           "就在下一个交易日开盘卖出"))
        r = h.get("reported")
        if r:
            text = f"你的持仓 {tk} {r['时间']}发布财报：" + earn_summary(r).split(" ", 2)[2].removeprefix("已发布，")
            if m.get("to_stop") is not None:
                text += f"；止损在 {_pct(m['to_stop'])}"
            out.append((2, text))
        p = h.get("preview")
        if p and p["days"] <= 1:
            bits = [f"{tk} {'今天' if p['days'] == 0 else '明天'}发财报"]
            if p.get("options"):
                bits.append(f"期权定价 ±{p['options']['move'] * 100:.1f}%")
            if p.get("gaps"):
                bits.append(f"过去最大跌幅 {_pct(p['gaps']['max_down'])}")
            if m.get("to_stop") is not None:
                bits.append(f"止损在 {_pct(m['to_stop'])}")
            out.append((4, "，".join(bits)))
        if (m.get("to_stop") is not None and m["to_stop"] < 0 and h["sd"] and -m["to_stop"] < NEAR_STOP_SIGMAS * h["sd"]
                and not h["below"]):
            near.append(f"{tk} {_pct(m['to_stop'])}")
        if h["chg"] is not None and h["sd"] and abs(h["chg"]) >= HOLDING_SIGMAS * h["sd"] and not r:
            out.append((7, f"{tk} 盘前 {_pct(h['chg'])}，约 {abs(h['chg']) / h['sd']:.1f} 倍日常波动"
                           + (f"；{h['row']['盘前拆分']}" if h["row"].get("盘前拆分") else "")))
    if near:
        out.append((5, f"{'、'.join(near)} 离止损的距离不到 {NEAR_STOP_SIGMAS:g} 倍日常波动，平常的波动就可能碰到（止损按收盘判断）"))
    high = [e for e in econ_today if e["重要性"] == "高"]
    if high:
        text = "今天的高重要性数据：" + "；".join(econ_text(e) for e in high)
        if any(e.get("已公布") for e in high):
            text += "。已公布的结果已经反映在盘前价格里"
        out.append((3, text))
    if es and es.get("sigmas") is not None and es["sigmas"] >= FUTURES_SIGMAS:
        out.append((6, f"标普期货 {_pct(es['change'])}，约 {es['sigmas']:.1f} 倍日常波动"))
    for r in related:
        out.append((8, f"{r['代码']}（{r['身份']}）财报后盘前 {r['盘前较前收']}"))
    out.sort(key=lambda x: x[0])
    return [t for _, t in out[:KEY_MAX]]


async def build_pack(session: date, now: datetime | None = None) -> dict:
    from scout import earnings_gaps, learned, portfolio

    now = now or datetime.now(ET)
    prev = previous_us_equity_trading_day(session)
    since = int(datetime.combine(prev, time(16, 0), ET).timestamp())
    watch = {r["ticker"]: r.get("theme") for r in await universe.load_watchlist()}
    held = await portfolio.trades(open_only=True)
    held_set = {t["ticker"] for t in held}
    quiet = learned.held_back(await learned.current())     # cards of rules the knowledge base rejects: tracked, not shown
    opps = [o for o in await db.fetch_all(
        "SELECT ticker, status, primary_detector, entry_lo, entry_hi, stop, current_stop, target1, target2, asof "
        "FROM scout.opportunities WHERE status = 'new' ORDER BY score DESC") if o["primary_detector"] not in quiet]
    major = await _tickers_ranked_by_liquidity()
    earnings = await db.fetch_all(
        "SELECT ticker, event_date, payload FROM scout.market_events WHERE event_type = 'earnings' "
        "AND event_date BETWEEN :a AND :b", {"a": prev, "b": session + timedelta(days=6)},
    )
    # sector peers of each holding with earnings in the window: a bellwether's report moves the whole sector
    sector_of = {r["ticker"]: r["sector"] for r in await db.fetch_all(
        "SELECT ticker, sector FROM scout.instruments WHERE kind = 'stock' AND sector IS NOT NULL")}
    peers: dict[str, list[str]] = {}
    for h in held:
        s = sector_of.get(h["ticker"])
        for t in sorted((t for t in major if s and sector_of.get(t) == s and t != h["ticker"]), key=major.get)[:PEERS]:
            peers.setdefault(t, []).append(h["ticker"])
    peers = {t: hs for t, hs in peers.items() if any(e["ticker"] == t for e in earnings)}
    # supply-chain neighbours named in annual reports (customers, suppliers, competitors), with earnings in the window
    chain: dict[str, list[str]] = {}
    try:
        from scout import company_profile

        for h in held:
            for t, rels in (await company_profile.neighbours(h["ticker"])).items():
                if any(e["ticker"] == t for e in earnings):
                    chain.setdefault(t, []).append(f"{h['ticker']} 的{'、'.join(r.split('（')[0] for r in rels)}")
    except Exception as exc:                # profiles are optional context
        logger.warning("supply-chain neighbours unavailable: %s", exc)
    focus = set(watch) | {o["ticker"] for o in opps} | held_set | set(peers) | set(chain)

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
    feats = {r["ticker"]: r["payload"] or {} for r in await db.fetch_all(
        "SELECT ticker, payload FROM scout.features_daily WHERE date = (SELECT max(date) FROM scout.features_daily WHERE date <= :p) "
        "AND ticker = ANY(:t)", {"p": prev, "t": sorted(shown | {"SPY"})})}

    def ident(t: str) -> str:
        tags = [lbl for cond, lbl in ((t in held_set, "持仓"), (t in {o["ticker"] for o in opps}, "待触发机会"),
                                      (t in watch, "自选"),
                                      (t in peers, f"持仓同板块（{'、'.join(sorted(set(peers.get(t, []))))}）"),
                                      (t in chain, f"持仓产业链（{'；'.join(chain.get(t, []))}）")) if cond]
        return "、".join(tags) or "大市值"

    def pm(t: str) -> dict:
        q = quotes.get(t)
        if not q or q["stale"]:
            return {}
        return {"盘前价": f"{q['last']:.2f}", "盘前较前收": _pct(q["change"])}

    yahoo = {e["ticker"]: await yahoo_consensus(e["ticker"], e["event_date"]) for e in reported}
    implied = {}
    for e in reported:
        try:
            implied[e["ticker"]] = await implied_move(e["ticker"], e["event_date"], (e["payload"] or {}).get("hour") or "", prev)
        except Exception as exc:          # the options snapshot is context; it must not cost the brief
            logger.warning("implied move %s failed: %s", e["ticker"], exc)

    def earn_row(prefix: str, e: dict, when: str) -> dict:
        p = e["payload"] or {}
        row = {"key": f"{prefix}.{e['ticker']}", "代码": e["ticker"], "名称": names.get(e["ticker"]), "身份": ident(e["ticker"]),
               "时间": when}
        if prefix != "er":
            row["EPS 预期"] = _eps(p.get("eps_estimate"))
            if p.get("revenue_estimate"):
                row["营收预期"] = _revenue(p["revenue_estimate"])
            return row
        row.update({"EPS 实际": _eps(p.get("eps_actual")) if p.get("eps_actual") is not None else "数据待更新",
                    **({"营收实际": _revenue(p.get("revenue_actual"))} if p.get("revenue_actual") else {}), **pm(e["ticker"])})
        mv = implied.get(e["ticker"])
        q = quotes.get(e["ticker"])
        if mv:
            row["财报前期权定价"] = f"±{mv['move'] * 100:.1f}%（到 {mv['expiry'][5:]} 到期）"
            if q and not q["stale"] and q["change"] is not None:
                row["盘前反应 / 期权定价"] = f"{abs(q['change']) / mv['move']:.1f} 倍"
        pre = consensus(p, yahoo.get(e["ticker"], {}))
        if not pre:
            row["预期"] = "没有财报前的一致预期，不判断超预期与否"
            return row
        src, eps_est, rev_est = pre
        row["预期来源"] = f"{src}（财报前）"
        row["EPS 预期"] = _eps(eps_est)
        surprise = eps_surprise(p.get("eps_actual"), eps_est)
        if surprise is not None:
            row["EPS 超预期幅度"] = _pct(surprise)
        if rev_est:
            row["营收预期"] = _revenue(rev_est)
        verdict = revenue_verdict(p.get("revenue_actual"), rev_est, src)
        if verdict:
            row["营收对比预期"] = verdict
        return row

    er_rows = [earn_row("er", e, "昨日盘后" if e["event_date"] == prev else "今日盘前") for e in reported]
    es_q = quotes.get("ES=F")
    spy_sd = sigma_day(feats.get("SPY"))
    es = None
    if es_q and not es_q["stale"] and es_q["change"] is not None:
        es = {"change": es_q["change"], "sigmas": abs(es_q["change"]) / spy_sd if spy_sd else None}
    overnight = [quote_row(s, n, quotes.get(s)) for s, n in OVERNIGHT]
    if es and es["sigmas"] is not None:
        overnight[0]["相对日常波动"] = f"约 {es['sigmas']:.1f} 倍（SPY 日常波动 ±{spy_sd * 100:.1f}%）"

    # holdings: attention first (a step due, below the stop, earnings), then by value
    news = await _direct_news(sorted(held_set | set(watch)), since)
    hs = [holding_record(t, quotes.get(t["ticker"]), feats.get(t["ticker"], {}), es and es["change"]) for t in held]
    er_by = {r["代码"]: r for r in er_rows}
    for h in hs:
        tk, row = h["ticker"], h["row"]
        if tk in er_by:
            h["reported"] = er_by[tk]
            row["财报"] = earn_summary(er_by[tk])
        nxt = [e for e in today + week if e["ticker"] == tk]
        if nxt and (nxt[0]["event_date"] - session).days <= holding_notes.EARNINGS_SOON:
            try:
                p = await holding_notes.earnings_preview(tk, nxt[0]["event_date"], prev, h["m"].get("to_stop"), await earnings_gaps.history(tk))
                p["days"] = (nxt[0]["event_date"] - session).days
                h["preview"] = p
                row["财报前瞻"] = holding_notes.preview_lines(p)
            except Exception as exc:
                logger.warning("earnings preview %s failed: %s", tk, exc)
        rel = [r for r in er_rows if tk in r["身份"] and r["代码"] != tk]
        if rel:
            row["相关财报"] = [f"{earn_summary(r)}（{r['身份']}）" for r in rel]
        row["news"] = [{"index": i, "标题": n.get("headline"), "摘要": (n.get("llm_summary") or "")[:400],
                        "情绪（新闻库）": n.get("sentiment"), "硬事件": bool(n.get("is_hard_event"))}
                       for i, n in enumerate(news.get(tk, []))]
    hs.sort(key=lambda h: (not (h["step"].startswith("⚠️") or h["below"] or h.get("reported") or h.get("preview")), -h["value"]))
    related = [r for r in er_rows if r["代码"] not in held_set and ("持仓同板块" in r["身份"] or "持仓产业链" in r["身份"])
               and abs((quotes.get(r["代码"]) or {}).get("change") or 0) >= RELATED_MOVE]

    opp_rows, seen = [], set()
    for o in opps:
        q = quotes.get(o["ticker"])
        if o["ticker"] in seen or o["ticker"] in held_set or not q or q["stale"]:
            continue
        seen.add(o["ticker"])
        opp_rows.append({"key": f"opp.{o['ticker']}", "代码": o["ticker"], "名称": names.get(o["ticker"]), "规则": o["primary_detector"],
                         "入场区间": f"{o['entry_lo']:.2f}–{o['entry_hi']:.2f}", "止损": f"{(o.get('current_stop') or o['stop']):.2f}",
                         "目标 1": f"{o['target1']:.2f}", "盘前价": f"{q['last']:.2f}", "盘前较前收": _pct(q["change"]),
                         "位置判断": position_label(q["last"], o)})
    opp_rows = opp_rows[:OPP_MAX]
    moves = []
    for t in watch:
        q, sd = quotes.get(t), sigma_day(feats.get(t))
        if t in held_set or not q or q["stale"] or q["change"] is None:
            continue
        if abs(q["change"]) >= max(MOVE_THRESHOLD, WATCH_SIGMAS * (sd or 0)):
            moves.append((t, q, abs(q["change"]) / sd if sd else None))
    moves.sort(key=lambda x: -(x[2] or 0))
    econ = await context.economic_calendar()
    released = now.strftime("%H:%M")

    def is_out(e: dict) -> bool:
        return e["date"] == session.isoformat() and e["time_et"] < released and now.date() == session

    out_bls = sorted({bls.RELEASES[e["title"]][0] for e in econ if is_out(e) and e["title"] in bls.RELEASES})
    official = await bls.monthly(out_bls, session.year - 2, session.year) if out_bls else {}

    def econ_row(key: str, e: dict) -> dict:
        row = {"key": key, "事件": e["title_zh"], "日期": e["date"], "美东时间": e["time_et"], "北京时间": e["time_bj"],
               "重要性": e["impact"], **({"预期": e["forecast"]} if e["forecast"] else {}),
               **({"前值": e["previous"]} if e["previous"] else {})}
        if is_out(e):
            value = bls.actual(e["title"], official, session)
            if value:
                row.update({"实际": value, "已公布": "已公布（在本简报之前），实际值来自 BLS 官方数据"},
                           **({"对比预期": bls.versus(value, e["forecast"])} if bls.versus(value, e["forecast"]) else {}))
            else:
                row["已公布"] = "已公布（在本简报之前），数值未取到，结果见宏观新闻"
        return row

    econ_today = [econ_row(f"econ_{i + 1}", e) for i, e in enumerate(e for e in econ if e["date"] == session.isoformat())]
    econ_later = [e for e in econ if e["date"] > session.isoformat() and e["impact"] == "高"]
    other_news = await _news_since(sorted(shown - held_set), since)
    macro = await _macro_since(since)
    from scout import event_calendar, prediction

    upcoming = await event_calendar.load(session, session + timedelta(days=7))
    calendar = [e for e in upcoming if e["source"] != "ff" and e["importance"] == "高"]
    odds = []
    for e in upcoming:
        if e["importance"] == "高":
            odds += [{**x, "事件": e["title"]} for x in prediction.pack_rows(await prediction.board(link=e["key"]))]
    pack = {
        "version": 2, "session": session.isoformat(), "weekday": f"周{WEEKDAYS[session.weekday()]}", "prev_session": prev.isoformat(),
        "overnight": overnight,
        "positions": [h["row"] for h in hs],
        "opportunities": opp_rows,
        "watchlist_moves": [{"key": f"pm.{t}", "代码": t, "名称": names.get(t), "主题": watch.get(t), "盘前价": f"{q['last']:.2f}",
                             "盘前较前收": _pct(q["change"]), **({"相对日常波动": f"约 {k:.1f} 倍"} if k else {}),
                             **({"新闻": [(n.get("headline") or "")[:160] for n in news.get(t, [])[:2]]} if news.get(t) else {})}
                            for t, q, k in moves[:WATCH_MAX]],
        "earnings_reported": er_rows,
        "earnings_today": [earn_row("et", e, HOUR_ZH.get((e["payload"] or {}).get("hour"), "时间未定")) for e in today],
        "earnings_week": [earn_row("ew", e, f"{e['event_date']:%m-%d} 周{WEEKDAYS[e['event_date'].weekday()]} "
                                            f"{HOUR_ZH.get((e['payload'] or {}).get('hour'), '')}".strip()) for e in week],
        "economic_today": econ_today,
        "economic_week": [econ_row(f"econw_{i + 1}", e) for i, e in enumerate(econ_later[:6])],
        "news": [{"key": f"news_{i + 1}", "标题": n["headline"][:200], "来源": n["source"], "时间（美东）": _stamp(n["datetime_utc"]),
                  "相关股票": sorted(n["tickers"])[:4], "情绪": n["sentiment"],
                  **({"摘要": n["llm_summary"][:200]} if n.get("llm_summary") else {})} for i, n in enumerate(other_news)],
        "macro_news": [{"key": f"macro_{i + 1}", "标题": (m.get("headline") or "")[:200], "来源": m.get("source"),
                        "时间（美东）": _stamp(m.get("datetime"))} for i, m in enumerate(macro)],
        "calendar": [{"key": f"cal_{i + 1}", **event_calendar.event_row(e)} for i, e in enumerate(calendar)],
        "prediction": odds,
    }
    pack["key_items"] = key_items(hs, es, econ_today, related)
    return pack


def pack_tickers(pack: dict) -> set[str]:
    return {row["代码"] for part in ("positions", "opportunities", "watchlist_moves", "earnings_reported", "earnings_today", "earnings_week")
            for row in pack.get(part, [])} | {t for n in pack["news"] for t in n["相关股票"]}


# ── The model's prose ──

class WatchNote(BaseModel):
    ticker: str
    note: str = Field(description="1 句：今天开盘该看它什么")


class Notes(BaseModel):
    headline: str = Field(description="一句话概括今天开盘前的局面，不超过 40 字")
    overnight: str = Field(description="隔夜市场 1–2 句：期货、利率、美元、原油和宏观新闻说明了什么")
    holdings: list[holding_notes.HoldingNote]
    watch: list[WatchNote] = Field(description="0–4 只持仓以外今天值得看的股票（财报、自选异动、待触发机会）")


PROMPT = """你是一位专业的股票分析师，在美股开盘前给自己的客户（一位个人投资者）写盘前简报。证据包由 Python 整理，数字都已算好：
- key_items：Python 按轻重排好的今天最重要的事（已经会原样放在简报开头，你不必重复，但 headline 要和它一致）。
- positions：客户的持仓，按盘前价算的盈亏和离止损的距离、盘前涨跌按 β 拆出的大盘部分、日常波动、系统按客户计划给出的提示、
  客户记录的买入理由、自己的财报（已公布的结果或即将公布的前瞻）、相关公司的财报、上一收盘以来的直接新闻（英文）。
- overnight：股指期货、VIX、利率、美元、原油、黄金、比特币相对上一收盘（"说明"表示今天尚未交易）。
- earnings_reported / earnings_today / earnings_week：已公布、今天待公布、本周后续的财报；身份里有"持仓同板块（X）"或"持仓产业链（X 的…）"的会影响持仓 X。
- economic_today / economic_week / calendar / prediction：今天和本周的数据、事件、预测市场价格。已公布的数据有"实际"（BLS 官方数值）和"对比预期"时照这两项写，不要自己判断高于或低于预期；没有"实际"的只说已公布、数值未取到。
- opportunities（Scout 规则给出、还没触发的买点）、watchlist_moves（自选股里盘前涨跌超过 2 倍日常波动的）、news / macro_news。

要写的：
1. headline：一句话（不超过 40 字），今天开盘前最该知道的局面。
2. overnight：1–2 句，隔夜市场说明了什么（风险偏好、利率和美元是否一致、宏观新闻的主线）；引用宏观新闻时只写标题说了的。
3. holdings：对 positions 里的每只持仓：
   - status：一句白话（不超过 90 个字），今天开盘前这只持仓最值得知道的一点（例如按计划要卖、离止损多近、财报结果和盘前反应、
     盘前涨跌主要是大盘还是它自己）。像对客户说话，最多 2–3 个关键数字。
   - news：对它 news 列表里的每条（用 index 对应）写一句中文概括（zh）、对公司的影响 impact（利好 / 利空 / 中性）、
     和买入理由的关系 vs_reason（支持 / 削弱 / 无关；买入理由是"未记录"时一律填"没有记录理由"），以及一句为什么（why）。
4. watch：0–4 只持仓以外今天值得看的股票，每只一句：已公布财报的按结果和盘前反应说，待触发机会按"位置判断"说，自选异动按它的新闻说（没有新闻就说没有找到原因）。

硬性规则：
- 数字只能原样引用证据包里出现的数值，不要自己计算或换算。持仓的 status 只能用这只持仓那一行里的数字。
- 不预测涨跌：不要写"将上涨""有望反弹""会继续"之类；期货和盘前价格不代表开盘价。
- 不给计划以外的操作建议：不要写加仓、减仓、抄底、止盈、建议买入或卖出。只有"计划提示"写了卖出的持仓，才可以复述"按你的计划卖出"。
- 财报的营收是否超预期只能按"营收对比预期"表述；EPS 只能按"EPS 超预期幅度"表述（"财报超预期 / 不及预期"指 EPS），说的时候写明代码，
  如 英特尔（INTC）。财报后下跌不等于不及预期，原因多半在指引，要按新闻说明，新闻里没有就说没有找到原因。
- 待触发机会的位置只能按"位置判断"表述，不要自己推断是否追高或跌破止损。
- vs_reason 只有新闻内容直接涉及买入理由本身才填支持或削弱；只是同方向的泛泛消息填"无关"。新闻概括只写原文说了的。
- 涨跌数字自带正负号：写"亏损 28.1%"或"持仓 -28.1%"，不要写"亏损 -28.1%""下跌 -4.4%"这种双重否定。
- 只有 calendar 里"来源"是据外部报道的事件才写"据外部报道"；news / macro_news 是新闻库的文章，直接概括，不加这个前缀。
- 中文、专业、简洁；非投资建议。"""

_POSITION_CLAIMS = ("跌破止损", "追高", "入场区间内", "目标 1")
HEADLINE_MAX = 50


def validate_text(n: Notes, pack: dict) -> list[str]:
    """The headline, the overnight sentence and the watch notes (the holdings are checked by holding_notes.validate)."""
    issues = []
    texts = [n.headline, n.overnight, *(w.note for w in n.watch)]
    bad = unsupported_numbers("\n".join(texts), allowed_numbers(pack))
    if bad:
        issues.append(f"以下数字不在证据包中：{', '.join(sorted(set(bad)))}")
    if len(n.headline) > HEADLINE_MAX:
        issues.append(f"headline 太长（{len(n.headline)} 字）")
    if len(n.watch) > 4:
        issues.append("watch 最多 4 只")
    stray = sorted({w.ticker for w in n.watch} - pack_tickers(pack))
    if stray:
        issues.append(f"watch 只能来自证据包里的股票：{', '.join(stray)} 不在其中")
    held = {r["代码"] for r in pack.get("positions", [])}
    dup = sorted({w.ticker for w in n.watch} & held)
    if dup:
        issues.append(f"watch 不要放持仓（{', '.join(dup)}），持仓写在 holdings 里")
    labels = {o["代码"]: o["位置判断"] for o in pack["opportunities"]}
    for w in n.watch:
        if w.ticker in labels:
            wrong = [c for c in _POSITION_CLAIMS if c in w.note and c not in labels[w.ticker]]
            if wrong:
                issues.append(f"{w.ticker} 的位置判断是「{labels[w.ticker]}」，文中却写「{'、'.join(wrong)}」")
    joined = "\n".join(texts)
    words = [w for w in holding_notes.FORECAST if w in joined]
    if words:
        issues.append(f"不要预测涨跌：{'、'.join(words)}")
    acts = [w for w in holding_notes.ACTIONS if w in joined]
    if acts:
        issues.append(f"不要给操作建议：{'、'.join(acts)}")
    doubled = holding_notes.DOUBLE_NEG.findall(joined)
    if doubled:
        issues.append(f"有双重否定：{'、'.join(doubled)}")
    all_texts = texts + [h.status for h in n.holdings]
    issues += revenue_claim_issues(all_texts, pack.get("earnings_reported", []))
    issues += eps_claim_issues(all_texts, pack.get("earnings_reported", []))
    return issues


def validate(n: Notes, pack: dict) -> list[str]:
    return validate_text(n, pack) + holding_notes.validate(holding_notes.Notes(holdings=n.holdings), {"holdings": pack["positions"]})


def fallback_headline(pack: dict) -> str:
    es = next((r for r in pack["overnight"] if r["key"] == "q.ES=F"), {})
    return f"标普期货 {es.get('较前收', es.get('最新', '—'))}"


async def write(pack: dict) -> dict:
    """{"brief": {headline, overnight, watch}, "notes": {ticker: note}, "source", "validation"}; each part falls back on
    its own (rule headline, no overnight sentence, no watch notes; holdings keep their rule-based lines)."""
    from scout.viewdesk import strict_schema

    settings = get_scout_settings()
    rule = {"headline": fallback_headline(pack), "overnight": "", "watch": []}
    if not settings.openai_api_key:
        return {"brief": rule, "notes": {}, "source": "rules", "validation": []}
    messages = [{"role": "user", "content": json.dumps(pack, ensure_ascii=False, default=str)}]
    tries, parsed = [], None
    for _ in (1, 2):
        try:
            data = await responses.create({
                "model": settings.scout_agent_model, "reasoning": {"effort": "low"}, "instructions": PROMPT, "input": messages,
                "text": {"format": {"type": "json_schema", "name": "premarket", "schema": strict_schema(Notes), "strict": True}}},
                timeout=240, purpose="premarket")
            parsed = Notes.model_validate_json(responses.output_text(data))
        except Exception as exc:
            logger.warning("premarket LLM call failed: %s", exc)
            tries.append([f"LLM 调用失败：{type(exc).__name__}"])
            break
        issues = validate(parsed, pack)
        tries.append(issues)
        if not issues:
            return {"brief": {"headline": parsed.headline, "overnight": parsed.overnight, "watch": [w.model_dump() for w in parsed.watch]},
                    "notes": {h.ticker: h.model_dump() for h in parsed.holdings}, "source": "llm", "validation": tries}
        messages += [{"role": "assistant", "content": parsed.model_dump_json()},
                     {"role": "user", "content": "校验未通过，请修正后完整重写：\n" + "\n".join(issues)}]
    if parsed is None:
        return {"brief": rule, "notes": {}, "source": "rules", "validation": tries}
    text_ok = not validate_text(parsed, pack)
    brief = ({"headline": parsed.headline, "overnight": parsed.overnight, "watch": [w.model_dump() for w in parsed.watch]}
             if text_ok else rule)
    notes = holding_notes.accepted(holding_notes.Notes(holdings=parsed.holdings), {"holdings": pack["positions"]})
    return {"brief": brief, "notes": notes, "source": "partial" if text_ok or notes else "rules", "validation": tries}


# ── Text ──

GLOSSARY = ("术语：β = 大盘涨跌 1% 时它平均涨跌多少；日常波动 = 按最近 63 日波动，一天的正常涨跌幅度；"
            "期权定价 = 平值跨式期权价格隐含的涨跌幅；回落止损 = 收盘低于买入以来最高收盘的 80% 就在下一个交易日开盘卖出。")


def overnight_line(pack: dict) -> str:
    parts = []
    for r in pack["overnight"]:
        if "较前收" in r:
            parts.append(f"{r['名称']} {r['较前收']}" if r["key"] != "q.^TNX" else f"{r['名称']} {r['最新']}（{r['较前收']}）")
        elif r.get("最新") not in (None, "无数据"):
            parts.append(f"{r['名称']} {r['最新']}（上一交易日）")
    return " · ".join(parts)


def holding_lines(row: dict, note: dict | None) -> list[str]:
    tk = row["代码"]
    head = f"• {tk} " + (f"盘前 {row['盘前价']}（{row['盘前较前收']}）" if row.get("盘前价") else f"盘前暂无成交（上一收盘 {row.get('上一收盘', '—')}）")
    head += f" · 持仓 {row['持仓盈亏']}" if row.get("持仓盈亏") else ""
    if row.get("止损"):
        head += f" · {row['止损']}" if row["止损"].startswith("没有") else f" · 止损 {row['止损']}"
    out = [head]
    if note and note.get("status"):
        out.append(f"  💬 {note['status']}")
    if row.get("盘前拆分"):
        out.append(f"  📈 {row['盘前拆分']}" + (f"（日常波动 {row['日常波动'].split('（')[0]}）" if row.get("日常波动") else ""))
    if row.get("计划提示", "").startswith("⚠️"):
        out.append(f"  {row['计划提示']}")
    if row.get("财报"):
        out.append(f"  📣 财报：{row['财报']}")
    out += [f"  {x}" for x in row.get("财报前瞻", [])]
    out += [f"  🔗 相关财报：{x}" for x in row.get("相关财报", [])]
    raw = [{"headline": n["标题"], "sentiment": n.get("情绪（新闻库）")} for n in row.get("news", [])]
    out += [f"  {x}" for x in holding_notes.news_lines(note, raw)]
    return out


def render_text(pack: dict, brief: dict, notes: dict) -> str:
    lines = [f"【盘前简报 · {pack['session']} {pack['weekday']}】{brief['headline']}"]
    if pack.get("key_items"):
        lines += ["", "今天开盘前最重要的："] + [f"{i}. {x}" for i, x in enumerate(pack["key_items"], 1)]
    if pack.get("positions"):
        lines += ["", "一、我的持仓（按盘前价）"]
        for row in pack["positions"]:
            lines += holding_lines(row, notes.get(row["代码"]))
    lines += ["", "二、隔夜市场", f"隔夜：{overnight_line(pack)}"]
    es = next((r for r in pack["overnight"] if r["key"] == "q.ES=F"), {})
    if es.get("相对日常波动"):
        lines.append(f"标普期货的变化{es['相对日常波动']}")
    if brief.get("overnight"):
        lines.append(brief["overnight"])
    lines += ["", "三、今天的日程（北京时间 / 美东时间）"]
    lines += [f"· {e['北京时间'][-5:]} / {e['美东时间']} [{e['重要性']}] {e['事件']}" + (f" 预期 {e['预期']}" if e.get("预期") else "")
              + (f" 前值 {e['前值']}" if e.get("前值") else "")
              + (f" 公布 {e['实际']}" + (f"（{e['对比预期']}）" if e.get("对比预期") else "") if e.get("实际")
                 else "（已公布，数值未取到）" if e.get("已公布") else "")
              for e in pack["economic_today"]] or ["· 今天没有重要经济数据"]

    def tag(e: dict) -> str:
        return "" if e["身份"] == "大市值" else f"（{e['身份']}）"

    if pack["earnings_today"]:
        lines.append("· 今天待公布财报：" + "、".join(f"{e['代码']} {e['时间']}{tag(e)}" for e in pack["earnings_today"]))
    for e in pack.get("calendar", []):
        lines.append(f"· {e['日期'][5:]}" + (f" 美东 {e['美东时间']}" if e.get("美东时间") else "") + f" [高] {e['标题']}（{e['类别']}）"
                     + ("（据外部报道）" if e["来源"].startswith("据外部报道") else ""))
    for x in pack.get("prediction", []):
        lines.append(f"· 预测市场：{x['事件']}：" + "；".join(f"{o['结果']} {o['现在']}" for o in x["各结果概率"])
                     + f"（{x['来源']}，成交 {x['成交量']}；市场价格，不是 Scout 的判断）")
    later = [f"{e['日期'][5:]} {e['事件']}" for e in pack["economic_week"]]
    later += [f"{e['代码']} {e['时间']}财报{tag(e)}" for e in pack["earnings_week"] if e["身份"] != "大市值"]
    if later:
        lines.append("本周后续：" + "；".join(later))
    if pack["earnings_reported"]:
        lines += ["", "四、隔夜财报（营收和 EPS 的判断对照财报前的一致预期）"]
        rows = pack["earnings_reported"]
        lines += [f"· {earn_summary(e)}{tag(e)}" for e in rows[:REPORTED_SHOWN]]
        if len(rows) > REPORTED_SHOWN:
            lines.append(f"· 另有 {len(rows) - REPORTED_SHOWN} 家：" + "、".join(e["代码"] for e in rows[REPORTED_SHOWN:]))
    notes_w = brief.get("watch") or []
    if pack["opportunities"] or pack["watchlist_moves"] or notes_w:
        lines += ["", "五、机会与自选"]
        if pack["opportunities"]:
            lines.append("待触发机会（Scout 规则给出的买点，还没成交）：")
            lines += [f"· {o['代码']} 盘前 {o['盘前价']}（{o['盘前较前收']}）{o['位置判断']}" for o in pack["opportunities"]]
        if pack["watchlist_moves"]:
            lines.append("自选股盘前异动（超过 2 倍日常波动）：" + "、".join(
                f"{m['代码']} {m['盘前较前收']}" + (f"（{m['相对日常波动']}）" if m.get("相对日常波动") else "") for m in pack["watchlist_moves"]))
        lines += [f"💬 {w['ticker']}：{w['note']}" for w in notes_w]
    lines += ["", GLOSSARY, "（期货和盘前价格不代表开盘价；非投资建议）"]
    return "\n".join(lines)


async def generate(session: date) -> dict:
    pack = await build_pack(session)
    res = await write(pack)
    settings = get_scout_settings()
    record = {"version": 2, "source": res["source"], "model": settings.scout_agent_model if res["source"] != "rules" else None,
              "brief": res["brief"], "notes": res["notes"], "pack": pack, "validation": res["validation"]}
    await db.execute(
        "INSERT INTO scout.briefs (kind, asof, content, payload) VALUES ('premarket', :d, :c, CAST(:p AS JSONB))",
        {"d": session, "c": render_text(pack, res["brief"], res["notes"]), "p": db.jsonb(record)},
    )
    return record


async def latest() -> dict | None:
    row = await db.fetch_one(
        "SELECT asof, content, payload, created_at FROM scout.briefs WHERE kind = 'premarket' ORDER BY id DESC LIMIT 1")
    return {**row["payload"], "content": row["content"], "created_at": row["created_at"], "asof": row["asof"]} if row else None


SOURCE_NOTE = {"llm": "", "partial": "\n（部分 AI 点评未通过校验，这些部分按规则整理）", "rules": "\n（AI 点评未通过校验，按规则整理）"}


# ── Job ──

RUN_WINDOW_ET = (time(8, 45), time(10, 0))   # the cron fires at 13:00 and 14:00 UTC; only the one near 9:00 ET runs


def due(now: datetime) -> bool:
    """True inside the run window in New York time, so one of the two UTC firings runs in summer and winter time."""
    t = now.astimezone(ZoneInfo("America/New_York")).time()
    return RUN_WINDOW_ET[0] <= t < RUN_WINDOW_ET[1]


async def run(push: bool = True, force: bool = False) -> dict:
    if not force and not due(datetime.now(ZoneInfo("UTC"))):
        return {"skipped": "outside 8:45–10:00 New York time (the other UTC firing runs)"}
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
    from scout import event_calendar

    try:                                # the calendar feeds the brief; a failure must not cost the brief
        summary["calendar"] = await event_calendar.refresh(session)
    except Exception as exc:
        summary["calendar"] = f"failed: {type(exc).__name__}"
    try:                                # prediction markets: link to the calendar / topics / holdings, snapshot
        from scout import prediction

        summary["prediction"] = await prediction.run(session, "premarket")
    except Exception as exc:
        summary["prediction"] = f"failed: {type(exc).__name__}"
    record = await generate(session)
    summary.update(source=record["source"], holdings=len(record["pack"]["positions"]), key_items=len(record["pack"]["key_items"]),
                   economic=len(record["pack"]["economic_today"]), earnings=len(record["pack"]["earnings_reported"]))
    if push:
        from scout import telegram

        content = render_text(record["pack"], record["brief"], record["notes"])
        await telegram.send(telegram.esc(content) + SOURCE_NOTE.get(record["source"], ""))
        summary["pushed"] = True
    try:
        summary["previews"] = await event_calendar.send_previews(session, push=push)
    except Exception as exc:
        summary["previews"] = f"failed: {type(exc).__name__}"
    return summary
