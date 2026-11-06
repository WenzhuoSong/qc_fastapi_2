"""Intraday monitor (redesigned on the user's request 2026-11-06): what changed during the session for the user's money.

Every 15 minutes while the market is open, on Yahoo quotes (a few minutes late):
- market: SPY moving as much as `market_day` calls a big day (2 daily sigmas or 2.5 %), again at twice that: the indices,
  VIX, the sectors leading and lagging, the holdings' P&L today and its β part, and where the market filter stands (it
  decides only at a month-end close, so the alert gives the close this month must hold); core ETFs are covered here only;
- each other holding as one block: a move of at least its bar (1.5 daily sigmas or 5 %, never below 2 %), again at 2×
  and 3× the bar; the price below the system stop; new direct news that is not neutral or is a hard event (question
  headlines are opinion pieces and are dropped; at most NEWS_PER_DAY a day). The block gives the
  move, today's dollar P&L, the position's P&L, the stop distance, peers (competitors named in the 10-K first, else the
  most traded stocks of its industry), the sector ETF, market × β, which part carries the move, a cause search when it
  is the company's own, the news, and the plan's line (stops act on closes);
- buy ideas: a live rule's card entering its zone, only while the plan allows a buy (market filter on, satellite room);
- watchlist names and cards: hard-event news only, at most OTHER_NEWS_PER_DAY a day.
News is triaged in one SCOUT_PARSE_MODEL call: event or commentary (only events are pushed; without the model the rule
filter alone decides), with a Chinese headline (one whose numbers do not stand in the original is dropped for the original). Each event
is claimed once (scout.alerts). Alerts never change an opportunity's status: the after-close tracker settles fills and
exits with the same simulator the replay uses.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import statistics
import time
from datetime import date, datetime
from zoneinfo import ZoneInfo

from pydantic import BaseModel

from scout import context, db, universe
from scout.data.yahoo import latest_quotes
from scout.market_calendar import is_us_equity_trading_day, us_equity_market_status

logger = logging.getLogger("scout.intraday")
ET = ZoneInfo("America/New_York")
NEWS_LOOKBACK_S = 3 * 3600        # a missed run (or a slow news cron) is caught by the next one
HOLD_SIGMAS = 1.5                 # a holding moving this many of its own daily sigmas (63-day vol) is reported, with context,
HOLD_ABS_MOVE = 0.05              # or this much whatever its volatility (a 5 % day is news to the holder),
HOLD_MIN_MOVE = 0.02              # never below this
HOLD_LEVELS = 3                   # again at 2× and 3× the bar
MARKET_LEVELS = 2
NEWS_PER_DAY = 3                  # per holding
OTHER_NEWS_PER_DAY = 1            # per watchlist name or card (the same story comes from several outlets)
PEERS = 4
SHARE = 0.6                       # a part carrying this share of the move (same sign) names the cause
MAX_CAUSE_SEARCHES = 3
INDICES = (("SPY", "标普 500"), ("QQQ", "纳指 100"), ("IWM", "罗素 2000"))
SECTORS_SHOWN = 2


def _pct(x: float | None, d: int = 1) -> str:
    return "—" if x is None else f"{x * 100:+.{d}f}%"


def _usd(x: float) -> str:
    return f"{'+' if x >= 0 else '−'}${abs(x):,.0f}"


# ── thresholds ──

def hold_bar(sd: float) -> float:
    return max(min(HOLD_SIGMAS * sd, HOLD_ABS_MOVE), HOLD_MIN_MOVE)


def market_bar(sd: float | None) -> float:
    from scout import market_day

    return min(market_day.MIN_MOVE, market_day.SIGMAS * sd) if sd else market_day.MIN_MOVE


def level(change: float, bar: float, top: int) -> int:
    """How many bars the move has crossed (0 = none), at most `top`."""
    return min(top, int(abs(change) / bar + 1e-9)) if bar > 0 else 0


def move_verdict(change: float, beta: float | None, spy: float | None, peers: list[float]) -> str:
    """Which part carries the holding's move: its industry (peers' median), the market (β × SPY), or itself."""
    if peers:
        med = statistics.median(peers)
        if med * change > 0 and abs(med) >= SHARE * abs(change):
            return "行业"
    if beta is not None and spy is not None and beta * spy * change > 0 and abs(beta * spy) >= SHARE * abs(change):
        return "大盘"
    return "公司自身"


def sessions_since(start: date, end: date) -> int:
    """Trading sessions after `start` up to and including `end`."""
    n, d = 0, start
    while d < end:
        d = date.fromordinal(d.toordinal() + 1)
        n += is_us_equity_trading_day(d)
    return n


# ── holdings ──

def position(t: dict, q: dict, f: dict) -> dict:
    """One open trade at the live price: today's move and dollar P&L, the position's P&L, the stop and the plan's line."""
    from scout import levels, portfolio
    from scout.telegram import next_step

    m0 = portfolio.metrics(t)
    price, chg = q["last"], q["change"]
    m = portfolio.metrics({**t, "last_close": price})
    sd = (f.get("vol_63") or 0) / 252 ** 0.5 or None
    rem = m.get("remaining") or 0
    today = rem * price * chg / (1 + chg) if chg is not None else None
    step = next_step(t, m0) if "pnl" in m0 else ""
    stop = t.get("stop")
    below = bool(stop and not levels.is_core(t["ticker"]) and price <= stop)
    if step.startswith("⚠️"):
        plan = step
    elif below:
        plan = f"止损按收盘判断：收盘仍低于 {stop:.2f}，下一个交易日开盘卖出全部；收回止损上方则继续持有"
    elif m.get("to_stop") is not None:
        n = f"，约 {-m['to_stop'] / sd:.1f} 倍日常波动" if sd else ""
        plan = f"距止损 {stop:.2f} 还有 {-m['to_stop'] * 100:.1f}%{n}；止损按收盘判断，盘中不需要操作"
    else:
        plan = "按计划持有，盘中不需要操作"
    return {"ticker": t["ticker"], "name": t.get("name"), "price": price, "change": chg, "time_et": q["time_et"], "sd": sd,
            "sigmas": abs(chg) / sd if sd and chg is not None else None, "today": today, "pnl": m.get("pnl"),
            "pnl_pct": m.get("pnl_pct"), "stop": stop, "below": below, "plan": plan, "beta": f.get("beta"),
            "value_prev": rem * price / (1 + chg) if chg is not None else None, "core": levels.is_core(t["ticker"])}


async def peers_of(movers: list[str]) -> dict[str, tuple[list[str], str]]:
    """Per ticker: up to PEERS peers and where they come from — competitors its 10-K (or theirs) names, else the most
    traded stocks of its industry."""
    from scout import company_profile, premarket

    inst = {r["ticker"]: r for r in await db.fetch_all(
        "SELECT ticker, industry FROM scout.instruments WHERE ticker = ANY(:t)", {"t": movers})}
    rank = await premarket._tickers_ranked_by_liquidity()
    out = {}
    for t in movers:
        try:
            named = [l["target"] for l in await company_profile.links(t) if l["relation"] == "competitor" and l["target"] != t]
        except Exception:
            named = []
        if len(named) >= 2:
            out[t] = (named[:PEERS], "年报列的竞争对手")
            continue
        ind = (inst.get(t) or {}).get("industry")
        same = [r["ticker"] for r in await db.fetch_all(
            "SELECT ticker FROM scout.instruments WHERE industry = :i AND ticker <> :t", {"i": ind, "t": t})] if ind else []
        out[t] = ((named + sorted((x for x in same if x in rank and x not in named), key=lambda x: rank[x]))[:PEERS], "同行业")
    return out


async def move_context(movers: list[dict], quotes: dict, today: date) -> None:
    """Peers, sector ETF, the industry's live metrics and the market for each moving holding, in place, with the verdict:
    the comparison a desk makes before reading any news."""
    from scout import industry_kpis

    if not movers:
        return
    tickers = [p["ticker"] for p in movers]
    inst = {r["ticker"]: r for r in await db.fetch_all(
        "SELECT ticker, sector, industry FROM scout.instruments WHERE ticker = ANY(:t)", {"t": tickers})}
    peers = await peers_of(tickers)
    etf = {t: universe.SECTOR_ETFS.get((inst.get(t) or {}).get("sector")) for t in tickers}
    extra = industry_kpis.live_symbols({t: (inst.get(t) or {}).get("industry") for t in tickers})
    more = await latest_quotes(sorted({x for v, _ in peers.values() for x in v} | {e for e in etf.values() if e} | set(extra)), today)
    spy = (quotes.get("SPY") or more.get("SPY") or {}).get("change")
    for p in movers:
        t = p["ticker"]
        pq = {x: more[x]["change"] for x in peers[t][0] if more.get(x) and more[x].get("change") is not None and not more[x]["stale"]}
        kpis = await industry_kpis.for_ticker(t, more, today)
        e = etf.get(t)
        p.update({"peers": pq, "peer_source": peers[t][1], "etf": e, "etf_change": (more.get(e) or {}).get("change") if e else None,
                  "spy": spy, "industry": (kpis or {}).get("industry"),
                  "live": [m["name"] + f" 今日 {m['live']['pct_change'] * 100:+.1f}%" for m in (kpis or {}).get("metrics", [])
                           if m.get("live") and m["live"].get("pct_change") is not None],
                  "verdict": move_verdict(p["change"], p["beta"], spy, list(pq.values()))})


async def research_causes(moves: list[dict], today: date) -> None:
    """Attach a researched cause to holdings moving on their own (web search, cached per ticker and day)."""
    from scout import event_cause

    if not moves:
        return

    async def one(p: dict) -> None:
        ch = p["change"]
        ev = {"date": today.isoformat(), "ret": ch, "idio": ch, "sigma_mult": None, "gap": 0.0, "intraday": ch,
              "timing": "盘中", "vol_ratio": None, "spy_ret": None, "sector_ret": None, "after_ret": 0.0, "sessions_ago": 0,
              "live": True}
        try:
            c = await event_cause.explain(p["ticker"], p.get("name") or p["ticker"], ev)
            p["cause"] = {"type": c["cause_type"], "summary": c["summary"], "external": "web" in (c.get("sources") or [])}
        except Exception as exc:
            p["cause"] = None
            logger.warning("cause search for %s failed: %s", p["ticker"], exc)

    await asyncio.gather(*(one(p) for p in moves[:MAX_CAUSE_SEARCHES]))


# ── market ──

async def market_context(quotes: dict, held: list[dict], spy_sd: float | None, settings: dict, today: date) -> dict:
    from scout import market_filter

    sec = await latest_quotes(sorted(set(universe.SECTOR_ETFS.values()) | {"QQQ", "IWM", "^VIX"}), today)
    q = {**sec, **{k: v for k, v in quotes.items() if v}}
    live = lambda s: q.get(s) and not q[s]["stale"] and q[s].get("change") is not None
    names = {etf: universe.SECTOR_ZH.get(name, name) for name, etf in universe.SECTOR_ETFS.items()}
    secs = sorted(((names[e], e, q[e]["change"]) for e in names if live(e)), key=lambda x: x[2])
    spy = q["SPY"]["change"]
    today_pnl = sum(p["today"] for p in held if p.get("today") is not None)
    beta_part = sum(p["value_prev"] * (p["beta"] if p["beta"] is not None else (1.0 if p["core"] else 0.0)) * spy
                    for p in held if p.get("value_prev"))
    bar = await market_filter.bar_for(today)
    account = settings.get("account_size")
    return {"spy": spy, "spy_price": q["SPY"]["last"], "time_et": q["SPY"]["time_et"],
            "sigmas": abs(spy) / spy_sd if spy_sd else None,
            "indices": [[n, q[s]["change"]] for s, n in INDICES if live(s)],
            "vix": [q["^VIX"]["last"], q["^VIX"]["change"]] if live("^VIX") else None,
            "least": [list(x) for x in secs[:SECTORS_SHOWN]], "lead": [list(x) for x in secs[::-1][:SECTORS_SHOWN]],
            "today": today_pnl, "today_pct": today_pnl / account if account else None, "beta_part": beta_part,
            "betas_missing": [p["ticker"] for p in held if p["beta"] is None and not p["core"]],
            "filter_bar": bar, "filter_gap": q["SPY"]["last"] / bar - 1 if bar else None}


# ── news ──

QUESTION = re.compile(r"[?？]")


def news_tag(r: dict, held: set[str], cards: set[str], watch: set[str]) -> str | None:
    """'持仓' / '机会' / '自选' when the article may be worth a push (before the model's triage), else None."""
    if QUESTION.search(r["headline"] or ""):        # "Is X a buy?", "Should you …?": opinion pieces
        return None
    hard = bool(r.get("is_hard_event")) or bool(context.event_flags([{"headline": r["headline"], "summary": r.get("llm_summary")}]))
    if r["ticker"] in held:
        return "持仓" if hard or (r.get("sentiment") or "neutral") != "neutral" else None
    if not hard:              # watchlist names and cards: only event-driven news, or the channel turns into a feed
        return None
    return "机会" if r["ticker"] in cards else "自选" if r["ticker"] in watch else None


class Title(BaseModel):
    id: str
    kind: str                 # 事件 / 评论
    zh: str


class Titles(BaseModel):
    items: list[Title]


TRIAGE = (
    "每条是一个英文财经新闻标题（附摘要）。对每条：\n"
    "1. kind：标题报道的是这家公司新发生的事（公告、财报、指引、并购、监管、诉讼、管理层变动、评级调整、重大合同、停产等）写「事件」；"
    "观点、分析、盘点、投资建议、股价复盘（只说涨跌而没有新原因）、名人点评、基金持仓介绍、「几个要点」之类写「评论」。拿不准写「评论」。\n"
    "2. zh：把标题译成简洁的中文（不超过 40 个字），只译意思，不加评价、推测或原文没有的信息。数字照原文（billion 可写成十亿或亿，"
    "例如 $4.5 billion 写成 45 亿美元），公司名用常见中文名，没有就保留英文。\n每条按 id 返回。")
TRIAGE_KINDS = {"事件", "评论"}


async def news_alerts(held: set[str], cards: set[str], watch: set[str], since: int, today: date) -> list[dict]:
    """New articles as alert rows: kind 'news' (pushed) or 'news_dropped' (claimed so it is not judged again, never
    pushed): commentary by the model's triage, or past the day's cap. Without the model the rule filter alone decides."""
    tickers = sorted(held | cards | watch)
    if not tickers:
        return []
    try:
        rows = await db.fetch_all(
            "SELECT ticker, url, headline, source, llm_summary, sentiment, is_hard_event, datetime_utc "
            "FROM public.ticker_news_library WHERE ticker = ANY(:t) AND datetime_utc >= :s AND relevance = 'direct' "
            "ORDER BY datetime_utc", {"t": tickers, "s": since})
    except Exception:
        return []
    keys = {f"news:{r['ticker']}:{r['url']}" for r in rows}
    seen = {r["key"] for r in await db.fetch_all("SELECT key FROM scout.alerts WHERE key = ANY(:k)", {"k": sorted(keys)})} if keys else set()
    cands, dup = [], set()
    for r in sorted(rows, key=lambda r: (not r["is_hard_event"], -(r["datetime_utc"] or 0))):
        key = f"news:{r['ticker']}:{r['url']}"
        tag = news_tag(r, held, cards, watch)
        if not tag or key in seen or key in dup:         # filtered by rule each run: nothing to claim
            continue
        dup.add(key)
        flags = context.event_flags([{"headline": r["headline"], "summary": r["llm_summary"]}])
        cands.append({"key": key, "kind": "news", "ticker": r["ticker"],
                      "payload": {"tag": tag, "headline": r["headline"], "summary": r["llm_summary"], "source": r["source"],
                                  "sentiment": r["sentiment"], "hard": bool(r["is_hard_event"]),
                                  "flags": [f["type"] for f in flags], "url": r["url"]}})
    judged = await triage(cands)
    start = datetime(today.year, today.month, today.day, tzinfo=ET)
    count = {r["ticker"]: r["n"] for r in await db.fetch_all(
        "SELECT ticker, count(*) AS n FROM scout.alerts WHERE kind = 'news' AND created_at >= :s AND ticker = ANY(:t) GROUP BY ticker",
        {"s": start, "t": tickers})}
    for a in cands:
        p = a["payload"]
        keep = p.get("kind") == "事件" if judged else (p["tag"] == "持仓" or p["hard"] or bool(p["flags"]))
        cap = NEWS_PER_DAY if p["tag"] == "持仓" else OTHER_NEWS_PER_DAY
        if not keep or count.get(a["ticker"], 0) >= cap:
            a["kind"] = "news_dropped"
            continue
        count[a["ticker"]] = count.get(a["ticker"], 0) + 1
    return cands

NUM = re.compile(r"\d+(?:[.,]\d+)*")


def _nums(s: str) -> list[float]:
    return [float(x.replace(",", "")) for x in NUM.findall(s or "")]


def numbers_ok(zh: str, original: str) -> bool:
    """Every number in the translation stands in the original, up to a power of ten (4.5 billion = 45 亿)."""
    src = _nums(original)
    return all(any(abs(n - m * 10 ** k) <= 1e-6 * max(1.0, n) for m in src for k in range(-4, 5)) for n in _nums(zh))


async def triage(items: list[dict]) -> bool:
    """Event or commentary, and a Chinese headline, in place (payload 'kind', 'zh'); False when the call failed. A
    translation with a number not in the original is dropped for the original; a missing kind counts as commentary."""
    from scout import responses
    from scout.config import get_scout_settings
    from scout.viewdesk import strict_schema

    if not items:
        return True
    settings = get_scout_settings()
    try:
        data = await responses.create({
            "model": settings.scout_parse_model, "instructions": TRIAGE,
            "input": [{"role": "user", "content": json.dumps(
                [{"id": str(i), "company": a["ticker"], "headline": a["payload"]["headline"], "summary": a["payload"].get("summary")}
                 for i, a in enumerate(items)], ensure_ascii=False)}],
            "text": {"format": {"type": "json_schema", "name": "titles", "schema": strict_schema(Titles), "strict": True}}},
            timeout=60, purpose="intraday")
        got = {x.id: x for x in Titles.model_validate_json(responses.output_text(data)).items}
    except Exception as exc:
        logger.warning("news triage failed: %s", exc)
        return False
    for i, a in enumerate(items):
        x = got.get(str(i))
        a["payload"]["kind"] = x.kind if x and x.kind in TRIAGE_KINDS else "评论"
        zh = x.zh.strip() if x else ""
        if zh and len(zh) <= 60 and numbers_ok(zh, a["payload"]["headline"]):
            a["payload"]["zh"] = zh
    return True


# ── buy ideas ──

def opportunity_events(o: dict, price: float, today: date, trigger_window: int, live: bool) -> list[tuple[str, str]]:
    """(kind, text) for one card still waiting for its entry, at `price`."""
    if o["status"] != "new" or sessions_since(o["asof"], today) > trigger_window:
        return []
    if price <= o["stop"]:
        return [("opp_void", f"跌破规则止损位 {o['stop']:.2f}（还没到入场），这条机会的前提可能已经失效，不要再按它买入")]
    if live and o["entry_lo"] <= price <= o["entry_hi"]:
        return [("opp_zone", f"进入入场区间 {o['entry_lo']:.2f}–{o['entry_hi']:.2f}")]
    return []


async def buy_alerts(quotes: dict, settings: dict, today: date) -> tuple[list[dict], str | None]:
    """Zone / void alerts for live rules' cards, only while the plan allows a buy; else (no alerts, the reason)."""
    from scout import learned, market_filter, portfolio
    from scout.detectors import registry

    mf = await market_filter.current()
    if mf and mf["off"]:
        return [], "大盘过滤为避险"
    sat = portfolio.satellite(settings)
    if sat and sat["room"] <= 0:
        return [], "卫星仓已到上限"
    rules = {d.name: d for d in registry(include_candidates=True)}
    quiet = learned.held_back(await learned.current())
    out = []
    for o in await db.fetch_all(
            "SELECT id, ticker, status, asof, primary_detector, entry_lo, entry_hi, stop, target1, evidence "
            "FROM scout.opportunities WHERE status = 'new'"):
        det = rules.get(o["primary_detector"])
        q = quotes.get(o["ticker"])
        if not det or not det.live or o["primary_detector"] in quiet or not q or q["stale"]:
            continue
        for kind, text in opportunity_events(o, q["last"], today, det.trigger_window, True):
            mid = (o["entry_lo"] + o["entry_hi"]) / 2
            sz = portfolio.size_text(portfolio.size(mid, portfolio.holding_stop(mid), settings, portfolio.opp_vol(o))) if kind == "opp_zone" else None
            out.append({"key": f"{kind}:{o['id']}", "kind": kind, "ticker": o["ticker"],
                        "payload": {"text": text, "price": q["last"], "time_et": q["time_et"], "rule": det.label, "size": sz}})
    return out, None


# ── scan ──

async def _claim(rows: list[dict]) -> list[dict]:
    """Insert alert keys; return only the ones not sent before."""
    if not rows:
        return []
    sent = {r["key"] for r in await db.fetch_all("SELECT key FROM scout.alerts WHERE key = ANY(:k)",
                                                   {"k": [r["key"] for r in rows]})}
    fresh = [r for r in rows if r["key"] not in sent]
    if fresh:
        await db.execute_many(
            "INSERT INTO scout.alerts (key, kind, ticker, payload) VALUES (:key, :kind, :ticker, CAST(:payload AS JSONB)) "
            "ON CONFLICT (key) DO NOTHING",
            [{**r, "payload": db.jsonb(r.get("payload"))} for r in fresh])
    return fresh


def _levels(prefix: str, change: float, n: int, payload: dict, ticker: str | None) -> list[dict]:
    """One key per crossed level (all claimed, so a move that jumps to level 2 never reports level 1 later)."""
    side = "up" if change > 0 else "down"
    return [{"key": f"{prefix}:{side}{k}", "kind": prefix.split(":")[0], "ticker": ticker, "payload": {**payload, "level": k}}
            for k in range(1, n + 1)]


def _top(fresh: list[dict], kind: str) -> dict[str | None, dict]:
    """The highest fresh level per ticker."""
    out: dict = {}
    for a in fresh:
        if a["kind"] == kind and (a["ticker"] not in out or a["payload"]["level"] > out[a["ticker"]]["payload"]["level"]):
            out[a["ticker"]] = a
    return out


async def scan(today: date) -> dict:
    from scout import levels, portfolio

    held_rows = await portfolio.trades(open_only=True)
    settings = await portfolio.get_settings()
    tickers = sorted({t["ticker"] for t in held_rows})
    feats = {r["ticker"]: r["payload"] or {} for r in await db.fetch_all(
        "SELECT DISTINCT ON (ticker) ticker, payload FROM scout.features_daily WHERE ticker = ANY(:t) ORDER BY ticker, date DESC",
        {"t": tickers + ["SPY"]})}
    cards = {r["ticker"] for r in await db.fetch_all("SELECT ticker FROM scout.opportunities WHERE status IN ('new', 'triggered')")}
    watch = {r["ticker"] for r in await universe.load_watchlist()}
    new_cards = [r["ticker"] for r in await db.fetch_all("SELECT ticker FROM scout.opportunities WHERE status = 'new'")]
    quotes = await latest_quotes(sorted(set(tickers) | set(new_cards) | {"SPY"}), today)

    pos = {}
    for t in held_rows:
        q = quotes.get(t["ticker"])
        if q and not q["stale"] and q.get("change") is not None:
            pos.setdefault(t["ticker"], []).append(position(t, q, feats.get(t["ticker"]) or {}))
    held = [p for ps in pos.values() for p in ps]
    first = {tk: ps[0] for tk, ps in pos.items()}

    alerts: list[dict] = []
    spy_sd = (feats.get("SPY") or {}).get("vol_63")
    spy_sd = spy_sd / 252 ** 0.5 if spy_sd else None
    spy = (quotes.get("SPY") or {}).get("change")
    if spy is not None and not quotes["SPY"]["stale"]:
        n = level(spy, market_bar(spy_sd), MARKET_LEVELS)
        if n:
            alerts += _levels(f"market:{today}", spy, n, {}, None)
    for tk, p in first.items():
        if p["core"]:
            continue
        if p["sd"]:
            n = level(p["change"], hold_bar(p["sd"]), HOLD_LEVELS)
            if n:
                alerts += _levels(f"hold_move:{tk}:{today}", p["change"], n, {}, tk)
        if p["below"]:
            alerts.append({"key": f"trade_stop:{tk}:{today}", "kind": "trade_stop", "ticker": tk, "payload": {}})
    non_core = {tk for tk in tickers if not levels.is_core(tk)}       # core ETFs' "news" is market commentary
    alerts += await news_alerts(non_core, cards - set(tickers), watch - set(tickers), int(time.time()) - NEWS_LOOKBACK_S, today)
    buys, why_not = await buy_alerts(quotes, settings, today)
    alerts += buys

    fresh = await _claim(alerts)
    moves, market = _top(fresh, "hold_move"), _top(fresh, "market").get(None)
    blocks = sorted({a["ticker"] for a in fresh if a["kind"] in ("hold_move", "trade_stop") or (a["kind"] == "news" and a["ticker"] in first)})
    movers = [first[tk] for tk in blocks if tk in first]
    await move_context([p for p in movers if p["ticker"] in moves or p["below"]], quotes, today)
    await research_causes([p for p in movers if p.get("verdict") == "公司自身" and p["ticker"] in moves], today)
    mk = None
    if market:
        mk = {**await market_context(quotes, held, spy_sd, settings, today), "level": market["payload"]["level"]}
    out = {"market": mk,
           "holdings": [{**first[tk], "level": moves[tk]["payload"]["level"] if tk in moves else 0,
                         "news": [a["payload"] for a in fresh if a["kind"] == "news" and a["ticker"] == tk]}
                        for tk in sorted(blocks, key=lambda tk: (not first[tk]["below"], -abs(first[tk]["change"] or 0))) if tk in first],
           "buys": [a for a in fresh if a["kind"] in ("opp_zone", "opp_void")],
           "other_news": [{"ticker": a["ticker"], **a["payload"]} for a in fresh if a["kind"] == "news" and a["ticker"] not in first],
           "buy_block": why_not}
    for a in fresh:                    # what was pushed, for the record
        if a["kind"] in ("hold_move", "trade_stop", "market"):
            snap = out["market"] if a["kind"] == "market" else next((h for h in out["holdings"] if h["ticker"] == a["ticker"]), None)
            if snap is not None:
                await db.execute("UPDATE scout.alerts SET payload = CAST(:p AS JSONB) WHERE key = :k",
                                 {"k": a["key"], "p": db.jsonb({**snap, "level": a["payload"].get("level")})})
    return {"checked": {"trades": len(held_rows), "quotes": len(quotes)}, "alerts": fresh, "out": out}


# ── render ──

def market_lines(m: dict) -> list[str]:
    from scout.telegram import esc

    sig = f"，约 {m['sigmas']:.1f} 倍日常波动" if m.get("sigmas") else ""
    more = ("，跌幅继续扩大" if m["spy"] < 0 else "，涨幅继续扩大") if m["level"] > 1 else ""
    head = f"<b>【大盘】</b>标普 500 {_pct(m['spy'])}（{m['spy_price']:.2f}，{esc(m['time_et'][-5:])}{sig}{more}）"
    idx = " · ".join(f"{n} {_pct(c)}" for n, c in m["indices"] if n != "标普 500")
    if m.get("vix"):
        idx += f" · VIX {m['vix'][0]:.1f}（{_pct(m['vix'][1], 0)}）"
    lines = [head, f"  {idx}" if idx else None]
    if m["least"]:
        down = m["spy"] < 0
        first, second = (m["least"], m["lead"]) if down else (m["lead"], m["least"])
        lines.append(f"  {'跌得最多' if down else '涨得最多'}：" + "、".join(f"{n} {e} {_pct(c)}" for n, e, c in first)
                     + f"；{'最抗跌' if down else '最弱'}：" + "、".join(f"{n} {e} {_pct(c)}" for n, e, c in second))
    pnl = f"  你的持仓今天 {_usd(m['today'])}" + (f"（账户 {_pct(m['today_pct'])}）" if m.get("today_pct") is not None else "")
    pnl += f"，其中按 β 算跟着大盘的约 {_usd(m['beta_part'])}"
    if m["betas_missing"]:
        pnl += f"（{'、'.join(m['betas_missing'])} 暂无 β，未计入）"
    lines.append(pnl)
    if m.get("filter_bar"):
        lines.append(f"  大盘过滤只看月末收盘：这个月最后一个交易日 SPY 收在 {m['filter_bar']:.2f} 以下才转为避险，"
                     f"现在{'高出' if m['filter_gap'] >= 0 else '低于'}它 {abs(m['filter_gap']) * 100:.1f}%；核心仓位盘中不需要操作")
    return [x for x in lines if x]


def news_line(n: dict, tag: str | None = None) -> str:
    from scout.telegram import esc

    flags = f"⚡{'、'.join(n['flags'])} " if n.get("flags") else ""
    title = n.get("zh") or n["headline"]
    return f"📰 {f'[{esc(tag)}] ' if tag else ''}{flags}{esc(title)}（{esc(n['source'])}）"


def holding_lines(h: dict) -> list[str]:
    from scout.telegram import esc

    icon = "🔻" if h["below"] else "📉" if (h["change"] or 0) < 0 else "📈"
    sig = f"，约 {h['sigmas']:.1f} 倍日常波动" if h.get("sigmas") else ""
    more = ("，跌幅继续扩大" if h["change"] < 0 else "，涨幅继续扩大") if h["level"] > 1 else ""
    today = f"；今天 {_usd(h['today'])}" if h.get("today") is not None else ""
    lines = [f"{icon} <b>{esc(h['ticker'])}</b> {_pct(h['change'])}（{h['price']:.2f}，{esc(h['time_et'][-5:])}{sig}{more}{today}）"]
    pos = f"  持仓 {_pct(h['pnl_pct'])}（{_usd(h['pnl'])}）" if h.get("pnl") is not None else "  "
    if h["below"]:
        pos += f" · ⚠️ 现价已低于系统止损 {h['stop']:.2f}"
    lines.append(pos)
    if h.get("verdict"):
        parts = []
        if h.get("peers"):
            parts.append(f"{h['peer_source']} " + "、".join(f"{t} {_pct(c)}" for t, c in h["peers"].items()))
        if h.get("etf") and h.get("etf_change") is not None:
            parts.append(f"板块 {h['etf']} {_pct(h['etf_change'])}")
        parts += h.get("live") or []
        if h.get("spy") is not None:
            parts.append(f"大盘 SPY {_pct(h['spy'])}" + (f" × β {h['beta']:.2f} ≈ {_pct(h['beta'] * h['spy'])}" if h.get("beta") is not None else ""))
        why = {"行业": "主要是整个行业在动，不是公司自己的消息", "大盘": "主要是跟着大盘",
               "公司自身": "同行和大盘解释不了，多半是公司自己的原因"}[h["verdict"]]
        c = h.get("cause")
        cause = f"；原因：{esc(c['type'])}——{esc(c['summary'])}" + ("（据外部报道，未经核实）" if c.get("external") else "") if c else ""
        lines.append("  " + esc(" · ".join(parts)))
        lines.append(f"  → {why}{cause}")
    lines += ["  " + news_line(n) for n in h.get("news") or []]
    lines.append(f"  👉 {esc(h['plan'])}")
    return lines


def render(out: dict, now: datetime) -> str | None:
    from scout.telegram import esc

    if not (out.get("market") or out.get("holdings") or out.get("buys") or out.get("other_news")):
        return None
    lines = [f"⏱ <b>盘中提醒</b> {now.astimezone(ET):%H:%M} 美东"]
    if out.get("market"):
        lines += market_lines(out["market"])
    if out.get("holdings"):
        lines.append("<b>【我的持仓】</b>")
        for h in out["holdings"]:
            lines += holding_lines(h)
    if out.get("buys") or out.get("other_news"):
        lines.append("<b>【机会与自选】</b>")
    for a in out.get("buys") or []:
        p = a["payload"]
        icon = "🟦" if a["kind"] == "opp_zone" else "⚪"
        lines.append(f"{icon} <b>{esc(a['ticker'])}</b> {esc(p['rule'])}：{esc(p['text'])} · 现价 {p['price']:.2f}（{esc(p['time_et'][-5:])}）")
        if p.get("size"):
            lines.append(f"  📐 {esc(p['size'])}；买入后按持仓计划管理（回落 20% 止损）")
    for n in out.get("other_news") or []:
        lines.append(news_line(n, n["tag"]).replace("📰 ", f"📰 <b>{esc(n['ticker'])}</b> ", 1))
    lines.append("价格来自 Yahoo，可能有几分钟延迟；止损都按收盘价判断。")
    return "\n".join(lines)


async def run(push: bool = True, force: bool = False) -> dict:
    status = us_equity_market_status()
    if not status.is_open and not force:
        return {"skipped": status.phase}
    today = date.fromisoformat(status.market_date)
    result = await scan(today)
    out = result["out"]
    summary = {**result["checked"], "alerts": len(result["alerts"]), "kinds": sorted({a["kind"] for a in result["alerts"]}),
               **({"buys_paused": out["buy_block"]} if out.get("buy_block") else {})}
    text = render(out, datetime.now(ET))
    try:                               # prediction markets: near events and the holdings' markets, moves reported once
        from scout import prediction

        pm = await prediction.run(today, "intraday")
        summary["prediction"] = {"refreshed": pm.get("refreshed"), "moves": len(pm.get("moves") or [])}
        if pm.get("moves"):
            from scout.telegram import esc

            extra = "\n".join(esc(prediction.move_line(m)) for m in pm["moves"])
            text = (text + "\n" + extra) if text else f"⏱ <b>盘中提醒</b> {datetime.now(ET):%H:%M} 美东\n" + extra
    except Exception as exc:
        logger.warning("prediction markets failed: %s", exc)
    if push and text:
        from scout import telegram

        await telegram.send(text)
        summary["pushed"] = True
    return summary
