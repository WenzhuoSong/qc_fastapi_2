"""Intraday monitor: price levels and news during the US session.

Every 15 minutes while the market is open it checks near-real-time Yahoo
prices of the user's open trades and Scout's active opportunities against
their levels, and new direct news on holdings, opportunities and the
watchlist. Each event is pushed once (scout.alerts). Alerts never change an
opportunity's status: the after-close tracker settles fills and exits with
the same simulator the replay uses.
"""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import date, datetime
from zoneinfo import ZoneInfo

from scout import context, db, universe
from scout.data.yahoo import latest_quotes
from scout.market_calendar import is_us_equity_trading_day, us_equity_market_status

ET = ZoneInfo("America/New_York")
NEWS_LOOKBACK_S = 3 * 3600        # a missed run (or a slow news cron) is caught by the next one
BIG_MOVE = 0.08                   # session move of an idea that gets a cause search
MAX_CAUSE_SEARCHES = 3
HOLD_SIGMAS = 1.5                 # a holding moving this many of its own daily sigmas (63-day vol) is reported, with context,
HOLD_ABS_MOVE = 0.05              # or this much whatever its volatility (a 5 % day is news to the holder),
HOLD_MIN_MOVE = 0.02              # never below this
PEERS = 4
SHARE = 0.6                       # a part carrying this share of the move (same sign) names the cause


def move_verdict(change: float, beta: float | None, spy: float | None, peers: list[float]) -> str:
    """Which part carries the holding's move: its industry (peers' median), the market (β × SPY), or itself."""
    import statistics

    if peers:
        med = statistics.median(peers)
        if med * change > 0 and abs(med) >= SHARE * abs(change):
            return "行业"
    if beta is not None and spy is not None and beta * spy * change > 0 and abs(beta * spy) >= SHARE * abs(change):
        return "大盘"
    return "公司自身"


async def holding_moves(held: list[dict], quotes: dict, today: date) -> list[dict]:
    """Holdings that moved ≥ HOLD_SIGMAS daily sigmas or ≥ HOLD_ABS_MOVE (never below HOLD_MIN_MOVE): peers, sector ETF, the industry's live metrics,
    the market, and which of them carries the move — the comparison a desk makes before reading any news."""
    from scout import industry_kpis, premarket

    tickers = sorted({t["ticker"] for t in held})
    feats = {r["ticker"]: r["payload"] or {} for r in await db.fetch_all(
        "SELECT DISTINCT ON (ticker) ticker, payload FROM scout.features_daily WHERE ticker = ANY(:t) ORDER BY ticker, date DESC",
        {"t": tickers})} if tickers else {}
    movers = []
    for t in tickers:
        q, vol = quotes.get(t), (feats.get(t) or {}).get("vol_63")
        if not q or q["stale"] or q["change"] is None or not vol:
            continue
        sigma = vol / 252 ** 0.5
        if abs(q["change"]) >= max(min(HOLD_SIGMAS * sigma, HOLD_ABS_MOVE), HOLD_MIN_MOVE):
            movers.append((t, q, sigma))
    if not movers:
        return []
    inst = {r["ticker"]: r for r in await db.fetch_all(
        "SELECT ticker, sector, industry FROM scout.instruments WHERE ticker = ANY(:t)", {"t": [m[0] for m in movers]})}
    rank = await premarket._tickers_ranked_by_liquidity()
    peers: dict[str, list[str]] = {}
    for t, _, _ in movers:
        ind = (inst.get(t) or {}).get("industry")
        same = [r["ticker"] for r in await db.fetch_all(
            "SELECT ticker FROM scout.instruments WHERE industry = :i AND ticker <> :t", {"i": ind, "t": t})] if ind else []
        peers[t] = sorted((x for x in same if x in rank), key=lambda x: rank[x])[:PEERS]
    etf = {t: universe.SECTOR_ETFS.get((inst.get(t) or {}).get("sector")) for t, _, _ in movers}
    extra = industry_kpis.live_symbols({t: (inst.get(t) or {}).get("industry") for t, _, _ in movers})
    more = await latest_quotes(sorted({x for v in peers.values() for x in v} | {e for e in etf.values() if e} | set(extra)), today)
    spy = (quotes.get("SPY") or more.get("SPY") or {}).get("change")
    out = []
    for t, q, sigma in movers:
        pq = {x: more[x]["change"] for x in peers[t] if more.get(x) and more[x].get("change") is not None and not more[x]["stale"]}
        beta = (feats.get(t) or {}).get("beta_60")
        kpis = await industry_kpis.for_ticker(t, more, today)
        live = [m["name"] + f" 今日 {m['live']['pct_change'] * 100:+.1f}%" for m in (kpis or {}).get("metrics", [])
                if m.get("live") and m["live"].get("pct_change") is not None]
        e = etf.get(t)
        out.append({"key": f"hold_move:{t}:{today}", "kind": "hold_move", "ticker": t, "payload": {
            "change": q["change"], "price": q["last"], "time_et": q["time_et"], "sigmas": abs(q["change"]) / sigma,
            "peers": pq, "etf": e, "etf_change": (more.get(e) or {}).get("change") if e else None, "spy": spy, "beta": beta,
            "industry": (kpis or {}).get("industry"), "live": live, "verdict": move_verdict(q["change"], beta, spy, list(pq.values()))}})
    return out


def sessions_since(start: date, end: date) -> int:
    """Trading sessions after `start` up to and including `end`."""
    n, d = 0, start
    while d < end:
        d = date.fromordinal(d.toordinal() + 1)
        n += is_us_equity_trading_day(d)
    return n


def opportunity_events(o: dict, price: float, today: date, trigger_window: int, live: bool) -> list[tuple[str, str]]:
    """(kind, text) for one opportunity at `price`."""
    stop = o.get("current_stop") or o["stop"]
    if o["status"] == "new":
        if sessions_since(o["asof"], today) > trigger_window:
            return []
        if price <= o["stop"]:
            return [("opp_void", f"跌破止损位 {o['stop']:.2f}（尚未入场），这条机会的前提可能已经失效")]
        if live and o["entry_lo"] <= price <= o["entry_hi"]:
            return [("opp_zone", f"进入入场区间 {o['entry_lo']:.2f}–{o['entry_hi']:.2f}，止损 {o['stop']:.2f}，目标 1 {o['target1']:.2f}")]
        return []
    if price <= stop:
        return [("opp_stop", f"跌破止损 {stop:.2f}（收盘后按规则结算）")]
    if price >= o["target1"]:
        return [("opp_target", f"到达目标 1 {o['target1']:.2f}（收盘后按规则结算）")]
    return []


def trade_events(t: dict, price: float) -> list[tuple[str, str]]:
    out = []
    pnl = price / t["entry_price"] - 1
    if t.get("stop") and price <= t["stop"]:
        out.append(("trade_stop", f"盘中低于回落止损 {t['stop']:.2f}（成本 {t['entry_price']:.2f}，{pnl * 100:+.1f}%）："
                                  "按收盘判断，收盘仍低于止损则下一个交易日开盘卖出全部"))
    return out


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


async def _news_alerts(held: set[str], opps: set[str], watch: set[str], since: int) -> list[dict]:
    try:
        rows = await db.fetch_all(
            "SELECT ticker, url, headline, source, llm_summary, sentiment, is_hard_event, datetime_utc "
            "FROM public.ticker_news_library WHERE ticker = ANY(:t) AND datetime_utc >= :s AND relevance = 'direct' "
            "ORDER BY datetime_utc", {"t": sorted(held | opps | watch), "s": since})
    except Exception:
        return []
    out = []
    for r in rows:
        flags = context.event_flags([{"headline": r["headline"], "summary": r["llm_summary"]}])
        important = r["ticker"] in held or r["ticker"] in opps or bool(flags) or r["is_hard_event"]
        if not important:          # watchlist names: only event-driven news, or the channel turns into a feed
            continue
        tag = "持仓" if r["ticker"] in held else "机会" if r["ticker"] in opps else "自选"
        out.append({"key": f"news:{r['ticker']}:{r['url']}", "kind": "news", "ticker": r["ticker"],
                    "payload": {"tag": tag, "headline": r["headline"], "source": r["source"], "sentiment": r["sentiment"],
                                "flags": [f["type"] for f in flags], "url": r["url"]}})
    return out


async def _research_moves(moves: list[dict], today: date) -> None:
    """Attach a researched cause to big session moves (web search, cached per ticker and day)."""
    from scout import event_cause

    if not moves:
        return
    names = {r["ticker"]: r["name"] for r in await db.fetch_all(
        "SELECT ticker, name FROM scout.instruments WHERE ticker = ANY(:t)", {"t": [m["ticker"] for m in moves]})}

    async def one(m: dict) -> None:
        ch = m["payload"]["change"]
        ev = {"date": today.isoformat(), "ret": ch, "idio": ch, "sigma_mult": None, "gap": 0.0, "intraday": ch,
              "timing": "盘中", "vol_ratio": None, "spy_ret": None, "sector_ret": None, "after_ret": 0.0, "sessions_ago": 0,
              "live": True}
        try:
            c = await event_cause.explain(m["ticker"], names.get(m["ticker"]) or m["ticker"], ev)
            m["payload"]["cause"] = {"type": c["cause_type"], "summary": c["summary"],
                                     "external": "web" in (c.get("sources") or [])}
        except Exception as exc:
            m["payload"]["cause"] = None
            logging.getLogger("scout.intraday").warning("cause search for %s failed: %s", m["ticker"], exc)

    await asyncio.gather(*(one(m) for m in moves[:MAX_CAUSE_SEARCHES]))


async def scan(today: date) -> dict:
    from scout.detectors import registry

    rules = {d.name: d for d in registry(include_candidates=True)}
    held = await db.fetch_all(
        "SELECT t.id, t.ticker, t.entry_price, t.shares, t.stop, t.target, t.plan, "
        "(SELECT coalesce(sum(shares), 0) FROM scout.trade_sells WHERE trade_id = t.id) AS sold_shares "
        "FROM scout.trades t WHERE t.closed_on IS NULL")
    from scout import learned

    quiet = learned.held_back(await learned.current())
    # cards of rules the knowledge base rejects are tracked, not pushed (as after the close): no intraday alerts either
    opps = [o for o in await db.fetch_all(
        "SELECT id, ticker, status, asof, primary_detector, entry_lo, entry_hi, stop, current_stop, target1 "
        "FROM scout.opportunities WHERE status IN ('new', 'triggered')") if o["primary_detector"] not in quiet]
    watch = {r["ticker"] for r in await universe.load_watchlist()}
    quotes = await latest_quotes(sorted({t["ticker"] for t in held} | {o["ticker"] for o in opps}), today)
    alerts = []
    for t in held:
        q = quotes.get(t["ticker"])
        if q and not q["stale"]:
            for kind, text in trade_events(t, q["last"]):
                alerts.append({"key": f"{kind}:{t['id']}:{today}", "kind": kind, "ticker": t["ticker"],
                               "payload": {"text": text, "price": q["last"], "time_et": q["time_et"]}})
    for o in opps:
        q, det = quotes.get(o["ticker"]), rules.get(o["primary_detector"])
        if q and not q["stale"]:
            window = det.trigger_window if det else 3
            for kind, text in opportunity_events(o, q["last"], today, window, bool(det and det.live)):
                alerts.append({"key": f"{kind}:{o['id']}", "kind": kind, "ticker": o["ticker"],
                               "payload": {"text": text, "price": q["last"], "time_et": q["time_et"],
                                           "rule": det.label if det else o["primary_detector"]}})
    alerts += await holding_moves(held, quotes, today)
    held_set = {t["ticker"] for t in held}
    tracked = {o["ticker"]: "机会" for o in opps if o["ticker"] not in held_set}    # holdings: `holding_moves`
    for tk, tag in tracked.items():
        q = quotes.get(tk)
        if q and not q["stale"] and q["change"] is not None and abs(q["change"]) >= BIG_MOVE:
            alerts.append({"key": f"move:{tk}:{today}", "kind": "big_move", "ticker": tk,
                           "payload": {"tag": tag, "change": q["change"], "price": q["last"], "time_et": q["time_et"]}})
    alerts += await _news_alerts({t["ticker"] for t in held}, {o["ticker"] for o in opps}, watch,
                                 int(time.time()) - NEWS_LOOKBACK_S)
    fresh = await _claim(alerts)
    await _research_moves([a for a in fresh if a["kind"] == "big_move" or (a["kind"] == "hold_move" and a["payload"]["verdict"] == "公司自身")],
                          today)
    return {"checked": {"trades": len(held), "opportunities": len(opps), "quotes": len(quotes)},
            "alerts": fresh}


ICON = {"trade_stop": "🔻", "trade_target": "🎯", "opp_stop": "🔻", "opp_target": "🎯", "opp_zone": "🟦", "opp_void": "⚪",
        "big_move": "⚡", "news": "📰", "hold_move": "📊"}
ORDER = ("trade_stop", "trade_target", "hold_move", "big_move", "opp_stop", "opp_target", "opp_zone", "opp_void", "news")


def hold_move_line(a: dict) -> str:
    from scout.telegram import esc

    p = a["payload"]
    pct = lambda x: f"{x * 100:+.1f}%"
    parts = []
    if p["peers"]:
        parts.append("同行 " + "、".join(f"{t} {pct(c)}" for t, c in p["peers"].items()))
    if p.get("etf") and p.get("etf_change") is not None:
        parts.append(f"板块 {p['etf']} {pct(p['etf_change'])}")
    parts += p.get("live") or []
    if p.get("spy") is not None:
        parts.append(f"大盘 SPY {pct(p['spy'])}" + (f"（它的 β {p['beta']:.1f}）" if p.get("beta") is not None else ""))
    why = {"行业": "主要是整个行业在动，不是公司自己的消息", "大盘": "主要是跟着大盘",
           "公司自身": "同行和大盘解释不了，多半是公司自己的原因"}[p["verdict"]]
    c = p.get("cause")
    cause = f"；原因：{esc(c['type'])}——{esc(c['summary'])}" + ("（据外部报道）" if c.get("external") else "") if c else ""
    return (f"{ICON['hold_move']} [你的持仓] <b>{esc(a['ticker'])}</b> 盘中 {pct(p['change'])}（{p['price']:.2f}，{esc(p['time_et'])}，"
            f"约 {p['sigmas']:.1f} 倍日常波动）\n  " + esc(" · ".join(parts)) + f"\n  → {why}{cause}。止损按收盘判断，盘中不需要操作")


def render(alerts: list[dict], now: datetime) -> str | None:
    from scout.telegram import esc

    if not alerts:
        return None
    lines = [f"⏱ <b>盘中提醒</b> {now.astimezone(ET):%H:%M} 美东"]
    for a in sorted(alerts, key=lambda a: ORDER.index(a["kind"])):
        p = a["payload"]
        if a["kind"] == "hold_move":
            lines.append(hold_move_line(a))
        elif a["kind"] == "big_move":
            c = p.get("cause")
            why = (f"{esc(c['type'])}：{esc(c['summary'])}" + ("（据外部报道）" if c.get("external") else "")) if c else "原因未查到"
            lines.append(f"{ICON['big_move']} [{esc(p['tag'])}] <b>{esc(a['ticker'])}</b> 盘中 {p['change'] * 100:+.1f}%"
                         f"（{p['price']:.2f}，{esc(p['time_et'])}）· {why}")
        elif a["kind"] == "news":
            flags = f"⚡{'、'.join(p['flags'])} " if p["flags"] else ""
            lines.append(f"{ICON['news']} [{esc(p['tag'])}] <b>{esc(a['ticker'])}</b> {flags}{esc(p['headline'])}（{esc(p['source'])}）")
        else:
            who = "你的持仓" if a["kind"].startswith("trade") else f"机会卡·{esc(p.get('rule', ''))}（不是你的持仓）"
            lines.append(f"{ICON[a['kind']]} <b>{esc(a['ticker'])}</b> {who}：{esc(p['text'])} · 现价 {p['price']:.2f}（{esc(p['time_et'])}）")
    lines.append("价格来自 Yahoo，可能有几分钟延迟；机会状态以收盘后结算为准。")
    return "\n".join(lines)


async def run(push: bool = True, force: bool = False) -> dict:
    status = us_equity_market_status()
    if not status.is_open and not force:
        return {"skipped": status.phase}
    today = date.fromisoformat(status.market_date)
    result = await scan(today)
    summary = {**result["checked"], "alerts": len(result["alerts"]),
               "kinds": sorted({a["kind"] for a in result["alerts"]})}
    text = render(result["alerts"], datetime.now(ET))
    if push and text:
        from scout import telegram

        await telegram.send(text)
        summary["pushed"] = True
    return summary
