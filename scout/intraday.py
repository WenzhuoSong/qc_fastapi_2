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
BIG_MOVE = 0.08                   # session move of a holding / idea that gets a cause search
MAX_CAUSE_SEARCHES = 3


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
        out.append(("trade_stop", f"跌破持仓止损 {t['stop']:.2f}（成本 {t['entry_price']:.2f}，{pnl * 100:+.1f}%）"))
    if t.get("target") and price >= t["target"]:
        advice = ""
        if "target1" in (t.get("plan") or {}) and t.get("shares"):
            from scout import levels

            remaining = t["shares"] - (t.get("sold_shares") or 0)
            keep = next(st["keep_after"] for st in levels.exit_plan(t["plan"], t["entry_price"], t["shares"], remaining, t.get("stop"))
                        if st["stage"] == "目标 1")
            if remaining > keep:
                advice = f"，按计划建议卖出 {remaining - keep:g} 股（剩余 {remaining:g} 股），卖出后用 /sell 记录"
        out.append(("trade_target", f"到达持仓止盈位 {t['target']:.2f}（成本 {t['entry_price']:.2f}，{pnl * 100:+.1f}%）{advice}"))
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
              "timing": "盘中", "vol_ratio": None, "spy_ret": None, "sector_ret": None, "after_ret": 0.0, "sessions_ago": 0}
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
    opps = await db.fetch_all(
        "SELECT id, ticker, status, asof, primary_detector, entry_lo, entry_hi, stop, current_stop, target1 "
        "FROM scout.opportunities WHERE status IN ('new', 'triggered')")
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
    tracked = {t["ticker"]: "持仓" for t in held} | {o["ticker"]: "机会" for o in opps if o["ticker"] not in {t["ticker"] for t in held}}
    for tk, tag in tracked.items():
        q = quotes.get(tk)
        if q and not q["stale"] and q["change"] is not None and abs(q["change"]) >= BIG_MOVE:
            alerts.append({"key": f"move:{tk}:{today}", "kind": "big_move", "ticker": tk,
                           "payload": {"tag": tag, "change": q["change"], "price": q["last"], "time_et": q["time_et"]}})
    alerts += await _news_alerts({t["ticker"] for t in held}, {o["ticker"] for o in opps}, watch,
                                 int(time.time()) - NEWS_LOOKBACK_S)
    fresh = await _claim(alerts)
    await _research_moves([a for a in fresh if a["kind"] == "big_move"], today)
    return {"checked": {"trades": len(held), "opportunities": len(opps), "quotes": len(quotes)},
            "alerts": fresh}


ICON = {"trade_stop": "🔻", "trade_target": "🎯", "opp_stop": "🔻", "opp_target": "🎯", "opp_zone": "🟦", "opp_void": "⚪",
        "big_move": "⚡", "news": "📰"}
ORDER = ("trade_stop", "trade_target", "big_move", "opp_stop", "opp_target", "opp_zone", "opp_void", "news")


def render(alerts: list[dict], now: datetime) -> str | None:
    from scout.telegram import esc

    if not alerts:
        return None
    lines = [f"⏱ <b>盘中提醒</b> {now.astimezone(ET):%H:%M} 美东"]
    for a in sorted(alerts, key=lambda a: ORDER.index(a["kind"])):
        p = a["payload"]
        if a["kind"] == "big_move":
            c = p.get("cause")
            why = (f"{esc(c['type'])}：{esc(c['summary'])}" + ("（据外部报道）" if c.get("external") else "")) if c else "原因未查到"
            lines.append(f"{ICON['big_move']} [{esc(p['tag'])}] <b>{esc(a['ticker'])}</b> 盘中 {p['change'] * 100:+.1f}%"
                         f"（{p['price']:.2f}，{esc(p['time_et'])}）· {why}")
        elif a["kind"] == "news":
            flags = f"⚡{'、'.join(p['flags'])} " if p["flags"] else ""
            lines.append(f"{ICON['news']} [{esc(p['tag'])}] <b>{esc(a['ticker'])}</b> {flags}{esc(p['headline'])}（{esc(p['source'])}）")
        else:
            who = "你的持仓" if a["kind"].startswith("trade") else esc(p.get("rule", ""))
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
