"""The after-close digest: one message per session, ordered by what the user owns.

Market (state, indices, the market filter, the day's focus) → sectors (hot / cold, rotation, and the sectors the
holdings sit in) → each holding (today's move split into market / sector / company, P&L, distance to the 20 %
stop, what the system changed, news, supply-chain hints, earnings soon), the ones needing attention first →
watchlist names that changed today → the rest (pass-through hints about other stocks, opportunity cards).
It replaces the separate stop-update and pass-through pushes. Every number comes from Python.
"""
from __future__ import annotations

import time
from datetime import date

from scout import db, earnings_gaps, opportunities, portfolio, sectors, universe
from scout.telegram import esc, market_split_text, pct, sector_text

NEAR_STOP_SIGMAS = 2.0      # a holding within this many daily sigmas of its stop needs attention
EARNINGS_DAYS = 7           # calendar days ahead that count as "earnings soon"
WATCH_MOVE = 0.03           # a watchlist name's company-own move that counts as a change
WATCH_MAX = 8
NEWS_MAX = 2
OTHER_PROP_MAX = 3
NEWS_HOURS = 24


async def gather(asof: date, earnings: dict[str, list[dict]], prop_signals: list[dict], managed: list[dict]) -> dict:
    """Holdings and watchlist inputs for `render`."""
    held_rows = await portfolio.trades(open_only=True)
    watch_rows = await universe.load_watchlist()
    held = sorted({t["ticker"] for t in held_rows})
    watch = sorted({r["ticker"] for r in watch_rows} - set(held))
    names = held + watch
    feats = {r["ticker"]: r["payload"] or {} for r in await db.fetch_all(
        "SELECT ticker, payload FROM scout.features_daily WHERE date = :d AND ticker = ANY(:t)", {"d": asof, "t": names})} if names else {}
    news: dict[str, list[dict]] = {}
    if names:
        for r in await db.fetch_all(
                "SELECT DISTINCT ON (ticker, url) ticker, url, headline, sentiment, is_hard_event, datetime_utc FROM public.ticker_news_library "
                "WHERE ticker = ANY(:t) AND datetime_utc >= :s AND relevance = 'direct' "
                "AND (coalesce(sentiment, 'neutral') <> 'neutral' OR is_hard_event) ORDER BY ticker, url, datetime_utc DESC",
                {"t": names, "s": int(time.time()) - NEWS_HOURS * 3600}):
            news.setdefault(r["ticker"], []).append(r)
    info = {r["ticker"]: r for r in await db.fetch_all(
        "SELECT ticker, name, sector FROM scout.instruments WHERE ticker = ANY(:t)", {"t": names})} if names else {}
    nxt = {}
    for tk, evs in earnings.items():
        future = [e["date"] for e in evs if e["date"] > asof]
        if future and (future[0] - asof).days <= EARNINGS_DAYS:
            nxt[tk] = future[0]
    gaps = {}
    for tk in set(held) & set(nxt):
        s = await earnings_gaps.history(tk)
        if s:
            gaps[tk] = s
    return {"held_rows": held_rows, "watch": watch, "feats": feats, "news": news, "info": info, "earnings": nxt, "gaps": gaps,
            "prop": prop_signals, "managed": {m["ticker"]: m["messages"] for m in managed}}


def _news_lines(items: list[dict]) -> list[str]:
    icon = {"positive": "🟢", "negative": "🔴"}
    items = sorted(items, key=lambda r: (not r.get("is_hard_event"), -(r.get("datetime_utc") or 0)))[:NEWS_MAX]
    return [f"📰 {icon.get(r.get('sentiment'), '⚪')} {esc((r['headline'] or '')[:110])}" for r in items]


def _prop_lines(signals: list[dict]) -> list[str]:
    from scout.propagation import REL_ZH

    return [f"🔗 {'🟢 利好' if s['impact'] > 0 else '🔴 利空'}（{esc(s['src']['ticker'])} 的{REL_ZH[s['link']['relation']]}，"
            f"置信度 {s.get('confidence')}）：{esc(s['mechanism'])}" for s in signals]


def holding_items(g: dict, asof: date) -> list[dict]:
    """One item per open trade with its lines; attention first, then by position value."""
    from scout.telegram import next_step

    out = []
    for t in g["held_rows"]:
        tk = t["ticker"]
        m = portfolio.metrics(t)
        f = g["feats"].get(tk, {})
        lines, attention = [], False
        if "pnl" in m:
            stop = f" · 止损 {t['stop']:.2f}（{pct(m['to_stop'], 1, True)}）" if "to_stop" in m else ""
            head = (f"<b>{esc(tk)}</b> {m['price']:.2f} · 持仓 {pct(m['pnl_pct'], 1, True)}（{m['pnl']:+,.0f}）{stop}")
        else:
            head = f"<b>{esc(tk)}</b> · 无行情"
        split = market_split_text(f)
        if split:
            lines.append(f"📈 {esc(split)}")
        step = next_step(t, m) if "pnl" in m else ""
        sigma = (f.get("vol_63") or 0) / 252 ** 0.5
        if "to_stop" in m and sigma and -m["to_stop"] < NEAR_STOP_SIGMAS * sigma:
            attention = True
            lines.append(f"⚠️ 距止损 {pct(m['to_stop'], 1, True)}，不到 {NEAR_STOP_SIGMAS:g} 倍日常波动（日波动约 {pct(sigma, 1)}）")
        if step.startswith("⚠️"):
            attention = True
            lines.append(esc(step))
        for msg in g["managed"].get(tk, []):
            attention = True
            lines.append(f"📌 {esc(msg)}")
        if tk in g["earnings"]:
            attention = True
            lines.append(f"📅 财报 {g['earnings'][tk]}（{(g['earnings'][tk] - asof).days} 天后）")
            if tk in g.get("gaps", {}):
                lines.append(f"⚠️ {esc(earnings_gaps.text(g['gaps'][tk], -m['to_stop'] if 'to_stop' in m else None))}")
        props = [s for s in g["prop"] if s["ticker"] == tk]
        if any(s["impact"] < 0 for s in props):
            attention = True
        lines += _prop_lines(props)
        lines += _news_lines(g["news"].get(tk, []))
        value = (m.get("price") or 0) * m.get("remaining", 0)
        out.append({"ticker": tk, "sector": t.get("sector"), "head": head, "lines": lines, "attention": attention,
                    "value": value, "pnl": m.get("pnl")})
    return sorted(out, key=lambda x: (not x["attention"], -x["value"]))


def watch_items(g: dict, asof: date) -> list[dict]:
    """Watchlist names that changed today: a large company-own move, a hard-event article, a pass-through hint or
    earnings soon (ordinary news alone is not a change); the biggest own moves first."""
    out = []
    for tk in g["watch"]:
        f = g["feats"].get(tk, {})
        own = f.get("own_ret_1d") if f.get("own_ret_1d") is not None else f.get("idio_ret_1d")
        props = [s for s in g["prop"] if s["ticker"] == tk]
        moved = own is not None and abs(own) >= WATCH_MOVE
        hard = [r for r in g["news"].get(tk, []) if r.get("is_hard_event")]
        if not (moved or props or hard or tk in g["earnings"]):
            continue
        lines = []
        split = market_split_text(f)
        if split:
            lines.append(f"📈 {esc(split)}")
        if tk in g["earnings"]:
            lines.append(f"📅 财报 {g['earnings'][tk]}（{(g['earnings'][tk] - asof).days} 天后）")
        lines += _prop_lines(props) + _news_lines(g["news"].get(tk, []))
        out.append({"ticker": tk, "head": f"<b>{esc(tk)}</b> {esc((g['info'].get(tk) or {}).get('name') or '')}".rstrip(),
                    "lines": lines, "rank": (-abs(own) if own is not None else 0.0)})
    return sorted(out, key=lambda x: x["rank"])


def render(asof: date, market: dict, sector_rows: list[dict], focus: dict | None, mfilter: dict | None,
           g: dict, satellite: dict | None, scan_lines: list[str]) -> str:
    from scout import market_filter
    from scout.propagation import PUSH_CONFIDENCE, REL_ZH

    lines = [f"📋 <b>Scout 收盘汇总 {asof}</b>", "\n<b>一、市场</b>",
             esc(opportunities.state_text(market, detail=True)),
             f"SPY 当日 {pct(market.get('spy_ret_1d'), 1, True)}，20 日 {pct(market.get('spy_ret_20d'), 1, True)} | "
             f"QQQ 20 日 {pct(market.get('qqq_ret_20d'), 1, True)} | IWM 20 日 {pct(market.get('iwm_ret_20d'), 1, True)} | "
             f"SMH 20 日 {pct(market.get('smh_ret_20d'), 1, True)}"]
    if mfilter:
        lines.append(("🛑 " if mfilter["off"] else "🟢 ") + esc(market_filter.text(mfilter)))
    if focus and focus.get("focus"):
        f = focus["focus"]
        lines.append(f"🧭 市场关注点：{esc(f['headline'])}")
        lines += [f"{i}. {esc(t['title'])}" for i, t in enumerate(f.get("themes", []), 1)]

    lines.append("\n<b>二、板块</b>")
    rotation = sectors.rotation_line(sector_rows)
    if rotation:
        lines.append(esc(rotation))
    held = holding_items(g, asof)
    by_sector = {r["sector"]: r for r in sector_rows if r.get("kind") == "sector"}
    mine = sorted({h["sector"] for h in held if h.get("sector") in by_sector})
    for s in mine:
        r = by_sector[s]
        names = "、".join(h["ticker"] for h in held if h.get("sector") == s)
        lines.append(f"• {esc(sector_text(sectors.card_context(r)) or r['name'])}；当日 {pct(r.get('ret_1d'), 1, True)}，"
                     f"20 日相对 SPY {pct(r.get('rel_20'), 1, True)}（持有 {esc(names)}）")
    if not mine:
        lines.append("（持仓所在板块：无持仓）" if not held else "（持仓所在板块暂无数据）")

    lines.append("\n<b>三、我的持仓</b>（需要注意的在前，其余按市值）")
    if not held:
        lines.append("没有记录的持仓。")
    for h in held:
        lines.append(("⚠️ " if h["attention"] else "• ") + h["head"])
        lines += [f"  {x}" for x in h["lines"]]
    pnl = [h["pnl"] for h in held if h["pnl"] is not None]
    if pnl:
        lines.append(f"合计盈亏 {sum(pnl):+,.0f}（含已实现）")
    if satellite:
        lines.append(f"卫星仓 ${satellite['used']:,.0f}，占账户 {satellite['pct'] * 100:.1f}%（上限 {satellite['cap_pct'] * 100:.0f}%"
                     + ("，超出，按计划不新买入个股）" if satellite["over"] else f"，剩余额度 ${satellite['room']:,.0f}）"))

    watched = watch_items(g, asof)
    if watched:
        lines.append("\n<b>四、自选股变化</b>")
        for w in watched[:WATCH_MAX]:
            lines.append("• " + w["head"])
            lines += [f"  {x}" for x in w["lines"]]
        if len(watched) > WATCH_MAX:
            lines.append(f"另有 {len(watched) - WATCH_MAX} 只有变化，问助手或看 Dashboard。")

    mine_or_watch = {h["ticker"] for h in held} | set(g["watch"])
    others = [s for s in g["prop"] if s["ticker"] not in mine_or_watch and s.get("confidence") in PUSH_CONFIDENCE]
    lines.append("\n<b>五、其他</b>")
    if others:
        lines.append(f"🔗 产业链传导提示 {len(others)} 条（推断，未经检验，记入判断台账打分），例如：")
        for s in others[:OTHER_PROP_MAX]:
            lines.append(f"• <b>{esc(s['ticker'])}</b>（{esc(s['src']['ticker'])} 的{REL_ZH[s['link']['relation']]}）"
                         f"{'🟢 利好' if s['impact'] > 0 else '🔴 利空'}：{esc(s['mechanism'])}")
    lines += scan_lines
    return "\n".join(lines)
