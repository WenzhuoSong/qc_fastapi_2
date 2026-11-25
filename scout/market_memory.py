"""Memory for the daily market read (user's request 2026-11-26: explain today against the recent history, not alone).

Three layers handed to the read: the last RECENT reads (headline, drivers with strength, story status), the market
themes (`scout.market_themes`: each driver names its theme; a theme keeps its start, the days it was a driver, the
symbols its evidence named and, computed here, how they moved since it started; FADE_SESSIONS without it = faded,
seen again = active again) and the last WEEKS weekly market summaries. Python keeps the bookkeeping; the model only
names the theme of each driver (an existing title verbatim, or a new one).
"""
from __future__ import annotations

from datetime import date

import pandas as pd

from scout import db

RECENT = 10
WEEKS = 4
FADE_SESSIONS = 10
FADED_SHOWN_DAYS = 45              # faded themes stay in the pack this long (calendar days), so a return is recognised


def anchors_of(evidence: list[str]) -> list[str]:
    """Symbols named by evidence keys: move.SMH → SMH, move.TNX → ^TNX, global.N225 → ^N225, global.000001SS → 000001.SS."""
    from scout import global_markets as gm

    clean = {s.strip("^").replace("=", "").replace(".", ""): s for s, *_ in gm.ALL}
    out = []
    for k in evidence:
        kind, _, name = k.partition(".")
        if kind == "move" and name:
            out.append(f"^{name}" if name in ("TNX", "VIX", "VIX3M") else name)
        elif kind == "global" and name in clean:
            out.append(clean[name])
    return list(dict.fromkeys(out))


def since(series: pd.Series | None, start: date, asof: date, level: bool = False) -> float | None:
    """Move from the last close before `start` to the close on or before `asof` (a level change for yields)."""
    if series is None:
        return None
    s = series.dropna()
    before, upto = s[s.index < start], s[s.index <= asof]
    if not len(before) or not len(upto):
        return None
    return float(upto.iloc[-1] - before.iloc[-1]) if level else float(upto.iloc[-1] / before.iloc[-1] - 1)


async def recent(asof: date) -> list[dict]:
    rows = await db.fetch_all("SELECT DISTINCT ON (asof) asof, payload FROM scout.briefs WHERE kind = 'market_read' AND asof < :d "
                              "ORDER BY asof DESC, id DESC LIMIT :n", {"d": asof, "n": RECENT})
    out = []
    for r in rows:
        rd = (r["payload"] or {}).get("read") or {}
        out.append({"日期": str(r["asof"]), "标题": rd.get("headline"), "故事线": rd.get("story_status"),
                    "推动因素": [f"{d['title']}（{d['strength']}" + (f"，主线：{d['theme']}" if d.get("theme") else "") + "）"
                                 for d in rd.get("drivers") or []]})
    return out


async def weekly(asof: date) -> list[dict]:
    rows = await db.fetch_all("SELECT asof, payload->'market'->>'week' AS week FROM scout.briefs WHERE kind = 'weekly_view' AND asof < :d "
                              "AND payload->'market'->>'week' IS NOT NULL ORDER BY asof DESC LIMIT :n", {"d": asof, "n": WEEKS})
    return [{"周": str(r["asof"]), "概述": r["week"]} for r in rows]


async def themes(asof: date, series: dict[str, pd.Series]) -> list[dict]:
    """Active themes and those faded within FADED_SHOWN_DAYS, newest first, with their anchors' moves since the start."""
    rows = await db.fetch_all(
        "SELECT * FROM scout.market_themes WHERE status = 'active' OR last_seen >= CAST(:d AS DATE) - :k ORDER BY last_seen DESC, id DESC",
        {"d": asof, "k": FADED_SHOWN_DAYS})
    out = []
    for i, r in enumerate(rows, 1):
        moves = []
        for sym in r["anchors"] or []:
            level = sym == "^TNX"
            x = since(series.get(sym), r["first_seen"], asof, level)
            if x is not None:
                moves.append(f"{sym} 开始以来 {x:+.2f} 个百分点" if level else f"{sym} 开始以来 {x * 100:+.1f}%")
        last = (r["entries"] or [])[-3:]
        out.append({"key": f"theme.{i}", "标题": r["title"], "状态": "进行中" if r["status"] == "active" else "已淡出",
                    "开始": str(r["first_seen"]), "最近一次": str(r["last_seen"]), "作为推动因素的天数": r["days"],
                    "开始以来": moves, "最近几次": [f"{e['date']}（{e['strength']}）{e['explanation']}" for e in last]})
    return out


async def update(drivers: list[dict], asof: date) -> dict:
    """Book today's drivers under their themes; fade the themes not seen for FADE_SESSIONS sessions."""
    from scout.market_calendar import previous_us_equity_trading_day

    seen = {}
    for d in drivers:
        t = (d.get("theme") or "").strip()[:40]
        if not t or t in seen:
            continue
        seen[t] = d
        entry = [{"date": asof.isoformat(), "strength": d["strength"], "explanation": d["explanation"][:200]}]
        await db.execute(
            "INSERT INTO scout.market_themes (title, first_seen, last_seen, days, anchors, entries) "
            "VALUES (:t, :d, :d, 1, CAST(:a AS JSONB), CAST(:e AS JSONB)) "
            "ON CONFLICT (title) DO UPDATE SET status = 'active', updated_at = now(), "
            "days = scout.market_themes.days + (CASE WHEN scout.market_themes.last_seen < EXCLUDED.last_seen THEN 1 ELSE 0 END), "
            "last_seen = GREATEST(scout.market_themes.last_seen, EXCLUDED.last_seen), "
            "anchors = coalesce((SELECT to_jsonb(array_agg(DISTINCT x)) FROM jsonb_array_elements_text(scout.market_themes.anchors || EXCLUDED.anchors) x), '[]'), "
            "entries = (CASE WHEN scout.market_themes.last_seen < EXCLUDED.last_seen THEN scout.market_themes.entries || EXCLUDED.entries "
            "ELSE scout.market_themes.entries END)",
            {"t": t, "d": asof, "a": db.jsonb(anchors_of(d.get("evidence") or [])), "e": db.jsonb(entry)})
    cutoff = asof
    for _ in range(FADE_SESSIONS):
        cutoff = previous_us_equity_trading_day(cutoff)
    await db.execute("UPDATE scout.market_themes SET status = 'faded', updated_at = now() WHERE status = 'active' AND last_seen < :c",
                     {"c": cutoff})
    return {"themes": list(seen)}


async def theme_lines(asof: date) -> list[str]:
    """Active themes for the weekly push: title, start, days as a driver, the anchors' moves since the start."""
    from scout import global_markets as gm

    rows = await db.fetch_all("SELECT anchors FROM scout.market_themes WHERE status = 'active'")
    syms = sorted({a for r in rows for a in (r["anchors"] or [])})
    if not rows:
        return []
    bars = await db.fetch_all("SELECT ticker, date, close FROM scout.bars_daily WHERE ticker = ANY(:t) AND date >= CAST(:d AS DATE) - 400",
                              {"t": syms, "d": asof})
    series: dict[str, pd.Series] = {}
    for t in syms:
        sub = {b["date"]: b["close"] for b in bars if b["ticker"] == t}
        if sub:
            series[t] = pd.Series(sub).sort_index()
    abroad = [x for x in gm.ALL if x[0] in syms]
    if abroad:
        series.update(await gm.closes(date.fromordinal(asof.toordinal() - 400), tuple(abroad)))
    return [f"{t['标题']}：{t['开始'][5:]} 开始，{t['作为推动因素的天数']} 天" + (f"，{'、'.join(t['开始以来'])}" if t["开始以来"] else "")
            for t in await themes(asof, series) if t["状态"] == "进行中"]
