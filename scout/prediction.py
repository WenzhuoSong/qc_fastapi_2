"""Prediction-market odds as context (user's decision 2026-11-04): what Polymarket and Kalshi price for the events that
can move the market, the sectors or the holdings — read-only, never a signal.

Which markets (decided by rules, not by results):
1. calendar — every 高 event of the next 30 days: FOMC → Kalshi KXFEDDECISION + Polymarket "Fed decision";
   data releases by fixed series (payrolls, unemployment, CPI, GDP); other events by an English query and a relevance
   check that the market resolves on the same event;
2. macro — a fixed topic list (recession, the Fed's path, shutdown, tariffs, Iran, Russia–Ukraine, oil, China–Taiwan);
3. holding — markets about a held company;
4. discovery — the day's highest-volume Polymarket events, kept when a relevance check says they could move US stocks.
Only events with ≥ MIN_VOLUME traded are followed (thin books are easy to push around).

When to look: snapshots before the open, during the session (events within NEAR_DAYS and the holdings' markets) and after
the close, all stored point in time. A move is ≥ MOVE pts in a day or a crossing of 50 % (≥ 5 pts); each is reported
once, with a cause search through the facts store and the sectors / holdings it touches.
"""
from __future__ import annotations

import json
import logging
import re
from datetime import date, timedelta

from scout import db
from scout.config import get_scout_settings
from scout.data import prediction as feed

logger = logging.getLogger("scout.prediction")

MIN_VOLUME = 1_000_000           # dollars (Polymarket) / contracts (Kalshi) traded over the event's life
MOVE = 0.15                      # a day's move in probability that is reported
CROSS_MIN = 0.05                 # a crossing of 50 % counts when the move is at least this
CAL_DAYS = 30
NEAR_DAYS = 2                    # events this close are refreshed during the session
LOOKBACK_HOURS = 20
MAX_CAUSE = 3
DISCOVERY_SCAN = 50
DISCOVERY_KEEP = 10
DISCOVERY_DAYS = 120             # discovery keeps events resolving within this many days
SHOWN = 3                        # outcomes shown per event

FF_SERIES = {"Non-Farm Employment Change": "KXPAYROLLS", "Unemployment Rate": "KXU3", "CPI m/m": "KXCPI", "CPI y/y": "KXCPIYOY",
             "Advance GDP q/q": "KXGDP"}

MACRO_TOPICS = (
    {"id": "recession", "name": "美国经济衰退", "queries": ("US recession 2026",), "kalshi": ("KXRECSSNBER",),
     "sectors": ("Financials", "Consumer Discretionary", "Industrials")},
    {"id": "fed_path", "name": "美联储利率路径", "queries": ("Fed rate hikes 2026", "Fed rate cuts 2026"), "kalshi": ("KXFED",),
     "sectors": ("Financials", "Real Estate", "Utilities", "Information Technology")},
    {"id": "shutdown", "name": "政府停摆", "queries": ("government shutdown",), "kalshi": (), "sectors": ("Industrials",)},
    {"id": "tariffs", "name": "关税", "queries": ("tariffs",), "kalshi": (),
     "sectors": ("Consumer Discretionary", "Industrials", "Materials", "Information Technology")},
    {"id": "iran", "name": "伊朗局势", "queries": ("Iran",), "kalshi": (), "sectors": ("Energy",)},
    {"id": "russia_ukraine", "name": "俄乌停火", "queries": ("Russia Ukraine ceasefire",), "kalshi": (), "sectors": ("Energy", "Industrials")},
    {"id": "oil", "name": "油价", "queries": ("oil price", "WTI crude"), "kalshi": (), "sectors": ("Energy",)},
    {"id": "china_taiwan", "name": "中国台湾局势", "queries": ("China Taiwan",), "kalshi": (), "sectors": ("Information Technology",)},
)
TOPIC_NAME = {t["id"]: t["name"] for t in MACRO_TOPICS}


def key_of(ev: dict) -> str:
    return f"{ev['source']}:{ev['id']}"


def liquid(ev: dict) -> bool:
    """Enough traded, and not decided in all but name (some outcome still between 1 % and 99 %)."""
    open_ = any(o.get("prob") is not None and 0.01 < o["prob"] < 0.99 for o in ev.get("outcomes") or [])
    return (ev.get("volume") or 0) >= MIN_VOLUME and open_


def shown(outcomes: list[dict]) -> list[dict]:
    """The outcomes worth a line. Settled-in-all-but-name ones (≥ 99 % / ≤ 1 %) are left out; for threshold ladders
    ("Above 50,000", …) the rungs nearest 50 %; otherwise the most likely ones of at least 3 %; never nothing."""
    live = [o for o in outcomes if o.get("prob") is not None and 0.01 < o["prob"] < 0.99]
    if live and all(re.match(r"(above|below|over|under|more than|less than|>|<)", (o["label"] or "").strip(), re.I) for o in live):
        pick = sorted(live, key=lambda o: abs(o["prob"] - 0.5))[:SHOWN]
    else:
        pick = [o for o in live if o["prob"] >= 0.03][:SHOWN * 3]
    if not pick:
        pick = sorted((o for o in outcomes if o.get("prob") is not None), key=lambda o: -o["prob"])[:1]
    return sorted(pick, key=lambda o: -o["prob"])[:SHOWN]


def move(old: float | None, new: float | None) -> float | None:
    """The reportable change: ≥ MOVE, or a crossing of 50 % by ≥ CROSS_MIN; None otherwise."""
    if old is None or new is None:
        return None
    d = new - old
    crossed = (old < 0.5 <= new) or (new < 0.5 <= old)
    return d if abs(d) >= MOVE or (crossed and abs(d) >= CROSS_MIN) else None


# ── storage ──

async def track(ev: dict, topic: str, link: str | None, sectors: tuple | list = (), tickers: tuple | list = (),
                relevance: dict | None = None) -> str:
    k = key_of(ev)
    await db.execute(
        "INSERT INTO scout.prediction_events (key, source, title, url, end_date, topic, link, sectors, tickers, rules, relevance) "
        "VALUES (:k, :s, :t, :u, :e, :topic, :link, :sec, :tk, :r, CAST(:rel AS JSONB)) "
        "ON CONFLICT (key) DO UPDATE SET title = EXCLUDED.title, end_date = EXCLUDED.end_date, last_seen = now(), active = true, "
        # a calendar link wins over a topic the event was first found under
        "topic = CASE WHEN EXCLUDED.topic = 'calendar' THEN 'calendar' ELSE scout.prediction_events.topic END, "
        "link = CASE WHEN EXCLUDED.topic = 'calendar' THEN EXCLUDED.link ELSE scout.prediction_events.link END, "
        "relevance = coalesce(scout.prediction_events.relevance, EXCLUDED.relevance)",
        {"k": k, "s": ev["source"], "t": ev["title"], "u": ev["url"], "e": ev.get("end_date"), "topic": topic, "link": link,
         "sec": list(sectors), "tk": list(tickers), "r": ev.get("rules"), "rel": db.jsonb(relevance) if relevance else None})
    await snapshot(ev)
    return k


async def snapshot(ev: dict) -> None:
    rows = [{"k": key_of(ev), "o": o["id"], "l": o["label"], "p": o.get("prob"), "v": o.get("volume"), "v24": o.get("volume_24h"),
             "ev": ev.get("volume")} for o in ev["outcomes"]]
    if rows:
        await db.execute_many(
            "INSERT INTO scout.prediction_snapshots (event_key, outcome_id, label, prob, volume, volume_24h, event_volume) "
            "VALUES (:k, :o, :l, :p, :v, :v24, :ev)", rows)


async def tracked(active_only: bool = True) -> list[dict]:
    return await db.fetch_all("SELECT * FROM scout.prediction_events" + (" WHERE active" if active_only else "") + " ORDER BY key")


async def fetch(key: str) -> dict | None:
    source, _, id_ = key.partition(":")
    return await (feed.polymarket_get(id_) if source == "polymarket" else feed.kalshi_get(id_))


# ── relevance (one structured call per batch; cached on the event) ──

REL_SCHEMA = {
    "type": "object", "additionalProperties": False, "required": ["items"],
    "properties": {"items": {"type": "array", "items": {
        "type": "object", "additionalProperties": False, "required": ["n", "relevant", "title_zh", "why_zh", "sectors"],
        "properties": {"n": {"type": "integer"}, "relevant": {"type": "boolean"},
                       "title_zh": {"type": "string", "description": "市场题目的中文翻译，≤ 30 字"},
                       "why_zh": {"type": "string", "description": "一句：它和美股 / 这些板块有什么关系；不相关写原因"},
                       "sectors": {"type": "array", "items": {"type": "string", "enum": [
                           "Information Technology", "Financials", "Health Care", "Energy", "Industrials", "Consumer Staples",
                           "Consumer Discretionary", "Utilities", "Materials", "Real Estate", "Communication Services"]}}}}}},
}


async def relevance(candidates: list[dict], question: str) -> dict[str, dict]:
    """{event key: {relevant, title_zh, why_zh, sectors}} for the candidates, judged against `question`."""
    from scout import responses

    if not candidates or not get_scout_settings().openai_api_key:
        return {}
    lines = [f"{i}. {c['title']}（截止 {c.get('end_date')}）：{'；'.join(o['label'] for o in c['outcomes'][:6])}" for i, c in enumerate(candidates)]
    try:
        data = await responses.create({
            "model": get_scout_settings().scout_parse_model,
            "instructions": "逐个判断预测市场是否符合要求，n 用列表序号。拿不准就判为不相关。" + question,
            "input": [{"role": "user", "content": "\n".join(lines)}],
            "text": {"format": {"type": "json_schema", "name": "relevance", "schema": REL_SCHEMA, "strict": True}}},
            timeout=120, purpose="prediction_relevance")
        items = json.loads(responses.output_text(data) or "{}").get("items", [])
    except Exception as exc:
        logger.warning("prediction relevance failed: %s", exc)
        return {}
    return {key_of(candidates[x["n"]]): x for x in items if 0 <= x["n"] < len(candidates)}


# ── which markets ──

async def link_calendar(today: date) -> int:
    from scout import event_calendar

    n = 0
    events = [e for e in await event_calendar.load(today, today + timedelta(days=CAL_DAYS)) if e["importance"] == "高"]
    for e in events:
        d, title_en = e["event_date"], (e["detail"] or {}).get("title_en") or ""
        found: list[dict] = []
        if e["kind"] == "fomc":
            for t in await feed.kalshi_series("KXFEDDECISION"):
                ev = await feed.kalshi_get(t)
                if ev and ev["end_date"] == d:
                    found.append(ev)
            found += [x for x in await feed.polymarket_search(f"Fed decision {d:%B}")
                      if "fed decision" in x["title"].lower() and x["end_date"] and 0 <= (x["end_date"] - d).days <= 2]
        elif e["source"] == "ff" and title_en in FF_SERIES:
            for t in await feed.kalshi_series(FF_SERIES[title_en]):
                ev = await feed.kalshi_get(t)
                if ev and ev["end_date"] == d:
                    found.append(ev)
        else:                         # elections, policy and other dated events: search, then check it is the same event
            from scout import responses

            query = title_en or e["title"]
            if not title_en:
                try:
                    data = await responses.create({"model": get_scout_settings().scout_parse_model,
                                                   "instructions": "把这个事件改写成 3–6 个英文关键词，用于在 Polymarket 搜索，只输出关键词。",
                                                   "input": [{"role": "user", "content": e["title"]}]}, timeout=60, purpose="prediction_query")
                    query = responses.output_text(data).strip()[:80] or query
                except Exception:
                    pass
            cands = [x for x in await feed.polymarket_search(query) if liquid(x) and x["end_date"]
                     and -1 <= (x["end_date"] - d).days <= 14 and not await _known(x)]
            rel = await relevance(cands, f"要求：市场结算的是 {d} 的这件事——{e['title']}（同一件事，不是相关话题）。")
            for x in cands:
                r = rel.get(key_of(x))
                if r and r["relevant"]:
                    await track(x, "calendar", e["key"], r["sectors"], (), r)
                    n += 1
            continue
        for ev in found:
            if liquid(ev) or ev["source"] == "kalshi":       # the calendar's own series are followed even when young
                await track(ev, "calendar", e["key"], (), (), {"title_zh": e["title"] + "（预测市场）", "why_zh": (e["detail"] or {}).get("why")})
                n += 1
    return n


async def _known(ev: dict) -> bool:
    return bool(await db.fetch_one("SELECT 1 AS x FROM scout.prediction_events WHERE key = :k", {"k": key_of(ev)}))


async def link_macro(today: date) -> int:
    n = 0
    for t in MACRO_TOPICS:
        cands: list[dict] = []
        for s in t["kalshi"]:
            for tk in (await feed.kalshi_series(s))[:2]:
                ev = await feed.kalshi_get(tk)
                if ev and liquid(ev):
                    cands.append(ev)
        for q in t["queries"]:
            cands += [x for x in await feed.polymarket_search(q) if liquid(x) and x["end_date"] and 0 <= (x["end_date"] - today).days <= 400]
        new = [c for c in {key_of(c): c for c in cands}.values() if not await _known(c)]
        rel = await relevance(new, f"要求：市场与主题「{t['name']}」直接相关，并且结果会影响美股或这些板块：{', '.join(t['sectors'])}。")
        kept = sorted((c for c in new if (rel.get(key_of(c)) or {}).get("relevant")), key=lambda c: -(c["volume"] or 0))[:2]
        for c in kept:
            r = rel[key_of(c)]
            await track(c, "macro", t["id"], r["sectors"] or t["sectors"], (), r)
            n += 1
    return n


async def link_holdings(today: date) -> int:
    from scout import levels

    n = 0
    rows = await db.fetch_all("SELECT DISTINCT t.ticker, i.name FROM scout.trades t LEFT JOIN scout.instruments i USING (ticker) "
                              "WHERE t.closed_on IS NULL")
    for r in rows:
        if levels.is_core(r["ticker"]) or not r["name"]:
            continue
        cands = [x for x in await feed.polymarket_search(r["name"].split(",")[0]) if liquid(x) and not await _known(x)]
        rel = await relevance(cands, f"要求：市场直接关于 {r['name']}（{r['ticker']}）这家公司（业绩、产品、监管、并购、股价等），不是只提到它。")
        for c in cands:
            x = rel.get(key_of(c))
            if x and x["relevant"]:
                await track(c, "holding", r["ticker"], x["sectors"], (r["ticker"],), x)
                n += 1
    return n


async def discover(today: date) -> int:
    cands = [x for x in await feed.polymarket_top(DISCOVERY_SCAN) if liquid(x) and x["end_date"]
             and 0 <= (x["end_date"] - today).days <= DISCOVERY_DAYS and not await _known(x)]
    rel = await relevance(cands, "要求：结果会在未来几个月内揭晓，并且可能明显影响美股整体或某个板块（宏观、政策、地缘、利率、商品、大公司）；"
                                 "几年后的选举和候选人提名、单个州的选举、体育、娱乐、加密币价格、个人八卦一律不相关。")
    kept = sorted((c for c in cands if (rel.get(key_of(c)) or {}).get("relevant")), key=lambda c: -(c["volume_24h"] or 0))[:DISCOVERY_KEEP]
    for c in kept:
        r = rel[key_of(c)]
        await track(c, "discovery", None, r["sectors"], (), r)
    return len(kept)


# ── refresh, moves ──

async def refresh(today: date, near_only: bool = False) -> int:
    """New snapshots of the followed events (only the near / holding ones during the session); closed events stop."""
    rows = await tracked()
    if near_only:
        from scout import event_calendar

        near = {e["key"] for e in await event_calendar.load(today, today + timedelta(days=NEAR_DAYS))}
        rows = [r for r in rows if r["topic"] == "holding" or r["link"] in near]
    n = 0
    for r in rows:
        ev = await fetch(r["key"])
        if ev is None:
            if r["end_date"] and r["end_date"] < today:
                await db.execute("UPDATE scout.prediction_events SET active = false WHERE key = :k", {"k": r["key"]})
            continue
        await snapshot(ev)
        n += 1
    return n


async def changes(hours: int = LOOKBACK_HOURS) -> list[dict]:
    """Per followed outcome: the latest price and the price ≥ `hours` ago (the oldest when the history is shorter)."""
    return await db.fetch_all(
        "WITH last AS (SELECT DISTINCT ON (event_key, outcome_id) event_key, outcome_id, label, prob, event_volume, captured_at "
        "  FROM scout.prediction_snapshots ORDER BY event_key, outcome_id, captured_at DESC), "
        "before AS (SELECT DISTINCT ON (s.event_key, s.outcome_id) s.event_key, s.outcome_id, s.prob, s.captured_at "
        "  FROM scout.prediction_snapshots s WHERE s.captured_at <= now() - make_interval(hours => :h) "
        "  ORDER BY s.event_key, s.outcome_id, s.captured_at DESC) "
        "SELECT l.*, b.prob AS prob_before, b.captured_at AS before_at, e.title, e.url, e.topic, e.link, e.sectors, e.tickers, e.relevance, e.source "
        "FROM last l JOIN scout.prediction_events e ON e.key = l.event_key AND e.active "
        "LEFT JOIN before b USING (event_key, outcome_id)", {"h": hours})


async def moves(today: date) -> list[dict]:
    out = []
    for r in await changes():
        if (r["event_volume"] or 0) < MIN_VOLUME:
            continue
        d = move(r["prob_before"], r["prob"])
        if d is not None:
            out.append({**r, "delta": d})
    return sorted(out, key=lambda r: -abs(r["delta"]))


async def report_moves(today: date) -> list[dict]:
    """Claim each move once (scout.alerts), search the cause for the largest ones."""
    from scout import external_facts

    fresh = []
    for m in await moves(today):
        k = f"pm:{m['event_key']}:{m['outcome_id']}:{today}"
        if await db.fetch_one("SELECT 1 AS x FROM scout.alerts WHERE key = :k", {"k": k}):
            continue
        payload = {"title": title_zh(m), "label": m["label"], "prob": m["prob"], "before": m["prob_before"], "delta": m["delta"],
                   "url": m["url"], "sectors": m["sectors"], "tickers": m["tickers"], "topic": m["topic"]}
        if len(fresh) < MAX_CAUSE:
            found = await external_facts.collect(
                f"Today is {today}. On the prediction market question '{m['title']}', the probability of '{m['label']}' moved from "
                f"{(m['prob_before'] or 0) * 100:.0f}% to {(m['prob'] or 0) * 100:.0f}% in the last day. What news caused it?",
                ["MACRO", *m["tickers"]], "prediction_cause", today=today)
            ok = [f for f in found["facts"] if f["status"] in ("verified", "unreachable")]
            payload["cause"] = [{"claim": f["claim"] if f["status"] == "verified" else "有报道称：" + f["claim"],
                                 "source": external_facts.label(f)} for f in ok[:2]]
        await db.execute("INSERT INTO scout.alerts (key, kind, ticker, payload) VALUES (:k, 'pm_move', :t, CAST(:p AS JSONB)) "
                         "ON CONFLICT (key) DO NOTHING", {"k": k, "t": (m["tickers"] or [None])[0], "p": db.jsonb(payload)})
        fresh.append(payload)
    return fresh


# ── reading back ──

def title_zh(r: dict) -> str:
    return ((r.get("relevance") or {}).get("title_zh") or r.get("title") or "").strip()


async def board(link: str | None = None, topic: str | None = None, week: bool = True) -> list[dict]:
    """Followed events with their shown outcomes, now and a week ago (from snapshots, else the feed's own week change)."""
    rows = await changes(hours=24 * 7 if week else LOOKBACK_HOURS)
    by: dict[str, dict] = {}
    for r in rows:
        if (link and r["link"] != link) or (topic and r["topic"] != topic):
            continue
        e = by.setdefault(r["event_key"], {"key": r["event_key"], "title": title_zh(r), "title_en": r["title"], "url": r["url"],
                                            "source": r["source"], "topic": r["topic"], "link": r["link"], "sectors": r["sectors"],
                                            "volume": r["event_volume"], "outcomes": []})
        e["outcomes"].append({"label": r["label"], "prob": r["prob"], "before": r["prob_before"], "since": r["before_at"]})
    for e in by.values():
        e["outcomes"] = shown(e["outcomes"])
    return sorted(by.values(), key=lambda e: -(e["volume"] or 0))


async def before_event(link: str, d: date) -> list[dict]:
    """The odds of the events linked to a calendar event as last seen before its day (for the review)."""
    rows = await db.fetch_all(
        "SELECT DISTINCT ON (s.event_key, s.outcome_id) s.event_key, s.label, s.prob, s.event_volume, e.title, e.source, e.relevance "
        "FROM scout.prediction_snapshots s JOIN scout.prediction_events e ON e.key = s.event_key "
        "WHERE e.link = :l AND s.captured_at < CAST(:d AS DATE) ORDER BY s.event_key, s.outcome_id, s.captured_at DESC", {"l": link, "d": d})
    by: dict[str, dict] = {}
    for r in rows:
        e = by.setdefault(r["event_key"], {"key": r["event_key"], "title": title_zh(r), "source": r["source"], "volume": r["event_volume"],
                                            "outcomes": []})
        e["outcomes"].append({"label": r["label"], "prob": r["prob"], "before": None})
    for e in by.values():
        e["outcomes"] = shown(e["outcomes"])
    return list(by.values())


def _pct(x: float | None) -> str:
    return "—" if x is None else f"{x * 100:.0f}%"


def event_text(e: dict) -> str:
    src = {"polymarket": "Polymarket", "kalshi": "Kalshi"}[e["source"]]
    vol = f"成交 {e['volume'] / 1e6:.1f} 百万" if e.get("volume") else ""
    outs = "；".join(f"{o['label']} {_pct(o['prob'])}" + (f"（一周前 {_pct(o['before'])}）" if o.get("before") is not None else "")
                    for o in e["outcomes"])
    return f"{e['title']}（{src}，{vol}）：{outs}"


def pack_rows(events: list[dict]) -> list[dict]:
    """For an LLM pack: odds as formatted strings with what they are."""
    return [{"市场": e["title"], "来源": e["source"], "成交量": f"{(e['volume'] or 0) / 1e6:.1f} 百万",
             "各结果概率": [{"结果": o["label"], "现在": _pct(o["prob"]), "一周前": _pct(o.get("before"))} for o in e["outcomes"]],
             "说明": "预测市场价格（赌这件事发生的合约价），大致可看作市场认为的概率；成交小的市场容易被少数人左右，不是 Scout 的判断"}
            for e in events]


def move_line(m: dict) -> str:
    cause = "；".join(f"{c['claim']}（{c['source']}）" for c in m.get("cause") or []) or "原因未查到"
    tick = f"，相关持仓 {'、'.join(m['tickers'])}" if m.get("tickers") else ""
    return (f"🎲 {m['title']}：「{m['label']}」{_pct(m['before'])} → {_pct(m['prob'])}"
            f"（{m['delta'] * 100:+.0f} 个百分点{tick}）· {cause}")


async def run(today: date, mode: str) -> dict:
    """premarket: link (calendar, macro, holdings, discovery) + snapshot all; intraday: near events + moves;
    eod: snapshot all + moves (for the digest)."""
    out: dict = {}
    if mode == "premarket":
        for name, fn in (("calendar", link_calendar), ("macro", link_macro), ("holdings", link_holdings), ("discovery", discover)):
            try:
                out[name] = await fn(today)
            except Exception as exc:
                logger.warning("prediction %s failed: %s", name, exc)
                out[name] = f"failed: {type(exc).__name__}"
    out["refreshed"] = await refresh(today, near_only=mode == "intraday")
    if mode in ("intraday", "eod"):
        out["moves"] = await report_moves(today)
    return out
