"""Judgment ledger: what the system said, and how it turned out.

Every judgment is stored with its data date and price: each AI
interpretation (overall and per lens), each opportunity card, each sector's
daily heat. After 5, 20 and 60 sessions it is scored against SPY and its
sector. The calibration table ("when this lens said bullish, how often did
the stock beat SPY over 20 sessions") is shown to the user and to the AI;
with fewer than MIN_SAMPLES it is shown as "not enough samples" and never
used to change anything.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta

import pandas as pd

from scout import db, universe

logger = logging.getLogger("scout.ledger")

HORIZONS = (5, 20, 60)
NEUTRAL_BAND = {5: 0.015, 20: 0.03, 60: 0.06}     # "neutral" is right when the excess stays inside this
MIN_SAMPLES = 30
STANCE = {"偏多": 1, "中性": 0, "偏空": -1}
STANCE_ZH = {1: "偏多", 0: "中性", -1: "偏空"}
HEAT_STANCE = {"热": 1, "温": 0, "冷": -1}
KIND_ZH = {"insight": "AI 解读·综合", "insight_lens": "AI 解读·视角", "opportunity": "规则机会卡", "sector_heat": "板块热度"}


def hit(stance: int, excess: float | None, horizon: int) -> bool | None:
    if excess is None:
        return None
    if stance > 0:
        return excess > 0
    if stance < 0:
        return excess < 0
    return abs(excess) <= NEUTRAL_BAND[horizon]


def overall_stance(verdicts: list[str]) -> int:
    """Net direction of the lens verdicts (不适用 ignored); ties are neutral."""
    score = sum(STANCE.get(v, 0) for v in verdicts)
    return (score > 0) - (score < 0)


async def _close(ticker: str, asof: date) -> float | None:
    row = await db.fetch_one("SELECT close FROM scout.bars_daily WHERE ticker = :t AND date <= :d ORDER BY date DESC LIMIT 1",
                             {"t": ticker, "d": asof})
    return row["close"] if row else None


async def record(kind: str, ticker: str, asof: date, stance: int, lens: str = "", sector_etf: str | None = None,
                 source_ref: str | None = None, model: str | None = None, payload: dict | None = None) -> None:
    """Upsert one judgment; re-generating the same view on the same data date replaces it."""
    await db.execute(
        "INSERT INTO scout.judgments (kind, ticker, asof, lens, stance, entry_price, sector_etf, source_ref, model, payload) "
        "VALUES (:k, :t, :d, :l, :s, :p, :e, :r, :m, CAST(:x AS JSONB)) "
        "ON CONFLICT (kind, ticker, asof, lens) DO UPDATE SET stance = EXCLUDED.stance, source_ref = EXCLUDED.source_ref, "
        "model = EXCLUDED.model, payload = EXCLUDED.payload, made_at = now()",
        {"k": kind, "t": ticker, "d": asof, "l": lens, "s": stance, "p": await _close(ticker, asof), "e": sector_etf,
         "r": source_ref, "m": model, "x": db.jsonb(payload or {})})


async def record_insight(rec: dict) -> None:
    ins, pack = rec["insight"], rec["pack"]
    asof = date.fromisoformat(str(rec["asof"]))
    inst = await db.fetch_one("SELECT sector FROM scout.instruments WHERE ticker = :t", {"t": rec["ticker"]}) or {}
    etf = universe.SECTOR_ETFS.get(inst.get("sector") or "")
    model = rec.get("model") or rec.get("source")
    verdicts = [lv["verdict"] for lv in ins["lenses"]]
    await record("insight", rec["ticker"], asof, overall_stance(verdicts), sector_etf=etf, source_ref="ticker_ai", model=model,
                 payload={"headline": ins["headline"], "recent_event": ins.get("recent_event"), "confidence": ins["confidence"],
                          "source": rec.get("source"), "verdicts": {lv["lens"]: lv["verdict"] for lv in ins["lenses"]}})
    for lv in ins["lenses"]:
        if lv["verdict"] in STANCE:
            await record("insight_lens", rec["ticker"], asof, STANCE[lv["verdict"]], lens=lv["lens"], sector_etf=etf,
                         source_ref="ticker_ai", model=model, payload={"points": lv["points"][:3]})


async def record_opportunity(opp: dict, sector_etf: str | None) -> None:
    await record("opportunity", opp["ticker"], opp["asof"], 1, lens=opp["primary_detector"], sector_etf=sector_etf,
                 source_ref=opp["id"], payload={"score": opp.get("score"), "entry": [opp.get("entry_lo"), opp.get("entry_hi")],
                                                "stop": opp.get("stop"), "target1": opp.get("target1")})


async def record_sector_heat(rows: list[dict], asof: date) -> int:
    n = 0
    for r in rows:
        if r.get("kind") == "sector" and r.get("heat_label") in HEAT_STANCE:
            await record("sector_heat", r["key"], asof, HEAT_STANCE[r["heat_label"]], payload={"heat": r.get("heat"),
                         "quadrant": r.get("quadrant")})
            n += 1
    return n


async def backfill_opportunities() -> int:
    """Opportunity cards made before the ledger existed."""
    rows = await db.fetch_all(
        "SELECT o.*, i.sector FROM scout.opportunities o LEFT JOIN scout.instruments i USING (ticker) "
        "WHERE NOT EXISTS (SELECT 1 FROM scout.judgments j WHERE j.kind = 'opportunity' AND j.source_ref = o.id)")
    for o in rows:
        await record_opportunity(o, universe.SECTOR_ETFS.get(o.get("sector") or ""))
    return len(rows)


# ── scoring ──

def forward(closes: pd.Series, asof: date, h: int) -> tuple[date, float] | None:
    """(end date, return) h sessions after the last close on or before asof."""
    idx = closes.index.searchsorted(asof, side="right") - 1
    if idx < 0 or idx + h >= len(closes):
        return None
    return closes.index[idx + h], float(closes.iloc[idx + h] / closes.iloc[idx] - 1)


async def score_due() -> int:
    """Score every (judgment, horizon) whose horizon has elapsed."""
    due = await db.fetch_all(
        "SELECT j.id, j.ticker, j.asof, j.stance, j.sector_etf, h.horizon FROM scout.judgments j "
        "CROSS JOIN (SELECT unnest(CAST(:hs AS int[])) AS horizon) h "
        "WHERE NOT EXISTS (SELECT 1 FROM scout.judgment_outcomes o WHERE o.judgment_id = j.id AND o.horizon = h.horizon)",
        {"hs": list(HORIZONS)})
    if not due:
        return 0
    tickers = sorted({d["ticker"] for d in due} | {d["sector_etf"] for d in due if d["sector_etf"]} | {universe.BENCHMARK})
    since = min(d["asof"] for d in due)
    bars = await db.fetch_all("SELECT ticker, date, close FROM scout.bars_daily WHERE ticker = ANY(:t) AND date >= :d ORDER BY date",
                              {"t": tickers, "d": since - timedelta(days=7)})
    closes: dict[str, pd.Series] = {}
    for t in tickers:
        sub = [(b["date"], b["close"]) for b in bars if b["ticker"] == t]
        if sub:
            closes[t] = pd.Series(dict(sub)).sort_index()
    spy = closes.get(universe.BENCHMARK)
    rows = []
    for d in due:
        c = closes.get(d["ticker"])
        if c is None or spy is None:
            continue
        own = forward(c, d["asof"], d["horizon"])
        if not own:
            continue
        end, ret = own
        spy_f = forward(spy, d["asof"], d["horizon"])
        sec = closes.get(d["sector_etf"]) if d["sector_etf"] else None
        sec_f = forward(sec, d["asof"], d["horizon"]) if sec is not None else None
        ex_spy = ret - spy_f[1] if spy_f else None
        rows.append({"id": d["id"], "h": d["horizon"], "end": end, "ret": ret, "spy": spy_f[1] if spy_f else None,
                     "sec": sec_f[1] if sec_f else None, "xs": ex_spy, "xsec": ret - sec_f[1] if sec_f else None,
                     "hit": hit(d["stance"], ex_spy, d["horizon"])})
    if rows:
        await db.execute_many(
            "INSERT INTO scout.judgment_outcomes (judgment_id, horizon, end_date, ret, spy_ret, sector_ret, excess_spy, excess_sector, hit) "
            "VALUES (:id, :h, :end, :ret, :spy, :sec, :xs, :xsec, :hit) ON CONFLICT (judgment_id, horizon) DO NOTHING", rows)
    return len(rows)


# ── reading back ──

async def calibration(horizon: int = 20) -> list[dict]:
    """Hit rate and excess return by kind × lens × stance."""
    rows = await db.fetch_all(
        "SELECT j.kind, j.lens, j.stance, count(*) AS n, avg(o.hit::int) AS hit_rate, avg(o.excess_spy) AS mean_excess, "
        "percentile_cont(0.5) WITHIN GROUP (ORDER BY o.excess_spy) AS median_excess "
        "FROM scout.judgments j JOIN scout.judgment_outcomes o ON o.judgment_id = j.id "
        "WHERE o.horizon = :h AND o.hit IS NOT NULL GROUP BY j.kind, j.lens, j.stance ORDER BY j.kind, j.lens, j.stance DESC",
        {"h": horizon})
    return [{**r, "enough": r["n"] >= MIN_SAMPLES} for r in rows]


async def lens_reliability(horizon: int = 20) -> dict[str, dict]:
    """For the AI: per lens and stance, how its past calls did (only when samples suffice)."""
    out: dict[str, dict] = {}
    for r in await calibration(horizon):
        if r["kind"] != "insight_lens":
            continue
        key = f"{r['lens']}·{STANCE_ZH[r['stance']]}"
        out[key] = ({"样本": r["n"], f"{horizon} 日命中率": f"{r['hit_rate'] * 100:.0f}%",
                     "相对 SPY 中位数": f"{r['median_excess'] * 100:+.1f}%"} if r["enough"] else {"样本": r["n"], "说明": "样本不足"})
    return out


async def history(ticker: str, before: date, limit: int = 3) -> list[dict]:
    """Previous overall AI views on this ticker with whatever outcome is known."""
    rows = await db.fetch_all(
        "SELECT j.id, j.asof, j.stance, j.entry_price, j.payload FROM scout.judgments j "
        "WHERE j.kind = 'insight' AND j.ticker = :t AND j.asof < :d ORDER BY j.asof DESC LIMIT :n",
        {"t": ticker, "d": before, "n": limit})
    out = []
    for r in rows:
        outs = {o["horizon"]: o for o in await db.fetch_all(
            "SELECT horizon, excess_spy, hit FROM scout.judgment_outcomes WHERE judgment_id = :i", {"i": r["id"]})}
        item = {"日期": str(r["asof"]), "当时结论": (r["payload"] or {}).get("headline"), "综合倾向": STANCE_ZH[r["stance"]]}
        for h in HORIZONS:
            if h in outs and outs[h]["excess_spy"] is not None:
                item[f"之后 {h} 日相对 SPY"] = f"{outs[h]['excess_spy'] * 100:+.1f}%"
                item[f"{h} 日是否符合"] = "符合" if outs[h]["hit"] else "不符合"
        if len(item) == 3:
            item["结果"] = "尚未到期"
        out.append(item)
    return out
