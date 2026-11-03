"""News passed along the supply chain: what a material article about one company may mean for its
customers, suppliers, competitors and partners (named in annual reports, `company_profile.links`).

After the close: the day's direct, non-neutral articles (and hard events) about companies that have links
are judged by a small model — for each linked company, good / bad / unclear and why, citing the article and
the filing quote of the link. Python validates (known target, a reason, the link's relation) and records each
signal with the target's move that day (was it priced already?). Every signal goes into the judgment ledger
(kind `propagation`, lens = the relation) and is scored at 5 / 20 / 60 sessions against SPY and the sector, so
the calibration shows which kinds of pass-through work. An inference, never tested before: labelled so.
Cohen & Frazzini (2008) found customer news predicts supplier returns; Scout measures it forward.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import date

from scout import company_profile, db, universe
from scout.config import get_scout_settings

logger = logging.getLogger("scout.propagation")

MAX_SOURCES = 60               # articles judged per day (cost cap)
MAX_TARGETS = 20               # linked companies shown to the model per article (strongest links first)
KEEP_TARGETS = 4               # signals kept per article (the most confident)
PUSH_CONFIDENCE = ("high", "medium")       # low-confidence signals are recorded and scored, not pushed
CONF_ORDER = {"high": 0, "medium": 1, "low": 2}
PUSH_MAX = 8
CONCURRENCY = 6
REL_ZH = {"customer": "客户", "supplier": "供应商", "competitor": "竞争对手", "partner": "合作方"}

SCHEMA = {
    "type": "object", "additionalProperties": False, "required": ["material", "targets"],
    "properties": {
        "material": {"type": "boolean", "description": "这条新闻是否可能影响关联公司的业务（订单、价格、份额、资本开支、供应）"},
        "targets": {"type": "array", "items": {
            "type": "object", "additionalProperties": False, "required": ["ticker", "impact", "mechanism", "confidence"],
            "properties": {
                "ticker": {"type": "string"},
                "impact": {"type": "string", "enum": ["利好", "利空", "不确定"]},
                "mechanism": {"type": "string", "description": "一句中文：通过什么渠道影响它（引用新闻里的事实和关系）"},
                "confidence": {"type": "string", "enum": ["low", "medium", "high"]}}}},
    },
}
PROMPT = """你是卖方产业链分析师。输入是一条关于公司 S 的新闻，以及年报里与 S 有业务关系的公司（关系、年报原文）。
判断这条新闻对每家关联公司的直接影响：利好、利空或不确定。只根据新闻里写了的事实和年报里写了的关系推断，
不要用你自己知道的其他信息；渠道必须具体（例如"S 上调资本开支 → 作为 S 的存储器供应商，订单可能增加"）。
竞争对手：只有新闻直接改变竞争地位时才判断（丢失或赢得客户、份额变化、事故或产品缺陷、价格战、产能变化），
并购、融资、股价评论、"可能带来风险"这类不直接改变竞争地位的，写"不确定"；同一个行业的价格或需求变化（例如产品价格下跌）
对竞争对手通常是同方向的影响。合作方：看合作内容。mechanism 里提到的关系必须和 relation_to_S 一致。
新闻和这些公司的业务无关、或只是股价评论、分析师观点时，material 为 false，targets 为空。看不出方向就写"不确定"。"""


async def sources(since: int) -> list[dict]:
    """Direct, non-neutral (or hard-event) articles since `since` about companies with links."""
    rows = await db.fetch_all(
        "SELECT ticker, url, headline, llm_summary, sentiment, is_hard_event, datetime_utc, credibility FROM ("
        "SELECT ticker, url, headline, llm_summary, sentiment, is_hard_event, datetime_utc, credibility, relevance FROM public.ticker_news_library "
        "UNION SELECT ticker, url, headline, llm_summary, sentiment, is_hard_event, datetime_utc, credibility, relevance FROM public.ticker_news_archive) n "
        "WHERE datetime_utc >= :s AND relevance = 'direct' AND (coalesce(sentiment, 'neutral') <> 'neutral' OR is_hard_event) "
        "AND ticker IN (SELECT DISTINCT ticker FROM scout.company_profiles) "
        "AND NOT EXISTS (SELECT 1 FROM scout.propagation_signals p WHERE p.news_url = n.url) "
        "ORDER BY is_hard_event DESC, credibility DESC NULLS LAST, datetime_utc DESC", {"s": since})
    seen, out = set(), []
    for r in rows:                              # one article can be filed under several tickers
        if r["url"] in seen:
            continue
        seen.add(r["url"])
        out.append(r)
    return out


def validate(raw: dict, links: list[dict]) -> list[dict]:
    """Keep targets that are linked, have a direction and a reason; attach the link."""
    by = {l["target"]: l for l in links}
    out = []
    targets = (raw.get("targets") or []) if raw.get("material") else []
    for t in targets:
        tk = universe.yahoo_symbol(t.get("ticker") or "")
        mech = (t.get("mechanism") or "").strip()
        if tk not in by or t.get("impact") not in ("利好", "利空") or len(mech) < 8:
            continue
        rel = by[tk]["relation"]
        same = {REL_ZH[rel], REL_ZH[company_profile.MIRROR[rel]]}      # "supplier of S" reads as "S is its customer"
        if not same & {w for w in REL_ZH.values() if w in mech} and any(w in mech for w in REL_ZH.values()):
            continue                            # the reason names only a different relation than the filing gives
        out.append({**t, "ticker": tk, "impact": 1 if t["impact"] == "利好" else -1, "link": by[tk]})
    out.sort(key=lambda x: CONF_ORDER.get(x.get("confidence"), 3))
    return out[:KEEP_TARGETS]


async def judge(src: dict, links: list[dict]) -> tuple[list[dict], str]:
    from scout import responses

    settings = get_scout_settings()
    body = {"news": {"company": src["ticker"], "headline": src["headline"], "summary": src.get("llm_summary"),
                     "sentiment": src.get("sentiment"), "hard_event": bool(src.get("is_hard_event"))},
            "linked": [{"ticker": l["target"], "relation_to_S": REL_ZH[l["relation"]], "filing_of": l["filing_of"],
                        "disclosed": l.get("detail"), "quote": (l["quote"] or "")[:300]} for l in links[:MAX_TARGETS]]}
    data = await responses.create({
        "model": settings.scout_parse_model, "instructions": PROMPT,
        "input": [{"role": "user", "content": json.dumps(body, ensure_ascii=False)}],
        "text": {"format": {"type": "json_schema", "name": "propagation", "schema": SCHEMA, "strict": True}}},
        timeout=90, purpose="propagation")
    return validate(json.loads(responses.output_text(data)), links), settings.scout_parse_model


async def run(asof: date, hours: int = 24) -> dict:
    """Judge the last `hours` of news; store, ledger, return the signals for the push."""
    from scout import ledger

    srcs = (await sources(int(time.time()) - hours * 3600))[:MAX_SOURCES]
    sem = asyncio.Semaphore(CONCURRENCY)
    signals: list[dict] = []
    counts = {"sources": len(srcs), "judged": 0, "signals": 0, "failed": 0}

    async def one(src: dict) -> None:
        links = await company_profile.links(src["ticker"])
        if not links:
            return
        async with sem:
            try:
                found, model = await judge(src, links)
            except Exception as exc:
                logger.warning("propagation for %s failed: %s", src["url"][:80], exc)
                counts["failed"] += 1
                return
        counts["judged"] += 1
        for f in found:
            signals.append({**f, "src": src, "model": model})

    await asyncio.gather(*(one(s) for s in srcs))
    if signals:
        feats = {r["ticker"]: r["payload"] or {} for r in await db.fetch_all(
            "SELECT ticker, payload FROM scout.features_daily WHERE date = :d AND ticker = ANY(:t)",
            {"d": asof, "t": list({s["ticker"] for s in signals})})}
        sectors = {r["ticker"]: r["sector"] for r in await db.fetch_all(
            "SELECT ticker, sector FROM scout.instruments WHERE ticker = ANY(:t)", {"t": list({s["ticker"] for s in signals})})}
        for s in signals:
            f = feats.get(s["ticker"], {})
            s["reaction"] = {k: f.get(k) for k in ("ret_1d", "own_ret_1d", "idio_ret_1d")}
            row = await db.execute_returning(
                "INSERT INTO scout.propagation_signals (asof, source_ticker, news_url, headline, target_ticker, relation, impact, mechanism, "
                "relation_quote, confidence, reaction, model) VALUES (:d, :st, :u, :h, :tt, :r, :i, :m, :q, :c, CAST(:x AS JSONB), :mo) "
                "ON CONFLICT (news_url, target_ticker) DO NOTHING RETURNING id",
                {"d": asof, "st": s["src"]["ticker"], "u": s["src"]["url"], "h": s["src"]["headline"], "tt": s["ticker"],
                 "r": s["link"]["relation"], "i": s["impact"], "m": s["mechanism"], "q": s["link"]["quote"], "c": s["confidence"],
                 "x": db.jsonb(s["reaction"]), "mo": s["model"]})
            if not row:
                continue
            counts["signals"] += 1
            try:
                await ledger.record("propagation", s["ticker"], asof, s["impact"], lens=f"{s['link']['relation']}:{s['confidence']}",
                                    sector_etf=universe.SECTOR_ETFS.get(sectors.get(s["ticker"]) or ""),
                                    source_ref=s["src"]["url"][:500], model=s["model"],
                                    payload={"source": s["src"]["ticker"], "headline": s["src"]["headline"], "mechanism": s["mechanism"]})
            except Exception as exc:
                logger.warning("ledger entry for propagation failed: %s", exc)
    return {**counts, "signals_list": signals}


def push_text(signals: list[dict], held: set[str], watch: set[str]) -> str | None:
    """The day's pass-through hints, the user's holdings and watchlist first."""
    from scout.telegram import esc, pct

    signals = [s for s in signals if s.get("confidence") in PUSH_CONFIDENCE]
    if not signals:
        return None
    rank = sorted(signals, key=lambda s: (s["ticker"] not in held, s["ticker"] not in watch, {"high": 0, "medium": 1}.get(s["confidence"], 2)))
    lines = ["💡 <b>产业链传导提示</b>（推断，未经检验；记入判断台账，5 / 20 / 60 日后打分）"]
    for s in rank[:PUSH_MAX]:
        tag = "【持仓】" if s["ticker"] in held else "【自选】" if s["ticker"] in watch else ""
        r = s.get("reaction") or {}
        moved = (f"；它今天公司自身 {pct(r['own_ret_1d'], 1, True)}" if r.get("own_ret_1d") is not None
                 else f"；它今天 {pct(r['ret_1d'], 1, True)}" if r.get("ret_1d") is not None else "")
        lines.append(f"• {tag}<b>{esc(s['ticker'])}</b>（{esc(s['src']['ticker'])} 的{REL_ZH[s['link']['relation']]}）"
                     f"{'🟢 利好' if s['impact'] > 0 else '🔴 利空'}：{esc(s['mechanism'])}{moved}"
                     f"\n  新闻：{esc(s['src']['headline'][:120])}")
    if len(rank) > PUSH_MAX:
        lines.append(f"另有 {len(rank) - PUSH_MAX} 条，见 Dashboard 或问助手。")
    return "\n".join(lines)


async def recent(ticker: str | None = None, days: int = 14) -> list[dict]:
    where, params = "asof >= CURRENT_DATE - make_interval(days => :n)", {"n": days}
    if ticker:
        where += " AND (target_ticker = :t OR source_ticker = :t)"
        params["t"] = universe.yahoo_symbol(ticker)
    return await db.fetch_all(f"SELECT * FROM scout.propagation_signals WHERE {where} ORDER BY asof DESC, id DESC LIMIT 60", params)
