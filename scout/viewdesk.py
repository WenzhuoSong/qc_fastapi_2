"""Views desk (user's request 2026-11-04): what different sources argue about a stock or a sector, checked against
Scout's own data, turned into one view a non-finance reader can follow, and scored like every other judgment.

Per subject (the holdings, the watchlist names that moved most, the sectors furthest ahead / behind, or any
ticker on demand):
1. a web search collects the bull and the bear arguments of the last 30 days from different kinds of sources —
   news, analyst actions as reported, the company itself, independent research, investor communities — each with
   outlet, date and link (external, unverified);
2. one structured call writes the view: every argument checked against Scout's evidence (agrees / contradicts /
   cannot check, citing the field), 2–4 data points explained in plain words (what it is, what it usually means,
   what it means for you), when the view holds, what would prove it wrong, how sure, and a direction against SPY
   over a horizon — so it can be scored;
3. Python checks the numbers, the cited fields and the confidence rule, counts the source kinds, stores the view
   (brief `view`) and records it in the judgment ledger (kind `view`, lens = confidence), scored at 5 / 20 / 60
   sessions vs SPY and the sector. The ledger, not the prose, says over time which views were worth listening to.
"""
from __future__ import annotations

import json
import logging
from datetime import date, timedelta
from typing import Literal

from pydantic import BaseModel, Field

from scout import db, ledger, universe
from scout.config import get_scout_settings
from scout.thesis import allowed_numbers, unsupported_numbers

logger = logging.getLogger("scout.viewdesk")

EXTERNAL = "据外部报道（未经核实）"
SOURCE_KINDS = ("新闻媒体", "分析师", "公司自身", "独立研究", "投资者社区")
WATCH_SUBJECTS = 4
SECTOR_SUBJECTS = 2
EVIDENCE_PER_LENS = 3
STANCE = {"看多": 1, "中性": 0, "看空": -1}

SEARCH_PROMPT = """Today is {today}. Collect the current debate about {subject} as an investment over the next one to three months.
Use sources published between {since} and {today}, and draw on different kinds of sources: (a) major news outlets,
(b) analyst rating or target changes as reported, (c) the company itself (filings, earnings call, guidance) — skip for an ETF,
(d) independent research (e.g. Morningstar, Seeking Alpha, Substack), (e) investor community discussion (Reddit, X,
StockTwits) as reported or visible. List the strongest bull arguments and the strongest bear arguments separately; for each
give the claim, the kind of source, the outlet, the publication date and the URL. Then say what upcoming data or events
would settle the debate. Do not invent arguments; say plainly when a kind of source has nothing recent."""


class Argument(BaseModel):
    claim: str = Field(description="论点，中文白话，一句")
    source_kind: Literal["新闻媒体", "分析师", "公司自身", "独立研究", "投资者社区"]
    outlet: str = Field(description="来源名称，如 Reuters、摩根士丹利、公司 10-Q、Reddit r/stocks")
    published: str = Field(description="发布日期 YYYY-MM-DD")
    check: Literal["Scout 数据一致", "Scout 数据矛盾", "无法用 Scout 数据核对"]
    check_note: str = Field(description="核对说明：一致或矛盾时写出对应的 Scout 数据（名称和数值）；无法核对写为什么")
    fields: list[str] = Field(description="核对用到的 Scout 证据字段（evidence 里的 字段）；无法核对时为空")


class Explained(BaseModel):
    field: str = Field(description="evidence 里的 字段")
    what: str = Field(description="这个数据是什么，半句白话")
    value: str = Field(description="数值，原样引用")
    usually: str = Field(description="这个读数通常意味着什么（按 含义 和 读数）")
    for_you: str = Field(description="对持有或考虑买入的人意味着什么，一句")


class View(BaseModel):
    summary: str = Field(description="一句话结论：各方怎么看、Scout 数据更支持哪边")
    stance: Literal["看多", "中性", "看空"] = Field(description="未来 horizon 个交易日相对 SPY：跑赢=看多，跑输=看空，说不清=中性")
    horizon: Literal[20, 60]
    bull: list[Argument]
    bear: list[Argument]
    explained: list[Explained] = Field(description="2–4 个最关键的 Scout 数据，用白话解释")
    holds_when: str = Field(description="这个观点在什么环境下成立（大盘、利率、行业周期等）")
    wrong_if: str = Field(description="什么情况说明看错了：具体、可观察（价格、相对强弱、数据、事件）")
    settle: str = Field(description="接下来什么数据或事件会给出答案（带日期的写日期）")
    confidence: Literal["低", "中", "高"]
    confidence_reason: str


VIEW_PROMPT = """你是给没有金融背景的投资者写"多来源观点"的研究员。证据包：subject（标的）、debate（外部检索：各类来源的看多 / 看空论点，
未经核实）、evidence（Scout 自己的数据，每项有 字段、名称、含义、数值、读数）、context（Scout 的规则读数、大盘、行业、近期判断）。

要求：
1. bull 和 bear 各写 1–4 条，只用 debate 里出现的论点，不要自己编；保留来源类型、来源名称和日期。每条用 evidence 核对：
   一致 / 矛盾时写出对应的 Scout 数据（名称和数值）并在 fields 填字段；核对不了就选"无法用 Scout 数据核对"并说明原因。
2. explained 选 2–4 个对结论最关键的 Scout 数据，按"是什么 → 通常意味着什么 → 对你意味着什么"写白话，专业词用半句话解释。
3. stance 是未来 horizon 个交易日相对 SPY 的方向，会被记账打分；两边论点势均力敌或 Scout 数据不支持任何一边时选"中性"。
4. confidence：只有至少两条论点被 Scout 数据证实、且没有被 Scout 数据否定的关键论点时才能写"高"；外部论点大多无法核对时最多"中"。
5. wrong_if 要具体可观察；settle 写接下来会给出答案的数据或事件。
6. 数字只能原样引用证据包里的数值，不要自己计算；外部报道里的数字要说明来自报道。不给买卖指令。中文。"""


def strict_schema(model: type[BaseModel]) -> dict:
    from openai.lib._pydantic import to_strict_json_schema

    return to_strict_json_schema(model)


def source_mix(v: View) -> dict[str, int]:
    out = {k: 0 for k in SOURCE_KINDS}
    for a in v.bull + v.bear:
        out[a.source_kind] += 1
    return out


def validate(v: View, pack: dict) -> list[str]:
    issues = []
    texts = [v.summary, v.holds_when, v.wrong_if, v.settle, v.confidence_reason,
             *[f"{a.claim} {a.check_note}" for a in v.bull + v.bear],
             *[f"{x.what} {x.value} {x.usually} {x.for_you}" for x in v.explained]]
    bad = unsupported_numbers("\n".join(texts), allowed_numbers(pack))
    if bad:
        issues.append(f"以下数字不在证据包中：{', '.join(sorted(set(bad)))}")
    if not v.bull or not v.bear:
        issues.append("bull 和 bear 都至少要有一条（某一边确实没有论点时，写一条说明'近期没有检索到看多 / 看空的论点'，选无法核对）")
    known = {x["字段"] for x in pack["evidence"]}
    unknown = sorted({f for a in v.bull + v.bear for f in a.fields} - known) + sorted({x.field for x in v.explained} - known)
    if unknown:
        issues.append(f"引用了不存在的证据字段：{', '.join(unknown[:8])}")
    if not 2 <= len(v.explained) <= 4:
        issues.append("explained 要写 2–4 项")
    checked = [a for a in v.bull + v.bear if a.check != "无法用 Scout 数据核对"]
    if any(not a.fields for a in checked):
        issues.append("写了一致 / 矛盾的论点要在 fields 填对应的证据字段")
    confirmed = sum(a.check == "Scout 数据一致" for a in v.bull + v.bear)
    contradicted = sum(a.check == "Scout 数据矛盾" for a in v.bull + v.bear)
    if v.confidence == "高" and (confirmed < 2 or contradicted > 0):
        issues.append("confidence 写「高」需要至少两条论点被 Scout 数据证实、且没有论点被 Scout 数据否定")
    if v.confidence == "高" and len(checked) * 2 < len(v.bull + v.bear):
        issues.append("大多数论点无法核对时 confidence 最多写「中」")
    return issues


async def debate(subject: str, today: date) -> dict:
    """The web search: arguments with citations; {} when nothing came back."""
    from scout.event_calendar import _search

    return await _search(SEARCH_PROMPT.format(today=today, since=today - timedelta(days=30), subject=subject),
                         "view_search", model=get_scout_settings().scout_agent_model)


async def evidence(ticker: str) -> dict | None:
    """Scout's own data on the subject: the first items of each lens (with meanings) and the context."""
    from scout import insight

    facts = await insight.gather(ticker)
    if not facts:
        return None
    pack = insight.build_pack(facts)
    items = [{k: x.get(k) for k in ("字段", "名称", "含义", "数值", "读数")} | {"视角": lv["title"]}
             for lv in pack["lenses"] for x in lv["evidence"][:EVIDENCE_PER_LENS]]
    ctx = pack["context"]
    return {"name": pack["name"], "type": pack["type"], "data_date": pack["data_date"], "evidence": items,
            "context": {k: ctx.get(k) for k in ("scout_view", "driver", "market", "sector", "history", "knowledge") if ctx.get(k)}}


async def write(ticker: str, today: date | None = None, sector: str | None = None) -> dict | None:
    """One view on one subject; stored and recorded in the ledger. None when Scout has no data on it."""
    from scout import responses

    today = today or date.today()
    done = await db.fetch_one("SELECT payload FROM scout.briefs WHERE kind = 'view' AND asof = :d AND payload->>'ticker' = :t "
                              "AND payload->'view' IS NOT NULL ORDER BY id DESC LIMIT 1", {"d": today, "t": ticker})
    if done:                               # one view per subject per day
        return done["payload"]
    ev = await evidence(ticker)
    if ev is None:
        return None
    label = f"{ev['name']} ({ticker})" + (f", the {sector} sector ETF" if sector else "")
    found = await debate(label, today)
    pack = {"subject": {"代码": ticker, "名称": ev["name"], "类型": ev["type"], "数据日期": ev["data_date"]},
            "debate": {"内容": found.get("text", "")[:6000], "说明": EXTERNAL} if found else None,
            "evidence": ev["evidence"], "context": ev["context"]}
    settings = get_scout_settings()
    view, source, tries = None, "none", []
    if found and settings.openai_api_key:
        messages = [{"role": "user", "content": json.dumps(pack, ensure_ascii=False, default=str)}]
        for attempt in (1, 2):
            try:
                data = await responses.create({
                    "model": settings.scout_agent_model, "reasoning": {"effort": "low"}, "instructions": VIEW_PROMPT,
                    "input": messages,
                    "text": {"format": {"type": "json_schema", "name": "view", "schema": strict_schema(View), "strict": True}}},
                    timeout=240, purpose="view_write")
                parsed = View.model_validate_json(responses.output_text(data))
            except Exception as exc:
                tries.append([f"LLM 调用失败：{type(exc).__name__}"])
                break
            issues = validate(parsed, pack)
            tries.append(issues)
            if not issues:
                view, source = parsed, "llm"
                break
            messages += [{"role": "assistant", "content": parsed.model_dump_json()},
                         {"role": "user", "content": "校验未通过，请修正后完整重写：\n" + "\n".join(issues)}]
    if view is None:                       # no unchecked prose goes out; the failure is stored for the record
        await db.execute("INSERT INTO scout.briefs (kind, asof, content, payload) VALUES ('view', :d, :c, CAST(:p AS JSONB))",
                         {"d": today, "c": f"{ticker}：未生成（{'没有检索结果' if not found else '未通过校验'}）",
                          "p": db.jsonb({"ticker": ticker, "source": source, "validation": tries})})
        return {"ticker": ticker, "source": source, "validation": tries}
    record = {"ticker": ticker, "name": ev["name"], "asof": today.isoformat(), "data_date": ev["data_date"], "source": source,
              "model": settings.scout_agent_model, "view": view.model_dump(), "mix": source_mix(view),
              "sources": found.get("sources", [])[:20], "validation": tries, "pack": pack}
    await db.execute("INSERT INTO scout.briefs (kind, asof, content, payload) VALUES ('view', :d, :c, CAST(:p AS JSONB))",
                     {"d": today, "c": render_text(record), "p": db.jsonb(record)})
    etf = None if sector else await _sector_etf(ticker)        # a sector ETF is scored against SPY only
    await ledger.record("view", ticker, date.fromisoformat(ev["data_date"]), STANCE[view.stance], lens=view.confidence,
                        sector_etf=etf, model=settings.scout_agent_model,
                        payload={"summary": view.summary, "horizon": view.horizon, "mix": record["mix"]})
    return record


async def _sector_etf(ticker: str) -> str | None:
    row = await db.fetch_one("SELECT sector FROM scout.instruments WHERE ticker = :t", {"t": ticker})
    return universe.SECTOR_ETFS.get((row or {}).get("sector"))


async def subjects(today: date) -> list[tuple[str, str | None]]:
    """The holdings (not core ETFs), the watchlist names that moved most on their own, the sectors furthest ahead / behind."""
    from scout import levels, sectors

    held = [r["ticker"] for r in await db.fetch_all("SELECT DISTINCT ticker FROM scout.trades WHERE closed_on IS NULL ORDER BY ticker")
            if not levels.is_core(r["ticker"])]
    watch = [r["ticker"] for r in await universe.load_watchlist() if r["ticker"] not in held]
    moved = await db.fetch_all(
        "SELECT DISTINCT ON (ticker) ticker, (payload->>'idio_ret_20d')::float AS x FROM scout.features_daily "
        "WHERE ticker = ANY(:t) AND date >= :d ORDER BY ticker, date DESC", {"t": watch, "d": today - timedelta(days=7)}) if watch else []
    top = [r["ticker"] for r in sorted((r for r in moved if r["x"] is not None), key=lambda r: -abs(r["x"]))[:WATCH_SUBJECTS]]
    _, rows = await sectors.latest("sector")
    ranked = sorted((r for r in rows if r.get("rel_20") is not None), key=lambda r: r["rel_20"])
    picks = ([ranked[-1], ranked[0]] if len(ranked) >= 2 else ranked)[:SECTOR_SUBJECTS]
    return [(t, None) for t in held + top] + [(r["key"], r.get("sector") or r.get("name")) for r in picks]


async def weekly(today: date | None = None, push: bool = True) -> dict:
    today = today or date.today()
    done, failed = [], []
    for ticker, sector in await subjects(today):
        try:
            rec = await write(ticker, today, sector)
        except Exception as exc:
            logger.warning("view on %s failed: %s", ticker, exc)
            failed.append(ticker)
            continue
        (done if rec and rec.get("view") else failed).append(rec if rec and rec.get("view") else ticker)
    if push and done:
        from scout import telegram

        await telegram.send(telegram.esc(digest_text(done)))
    return {"views": len(done), "failed": [f if isinstance(f, str) else f.get("ticker") for f in failed]}


# ── text ──

ICON = {"看多": "🟢", "中性": "⚪", "看空": "🔴"}


def render_text(rec: dict) -> str:
    v = rec["view"]
    lines = [f"{ICON[v['stance']]} {rec['name']}（{rec['ticker']}）· 未来 {v['horizon']} 个交易日相对 SPY {v['stance']} · 把握{v['confidence']}",
             v["summary"], "看多的理由："]
    for a in v["bull"]:
        lines.append(f"· {a['claim']}（{a['source_kind']}·{a['outlet']} {a['published']}）→ {a['check']}：{a['check_note']}")
    lines.append("看空的理由：")
    for a in v["bear"]:
        lines.append(f"· {a['claim']}（{a['source_kind']}·{a['outlet']} {a['published']}）→ {a['check']}：{a['check_note']}")
    lines.append("关键数据怎么读：")
    for x in v["explained"]:
        lines.append(f"· {x['what']}：{x['value']}。{x['usually']}；对你：{x['for_you']}")
    lines += [f"什么时候成立：{v['holds_when']}", f"什么情况说明错了：{v['wrong_if']}", f"接下来看：{v['settle']}",
              f"把握：{v['confidence']}——{v['confidence_reason']}",
              "来源构成：" + "、".join(f"{k} {n}" for k, n in rec["mix"].items() if n) + f"（论点{EXTERNAL}；方向记入判断台账打分）"]
    return "\n".join(lines)


def digest_text(recs: list[dict]) -> str:
    lines = ["🗂 本周多来源观点（外部论点未经核实，用 Scout 数据逐条核对；方向记入判断台账打分）"]
    for r in recs:
        v = r["view"]
        lines.append(f"{ICON[v['stance']]} {r['ticker']} {v['stance']}（{v['horizon']} 日，把握{v['confidence']}）：{v['summary']}")
    lines.append("每只的论点、核对和白话解读见 Dashboard「多来源观点」，或问助手「XX 各方怎么看」。")
    return "\n".join(lines)


async def latest(ticker: str | None = None, limit: int = 20) -> list[dict]:
    rows = await db.fetch_all(
        "SELECT payload FROM scout.briefs WHERE kind = 'view' AND payload->'view' IS NOT NULL "
        + ("AND payload->>'ticker' = :t " if ticker else "") + "ORDER BY id DESC LIMIT :n",
        {"t": ticker, "n": limit} if ticker else {"n": limit})
    return [r["payload"] for r in rows]
