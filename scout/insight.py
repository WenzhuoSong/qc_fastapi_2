"""AI interpretation ('AI 解读') as a fixed, verifiable workflow.

1. Python assembles one evidence pack: every number arrives as value + reading
   + definition from the feature dictionary, grouped by analysis lens.
2. Step one: the lenses are split into groups (technical & market, events &
   earnings, fundamentals & expectations, options & macro); one LLM call per
   group, in parallel, sees only its lenses' evidence and methods.
3. Step two: one synthesis call reads the lens views plus the context and
   writes the conclusion, the recent event, the conflicts between lenses,
   watch / invalidation conditions and the key risk.
4. Every output is checked deterministically (numbers traceable, citations
   exist, every lens covered, trend verdict consistent with the Python read,
   recent material moves explained); one retry per call, else a
   deterministic fallback for that part only.
The pack and the validation result are stored with the text for audit.
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import date, timedelta
from typing import Literal

from pydantic import BaseModel, Field

from scout import (context, db, event_cause, events, fundamentals, knowledge, learned, ledger, opportunities, research,
                   research_data, sectors, structure, universe)
from scout.analysis import attribution, scout_view
from scout.config import get_scout_settings
from scout.thesis import allowed_numbers, unsupported_numbers

logger = logging.getLogger("scout.insight")

NEWS_DAYS = 7
MAX_NEWS = 8
EVENT_RESEARCH_SESSIONS = 20     # abnormal days this recent get a cause search if not cached
EVENT_MUST_EXPLAIN_SESSIONS = 10 # ... and this recent must be explained first in the write-up
MAX_EVENT_SEARCHES = 2
Verdict = Literal["偏多", "中性", "偏空", "不适用"]


class LensView(BaseModel):
    lens: str = Field(description="视角 id，必须是 evidence.lenses 里给出的 id 之一")
    verdict: Verdict
    points: list[str] = Field(description="1–3 条要点，每条都要能在本视角的证据里找到依据")
    evidence: list[str] = Field(description="引用的证据 key（evidence 里出现的 字段/key）")


class Insight(BaseModel):
    headline: str = Field(description="一句话结论，≤ 40 字")
    recent_event: str | None = Field(
        default=None,
        description="context.events 里距今 10 个交易日内有异常波动时必填：1–2 句先解释它，写明日期（如 10-23）、幅度、"
                    "原因类型和对业务/预期的影响，外部检索来源写'据外部报道'；没有时为 null")
    driver: Literal["个股自身", "大盘/板块", "混合", "不适用"] = Field(
        description="近 20 日走势的驱动力；context.scout_view 给出了规则判定时必须一致")
    attribution: str = Field(description="走势归因：个股自身还是大盘/板块带动，1–2 句")
    lenses: list[LensView] = Field(description="evidence.lenses 里的每个视角各一条，按给出的顺序，一个都不能少")
    conflicts: list[str] = Field(default_factory=list, description="视角之间的矛盾，0–3 条")
    watch: list[str] = Field(description="值得关注的条件（触发什么会让机会成立），1–3 条")
    invalidation: list[str] = Field(description="失效条件（出现什么说明判断错了），1–3 条")
    key_risk: str
    confidence: Literal["low", "medium", "high"]
    confidence_reason: str


GROUPS: dict[str, tuple[str, ...]] = {
    "技术与市场": ("trend_dow", "support_resistance", "momentum_rs", "mean_reversion_vol", "market_context"),
    "事件与财报": ("catalysts", "events", "earnings"),
    "基本面与预期": ("fundamental_trend", "valuation", "expectations", "ownership"),
    "期权与宏观": ("options", "macro"),
}
# Context keys each group may read and cite; everything else stays out of its prompt.
# "knowledge" is further narrowed per group to the entries about its lenses (or its opportunity's rule).
GROUP_CONTEXT: dict[str, tuple[str, ...]] = {
    "技术与市场": ("scout_view", "market", "sector", "driver", "knowledge"),
    "事件与财报": ("scout_view", "news", "events", "earnings", "earnings_reactions", "opportunity", "knowledge"),
    "基本面与预期": ("research", "earnings", "sector", "knowledge"),
    "期权与宏观": ("options_activity", "macro_now", "earnings", "market", "knowledge"),
}
SYNTH_CONTEXT = ("scout_view", "driver", "market", "sector", "events", "earnings", "opportunity", "history", "lens_reliability",
                 "knowledge")


class GroupView(BaseModel):
    lenses: list[LensView] = Field(description="本组 lenses 里的每个视角各一条，按给出的顺序，一个都不能少")


class Synthesis(BaseModel):
    headline: str = Field(description="一句话结论，≤ 40 字")
    recent_event: str | None = Field(
        default=None,
        description="context.events 里距今 10 个交易日内有异常波动时必填：1–2 句先解释它，写明日期（如 10-23）、幅度、"
                    "原因类型和对业务/预期的影响，外部检索来源写'据外部报道'；没有时为 null")
    driver: Literal["个股自身", "大盘/板块", "混合", "不适用"]
    attribution: str = Field(description="走势归因：个股自身还是大盘/板块带动，1–2 句")
    conflicts: list[str] = Field(description="视角之间最重要的矛盾，0–3 条，例如'技术面转弱，但盈利预期在上调'")
    watch: list[str] = Field(description="值得关注的条件（触发什么会让机会成立），1–3 条")
    invalidation: list[str] = Field(description="失效条件（出现什么说明判断错了），1–3 条")
    key_risk: str
    confidence: Literal["low", "medium", "high"]
    confidence_reason: str


LENS_PROMPT = """你是一名资深美股分析师，负责多视角研究中的一组视角（本组的视角列在 lenses 里）。

输入是 Python 已经计算好的证据：
- lenses：本组的分析视角。每个视角有 method（方法论检查清单）和 evidence（数据）。每条数据给出 字段、名称、数值（已格式化）、含义；
  有 读数 的，读数就是该数值在数据字典里的标准解读。
- context：本组可以参考的背景（只包含与本组相关的部分）。

工作方法：
1. 逐个视角，按 method 的检查清单对照 evidence 判断：偏多 / 中性 / 偏空 / 不适用（数据不足时）。本组有几个视角就输出几条，不能遗漏。
2. 每个视角写 1–3 条要点，并在 evidence 里列出引用的 key（数据用"字段"值，context 用 context 里的 key）。
3. 结论要和要点自洽：要点方向不一致时（例如波段结构向上、但短期跌破 20 日线），写"中性"并说明为什么。
4. 正文不要出现英文字段名（如 swing_trend、sma50），用中文名称。

硬性规则：
- 数字只能原样引用证据中出现的"数值"或文字中的数字，不要自己计算或换算。
- 读数是标准口径；不同意时必须说明依据。
- Scout 规则分析（context.scout_view）里的趋势结论是确定性计算，不能与之相反地描述趋势方向。
- 不编造新闻、财报或任何证据里没有的事实；写"没有新闻/没有利好"之前先核对 context.news。
- 异常事件视角：近期有统计上的异常波动时必须引用对应的 event key，说明日期、幅度、原因类型（以 events 给出的为准，
  网络检索要写"据外部报道"）、对业务或预期的影响，以及历史上同类异动之后通常怎么走；不要用异动之后的新闻解释之前的下跌。
- 催化视角：异动之后的新消息（例如公司随后公布的利好）要写出来，并说明它是否修复了异动（看 异动后至今涨跌）。
- research.数据日期 里标着"已过期"的数据，要写明数据日期。
- context.knowledge 是 Scout 知识库里与本组相关的检验结论，按 用法 使用：已确认的可以作为依据；回测通过或前向验证中的，
  引用时必须写明"尚未验证"；否定的表示这种做法经检验无效，不能当作看多或看空的理由。引用时在 evidence 里列出它的 key（kb_N）。
- 标的是 ETF 时，用"ETF 自身"而不是"个股自身"。
- 中文、专业、简洁；不下达买卖指令。"""

SYNTH_PROMPT = """你是一名资深美股策略分析师。各视角的分析员已经给出判断（lens_views），你负责综合成一份结论，供一位有经验的个人投资者参考。

工作方法：
1. 先看 context.events：距今 10 个交易日内有统计上的异常波动时，先在 recent_event 里解释它（日期、幅度、原因类型、影响），
   结论也要围绕它；原因来自网络检索的写"据外部报道"。
2. driver 必须与 context.driver（规则按 20 日个股自身 vs 大盘贡献判定）一致；标的是 ETF 时用"ETF 自身"表述。
3. 综合各视角：哪些一致、哪些矛盾（写进 conflicts）。矛盾是最有价值的部分，例如技术面转弱但预期在上调、估值高位但增长放缓。
   context.lens_reliability 是各视角过去的命中率（样本不足的不要引用），可靠度低的视角要降低权重。
4. context.history 里有之前的判断时，在 confidence_reason 里回应：上次说了什么、后来符合还是不符合、这次为什么相同或不同；判断错了要承认。
5. context.knowledge 是知识库里经过检验的结论，按 用法 使用。context.opportunity 的规则在知识库里是"否定"时，
   不能把这条规则信号当作看多理由，并在 confidence_reason 里说明这条规则未通过公平检验。
6. 写一句话结论、关注条件、失效条件和主要风险（风险要具体到这只股票，不写空话）。

硬性规则：
- 数字只能原样引用 lens_views 或 context 里出现的数字，不要自己计算。
- 不编造事实；不下达买卖指令；中文、专业、简洁；正文不要出现英文字段名。"""


async def gather(ticker: str) -> dict | None:
    """Everything the workflow may cite, as plain data. None if the ticker is unknown."""
    t = universe.yahoo_symbol(ticker)
    feat = await db.fetch_one(
        "SELECT date, payload FROM scout.features_daily WHERE ticker = :t ORDER BY date DESC LIMIT 1", {"t": t}
    )
    if not feat:
        return None
    inst = await db.fetch_one("SELECT name, sector, kind FROM scout.instruments WHERE ticker = :t", {"t": t}) or {}
    bars = await db.fetch_all(
        "SELECT * FROM (SELECT date, open, high, low, close, volume FROM scout.bars_daily WHERE ticker = :t "
        "ORDER BY date DESC LIMIT 260) x ORDER BY date", {"t": t},
    )
    asof = feat["date"]
    earnings = (await context.earnings_map(asof, days_back=120, days_ahead=90)).get(t, [])
    opp = await db.fetch_one(
        "SELECT * FROM scout.opportunities WHERE ticker = :t AND status IN ('new', 'triggered') ORDER BY asof DESC LIMIT 1",
        {"t": t},
    )
    brief = await db.fetch_one("SELECT payload FROM scout.briefs WHERE kind = 'eod' ORDER BY asof DESC, id DESC LIMIT 1")
    sector_etf = universe.SECTOR_ETFS.get(inst.get("sector") or "")
    sector_feat = None
    if sector_etf:
        sector_feat = await db.fetch_one(
            "SELECT payload FROM scout.features_daily WHERE ticker = :t ORDER BY date DESC LIMIT 1", {"t": sector_etf}
        )
    evs, causes, fund, ev_rates = await _events_and_fundamentals(t, inst, sector_etf)
    earnings_summary = context.earnings_summary(earnings, asof) if earnings else {}
    if (inst.get("kind") or "stock") == "stock":
        try:                              # stocks outside the research pool get their data on demand
            await research_data.ensure_ticker(t)
        except Exception as exc:
            logger.warning("on-demand research data for %s failed: %s", t, exc)
    try:
        research_facts = await research.build(t, asof, feat["payload"] or {}, inst.get("sector"), earnings_summary, fund)
    except Exception as exc:              # research data is additive; the technical view must still work
        logger.warning("research data for %s failed: %s", t, exc)
        research_facts = {"values": {}, "context": {}, "unit": "USD"}
    past = await ledger.history(t, asof)
    reliability = await ledger.lens_reliability()
    learned_entries = await learned.for_insight(
        (opp or {}).get("primary_detector"), {lens.id for lens in knowledge.lenses()}, {"kind": inst.get("kind") or "stock"})
    return {
        "knowledge": learned_entries,
        "research": research_facts,
        "history": past,
        "reliability": reliability,
        "ticker": t,
        "asof": asof,
        "events": evs,
        "event_causes": causes,
        "event_rates": ev_rates,
        "fundamentals": fund,
        "instrument": inst,
        "features": feat["payload"] or {},
        "structure": structure.compute(bars),
        "news": await context.load_news(t, hours=NEWS_DAYS * 24, limit=MAX_NEWS),
        "earnings": earnings_summary,
        "opportunity": opp,
        "market": ((brief or {}).get("payload") or {}).get("market") or {},
        "sector": {"etf": sector_etf, "name": universe.SECTOR_ZH.get(inst.get("sector") or "", inst.get("sector")),
                   "features": (sector_feat or {}).get("payload") or {}},
        "sector_rotation": await sectors.context_for(inst.get("sector")),
    }


def material(ev: dict) -> bool:
    """Large enough to need an explanation: ≥ 3σ of its own moves or ≥ 8%."""
    return abs(ev.get("sigma_mult") or 0) >= events.EVENT_SIGMA or abs(ev["ret"]) >= events.EVENT_BIG_MOVE


async def _events_and_fundamentals(t: str, inst: dict, sector_etf: str | None) -> tuple[list, dict, dict, dict]:
    evs = await events.recent(t, sector_etf)
    causes = await event_cause.cached(t, [e["date"] for e in evs])
    todo = [e for e in evs if e["date"] not in causes and material(e) and e["sessions_ago"] <= EVENT_RESEARCH_SESSIONS]
    found = await asyncio.gather(*(event_cause.explain(t, inst.get("name") or t, e) for e in todo[:MAX_EVENT_SEARCHES]),
                                 return_exceptions=True)
    for e, rec in zip(todo, found):
        if isinstance(rec, dict):
            causes[e["date"]] = rec
        else:
            logger.warning("event cause for %s %s failed: %s", t, e["date"], rec)
    fund = {}
    if (inst.get("kind") or "stock") == "stock":
        try:
            fund = await fundamentals.get(t)
        except Exception as exc:
            logger.warning("fundamentals for %s failed: %s", t, exc)
    return evs, causes, fund, await events.load_base_rates()


def key_event(evs: list[dict]) -> dict | None:
    """The event the write-up leads with: the most recent material one, else the largest."""
    recent = [e for e in evs if material(e)]
    if recent:
        return min(recent, key=lambda e: e["sessions_ago"])
    return max(evs, key=lambda e: abs(e["idio"])) if evs else None


def event_values(ev: dict | None, rates: dict, above_200: bool | None) -> dict:
    if not ev:
        return {}
    out = {"event_ret": ev["ret"], "event_idio": ev["idio"], "event_sigma": ev.get("sigma_mult"), "event_gap": ev["gap"],
           "event_intraday": ev["intraday"], "event_vol_ratio": ev.get("vol_ratio"), "event_sector_ret": ev.get("sector_ret"),
           "event_after_ret": ev["after_ret"], "event_sessions_ago": ev["sessions_ago"]}
    br = events.base_rate_for(rates, ev, above_200)
    if br:
        out.update({"event_fwd20_median": br[1].get("ex20_median"), "event_fwd20_up_share": br[1].get("ex20_up_share"),
                    "event_fwd_n": br[1].get("n")})
    return out


SOURCE_LABEL = {"llm": "AI 归因（已校验）", "fallback": "规则判断"}


def event_block(i: int, ev: dict, cause: dict | None) -> dict:
    fmt = knowledge.fmt
    block = {"key": f"event_{i + 1}", "日期": ev["date"], "距今交易日": ev["sessions_ago"], "当日涨跌": fmt("event_ret", ev["ret"]),
             "个股自身": fmt("event_idio", ev["idio"]), "标准差倍数": fmt("event_sigma", ev.get("sigma_mult")),
             "发生时段": ev["timing"], "量比": fmt("event_vol_ratio", ev.get("vol_ratio")),
             "板块当日": fmt("event_sector_ret", ev.get("sector_ret")), "异动后至今": fmt("event_after_ret", ev["after_ret"])}
    if cause:
        web = "web" in (cause.get("sources") or [])
        block.update({"原因类型": cause["cause_type"], "原因": cause["summary"], "关联公司": cause.get("related_company"),
                      "业务影响": cause["business_impact"], "归因置信度": cause["confidence"],
                      "归因依据": ("网络检索（外部来源，未经核实）" if web else SOURCE_LABEL.get(cause.get("source"), "")),
                      "来源": [s["title"] for s in (cause.get("web_sources") or [])][:3]})
    else:
        block["原因类型"] = "原因未检索（不要写成'没有找到原因'）"
    return block


def build_pack(facts: dict) -> dict:
    """Evidence pack shown to the LLM (and stored for audit)."""
    feat = facts["features"]
    above_200 = (feat["close"] > feat["sma200"]) if feat.get("close") and feat.get("sma200") else None
    top = key_event(facts.get("events") or [])
    rs = facts.get("research") or {}
    values = {**feat, **facts["structure"], **(facts.get("fundamentals") or {}), **(rs.get("values") or {}),
              **event_values(top, facts.get("event_rates") or {}, above_200)}
    is_stock = (facts["instrument"].get("kind") or "stock") == "stock"
    lens_blocks = []
    for lens in knowledge.lenses():
        items = [knowledge.evidence(k, values[k]).as_dict() for k in lens.fields if values.get(k) is not None]
        lens_blocks.append({"id": lens.id, "title": lens.title, "method": lens.body, "evidence": items})

    headline, lines = scout_view(
        facts["features"], is_stock=is_stock, structure=facts["structure"],
        next_earnings=_next_earnings(facts), latest_opp=facts["opportunity"], today=facts["asof"],
        sector_ctx=facts.get("sector_rotation"),
    )
    ctx: dict = {
        "scout_view": {"结论": headline, "要点": {l.label: l.text for l in lines}},
        "market": _market_block(facts["market"]),
    }
    attr = attribution(facts["features"])
    if attr:
        ctx["driver"] = attr[0]
    sec = facts["sector"]
    if sec.get("etf") and sec["features"]:
        ctx["sector"] = {"行业": sec["name"], "ETF": sec["etf"],
                         "20 日涨跌": knowledge.fmt("ret_20d", sec["features"].get("ret_20d")),
                         "5 日涨跌": knowledge.fmt("ret_5d", sec["features"].get("ret_5d"))}
        rot = facts.get("sector_rotation")
        if rot:
            ctx["sector"].update({"轮动象限": rot["轮动象限"], "上周象限": rot["上周象限"], "热度": rot["热度"],
                                  "资金流向（规则）": rot["资金流向"],
                                  "板块站上 50 日线比例": knowledge.fmt("pct_above_50", rot["站上 50 日线比例"])})
    ctx["news"] = [
        {"key": f"news_{i + 1}", "时间": (n.get("published") or "")[:10], "来源": n.get("source"),
         "标题": n["headline"], **({"摘要": n["summary"]} if n.get("summary") else {})}
        for i, n in enumerate(facts["news"])
    ] or "近 7 天没有可匹配的新闻"
    kb_block, kb_scope = learned.context_block(facts.get("knowledge") or [])
    if kb_block:
        ctx["knowledge"] = kb_block
    ctx["history"] = facts.get("history") or "这是 Scout 第一次分析这只股票（或之前的判断还没有记录）"
    ctx["lens_reliability"] = facts.get("reliability") or "各视角的历史判断样本还在积累，暂无校准数据"
    evs = facts.get("events") or []
    ctx["events"] = ([event_block(i, e, (facts.get("event_causes") or {}).get(e["date"])) for i, e in enumerate(evs)]
                     or "近 60 个交易日没有统计上的异常波动")
    if facts["earnings"]:
        ctx["earnings"] = facts["earnings"]
    ctx.update(research_context(rs))
    opp = facts["opportunity"]
    if opp:
        br = opp.get("base_rate") or {}
        ctx["opportunity"] = {
            "规则": (opp.get("evidence") or {}).get("setup", {}).get("label", opp["primary_detector"]),
            "日期": str(opp["asof"]), "状态": opp["status"],
            "入场": f"{opp['entry_lo']:.2f}–{opp['entry_hi']:.2f}", "止损": f"{opp['stop']:.2f}",
            "目标": f"{opp['target1']:.2f}",
            "规则公平检验": _rule_check(opp["primary_detector"], kb_block, kb_scope),
        }
    return {
        "ticker": facts["ticker"],
        "name": facts["instrument"].get("name"),
        "type": "个股" if is_stock else "ETF",
        "data_date": str(facts["asof"]),
        "lenses": lens_blocks,
        "context": ctx,
        "knowledge_scope": kb_scope,
        "opportunity_detector": (opp or {}).get("primary_detector"),
    }


def _rule_check(detector: str, block: list[dict], scope: dict[str, dict]) -> str:
    """The rule's verdict from the knowledge base, replacing the biased vs-hold base rate."""
    for item in block:
        if detector in scope[item["key"]]["detectors"]:
            return f"{item['状态']}：{item['条目']}（见 {item['key']}）"
    return "尚未做公平基准检验"


def _money(v: float | None, unit: str) -> str:
    if v is None:
        return "—"
    if unit == "USD":
        return knowledge.fmt("fin_fcf_ttm", v)
    return f"{v / 1e9:.1f}B {unit}"


def research_context(rs: dict) -> dict:
    """Tables behind the new lenses, formatted the way the LLM must quote them."""
    c, unit, fmt = rs.get("context") or {}, rs.get("unit") or "USD", knowledge.fmt
    out: dict = {}
    block: dict = {}
    if c.get("quarters"):
        block["季度财务"] = [
            {"期末": q["period"], "营收": _money(q["revenue"], unit), "同比": fmt("fin_rev_yoy", q["yoy"]),
             "毛利率": fmt("fin_gross_margin", q["gross_margin"]), "营业利润率": fmt("fin_op_margin", q["op_margin"]),
             "自由现金流": _money(q["fcf"], unit)} for q in c["quarters"]]
        if unit != "USD":
            block["报表币种"] = unit
    if c.get("analyst_actions"):
        block["最近评级变动"] = [
            {"日期": a["date"], "机构": a["firm"], "评级": f"{a.get('from') or '—'} → {a.get('to') or '—'}",
             "目标价": f"{a.get('prior_target') or '—'} → {a['target']}" if a.get("target") else "—"}
            for a in c["analyst_actions"]]
    if c.get("top_holders"):
        block["前五大机构"] = [{"机构": h["holder"], "持股": fmt("own_inst_pct", h["pct"]),
                            "最近一期变化": fmt("own_top_holder_change", h["change"])} for h in c["top_holders"]]
    if c.get("freshness"):
        block["数据日期"] = c["freshness"]
    if block:
        out["research"] = block
    if c.get("earnings_reactions"):
        out["earnings_reactions"] = [{"财报日": r["date"], "股价反应": fmt("earn_last_move", r["move"]),
                                      "相对 SPY": fmt("earn_last_move", r["excess"])} for r in c["earnings_reactions"]]
    if c.get("unusual_options"):
        out["options_activity"] = [{"类型": "看涨" if u["type"] == "call" else "看跌", "到期": u["expiry"],
                                    "行权价": u["strike"], "成交量": u["volume"], "昨日持仓量": u["oi"],
                                    "价内外": fmt("ret_1d", u["moneyness"])} for u in c["unusual_options"]]
    if c.get("macro_now"):
        out["macro_now"] = c["macro_now"]
    return out


def _next_earnings(facts: dict) -> date | None:
    nxt = (facts["earnings"] or {}).get("next")
    return date.fromisoformat(nxt["date"]) if nxt else None


def _market_block(m: dict) -> dict:
    if not m:
        return {}
    return {
        "大盘状态": opportunities.state_text(m, detail=True),
        "SPY 当日": knowledge.fmt("ret_1d", m.get("spy_ret_1d")),
        "SPY 20 日": knowledge.fmt("ret_20d", m.get("spy_ret_20d")),
        "QQQ 20 日": knowledge.fmt("ret_20d", m.get("qqq_ret_20d")),
        "IWM 20 日": knowledge.fmt("ret_20d", m.get("iwm_ret_20d")),
    }


def evidence_keys(pack: dict) -> set[str]:
    keys = {item["字段"] for block in pack["lenses"] for item in block["evidence"]}
    ctx = pack["context"]
    keys |= set(ctx)
    if isinstance(ctx.get("news"), list):
        keys |= {n["key"] for n in ctx["news"]}
    if isinstance(ctx.get("events"), list):
        keys |= {e["key"] for e in ctx["events"]}
    keys |= {k["key"] for k in ctx.get("knowledge") or []}
    return keys


def must_explain(pack: dict) -> dict | None:
    """A material event in the last few sessions that the write-up has to lead with."""
    for e in pack["context"].get("events") or []:
        if isinstance(e, dict) and e["距今交易日"] <= EVENT_MUST_EXPLAIN_SESSIONS and (
                e["标准差倍数"] not in ("—",) and abs(float(e["标准差倍数"].rstrip("σ"))) >= events.EVENT_SIGMA
                or abs(float(e["当日涨跌"].rstrip("%"))) >= events.EVENT_BIG_MOVE * 100):
            return e
    return None


def date_marks(iso: str) -> list[str]:
    d = date.fromisoformat(iso)
    return [d.isoformat(), f"{d.month:02d}-{d.day:02d}", f"{d.month}-{d.day}", f"{d.month}月{d.day}日", f"{d.month}/{d.day}"]


def resolve_citations(views: list[LensView], pack: dict) -> list[LensView]:
    """Map the ways the model points at context to the context key it means:
    'context.macro_now', 'macro_now.10 年期美债' and '10 年期美债' all cite macro_now."""
    known = evidence_keys(pack)
    nested = {sub: top for top, v in pack["context"].items() if isinstance(v, dict) for sub in v}

    def one(key: str) -> str:
        k = key.strip().removeprefix("context.")
        if k in known:
            return k
        head = k.split(".")[0]
        if head in known:
            return head
        return nested.get(k, nested.get(head, key))

    return [lv.model_copy(update={"evidence": list(dict.fromkeys(one(k) for k in lv.evidence))}) for lv in views]


def validate_lenses(views: list[LensView], pack: dict, python_trend_tone: str, full_pack: dict | None = None) -> list[str]:
    """Checks on lens views against the (sub-)pack they were written from."""
    full_pack = full_pack or pack
    views = resolve_citations(views, pack)
    issues = []
    bad = unsupported_numbers("\n".join(p for lv in views for p in lv.points), allowed_numbers(pack))
    if bad:
        issues.append(f"以下数字不在证据包中：{', '.join(sorted(set(bad)))}")
    unknown = sorted({k for lv in views for k in lv.evidence} - evidence_keys(pack))
    if unknown:
        issues.append(f"引用了不存在的证据 key：{', '.join(unknown)}")
    lens_ids = [b["id"] for b in pack["lenses"]]
    got = [lv.lens for lv in views]
    if sorted(got) != sorted(lens_ids):
        missing = [x for x in lens_ids if x not in got]
        issues.append(f"必须且只能覆盖这些视角各一次：{', '.join(lens_ids)}（实际：{', '.join(got)}"
                      + (f"；缺少：{', '.join(missing)}，请补上" if missing else "") + "）")
    trend = next((lv for lv in views if lv.lens == "trend_dow"), None)
    if trend and ((python_trend_tone == "up" and trend.verdict == "偏空") or (python_trend_tone == "down" and trend.verdict == "偏多")):
        issues.append("道氏趋势视角的结论与 Scout 规则分析的趋势方向相反")
    for lv in views:
        if lv.verdict != "不适用" and not lv.evidence:
            issues.append(f"视角 {lv.lens} 没有引用任何证据")
    issues += swing_conflicts([p for lv in views for p in lv.points], pack)
    issues += insider_overreach(views, pack)
    ev = must_explain(full_pack)
    lens_ev = next((lv for lv in views if lv.lens == "events"), None)
    if ev and "events" in lens_ids and (not lens_ev or not any(k.startswith("event") for k in lens_ev.evidence)):
        issues.append(f"异常事件视角必须引用 {ev['key']}（{ev['日期']} {ev['当日涨跌']}）")
    return issues


_CONFIDENCE_CLAIMS = ("信心不足", "缺乏信心", "信心下降", "不看好", "看淡")


def insider_overreach(views: list[LensView], pack: dict) -> list[str]:
    """Selling without any insider buying says little (mostly planned 10b5-1 sales);
    it must not be read as insiders losing confidence."""
    buyers = next((i["数值"] for b in pack["lenses"] for i in b["evidence"] if i["字段"] == "insider_buyers_90d"), None)
    if buyers != "0":
        return []
    hits = sorted({c for lv in views for p in lv.points for c in _CONFIDENCE_CLAIMS if c in p})
    if hits:
        return [f"内部人只有卖出、没有买入时，卖出多为计划内交易，不能写成「{'、'.join(hits)}」；如实描述净卖出金额即可"]
    return []


def validate_synthesis(s, pack: dict) -> list[str]:
    """Checks on the synthesis (a Synthesis or an Insight: same fields)."""
    issues = []
    texts = [s.headline, s.attribution, s.key_risk, s.confidence_reason, *s.watch, *s.invalidation,
             *(getattr(s, "conflicts", None) or []), s.recent_event or ""]
    bad = unsupported_numbers("\n".join(texts), allowed_numbers(pack))
    if bad:
        issues.append(f"以下数字不在证据包中：{', '.join(sorted(set(bad)))}")
    expected_driver = pack["context"].get("driver")
    if expected_driver and s.driver != expected_driver:
        issues.append(f"驱动力判定应为「{expected_driver}」（按 20 日个股自身 vs 大盘贡献的规则），实际写成「{s.driver}」")
    issues += swing_conflicts([s.headline, s.attribution], pack)
    ev = must_explain(pack)
    if ev and (not s.recent_event or not any(m in s.recent_event for m in date_marks(ev["日期"]))):
        issues.append(f"近期有异常波动（{ev['日期']} {ev['当日涨跌']}，{ev['原因类型']}）：recent_event 必须解释它，"
                      f"并写明日期（例如 {ev['日期'][5:]}）")
    return issues


def validate(ins: Insight, pack: dict, python_trend_tone: str) -> list[str]:
    """The whole interpretation at once (lens views + synthesis)."""
    return validate_lenses(ins.lenses, pack, python_trend_tone) + validate_synthesis(ins, pack)


# Phrases that assert a swing shape; checked against the rule-computed swing_trend.
_SWING_CLAIMS = {
    "上升": ("高点降低", "低点降低", "高点和低点均降低", "高点和低点同时降低"),
    "下降": ("高点抬高", "低点抬高", "高点和低点均抬高", "高点和低点同时抬高", "高点和低点都抬高"),
}


def structure_conflicts(ins: Insight, pack: dict) -> list[str]:
    return swing_conflicts([ins.headline, ins.attribution, *[p for lv in ins.lenses for p in lv.points]], pack)


def swing_conflicts(texts: list[str], pack: dict) -> list[str]:
    """Qualitative misreads the number check cannot see: the swing shape."""
    swing = next((i["数值"] for b in pack["lenses"] for i in b["evidence"] if i["字段"] == "swing_trend"), "")
    shape = swing.split("：")[0]
    contradicting = _SWING_CLAIMS.get(shape, ())
    hits = sorted({c for c in contradicting for t in texts if c in t})
    if hits:
        return [f"波段结构是「{swing}」，但文中写成了「{'、'.join(hits)}」；swing_trend 是已确认的形态，swing_break 才是正在发生的突破或跌破"]
    return []


def complete_lenses(ins: Insight, pack: dict) -> Insight:
    """Lenses without any evidence (an ETF's fundamentals, no recent events) are filled
    as 不适用 deterministically; lenses with evidence stay the model's job. recent_event
    is kept only when a material move happened in the last few sessions."""
    if ins.recent_event and not must_explain(pack):      # the field is for the last 10 sessions only
        ins = ins.model_copy(update={"recent_event": None})
    have = {lv.lens for lv in ins.lenses}
    filler = [LensView(lens=b["id"], verdict="不适用", points=["没有相关数据"], evidence=[])
              for b in pack["lenses"] if b["id"] not in have and not b["evidence"]]
    if not filler:
        return ins
    order = {b["id"]: i for i, b in enumerate(pack["lenses"])}
    return ins.model_copy(update={"lenses": sorted(ins.lenses + filler, key=lambda lv: order.get(lv.lens, 99))})


def fallback(pack: dict) -> Insight:
    """Deterministic version: the readings themselves, lens by lens."""
    sv = pack["context"]["scout_view"]
    lens_views = []
    for block in pack["lenses"]:
        items = block["evidence"][:3]
        points = [f"{i['名称']} {i['数值']}" + (f"（{i['读数']}）" if i.get("读数") else "") for i in items]
        lens_views.append(LensView(lens=block["id"], verdict="中性" if items else "不适用",
                                   points=points or ["数据不足"], evidence=[i["字段"] for i in items]))
    ev = must_explain(pack)
    recent = None
    if ev:
        recent = (f"{ev['日期'][5:]} {ev['当日涨跌']}（个股自身 {ev['个股自身']}，{ev['标准差倍数']}，{ev['发生时段']}，量比 {ev['量比']}）"
                  + (f"：{ev['原因类型']}——{ev['原因']}" if ev.get("原因") else "，原因未检索"))
    return Insight(
        headline=sv["结论"], recent_event=recent, driver=pack["context"].get("driver", "不适用"),
        attribution=sv["要点"].get("归因", sv["要点"].get("大盘 vs 个股", "数据不足")),
        lenses=lens_views, watch=["见规则分析"], invalidation=["见规则分析"],
        key_risk=sv["要点"].get("风险", "—"), confidence="low", confidence_reason="AI 解读未通过校验，以下为规则读数。",
    )


def sub_pack(pack: dict, lens_ids: tuple[str, ...], ctx_keys: tuple[str, ...]) -> dict:
    """What one lens group sees: its lenses and the context it may cite. Knowledge entries
    reach a group when they are about one of its lenses, or about the opportunity's rule
    when the group reads the opportunity."""
    ctx = {k: v for k, v in pack["context"].items() if k in ctx_keys}
    if ctx.get("knowledge"):
        scope = pack.get("knowledge_scope") or {}
        rule = None
        if "opportunity" in ctx_keys and pack["context"].get("opportunity"):
            rule = pack.get("opportunity_detector")
        ctx["knowledge"] = [k for k in ctx["knowledge"]
                            if set(scope.get(k["key"], {}).get("lenses", [])) & set(lens_ids)
                            or (rule and rule in scope.get(k["key"], {}).get("detectors", []))]
        if not ctx["knowledge"]:
            del ctx["knowledge"]
    return {**{k: pack[k] for k in ("ticker", "name", "type", "data_date")},
            "lenses": [b for b in pack["lenses"] if b["id"] in lens_ids], "context": ctx}


def fill_lenses(views: list[LensView], pack: dict) -> list[LensView]:
    """Add 不适用 for lenses that have no evidence at all; keep pack order."""
    have = {lv.lens for lv in views}
    views = views + [LensView(lens=b["id"], verdict="不适用", points=["没有相关数据"], evidence=[])
                     for b in pack["lenses"] if b["id"] not in have and not b["evidence"]]
    order = {b["id"]: i for i, b in enumerate(pack["lenses"])}
    return sorted(views, key=lambda lv: order.get(lv.lens, 99))


def fallback_lenses(pack: dict) -> list[LensView]:
    return fallback({**pack, "context": {**pack["context"], "scout_view": {"结论": "", "要点": {}}}}).lenses


async def _parse(client, model: str, messages: list, fmt):
    resp = await client.beta.chat.completions.parse(model=model, messages=messages, response_format=fmt, temperature=0.2)
    return resp.choices[0].message.parsed


async def run_group(client, model: str, name: str, pack: dict, tone: str) -> tuple[list[LensView], str, list]:
    """Step one for one lens group: (views, source, attempts)."""
    sp = sub_pack(pack, GROUPS[name], GROUP_CONTEXT[name])
    if not sp["lenses"]:
        return [], "empty", []
    if not any(b["evidence"] for b in sp["lenses"]):            # nothing to read: no LLM call
        return fill_lenses([], sp), "no_data", []
    history = []
    if client:
        messages = [{"role": "system", "content": LENS_PROMPT},
                    {"role": "user", "content": json.dumps(sp, ensure_ascii=False, default=str)}]
        for attempt in (1, 2):
            try:
                parsed = await _parse(client, model, messages, GroupView)
            except Exception as exc:
                history.append({"attempt": attempt, "issues": [f"LLM 调用失败：{type(exc).__name__}"]})
                break
            views = resolve_citations(fill_lenses([lv for lv in parsed.lenses if lv.lens in GROUPS[name]], sp), sp)
            issues = validate_lenses(views, sp, tone, pack)
            history.append({"attempt": attempt, "issues": issues})
            if not issues:
                return [LensView(**knowledge.humanize_obj(v.model_dump())) for v in views], "llm", history
            messages += [{"role": "assistant", "content": parsed.model_dump_json()},
                         {"role": "user", "content": "校验未通过，请修正后完整重写本组：\n" + "\n".join(issues)}]
        logger.info("insight group %s for %s fell back: %s", name, pack["ticker"], history[-1]["issues"] if history else "")
    return fallback_lenses(sp), "fallback", history


async def run_synthesis(client, model: str, pack: dict, views: list[LensView]) -> tuple[Synthesis, str, list]:
    """Step two: conclusion, recent event, conflicts, conditions and risk from the lens views."""
    titles = {b["id"]: b["title"] for b in pack["lenses"]}
    payload = {**{k: pack[k] for k in ("ticker", "name", "type", "data_date")},
               "lens_views": [{"视角": titles.get(v.lens, v.lens), "判断": v.verdict, "要点": v.points} for v in views],
               "context": {k: v for k, v in pack["context"].items() if k in SYNTH_CONTEXT}}
    check_pack = {**pack, "lens_views": payload["lens_views"]}     # numbers in the lens points are citable too
    history = []
    if client:
        messages = [{"role": "system", "content": SYNTH_PROMPT},
                    {"role": "user", "content": json.dumps(payload, ensure_ascii=False, default=str)}]
        for attempt in (1, 2):
            try:
                parsed = await _parse(client, model, messages, Synthesis)
            except Exception as exc:
                history.append({"attempt": attempt, "issues": [f"LLM 调用失败：{type(exc).__name__}"]})
                break
            if parsed.recent_event and not must_explain(pack):
                parsed = parsed.model_copy(update={"recent_event": None})
            issues = validate_synthesis(parsed, check_pack)
            history.append({"attempt": attempt, "issues": issues})
            if not issues:
                return Synthesis(**knowledge.humanize_obj(parsed.model_dump())), "llm", history
            messages += [{"role": "assistant", "content": parsed.model_dump_json()},
                         {"role": "user", "content": "校验未通过，请修正后完整重写：\n" + "\n".join(issues)}]
    fb = fallback(pack)
    return Synthesis(**{k: getattr(fb, k) for k in Synthesis.model_fields if k != "conflicts"}, conflicts=[]), "fallback", history


async def generate(ticker: str) -> dict | None:
    """Run the workflow; returns the stored record (or None for unknown tickers)."""
    facts = await gather(ticker)
    if facts is None:
        return None
    pack = build_pack(facts)
    from scout.analysis import trend_state

    tone = trend_state(facts["features"])[1]
    settings = get_scout_settings()
    client = None
    if settings.openai_api_key:
        from openai import AsyncOpenAI

        client = AsyncOpenAI(api_key=settings.openai_api_key, timeout=90)
    model = settings.scout_insight_model
    results = await asyncio.gather(*(run_group(client, model, name, pack, tone) for name in GROUPS))
    groups = {name: {"source": src, "history": hist} for name, (_, src, hist) in zip(GROUPS, results)}
    views = fill_lenses([v for vs, _, _ in results for v in vs], pack)
    synth, synth_source, synth_history = await run_synthesis(client, model, pack, views)
    insight = Insight(**synth.model_dump(), lenses=views)
    fell_back = [n for n, g in groups.items() if g["source"] == "fallback"]
    source = "fallback" if synth_source == "fallback" else ("partial" if fell_back else "llm")
    attempts = max([len(g["history"]) for g in groups.values()] + [len(synth_history)])
    first_pass = all(not g["history"] or not g["history"][0]["issues"] for g in groups.values()) and \
        bool(synth_history) and not synth_history[0]["issues"]
    record = {
        "ticker": facts["ticker"], "asof": facts["asof"], "source": source,
        "model": model if source != "fallback" else None,
        "insight": insight.model_dump(), "pack": pack,
        "validation": {"attempts": attempts, "passed": source == "llm", "first_pass": first_pass,
                       "groups": groups, "synthesis": synth_history, "fallback_groups": fell_back,
                       "history": [{"part": n, **h} for n, g in groups.items() for h in g["history"]]
                       + [{"part": "综合", **h} for h in synth_history]},
    }
    await db.execute(
        "INSERT INTO scout.briefs (kind, asof, content, payload) VALUES ('ticker_ai', :d, :c, CAST(:p AS JSONB))",
        {"d": facts["asof"], "c": render_text(insight, pack), "p": db.jsonb(record)},
    )
    try:
        await ledger.record_insight(record)
    except Exception as exc:          # the ledger must never lose the user an interpretation
        logger.warning("ledger record for %s failed: %s", ticker, exc)
    return record


async def latest(ticker: str, max_age_days: int = 0) -> dict | None:
    t = universe.yahoo_symbol(ticker)
    row = await db.fetch_one(
        "SELECT asof, content, payload, created_at FROM scout.briefs WHERE kind = 'ticker_ai' "
        "AND payload->>'ticker' = :t AND payload ? 'insight' ORDER BY id DESC LIMIT 1", {"t": t},
    )
    if not row:
        return None
    feat = await db.fetch_one("SELECT max(date) AS d FROM scout.features_daily WHERE ticker = :t", {"t": t})
    newest = (feat or {}).get("d")
    if newest and row["asof"] < newest - timedelta(days=max_age_days):
        return None
    return {**row["payload"], "content": row["content"], "created_at": row["created_at"]}


LENS_TITLES = {lens.id: lens.title for lens in knowledge.lenses()}
VERDICT_ICON = {"偏多": "🟢", "中性": "⚪", "偏空": "🔴", "不适用": "▫️"}


def render_text(ins: Insight, pack: dict) -> str:
    lines = [f"【{pack['ticker']} · {ins.headline}】（数据日期 {pack['data_date']}）"]
    if ins.recent_event:
        lines.append(f"⚡ 近期异动：{ins.recent_event}")
    lines += [f"归因：{ins.attribution}", ""]
    for lv in ins.lenses:
        lines.append(f"{VERDICT_ICON.get(lv.verdict, '')} {LENS_TITLES.get(lv.lens, lv.lens)}：{lv.verdict}")
        lines += [f"  · {p}" for p in lv.points]
    if ins.conflicts:
        lines += ["", "视角矛盾：" + "；".join(ins.conflicts)]
    lines += ["", "关注条件：" + "；".join(ins.watch), "失效条件：" + "；".join(ins.invalidation),
              f"主要风险：{ins.key_risk}", f"置信度：{ {'low': '低', 'medium': '中', 'high': '高'}[ins.confidence]} — {ins.confidence_reason}"]
    return "\n".join(lines)
