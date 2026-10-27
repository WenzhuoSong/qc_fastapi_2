"""AI interpretation ('AI 解读') as a fixed, verifiable workflow.

1. Python assembles one evidence pack: every number arrives as value + reading
   + definition from the feature dictionary, grouped by analysis lens.
2. One LLM call writes a structured view per lens, citing evidence keys.
3. The output is checked deterministically (numbers traceable, citations
   exist, every lens covered, trend verdict consistent with the Python read);
   one retry with the problems listed, else a deterministic fallback.
The pack and the validation result are stored with the text for audit.
"""
from __future__ import annotations

import asyncio
import json
import logging
from datetime import date, timedelta
from typing import Literal

from pydantic import BaseModel, Field

from scout import (context, db, event_cause, events, fundamentals, knowledge, ledger, opportunities, sectors, structure,
                   universe)
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
    watch: list[str] = Field(description="值得关注的条件（触发什么会让机会成立），1–3 条")
    invalidation: list[str] = Field(description="失效条件（出现什么说明判断错了），1–3 条")
    key_risk: str
    confidence: Literal["low", "medium", "high"]
    confidence_reason: str


SYSTEM_PROMPT = """你是一名资深美股技术分析师，按"多视角工作流"为一位有经验的个人投资者分析单只股票或 ETF。

输入是 Python 已经计算好的证据包（JSON）：
- lenses：6 个分析视角。每个视角有 method（该视角的方法论检查清单，来自《股市趋势技术分析》等文献）和 evidence（该视角的数据）。
  每条数据都给出 字段、名称、数值（已格式化）、含义；有 读数 的，读数就是该数值在数据字典里的标准解读。
- context：大盘、所属板块、新闻、财报、活跃机会卡、Scout 规则分析（Python 的确定性结论）。

工作方法：
1. 逐个视角，按 method 里的检查清单对照 evidence 判断：偏多 / 中性 / 偏空 / 不适用（数据不足时）。
   lenses 共有多少个视角就输出多少条（包括 异常事件 和 基本面与预期），按 id 一一对应，不能遗漏。
2. 每个视角写 1–3 条要点，并在 evidence 字段列出所引用的 key（数据用"字段"值，context 用 context 里的 key）。
3. 先写走势归因：driver 必须与 context.driver（规则按 20 日个股自身 vs 大盘贡献判定）一致，再综合各视角给出一句话结论、值得关注的条件、失效条件和主要风险。
4. 视角之间的矛盾要明确指出（例如趋势向上但成交量不配合），这是专业分析最有价值的部分。
0. 先看 context.events：近期有统计上的异常波动时，先在 recent_event 里解释它，结论也要围绕它——日期、幅度（标准差倍数）、原因类型、
   对哪部分业务或预期有影响、事后历史上通常怎么走（异常事件视角的 同类异动后 统计）。原因以 events 给出的为准，
   来自网络检索的要写明"据外部报道"。不要用异动之后的新闻解释之前的下跌。
5a. context.history 里有之前的判断时，在 confidence_reason 里回应：上次说了什么、后来的走势符合还是不符合、这次为什么相同或不同；
   判断错了要直接承认。context.lens_reliability 是各视角过去的命中率（样本不足的不要引用），可靠度低的视角在结论里要降低权重。
5. 结论要和要点自洽：要点里方向不一致时（例如波段结构向上、但短期跌破 20 日线），结论写"中性"就要在要点里说明为什么不是偏多/偏空。
6. 正文不要出现英文字段名（如 swing_trend、sma50），引用放在 evidence 里，正文用中文名称。

硬性规则：
- 数字只能原样引用证据包里出现的"数值"或文字中的数字，不要自己计算或换算新的数字。
- 读数是标准口径；如果要表达不同看法，必须说明依据。
- Scout 规则分析里的趋势结论是确定性计算，不能与之相反地描述趋势方向。
- 不编造新闻、财报或任何证据包里没有的事实；写"没有新闻/没有利好"之前先核对 context.news。
- 异动之后的新消息（例如公司随后公布的利好数据）要在催化视角里写出来，并说明它是否修复了异动（看 异动后至今涨跌）。
- 标的是 ETF 时，用"ETF 自身"而不是"个股自身"来表述归因。
- 中文、专业、简洁；可以写"关注条件 / 失效条件"，不要下达买卖指令；不是投资建议。"""


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
    past = await ledger.history(t, asof)
    reliability = await ledger.lens_reliability()
    return {
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
        "earnings": context.earnings_summary(earnings, asof) if earnings else {},
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
    values = {**feat, **facts["structure"], **(facts.get("fundamentals") or {}),
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
    ctx["history"] = facts.get("history") or "这是 Scout 第一次分析这只股票（或之前的判断还没有记录）"
    ctx["lens_reliability"] = facts.get("reliability") or "各视角的历史判断样本还在积累，暂无校准数据"
    evs = facts.get("events") or []
    ctx["events"] = ([event_block(i, e, (facts.get("event_causes") or {}).get(e["date"])) for i, e in enumerate(evs)]
                     or "近 60 个交易日没有统计上的异常波动")
    if facts["earnings"]:
        ctx["earnings"] = facts["earnings"]
    opp = facts["opportunity"]
    if opp:
        br = opp.get("base_rate") or {}
        ctx["opportunity"] = {
            "规则": (opp.get("evidence") or {}).get("setup", {}).get("label", opp["primary_detector"]),
            "日期": str(opp["asof"]), "状态": opp["status"],
            "入场": f"{opp['entry_lo']:.2f}–{opp['entry_hi']:.2f}", "止损": f"{opp['stop']:.2f}",
            "目标": f"{opp['target1']:.2f}",
            "规则历史扣费后相对持有": knowledge.fmt("ret_1d", br.get("avg_edge_vs_hold")) if br.get("avg_edge_vs_hold") is not None else "—",
        }
    return {
        "ticker": facts["ticker"],
        "name": facts["instrument"].get("name"),
        "type": "个股" if is_stock else "ETF",
        "data_date": str(facts["asof"]),
        "lenses": lens_blocks,
        "context": ctx,
    }


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


def validate(ins: Insight, pack: dict, python_trend_tone: str) -> list[str]:
    issues = []
    texts = [ins.headline, ins.attribution, ins.key_risk, ins.confidence_reason, *ins.watch, *ins.invalidation]
    texts += [p for lv in ins.lenses for p in lv.points]
    bad = unsupported_numbers("\n".join(texts), allowed_numbers(pack))
    if bad:
        issues.append(f"以下数字不在证据包中：{', '.join(sorted(set(bad)))}")
    known = evidence_keys(pack)
    unknown = sorted({k for lv in ins.lenses for k in lv.evidence} - known)
    if unknown:
        issues.append(f"引用了不存在的证据 key：{', '.join(unknown)}")
    lens_ids = [b["id"] for b in pack["lenses"]]
    got = [lv.lens for lv in ins.lenses]
    if sorted(got) != sorted(lens_ids):
        missing = [x for x in lens_ids if x not in got]
        issues.append(f"必须且只能覆盖这些视角各一次：{', '.join(lens_ids)}（实际：{', '.join(got)}"
                      + (f"；缺少：{', '.join(missing)}，请补上" if missing else "") + "）")
    expected_driver = pack["context"].get("driver")
    if expected_driver and ins.driver != expected_driver:
        issues.append(f"驱动力判定应为「{expected_driver}」（按 20 日个股自身 vs 大盘贡献的规则），实际写成「{ins.driver}」")
    trend = next((lv for lv in ins.lenses if lv.lens == "trend_dow"), None)
    if trend and ((python_trend_tone == "up" and trend.verdict == "偏空") or (python_trend_tone == "down" and trend.verdict == "偏多")):
        issues.append("道氏趋势视角的结论与 Scout 规则分析的趋势方向相反")
    for lv in ins.lenses:
        if lv.verdict != "不适用" and not lv.evidence:
            issues.append(f"视角 {lv.lens} 没有引用任何证据")
    issues += structure_conflicts(ins, pack)
    ev = must_explain(pack)
    if ev:
        lens_ev = next((lv for lv in ins.lenses if lv.lens == "events"), None)
        if not lens_ev or not any(k.startswith("event") for k in lens_ev.evidence):
            issues.append(f"异常事件视角必须引用 {ev['key']}（{ev['日期']} {ev['当日涨跌']}）")
        if not ins.recent_event or not any(m in ins.recent_event for m in date_marks(ev["日期"])):
            issues.append(f"近期有异常波动（{ev['日期']} {ev['当日涨跌']}，{ev['原因类型']}）：recent_event 必须解释它，"
                          f"并写明日期（例如 {ev['日期'][5:]}）")
    return issues


# Phrases that assert a swing shape; checked against the rule-computed swing_trend.
_SWING_CLAIMS = {
    "上升": ("高点降低", "低点降低", "高点和低点均降低", "高点和低点同时降低"),
    "下降": ("高点抬高", "低点抬高", "高点和低点均抬高", "高点和低点同时抬高", "高点和低点都抬高"),
}


def structure_conflicts(ins: Insight, pack: dict) -> list[str]:
    """Qualitative misreads the number check cannot see: the swing shape."""
    swing = next((i["数值"] for b in pack["lenses"] for i in b["evidence"] if i["字段"] == "swing_trend"), "")
    shape = swing.split("：")[0]
    contradicting = _SWING_CLAIMS.get(shape, ())
    texts = [ins.headline, ins.attribution, *[p for lv in ins.lenses for p in lv.points]]
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


async def generate(ticker: str) -> dict | None:
    """Run the workflow; returns the stored record (or None for unknown tickers)."""
    facts = await gather(ticker)
    if facts is None:
        return None
    pack = build_pack(facts)
    from scout.analysis import trend_state

    tone = trend_state(facts["features"])[1]
    settings = get_scout_settings()
    insight, source, attempts, issues, history = None, "fallback", 0, [], []
    if settings.openai_api_key:
        from openai import AsyncOpenAI

        client = AsyncOpenAI(api_key=settings.openai_api_key, timeout=90)
        messages = [{"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": json.dumps(pack, ensure_ascii=False, default=str)}]
        for attempts in (1, 2):
            try:
                resp = await client.beta.chat.completions.parse(
                    model=settings.scout_insight_model, messages=messages, response_format=Insight, temperature=0.2,
                )
                parsed = resp.choices[0].message.parsed
            except Exception as exc:
                logger.warning("insight LLM call failed for %s: %s", ticker, exc)
                issues = [f"LLM 调用失败：{type(exc).__name__}"]
                break
            parsed = complete_lenses(parsed, pack)
            issues = validate(parsed, pack, tone)
            history.append({"attempt": attempts, "issues": issues})
            if not issues:
                insight, source = Insight(**knowledge.humanize_obj(parsed.model_dump())), "llm"
                break
            logger.info("insight for %s failed validation (attempt %d): %s", ticker, attempts, issues)
            messages += [{"role": "assistant", "content": parsed.model_dump_json()},
                         {"role": "user", "content": "校验未通过，请修正后完整重写：\n" + "\n".join(issues)}]
    if insight is None:
        insight = fallback(pack)
    record = {
        "ticker": facts["ticker"], "asof": facts["asof"], "source": source,
        "model": settings.scout_insight_model if source == "llm" else None,
        "insight": insight.model_dump(), "pack": pack,
        "validation": {"attempts": attempts, "passed": source == "llm", "history": history},
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
    lines += ["", "关注条件：" + "；".join(ins.watch), "失效条件：" + "；".join(ins.invalidation),
              f"主要风险：{ins.key_risk}", f"置信度：{ {'low': '低', 'medium': '中', 'high': '高'}[ins.confidence]} — {ins.confidence_reason}"]
    return "\n".join(lines)
