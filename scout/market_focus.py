"""Market focus ('市场关注点'): what the market is paying attention to, by sector.

Same workflow as scout.insight: Python builds an evidence pack (sector
rotation, breadth, money-flow proxies, news attention/tone and headlines),
one LLM call writes themes and per-sector views citing evidence keys, and
deterministic checks guard numbers, citations, coverage and the hot/cold
label (which is Python's, never the LLM's). Stored in scout.briefs.
"""
from __future__ import annotations

import json
import logging
import re
from datetime import date
from typing import Literal

from pydantic import BaseModel, Field

from scout import responses
from scout import db, knowledge, llm_usage, opportunities, sector_news, sector_trend, sectors
from scout.config import get_scout_settings
from scout.thesis import allowed_numbers, unsupported_numbers

logger = logging.getLogger("scout.market_focus")

SECTOR_METRICS = ("rel_20", "pct_above_50", "net_new_highs", "updown_vol_20", "cmf_20", "rel_dollar_vol",
                  "dollar_vol_share", "rs_ratio", "rs_momentum")


class Theme(BaseModel):
    title: str = Field(description="主题名，≤ 16 字，例如「科技转向防御」")
    summary: str = Field(description="2–3 句：发生了什么、证据是什么、对选股意味着什么")
    sectors: list[str] = Field(description="相关板块 ETF 代码，如 XLE")
    evidence: list[str] = Field(description="引用的证据 key")


class SectorView(BaseModel):
    sector: str = Field(description="板块 ETF 代码")
    stance: Literal["热", "温", "冷"] = Field(description="必须与证据包里该板块的 heat_label 一致")
    view: str = Field(description="1–2 句：状态与原因")
    evidence: list[str]


class MarketFocus(BaseModel):
    headline: str = Field(description="一句话概括当前市场，≤ 40 字")
    regime_view: str = Field(description="大盘状态与风险偏好，1–2 句")
    themes: list[Theme] = Field(description="2–5 个当前市场关注的主题，按重要性排序")
    sectors: list[SectorView] = Field(description="每个行业板块各一条")
    watch: list[str] = Field(description="接下来值得关注的信号或事件，1–3 条")
    risks: list[str] = Field(description="主要风险，1–3 条")
    confidence: Literal["low", "medium", "high"]
    confidence_reason: str


SYSTEM_PROMPT = """你是一名美股策略分析师，基于 Python 计算好的证据包，写一份"当前市场关注点"简报，帮助投资者决定先看哪些板块。

证据包：
- market：大盘状态与主要指数。
- sectors / themes 的 trend：相对 SPY 的趋势确认，分最新 5 日、短期 10 日、中期 20 日、较长 63 日，每个周期有状态、确认度、走势质量、成分股跑赢比例。
- sectors：11 个行业板块。每个板块有轮动象限（RRG 开放近似：领先→转弱→落后→改善）、热度 heat_label（Python 规则计算）、
  宽度、资金流代理指标（涨跌日量比、CMF，基于日线，不是真实买卖单）、成交额热度与占比、新闻关注度和情绪、头条新闻、今日规则命中数。
  每个指标都带 数值（已格式化）、含义 和 读数（标准解读）。
- themes：主题 ETF 的轮动象限。
- market_news：跨多个板块的市场综述文章和宏观新闻。

工作方法：
1. 先找出资金和注意力的流向：哪些板块同时满足"相对强度领先 + 宽度好 + 资金流入 + 新闻关注度上升"，哪些相反。
2. 把新闻和数据对上：新闻解释了什么价格行为？哪些板块"新闻很多但价格不跟"，哪些"价格在动但没有新闻"（后者值得警惕或提前关注）？
3. 提炼 2–5 个主题（例如板块轮动方向、宏观驱动、资金集中或撤离），每个主题引用证据 key。
4. 每个行业板块写一条：stance 必须等于该板块的 heat_label；view 说明它为什么热/冷、处在轮动的哪个阶段。
5. 趋势只能按 trend 里的「状态」表述（确认跑赢 / 确认跑输 / 初现跑赢 / 初现跑输 / 噪音），并说明周期（短期 10 日 / 中期 20 日 / 较长 63 日）：
   「确认」= 超出该板块平时相对波动 2 倍以上、成分股多数同向、相邻周期同向；「初现」要写成"初现"或"尚未确认"；「噪音」不要写成趋势。
   写「确认」的句子只放状态为确认的板块，确认和未确认的板块分句写。
6. 资金流向只能按 flow_label 表述（流入 / 流出 / 信号分歧：CMF 与涨跌日量比方向不一致时要明说"分歧"）；
   新闻关注度只能按 attention_label 表述（上升 / 正常 / 下降 / 基线不足）。一个主题涉及多个板块时，不要把标签不同的板块写成同一个结论。

硬性规则：
- 数字只能原样引用证据包中出现的数值，不要自行计算。
- evidence 只能填证据包里出现的 key（形如 XLE.rel_20、XLE.news_1、wrap_1、macro_2、market.regime）。
- 资金流指标是日线代理，表述时不要说成"机构买入"之类确定的结论；也不要据此推断"投资者信心""经济复苏预期"这类证据包里没有的结论。
- 趋势确认只说明走势不像偶然，不是预测：不要写"将延续""会继续""有望继续"之类，可以写"是否延续待观察"。
- 热度、轮动象限、资金流都是 20 日左右的读数，不是今天的涨跌：写"近 20 日领先 / 落后"，不要写"领涨 / 领跌"或"今天走强 / 走弱"。
- 不编造新闻或事实；中文、专业、简洁；不给买卖指令，非投资建议。"""


def _metric(key: str, value) -> dict:
    return knowledge.evidence(key, value).as_dict()


async def build_pack(market: dict | None = None) -> dict | None:
    """`market` = the session's market context; without it, the after-close brief of the same data date (never an
    older one: the after-close job writes the focus before that day's brief, 2026-11-05)."""
    asof, rows = await sectors.latest()
    if asof is None:
        return None
    news = await sector_news.by_sector()
    hits = await db.fetch_all(
        "SELECT i.sector, count(*) AS n FROM scout.signal_hits h JOIN scout.instruments i USING (ticker) "
        "WHERE h.asof = :d AND (h.payload->>'live')::boolean GROUP BY i.sector", {"d": asof},
    )
    hits_by_sector = {r["sector"]: r["n"] for r in hits}
    if market is None:
        brief = await db.fetch_one("SELECT payload FROM scout.briefs WHERE kind = 'eod' AND asof = :d ORDER BY id DESC LIMIT 1",
                                   {"d": asof})
        market = ((brief or {}).get("payload") or {}).get("market") or {}

    sector_blocks = []
    for r in sorted((x for x in rows if x["kind"] == "sector"), key=lambda x: -(x.get("heat") or 0)):
        k = r["key"]
        n = news.get(k, {})
        block = {
            "key": k, "name": r["name"], "heat_label": r.get("heat_label"),
            "flow_label": r.get("flow_label") or sectors.flow_label(r.get("cmf_20"), r.get("updown_vol_20")),
            "attention_label": sectors.attention_label(n.get("news_attention")),
            "heat": _metric("heat", r.get("heat")),
            "quadrant": {"字段": f"{k}.quadrant", "数值": sectors.QUADRANTS.get(r.get("quadrant"), r.get("quadrant")),
                         "上周": sectors.QUADRANTS.get(r.get("quadrant_prev"), r.get("quadrant_prev"))},
            "metrics": [{**_metric(m, r.get(m)), "字段": f"{k}.{m}"} for m in SECTOR_METRICS if r.get(m) is not None],
            "trend": trend_block(k, r),
            "rule_hits_today": hits_by_sector.get(r.get("sector"), 0),
        }
        if n:
            block["news"] = {
                "attention": {**_metric("news_attention", n.get("news_attention")), "字段": f"{k}.news_attention"}
                if n.get("news_attention") is not None else "基线不足",
                "tone": {**_metric("tone_net", n.get("tone_net")), "字段": f"{k}.tone_net"} if n.get("tone_net") is not None else None,
                "count_7d": n.get("news_7d"),
                "headlines": [{"key": f"{k}.news_{i + 1}", "标题": h["headline"], "来源": h["source"],
                               "时间": h["published"], "相关股票": h["tickers"], **({"摘要": h["summary"]} if h.get("summary") else {})}
                              for i, h in enumerate(n.get("headlines", [])[:3])],
            }
        sector_blocks.append(block)

    themes = [{"key": f"{r['key']}.quadrant", "ETF": r["key"], "主题": r["name"],
               "象限": sectors.QUADRANTS.get(r.get("quadrant")), "20 日涨跌": knowledge.fmt("ret_20d", r.get("ret_20d")),
               "trend": trend_block(r["key"], r)}
              for r in rows if r["kind"] == "theme"]
    wraps = await sector_news.market_wraps()
    macro = await sector_news.macro_headlines(8)
    return {
        "data_date": str(asof),
        "market": {"key": "market.regime", "大盘状态": opportunities.state_text(market, detail=True),
                   "SPY 当日": knowledge.fmt("ret_1d", market.get("spy_ret_1d")),
                   "SPY 20 日": knowledge.fmt("ret_20d", market.get("spy_ret_20d")),
                   "QQQ 20 日": knowledge.fmt("ret_20d", market.get("qqq_ret_20d")),
                   "IWM 20 日": knowledge.fmt("ret_20d", market.get("iwm_ret_20d"))},
        "sectors": sector_blocks,
        "themes": themes,
        "market_news": [{"key": f"wrap_{i + 1}", **w} for i, w in enumerate(wraps)]
                       + [{"key": f"macro_{i + 1}", **m} for i, m in enumerate(macro)],
    }


def trend_block(key: str, row: dict) -> list[dict]:
    """Per horizon: state, strength, quality, breadth — each with its meaning (sector_trend)."""
    out = []
    for c in row.get("trend") or []:
        item = {"字段": f"{key}.trend_{c['h']}", "周期": sector_trend.NAMES[c["h"]], "状态": c["label"],
                "相对 SPY": knowledge.fmt("trend_rel", c.get("rel")),
                "确认度": _metric("trend_z", c.get("z")), "走势质量": _metric("trend_er", c.get("er"))}
        if c.get("breadth") is not None:
            item["成分股跑赢 SPY 的比例"] = _metric("trend_breadth", c["breadth"])
        if c.get("missing"):
            item["未确认的原因"] = c["missing"]
        out.append(item)
    return out


def evidence_keys(pack: dict) -> set[str]:
    keys = {"market.regime"}
    keys |= {t["字段"] for s in pack["sectors"] + pack["themes"] for t in s.get("trend") or []}
    for s in pack["sectors"]:
        keys |= {s["key"], f"{s['key']}.quadrant", f"{s['key']}.heat"}
        keys |= {m["字段"] for m in s["metrics"]}
        news = s.get("news") or {}
        for part in ("attention", "tone"):
            if isinstance(news.get(part), dict):
                keys.add(news[part]["字段"])
        keys |= {h["key"] for h in news.get("headlines", [])}
    keys |= {t["key"] for t in pack["themes"]}
    keys |= {n["key"] for n in pack["market_news"]}
    return keys


def validate(mf: MarketFocus, pack: dict) -> list[str]:
    issues = []
    texts = [mf.headline, mf.regime_view, mf.confidence_reason, *mf.watch, *mf.risks]
    texts += [t.title for t in mf.themes] + [t.summary for t in mf.themes] + [s.view for s in mf.sectors]
    bad = unsupported_numbers("\n".join(texts), allowed_numbers(pack))
    if bad:
        issues.append(f"以下数字不在证据包中：{', '.join(sorted(set(bad)))}")
    known = evidence_keys(pack)
    cited = {k for t in mf.themes for k in t.evidence} | {k for s in mf.sectors for k in s.evidence}
    unknown = sorted(cited - known)
    if unknown:
        issues.append(f"引用了不存在的证据 key：{', '.join(unknown[:10])}")
    expected = {s["key"]: s["heat_label"] for s in pack["sectors"]}
    got = [s.sector for s in mf.sectors]
    if sorted(got) != sorted(expected):
        issues.append(f"必须且只能覆盖这些板块各一次：{', '.join(sorted(expected))}")
    wrong = [f"{s.sector} 应为「{expected[s.sector]}」" for s in mf.sectors if s.sector in expected and s.stance != expected[s.sector]]
    if wrong:
        issues.append("板块热度必须与规则计算一致：" + "；".join(wrong))
    issues += label_conflicts(mf, pack)
    issues += trend_conflicts(mf, pack)
    bad_theme_sectors = sorted({x for t in mf.themes for x in t.sectors} - set(expected) - {t["ETF"] for t in pack["themes"]})
    if bad_theme_sectors:
        issues.append(f"主题里出现了不存在的板块代码：{', '.join(bad_theme_sectors)}")
    if not 2 <= len(mf.themes) <= 5:
        issues.append("主题数量必须在 2–5 个之间")
    daily = sorted({w for w in DAILY_WORDS if any(w in t for t in texts)})
    if daily:
        issues.append(f"板块读数是 20 日左右的，不是当天涨跌，不要用：{'、'.join(daily)}（写「近 20 日领先 / 落后」）")
    return issues


# The sector readings are ~20-session ones; these words claim the day's move (2026-11-03: "防御板块领涨" on a day tech led).
DAILY_WORDS = ("领涨", "领跌", "今日走强", "今日走弱", "今天走强", "今天走弱")


# Phrases that assert a direction, checked against Python's per-sector labels.
_FLOW_CLAIMS = {"流入": ("资金流出", "资金撤离", "资金外流"), "流出": ("资金流入", "资金涌入"),
                "信号分歧": ("资金流入", "资金流出", "资金涌入", "资金撤离", "资金外流")}
_ATTENTION_CLAIMS = {"上升": ("关注度下降", "关注度降低"), "下降": ("关注度上升", "关注度提升", "关注度增加"),
                     "正常": ("关注度明显上升", "关注度大幅上升"), "基线不足": ("关注度上升", "关注度下降", "关注度提升")}


def label_conflicts(mf: MarketFocus, pack: dict) -> list[str]:
    """Qualitative misreads the number check cannot see: money-flow direction
    and news attention, per sector a statement is about."""
    labels = {s["key"]: (s["flow_label"], s["attention_label"]) for s in pack["sectors"]}
    names = {s["key"]: s["name"] for s in pack["sectors"]}

    def mentioned(text: str, declared: list[str]) -> list[str]:
        # Sectors named in the prose count too, not only the ones the model listed.
        return sorted({k for k in labels if k in declared or k in text or names[k] in text})

    statements = [(t.summary, mentioned(t.summary, t.sectors)) for t in mf.themes]
    statements += [(s.view, mentioned(s.view, [s.sector])) for s in mf.sectors if s.sector in labels]
    issues = []
    for text, keys in statements:
        for k in keys:
            flow, att = labels[k]
            for phrase in _FLOW_CLAIMS.get(flow, ()):
                if phrase in text:
                    issues.append(f"{k} 的资金流向规则判定为「{flow}」，文中却写「{phrase}」")
            for phrase in _ATTENTION_CLAIMS.get(att, ()):
                if phrase in text:
                    issues.append(f"{k} 的新闻关注度为「{att}」，文中却写「{phrase}」")
    return sorted(set(issues))


# A confirmed trend is not a forecast (sector rotation failed the gate; sector-ETF momentum is weak after 2000).
FORECAST_WORDS = ("将延续", "会延续", "有望延续", "将继续", "会继续", "有望继续", "将持续", "会持续")
_CLAUSE = re.compile(r"[。；;！!？?\n]")


def trend_conflicts(mf: MarketFocus, pack: dict) -> list[str]:
    """Trend claims checked against Python's states: '确认' only for a sector with a confirmed horizon in that
    direction (per clause, so confirmed and unconfirmed sectors must be written apart); no forecasts."""
    blocks = {s["key"]: (s["name"], s.get("trend") or []) for s in pack["sectors"]}
    blocks |= {t["ETF"]: (t["主题"], t.get("trend") or []) for t in pack["themes"]}
    texts = [mf.headline, mf.regime_view, *mf.watch, *mf.risks] + [t.summary for t in mf.themes] + [s.view for s in mf.sectors]
    issues = []
    for text in texts:
        for clause in _CLAUSE.split(text.replace("确认度", "")):
            if "确认" not in clause or "未确认" in clause or "尚未确认" in clause:
                continue
            for k, (name, trend) in blocks.items():
                if k not in clause and name not in clause:
                    continue
                confirmed = {t["状态"] for t in trend if t["状态"].startswith("确认")}
                if not confirmed:
                    issues.append(f"{k} 没有任何周期是「确认」，文中却写确认：{clause[:40]}")
                for word in ("确认跑赢", "确认跑输"):
                    if word in clause and word not in confirmed and confirmed:
                        issues.append(f"{k} 的确认方向是「{'、'.join(sorted(confirmed))}」，文中却写「{word}」")
    words = sorted({w for w in FORECAST_WORDS for t in texts if w in t})
    if words:
        issues.append(f"趋势确认不是预测，不要写：{'、'.join(words)}（可以写「是否延续待观察」）")
    return sorted(set(issues))


def fallback(pack: dict) -> MarketFocus:
    """Deterministic version: heat ranking and quadrants, no narrative."""
    hot = [s for s in pack["sectors"] if s["heat_label"] == "热"]
    cold = [s for s in pack["sectors"] if s["heat_label"] == "冷"]
    themes = [Theme(title="热门板块", summary="、".join(s["name"] for s in hot) or "无",
                    sectors=[s["key"] for s in hot], evidence=[f"{s['key']}.heat" for s in hot]),
              Theme(title="冷门板块", summary="、".join(s["name"] for s in cold) or "无",
                    sectors=[s["key"] for s in cold], evidence=[f"{s['key']}.heat" for s in cold])]
    def trend(s: dict) -> str:
        parts = [f"{t['周期']}{t['状态']}" for t in s.get("trend") or [] if not t["周期"].startswith("最新")]
        return f"；相对 SPY：{'，'.join(parts)}" if parts else ""

    views = [SectorView(sector=s["key"], stance=s["heat_label"],
                        view=f"轮动象限：{s['quadrant']['数值']}（上周 {s['quadrant']['上周']}）{trend(s)}",
                        evidence=[f"{s['key']}.quadrant"]) for s in pack["sectors"]]
    return MarketFocus(headline="市场关注点（规则读数）", regime_view=pack["market"].get("大盘状态") or "—", themes=themes,
                       sectors=views, watch=["见板块表"], risks=["见板块表"], confidence="low",
                       confidence_reason="AI 总结未通过校验，以下为规则读数。")


async def generate(market: dict | None = None) -> dict | None:
    pack = await build_pack(market)
    if pack is None:
        return None
    settings = get_scout_settings()
    mf, source, history = None, "fallback", []
    if settings.openai_api_key:
        from openai import AsyncOpenAI

        client = AsyncOpenAI(api_key=settings.openai_api_key, timeout=120)
        messages = [{"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": json.dumps(pack, ensure_ascii=False, default=str)}]
        for attempt in (1, 2):
            try:
                parsed = await responses.parse(client, settings.scout_insight_model, messages, MarketFocus, "market_focus", temperature=0.2)
            except Exception as exc:
                logger.warning("market focus LLM call failed: %s", exc)
                history.append({"attempt": attempt, "issues": [f"LLM 调用失败：{type(exc).__name__}"]})
                break
            issues = validate(parsed, pack)
            history.append({"attempt": attempt, "issues": issues})
            if not issues:
                mf, source = parsed, "llm"
                break
            messages += [{"role": "assistant", "content": parsed.model_dump_json()},
                         {"role": "user", "content": "校验未通过，请修正后完整重写：\n" + "\n".join(issues)}]
    if mf is None:
        mf = fallback(pack)
    record = {"source": source, "model": settings.scout_insight_model if source == "llm" else None,
              "focus": mf.model_dump(), "pack": pack, "validation": {"passed": source == "llm", "history": history}}
    await db.execute(
        "INSERT INTO scout.briefs (kind, asof, content, payload) VALUES ('market_focus', :d, :c, CAST(:p AS JSONB))",
        {"d": date.fromisoformat(pack["data_date"]), "c": render_text(mf, pack), "p": db.jsonb(record)},
    )
    return record


async def latest() -> dict | None:
    row = await db.fetch_one(
        "SELECT asof, content, payload, created_at FROM scout.briefs WHERE kind = 'market_focus' ORDER BY id DESC LIMIT 1")
    return {**row["payload"], "content": row["content"], "created_at": row["created_at"], "asof": row["asof"]} if row else None


def render_text(mf: MarketFocus, pack: dict) -> str:
    names = {s["key"]: s["name"] for s in pack["sectors"]}
    icon = {"热": "🔥", "温": "🌤", "冷": "🧊"}
    lines = [f"【市场关注点 · {pack['data_date']}】{mf.headline}", mf.regime_view, ""]
    for i, t in enumerate(mf.themes, 1):
        lines.append(f"{i}. {t.title}：{t.summary}")
    lines.append("")
    lines.append("板块：" + "  ".join(f"{icon[s.stance]}{names.get(s.sector, s.sector)}" for s in mf.sectors))
    lines += ["", "关注：" + "；".join(mf.watch), "风险：" + "；".join(mf.risks)]
    return "\n".join(lines)
