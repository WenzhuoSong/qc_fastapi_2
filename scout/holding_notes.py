"""The digest written like an analyst's note to the account owner (user's request 2026-11-06: "我要求 Scout 成为我的
专业股票专家，尤其是给我的消息"; all three of earnings previews, portfolio risk and news against the buy reason).

Python decides what matters and computes every number:
- `key_items`: at most five things for today, ranked by what is at stake. Order: a sell signal, the market
  filter, a big market day, earnings within 3 days, stops within the normal 1-month range, hard negative
  news, a satellite over its cap, an unusual move;
- `portfolio_risk`: volatility with correlations, the beta-weighted exposure (what a 5 % SPY drop does) and the
  sector mix;
- `earnings_preview`: the pre-report consensus, the options-implied move to the first expiry after the
  report, past reactions, and the stop against them.

The model (`SCOUT_AGENT_MODEL`, one structured call per session) only writes prose:
- a plain sentence on where each holding stands;
- each direct news item in Chinese, with its impact and its bearing on the user's recorded buy reason.

Every number in that prose must be in the pack (`thesis.unsupported_numbers`). There are no forecasts and no
actions beyond the user's own plan (a sell only where the plan's stop says so). One correction round; a holding
whose note fails keeps the rule-based lines and the original headlines.
"""
from __future__ import annotations

import json
import logging
import math
import re
from datetime import date

from pydantic import BaseModel, Field

from scout import db
from scout.telegram import pct

logger = logging.getLogger("scout.holding_notes")

KEY_MAX = 5
EARNINGS_SOON = 3             # calendar days: an earnings date this close is a key item
BIG_MOVE_SIGMAS = 2.0
SCENARIO_SPY = -0.05
NEWS_PER_HOLDING = 3
NOTE_MAX_CHARS = 90


def yi(x: float) -> str:
    """Dollars in 亿美元 (or 万美元 below 1 亿)."""
    return f"{x / 1e8:,.1f} 亿美元" if abs(x) >= 1e8 else f"{x / 1e4:,.0f} 万美元"


# ── earnings preview ──

def options_move(snap: dict | None, report_day: date) -> dict | None:
    """The straddle-implied move to the first expiry on or after the report day (it includes the report)."""
    if not snap or snap.get("quality") != "ok":
        return None
    best = None
    for e in (snap.get("expiries") or {}).values():
        if not e.get("straddle_move") or not e.get("expiry"):
            continue
        exp = date.fromisoformat(e["expiry"])
        if exp >= report_day and (best is None or exp < best[0]):
            best = (exp, e["straddle_move"])
    return {"expiry": best[0].isoformat(), "move": float(best[1])} if best else None


def preview_lines(p: dict) -> list[str]:
    """Plain-text lines for one holding's earnings preview."""
    from scout import earnings_gaps

    out = [f"📅 财报 {p['date']}（{p['days']} 天后）前瞻："]
    c = p.get("consensus")
    if c and (c.get("eps") is not None or c.get("revenue")):
        parts = ([f"每股收益 {c['eps']:.2f} 美元"] if c.get("eps") is not None else []) + ([f"营收 {yi(c['revenue'])}"] if c.get("revenue") else [])
        out.append(f"  · 市场预期：{'，'.join(parts)}（{c['source']}，报告前的分析师平均）")
    else:
        out.append("  · 市场预期：没有报告前的分析师预期数据")
    o = p.get("options")
    if o:
        out.append(f"  · 期权：到 {o['expiry'][5:]} 到期，市场定价的涨跌约 ±{o['move'] * 100:.1f}%（包含财报这一次）")
    g = p.get("gaps")
    stop = p.get("to_stop")
    if g:
        out.append("  · 历史：" + earnings_gaps.text(g))            # the stop comparison follows on its own line
    if stop is not None and stop < 0:
        worst = [x for x in ((o or {}).get("move"), -(g or {}).get("max_down", 0) if g else None) if x]
        if worst:
            gap = max(worst)
            if gap >= -stop:
                out.append(f"  · 对照止损：止损在 {pct(stop, 1, True)}，期权定价或过去最大跌幅已经够得着它")
            else:
                out.append(f"  · 对照止损：止损在 {pct(stop, 1, True)}，期权定价和过去最大跌幅都没到；止损只看收盘，极端跳空仍会越过")
    return out


async def earnings_preview(ticker: str, report_day: date, asof: date, to_stop: float | None, gaps: dict | None) -> dict:
    from scout import premarket, research_data

    yahoo = await premarket.yahoo_consensus(ticker, report_day)
    cons = {"source": "Yahoo", **yahoo} if (yahoo.get("eps") is not None or yahoo.get("revenue")) else None
    if cons is None:
        row = await db.fetch_one("SELECT payload FROM scout.market_events WHERE ticker = :t AND event_type = 'earnings' AND event_date = :d",
                                 {"t": ticker, "d": report_day})
        pl = (row or {}).get("payload") or {}
        got = premarket.consensus(pl, {})
        if got:
            cons = {"source": got[0], "eps": got[1], "revenue": got[2]}
    snap = await research_data.latest(ticker, "options", asof)
    return {"date": report_day.isoformat(), "days": (report_day - asof).days, "consensus": cons,
            "options": options_move((snap or {}).get("payload") if snap and not snap.get("stale") else None, report_day),
            "gaps": gaps, "to_stop": to_stop}


# ── portfolio risk ──

def risk_lines(r: dict | None) -> list[str]:
    if not r:
        return []
    out = []
    head = f"投入 {r['invested']:,.0f}" + (f"（账户 {pct(r['invested'] / r['account'], 0)}）" if r.get("account") else "")
    if r.get("beta_usd") is not None:
        loss = r["beta_usd"] * SCENARIO_SPY
        head += (f"；按 β 加总相当于 {r['beta_usd']:,.0f} 的大盘仓位，SPY 跌 5% 时持仓约 {loss:+,.0f}"
                 + (f"（账户 {pct(loss / r['account'], 1, True)}）" if r.get("account") else ""))
    out.append(head)
    v = r.get("vol")
    if v:
        top = "、".join(f"{c['ticker']} {c['share'] * 100:.0f}%" for c in v["contrib"][:3])
        out.append(f"按最近 {v['sessions']} 日的波动和相关性，持仓一个月的正常波动约 ±{v['month_usd']:,.0f}"
                   + (f"（账户 ±{v['month_pct_account'] * 100:.1f}%）" if v.get("month_pct_account") else "") + f"；风险来源：{top}")
    if r.get("sectors"):
        out.append("按市值：" + "、".join(f"{n} {s * 100:.0f}%" for n, s in r["sectors"]))
    return out


async def portfolio_risk(g: dict, account: float | None) -> dict | None:
    from scout import levels, portfolio
    from scout.universe import SECTOR_ETFS, SECTOR_ZH

    rows = g["held_rows"]
    if not rows:
        return None
    risk = await portfolio.holdings_risk(rows, account)
    values, beta_usd, by_sector = {}, 0.0, {}
    for t in rows:
        m = portfolio.metrics(t)
        v = (m.get("price") or 0) * m.get("remaining", 0)
        if not v:
            continue
        values[t["ticker"]] = v
        b = (g["feats"].get(t["ticker"]) or {}).get("beta")
        beta_usd += v * (b if b is not None else 1.0)
        sector = t.get("sector") or next((k for k, e in SECTOR_ETFS.items() if e == t["ticker"]), None)
        name = "大盘指数" if levels.is_core(t["ticker"]) else SECTOR_ZH.get(sector, sector or "其他")
        by_sector[name] = by_sector.get(name, 0.0) + v
    total = sum(values.values())
    if not total:
        return None
    return {"invested": total, "account": account, "beta_usd": beta_usd, "vol": risk.get("vol"),
            "sectors": sorted(((n, v / total) for n, v in by_sector.items()), key=lambda x: -x[1])}


# ── key items ──

def key_items(g: dict, items: list[dict], market_day: dict | None, mfilter: dict | None, satellite: dict | None,
              asof: date | None = None) -> list[str]:
    """At most KEY_MAX lines, the most consequential first. items: `digest.holding_items` rows."""
    out: list[tuple[int, str]] = []
    near = []
    for h in items:
        tk, m = h["ticker"], h["m"]
        if m.get("to_stop") is not None and m["to_stop"] > 0 and not h.get("core"):
            out.append((0, f"{tk} 触发回落止损：收盘 {m['price']:.2f} 低于 {h['stop']:.2f}，按你的计划下一个交易日开盘卖出全部 "
                           f"{m['remaining']:g} 股（持仓 {pct(m['pnl_pct'], 1, True)}，{m['pnl']:+,.0f}）"))
        elif h.get("step", "").startswith("⚠️"):
            out.append((0, f"{tk}：{h['step'][2:].strip()}"))
        sig = h.get("sigma21")
        if m.get("to_stop") is not None and m["to_stop"] < 0 and sig and -m["to_stop"] <= sig:
            near.append(f"{tk} {pct(m['to_stop'], 1, True)}")
        p = h.get("preview")
        if not p and asof and tk in g.get("earnings", {}):      # the preview failed: date and days only
            p = {"date": g["earnings"][tk].isoformat(), "days": (g["earnings"][tk] - asof).days}
        if p and p["days"] <= EARNINGS_SOON:
            bits = [f"{tk} {p['date'][5:]} 发财报（{p['days']} 天后）"]
            if p.get("options"):
                bits.append(f"期权定价 ±{p['options']['move'] * 100:.1f}%")
            if p.get("gaps"):
                bits.append(f"过去最大跌幅 {pct(p['gaps']['max_down'], 1, True)}")
            if m.get("to_stop") is not None and m["to_stop"] < 0:
                bits.append(f"止损在 {pct(m['to_stop'], 1, True)}")
            out.append((3, "，".join(bits)))
        if h.get("hard_bad"):
            out.append((5, f"{tk} 有 {h['hard_bad']} 条利空的硬消息（见持仓部分）"))
        r1, sd = h.get("ret_1d"), h.get("sigma_day")
        if r1 is not None and sd and abs(r1) >= BIG_MOVE_SIGMAS * sd and not market_day:
            out.append((7, f"{tk} 今日 {pct(r1, 1, True)}，约 {abs(r1) / sd:.1f} 倍日常波动"))
    if near:
        out.append((4, f"{'、'.join(near)} 离止损的距离在 1 个月正常波动之内，平常的波动就可能碰到"))
    if mfilter and mfilter.get("off"):
        out.append((1, "大盘过滤处于避险：按计划不新买入，持仓照旧按止损管理"))
    if market_day:
        hd = market_day.get("holdings") or {}
        word = "大涨日" if market_day["up"] else "大跌日"
        text = f"{word}：SPY {pct(market_day['ret'], 1, True)}"
        hist = market_day.get("history")
        if hist:
            text += f"，{hist['since'][:4]} 年以来第 {hist['rank']} 大的单日{'涨' if market_day['up'] else '跌'}幅"
        if hd.get("pnl"):
            text += f"；持仓合计 {hd['pnl']:+,.0f}" + (f"（账户 {pct(hd['pct'], 1, True)}）" if hd.get("pct") is not None else "")
            if hd.get("market") and hd["market"] * hd["pnl"] > 0:
                text += f"，其中约 {min(1.0, hd['market'] / hd['pnl']) * 100:.0f}% 是跟着大盘"
        out.append((2, text))
    if satellite and satellite.get("over"):
        out.append((6, f"卫星仓占账户 {satellite['pct'] * 100:.1f}%，超过你定的 {satellite['cap_pct'] * 100:.0f}% 上限"
                       f"（超出约 {satellite['used'] - satellite['cap']:,.0f}）：按计划不新买入个股"))
    out.sort(key=lambda x: x[0])
    return [t for _, t in out[:KEY_MAX]]


# ── the model's prose ──

class NewsNote(BaseModel):
    index: int = Field(description="新闻在该持仓 news 列表里的序号")
    zh: str = Field(description="一句中文概括这条新闻说了什么，只写原文有的")
    impact: str = Field(description="对这家公司：利好 / 利空 / 中性")
    vs_reason: str = Field(description="和用户买入理由的关系：支持 / 削弱 / 无关 / 没有记录理由")
    why: str = Field(description="一句话说明为什么，引用包里的事实")


class HoldingNote(BaseModel):
    ticker: str
    status: str = Field(description="一句白话：这只持仓现在处在什么状态（盈亏、离止损多远、今天为什么涨跌、有什么要注意的）")
    news: list[NewsNote]


class Notes(BaseModel):
    holdings: list[HoldingNote]


IMPACTS = ("利好", "利空", "中性")
REASON_REL = ("支持", "削弱", "无关", "没有记录理由")
NO_REASON = "未记录"
FORECAST = ("将上涨", "将下跌", "会上涨", "会下跌", "有望上涨", "有望反弹", "将反弹", "会反弹", "看涨", "看跌", "必然", "一定会",
            "将延续", "会继续", "将继续")
ACTIONS = ("加仓", "减仓", "建议买入", "建议卖出", "应该买", "应该卖", "可以买入", "可以卖出", "抄底", "止盈")

DOUBLE_NEG = re.compile(r"(?:亏损|下跌|跌幅|跌了|回落|下滑)(?:为|了|约)?\s*-\d+(?:\.\d+)?%?")

PROMPT = """你是一位专业的股票分析师，给自己的客户（一位个人投资者）写收盘后的持仓点评。证据包里每只持仓有：盈亏、止损、今日涨跌拆分
（大盘 / 行业 / 公司自身）、历史常态、财报前瞻、系统按用户计划给出的提示、用户自己记录的买入理由、当天的直接新闻（英文标题和摘要）。

对每只持仓写：
1. status：一句白话（不超过 90 个字），说清这只持仓现在怎么样，以及今天最值得知道的一点。像对客户说话，不堆砌数字，最多引用 2–3 个最关键的数字。
2. news：对包里的每条新闻（用 index 对应）写一句中文概括（zh）、对公司的影响 impact（利好 / 利空 / 中性）、
   和买入理由的关系 vs_reason（支持 / 削弱 / 无关；买入理由是"未记录"时一律填"没有记录理由"），以及一句为什么（why）。

硬性规则：
- 数字只能原样引用证据包里出现的数值，不要自己计算或换算。
- 不预测涨跌：不要写"将上涨""有望反弹""会继续"之类；可以写"是否延续待观察"。
- 不给计划以外的操作建议：不要写加仓、减仓、抄底、止盈、建议买入或卖出。只有证据包里"计划提示"写了卖出的持仓，才可以复述"按你的计划卖出"。
- 新闻概括只写原文说了的，不要补充背景或推测原因。
- vs_reason 只有新闻内容直接涉及买入理由本身（例如理由说"估值低"，新闻讲估值）才填支持或削弱；只是同方向的泛泛消息填"无关"。
- 涨跌数字自带正负号：写"亏损 28.1%"或"持仓 -28.1%"、"下跌 4.4%"或"今日 -4.4%"，不要写"亏损 -28.1%""下跌 -4.4%"这种双重否定。
- 中文、专业、简洁；非投资建议。"""


def pack(g: dict, items: list[dict]) -> dict:
    """The holdings as the model sees them; numbers pre-formatted as text."""
    from scout.telegram import market_split_text

    out = []
    for h in items:
        m, f = h["m"], g["feats"].get(h["ticker"], {})
        t = h["trade"]
        th = ((t.get("plan") or {}).get("thesis") or {})
        reason = th.get("reason") or t.get("note") or NO_REASON
        row = {"代码": h["ticker"], "名称": (g["info"].get(h["ticker"]) or {}).get("name"), "现价": m.get("price"),
               "持仓盈亏": f"{pct(m.get('pnl_pct'), 1, True)}（{m['pnl']:+,.0f} 美元）" if m.get("pnl") is not None else None,
               "止损": f"{h['stop']:.2f}（距现价 {pct(m['to_stop'], 1, True)}）" if m.get("to_stop") is not None else "没有止损（核心仓位，按大盘过滤管理）",
               "今日": market_split_text(f), "计划提示": h.get("step") or "按计划持有",
               "系统消息": g["managed"].get(h["ticker"], []), "买入理由": reason,
               "买入理由的证伪条件": th.get("falsification"),
               "历史常态": h.get("norm_line"), "财报前瞻": preview_lines(h["preview"]) if h.get("preview") else None,
               "news": [{"index": i, "标题": n.get("headline"), "摘要": (n.get("llm_summary") or n.get("summary") or "")[:400],
                         "情绪（新闻库）": n.get("sentiment"), "硬事件": bool(n.get("is_hard_event"))}
                        for i, n in enumerate(h["news"][:NEWS_PER_HOLDING])]}
        out.append({k: v for k, v in row.items() if v not in (None, [], "")})
    return {"holdings": out}


def validate(notes: Notes, pk: dict) -> list[str]:
    from scout import thesis

    issues = []
    rows = {r["代码"]: r for r in pk["holdings"]}
    seen = {n.ticker for n in notes.holdings}
    if seen != set(rows):
        issues.append(f"持仓不一致：应为 {sorted(rows)}，实际 {sorted(seen)}")
    for n in notes.holdings:
        r = rows.get(n.ticker)
        if not r:
            continue
        allowed = thesis.allowed_numbers(r)
        texts = [n.status] + [x for nn in n.news for x in (nn.zh, nn.why)]
        if len(n.status) > NOTE_MAX_CHARS + 20:
            issues.append(f"{n.ticker} 的 status 太长（{len(n.status)} 字）")
        idx = [nn.index for nn in n.news]
        want = [x["index"] for x in r.get("news", [])]
        if sorted(idx) != sorted(want):
            issues.append(f"{n.ticker} 的新闻序号应为 {want}，实际 {idx}")
        for nn in n.news:
            if nn.impact not in IMPACTS:
                issues.append(f"{n.ticker} 新闻 {nn.index} 的 impact 只能是 {'/'.join(IMPACTS)}")
            if nn.vs_reason not in REASON_REL:
                issues.append(f"{n.ticker} 新闻 {nn.index} 的 vs_reason 只能是 {'/'.join(REASON_REL)}")
            if r.get("买入理由") == NO_REASON and nn.vs_reason != "没有记录理由":
                issues.append(f"{n.ticker} 没有记录买入理由，vs_reason 必须是「没有记录理由」")
            if r.get("买入理由") != NO_REASON and nn.vs_reason == "没有记录理由":
                issues.append(f"{n.ticker} 有买入理由，vs_reason 不能是「没有记录理由」")
        bad = sorted({x for t in texts for x in thesis.unsupported_numbers(t, allowed)})
        if bad:
            issues.append(f"{n.ticker} 有证据包里没有的数字：{', '.join(bad)}")
        joined = "\n".join(texts)
        words = [w for w in FORECAST if w in joined]
        if words:
            issues.append(f"{n.ticker} 不要预测涨跌：{'、'.join(words)}")
        doubled = DOUBLE_NEG.findall(joined)
        if doubled:
            issues.append(f"{n.ticker} 有双重否定：{'、'.join(doubled)}（写成「亏损 28.1%」或「-28.1%」）")
        acts = [w for w in ACTIONS if w in joined]
        if "卖出" in joined and "卖出" not in (r.get("计划提示") or "") + "".join(r.get("系统消息") or []):
            acts.append("卖出")
        if acts:
            issues.append(f"{n.ticker} 不要给计划以外的操作建议：{'、'.join(acts)}")
    return issues


def accepted(notes: Notes, pk: dict) -> dict[str, dict]:
    """Per-holding notes that pass on their own (a failure costs only that holding)."""
    out = {}
    for n in notes.holdings:
        one = Notes(holdings=[n])
        sub = {"holdings": [r for r in pk["holdings"] if r["代码"] == n.ticker]}
        if sub["holdings"] and not validate(one, sub):
            out[n.ticker] = n.model_dump()
    return out


async def write(g: dict, items: list[dict]) -> dict:
    """{"notes": {ticker: note}, "source", "validation"}; an empty dict of notes means rule-based lines only."""
    from scout import responses
    from scout.config import get_scout_settings
    from scout.viewdesk import strict_schema

    pk = pack(g, items)
    settings = get_scout_settings()
    if not pk["holdings"] or not settings.openai_api_key:
        return {"notes": {}, "source": "none", "validation": []}
    messages = [{"role": "user", "content": json.dumps(pk, ensure_ascii=False, default=str)}]
    tries, parsed = [], None
    for _ in (1, 2):
        try:
            data = await responses.create({
                "model": settings.scout_agent_model, "reasoning": {"effort": "low"}, "instructions": PROMPT, "input": messages,
                "text": {"format": {"type": "json_schema", "name": "holding_notes", "schema": strict_schema(Notes), "strict": True}}},
                timeout=240, purpose="digest_notes")
            parsed = Notes.model_validate_json(responses.output_text(data))
        except Exception as exc:
            tries.append([f"LLM 调用失败：{type(exc).__name__}"])
            break
        issues = validate(parsed, pk)
        tries.append(issues)
        if not issues:
            return {"notes": {n.ticker: n.model_dump() for n in parsed.holdings}, "source": "llm", "validation": tries}
        messages += [{"role": "assistant", "content": parsed.model_dump_json()},
                     {"role": "user", "content": "校验未通过，请修正后完整重写：\n" + "\n".join(issues)}]
    ok = accepted(parsed, pk) if parsed else {}
    return {"notes": ok, "source": "partial" if ok else "rules", "validation": tries}


def news_lines(note: dict | None, raw: list[dict]) -> list[str]:
    """The model's news notes, else the raw headlines (the caller escapes)."""
    icon = {"利好": "🟢", "利空": "🔴", "中性": "⚪"}
    if note and note.get("news"):
        out = []
        for nn in sorted(note["news"], key=lambda x: x["index"]):
            rel = "" if nn["vs_reason"] in ("无关", "没有记录理由") else f"｜{nn['vs_reason']}你的买入理由"
            out.append(f"📰 {icon.get(nn['impact'], '⚪')} {nn['zh']}（{nn['impact']}{rel}：{nn['why']}）")
        return out
    sent = {"positive": "🟢", "negative": "🔴"}
    return [f"📰 {sent.get(r.get('sentiment'), '⚪')} {(r.get('headline') or '')[:110]}" for r in raw[:NEWS_PER_HOLDING]]


GLOSSARY = ("术语：β = 大盘涨跌 1% 时它平均涨跌多少；正常范围 = 按最近 63 日波动，约三次里两次落在其中的涨跌幅；"
            "回落止损 = 收盘低于买入以来最高收盘的 80% 就在下一个交易日开盘卖出；卫星仓 = 核心指数仓位以外的个股和行业仓位。")


def sigma_month(vol_63: float | None) -> float | None:
    return vol_63 * math.sqrt(21 / 252) if vol_63 else None
