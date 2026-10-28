"""Industry outlook (v3 P4): who drives the market, and AI theses on sectors and themes.

1. Market drivers (Python, descriptive, no prediction): sector and theme ETF returns vs SPY,
   style spreads inside the universe (momentum, low volatility, size, beta; portfolios formed
   at the start of each window, so no look-ahead), cap- vs equal-weight (SPY − RSP) and breadth.
2. Slow information per sector, point in time, from the research pool: revenue growth and its
   acceleration, capex growth, EPS-estimate revisions and their breadth, rating changes;
   plus sector news and macro.
3. Theses (AI, weekly, at most `MAX_THESES`): a sector or theme ETF, a direction, a horizon of
   60 or 120 sessions, the method (a `judgment` entry of the knowledge base), the mechanism,
   cited evidence (at least one item of slow information, not price alone) and what would
   falsify it. Checked deterministically; nothing is written when the checks fail.
   Each thesis goes into the judgment ledger (`sector_thesis`) and is scored against SPY and
   the equal-weighted S&P 500 (RSP). The method's knowledge entry moves to `forward` on first
   use: AI judgments are validated forward only (a model cannot be backtested on years it was
   trained on).
"""
from __future__ import annotations

import json
import logging
from datetime import date, timedelta
from statistics import median
from typing import Literal

import numpy as np
import pandas as pd
from pydantic import BaseModel, Field

from scout import db, learned, ledger, research, sector_news, universe
from scout.config import get_scout_settings
from scout.thesis import allowed_numbers, unsupported_numbers

logger = logging.getLogger("scout.outlook")

MAX_THESES = 3
HORIZONS = (60, 120)
WINDOWS = (5, 20, 60)
EQUAL_WEIGHT = "RSP"
PANEL_DAYS = 600          # 60-session style windows need a year of formation history before them
THEMES = ("SMH", "IGV", "XBI", "KRE", "ITA", "URA")      # industry themes; macro assets (GLD, TLT, USO, UUP) are not industries
TARGETS = (*universe.SECTOR_ETFS.values(), *THEMES)
SLOW_PREFIXES = ("fund_", "est_", "rating_", "news_", "macro")
MIN_COMPANIES = 5         # fewer research-pool members than this is not a sector trend: the aggregate is left out
METHOD_TOPIC = "sector_thesis"
ETF_ZH = {**{etf: universe.SECTOR_ZH[s] for s, etf in universe.SECTOR_ETFS.items()},
          **{t: universe.THEME_ETFS[t] for t in THEMES if t in universe.THEME_ETFS}}


def _pct(x: float | None, digits: int = 1) -> str:
    return "—" if x is None or not np.isfinite(x) else f"{x * 100:+.{digits}f}%"


# ── 1. market drivers ───────────────────────────────────────────────────

def _ret(close: pd.Series, a: int, b: int) -> float | None:
    if a < 0 or b >= len(close):
        return None
    x, y = close.iloc[a], close.iloc[b]
    return float(y / x - 1) if np.isfinite(x) and np.isfinite(y) and x > 0 else None


def _quintile_spread(signal: pd.Series, fwd: pd.Series) -> float | None:
    """Mean forward return of the top fifth of `signal` minus the bottom fifth."""
    both = pd.concat([signal, fwd], axis=1, join="inner").dropna()
    if len(both) < 50:
        return None
    q = both.iloc[:, 0].rank(pct=True)
    return float(both.iloc[:, 1][q > 0.8].mean() - both.iloc[:, 1][q <= 0.2].mean())


def style_spreads(close: pd.DataFrame, volume: pd.DataFrame, spy: pd.Series, window: int) -> dict:
    """Long-short returns over the last `window` sessions, portfolios formed at its start."""
    end = len(close) - 1
    t0 = end - window
    if t0 < 260:
        return {}
    fwd = close.iloc[end] / close.iloc[t0] - 1
    daily = close.pct_change()
    spy_d = spy.pct_change()
    hist = daily.iloc[t0 - 251: t0 + 1]
    sh = spy_d.iloc[t0 - 251: t0 + 1]
    beta = hist.apply(lambda col: col.cov(sh) / sh.var() if col.notna().sum() > 200 else np.nan)
    return {
        "momentum": _quintile_spread(close.iloc[t0 - 21] / close.iloc[t0 - 252] - 1, fwd),
        "low_vol": _quintile_spread(-daily.iloc[t0 - 59: t0 + 1].std(), fwd),
        "size": _quintile_spread((close * volume).iloc[t0 - 59: t0 + 1].mean(), fwd),
        "beta": _quintile_spread(beta, fwd),
    }


STYLE_ZH = {"momentum": "动量（过去 12 个月强 − 弱）", "low_vol": "低波动（低 − 高）", "size": "规模（大 − 小，按成交额）",
            "beta": "高 β − 低 β", "concentration": "市值加权 − 等权（SPY − RSP）"}


def breadth(close: pd.DataFrame) -> dict:
    last = close.iloc[-1]
    valid = last.notna()
    sma50, sma200 = close.iloc[-50:].mean(), close.iloc[-200:].mean()
    hi = close.iloc[-252:].max()
    return {"above_50": float((last > sma50)[valid].mean()), "above_200": float((last > sma200)[valid].mean()),
            "near_high": float((last >= 0.95 * hi)[valid].mean()), "n": int(valid.sum())}


def drivers(p) -> dict:
    close, volume = p["close"], p["volume"]
    stocks = [t for t in p.stocks if t in close.columns]
    spy = close[universe.BENCHMARK]
    end = len(close) - 1
    etfs = {}
    for etf in (*TARGETS, "QQQ", "IWM", EQUAL_WEIGHT):
        if etf not in close.columns:
            continue
        row = {}
        for w in WINDOWS:
            r, s = _ret(close[etf], end - w, end), _ret(spy, end - w, end)
            row[f"ret_{w}"] = r
            row[f"rel_{w}"] = None if r is None or s is None else r - s
        etfs[etf] = row
    styles = {w: style_spreads(close[stocks], volume[stocks], spy, w) for w in WINDOWS}
    for w in WINDOWS:
        a, b = _ret(spy, end - w, end), _ret(close[EQUAL_WEIGHT], end - w, end) if EQUAL_WEIGHT in close else None
        if styles[w] is not None and a is not None and b is not None:
            styles[w]["concentration"] = a - b
    return {"asof": str(p.dates[-1]), "spy": {f"ret_{w}": _ret(spy, end - w, end) for w in WINDOWS}, "etfs": etfs,
            "styles": styles, "breadth": breadth(close[stocks])}


# ── 2. slow information per sector ──────────────────────────────────────

def ticker_fundamentals(series: dict[str, list[tuple[date, float]]]) -> dict:
    """Latest quarter's revenue growth y/y, the quarter before (acceleration), capex TTM y/y."""
    out = {}
    rev = series.get("revenue") or []
    if len(rev) >= 6:
        def yoy(k: int) -> float | None:
            d, v = rev[-1 - k]
            prev = research._near(rev, d - timedelta(days=365))
            return v / prev - 1 if prev and prev > 0 else None
        out["rev_yoy"], out["rev_yoy_prev"] = yoy(0), yoy(1)
    capex = series.get("capex") or []
    if len(capex) >= 8:
        last4, prev4 = sum(abs(v) for _, v in capex[-4:]), sum(abs(v) for _, v in capex[-8:-4])
        out["capex_ttm_yoy"] = last4 / prev4 - 1 if prev4 > 0 else None
        out["capex_ttm"] = last4
        out["capex_ttm_prev"] = prev4
    return out


def ticker_estimates(est: dict | None) -> dict:
    if not est:
        return {}
    trend, rev = (est.get("eps_trend") or {}).get("0y") or {}, (est.get("eps_revisions") or {}).get("0y") or {}
    cur, m1, m3 = trend.get("current"), trend.get("30daysAgo"), trend.get("90daysAgo")
    out = {}
    if cur is not None and m1 and m1 > 0:
        out["eps_rev_30d"] = cur / m1 - 1
    if cur is not None and m3 and m3 > 0:
        out["eps_rev_90d"] = cur / m3 - 1
    if rev:
        out["up_30d"], out["down_30d"] = int(rev.get("upLast30days") or 0), int(rev.get("downLast30days") or 0)
    return out


def _med(xs: list) -> float | None:
    xs = [x for x in xs if x is not None and np.isfinite(x)]
    return float(median(xs)) if xs else None


def sector_block(members: dict[str, dict]) -> dict:
    """Aggregate of the research-pool members of one sector."""
    f = [m.get("fund") or {} for m in members.values()]
    e = [m.get("est") or {} for m in members.values()]
    accel = [x for x in f if x.get("rev_yoy") is not None and x.get("rev_yoy_prev") is not None]
    capex = [x for x in f if x.get("capex_ttm") and x.get("capex_ttm_prev")]
    up, down = sum(x.get("up_30d", 0) for x in e), sum(x.get("down_30d", 0) for x in e)
    revised = [x.get("eps_rev_30d") for x in e if x.get("eps_rev_30d") is not None]
    return {
        "companies": len(members),
        "rev_yoy_median": _med([x.get("rev_yoy") for x in f]),
        "rev_accel_share": (sum(x["rev_yoy"] > x["rev_yoy_prev"] for x in accel) / len(accel)) if accel else None,
        "capex_ttm_yoy": (sum(x["capex_ttm"] for x in capex) / sum(x["capex_ttm_prev"] for x in capex) - 1) if capex else None,
        "capex_companies": len(capex),
        "eps_rev_30d_median": _med(revised),
        "eps_rev_90d_median": _med([x.get("eps_rev_90d") for x in e]),
        "eps_up_share_30d": (sum(r > 0 for r in revised) / len(revised)) if revised else None,
        "revision_breadth": ((up - down) / (up + down)) if up + down else None,
        "analysts_up": up, "analysts_down": down,
    }


async def sector_research(asof: date) -> dict[str, dict]:
    """Per sector ETF: the aggregate block over the research pool, point in time."""
    pool = await db.fetch_all(
        "SELECT c.ticker, i.sector FROM scout.sec_companies c JOIN scout.instruments i USING (ticker) WHERE i.sector IS NOT NULL")
    est_rows = await db.fetch_all(
        "SELECT DISTINCT ON (ticker) ticker, payload FROM scout.research_snapshots WHERE kind = 'estimates' AND asof <= :d "
        "ORDER BY ticker, asof DESC", {"d": asof})
    est = {r["ticker"]: r["payload"] for r in est_rows}
    actions = await db.fetch_all(
        "SELECT i.sector, a.action, count(*) AS n FROM scout.analyst_actions a JOIN scout.instruments i USING (ticker) "
        "WHERE a.action_at >= :s AND a.action_at < :e AND a.action IN ('up', 'down') GROUP BY i.sector, a.action",
        {"s": asof - timedelta(days=30), "e": asof + timedelta(days=1)})
    by_sector: dict[str, dict[str, dict]] = {}
    for r in pool:
        series, _ = await research._series(r["ticker"], asof)
        by_sector.setdefault(r["sector"], {})[r["ticker"]] = {"fund": ticker_fundamentals(series),
                                                              "est": ticker_estimates(est.get(r["ticker"]))}
    out = {}
    for sector, etf in universe.SECTOR_ETFS.items():
        block = sector_block(by_sector.get(sector, {}))
        block["upgrades_30d"] = next((a["n"] for a in actions if a["sector"] == sector and a["action"] == "up"), 0)
        block["downgrades_30d"] = next((a["n"] for a in actions if a["sector"] == sector and a["action"] == "down"), 0)
        out[etf] = block
    return out


# ── 3. the evidence pack and the theses ─────────────────────────────────

def methods(entries: list[dict]) -> list[dict]:
    return [en for en in entries if en["kind"] == "judgment" and METHOD_TOPIC in ((en.get("applies") or {}).get("topics") or [])
            and en["status"] in ("hypothesis", "forward", "confirmed")]


def open_theses(rows: list[dict], today: date) -> dict[str, dict]:
    """Targets with a thesis still inside its horizon (≈ 1.45 calendar days per session)."""
    out = {}
    for r in rows:
        h = int((r.get("payload") or {}).get("horizon") or 60)
        if r["asof"] + timedelta(days=int(h * 1.45)) >= today:
            out[r["ticker"]] = {"日期": str(r["asof"]), "方向": ledger.STANCE_ZH[r["stance"]], "持有期": h}
    return out


def build_pack(drv: dict, research_blocks: dict, news: dict, macro: dict, method_entries: list[dict], open_: dict) -> dict:
    etfs = drv["etfs"]
    pack: dict = {"数据日期": drv["asof"],
                  # window lengths only appear in field names; listed here so prose may say "30 日" or "120 个交易日"
                  "窗口": "价格与风格 5 / 20 / 60 个交易日；预期修正 30 / 90 日；评级 30 日；判断持有期 60 / 120 个交易日；资本开支近 4 个季度",
                  "spy": {f"{w} 日": _pct(drv["spy"].get(f"ret_{w}")) for w in WINDOWS},
                  "style": {STYLE_ZH[k]: {f"{w} 日": _pct((drv["styles"].get(w) or {}).get(k)) for w in WINDOWS}
                            for k in STYLE_ZH},
                  "breadth": {"站上 50 日线比例": _pct(drv["breadth"]["above_50"], 0).lstrip("+"),
                              "站上 200 日线比例": _pct(drv["breadth"]["above_200"], 0).lstrip("+"),
                              "距 52 周高点 5% 以内比例": _pct(drv["breadth"]["near_high"], 0).lstrip("+"),
                              "股票数": drv["breadth"]["n"]},
                  "macro": macro or "没有宏观数据",
                  "targets": {}, "methods": [], "open_theses": open_ or "无"}
    for etf in TARGETS:
        if etf not in etfs:
            continue
        t = {f"price_{etf}": {f"{w} 日涨跌": _pct(etfs[etf].get(f"ret_{w}")) for w in WINDOWS}
             | {f"{w} 日相对 SPY": _pct(etfs[etf].get(f"rel_{w}")) for w in WINDOWS}}
        b = research_blocks.get(etf)
        if b and 0 < b["companies"] < MIN_COMPANIES:
            t[f"coverage_{etf}"] = f"研究池里只有 {b['companies']} 家公司，不足 {MIN_COMPANIES} 家，不提供行业基本面和预期汇总"
        if b and b["companies"] >= MIN_COMPANIES:
            t[f"fund_{etf}"] = {"研究池公司数": b["companies"], "最近一季收入同比（中位数）": _pct(b["rev_yoy_median"]),
                                "收入增速加快的公司比例": _pct(b["rev_accel_share"], 0).lstrip("+"),
                                "资本开支近四季同比（合计）": _pct(b["capex_ttm_yoy"]), "资本开支有数据的公司数": b["capex_companies"]}
            t[f"est_{etf}"] = {"本年 EPS 预期 30 日变化（中位数）": _pct(b["eps_rev_30d_median"], 2),
                               "本年 EPS 预期 90 日变化（中位数）": _pct(b["eps_rev_90d_median"], 2),
                               "EPS 预期 30 日上调的公司比例": _pct(b["eps_up_share_30d"], 0).lstrip("+"),
                               "分析师上调 − 下调占比（30 日）": _pct(b["revision_breadth"], 0),
                               "上调次数": b["analysts_up"], "下调次数": b["analysts_down"]}
            t[f"rating_{etf}"] = {"评级上调（30 日）": b["upgrades_30d"], "评级下调（30 日）": b["downgrades_30d"]}
        n = news.get(etf)
        if n:
            for i, h in enumerate(n.get("headlines") or []):
                t[f"news_{etf}_{i + 1}"] = {"时间": h["published"][:10], "标题": h["headline"], "来源": h["source"],
                                           **({"摘要": h["summary"]} if h.get("summary") else {})}
        pack["targets"][etf] = {"名称": ETF_ZH.get(etf, etf), **t}
    pack["methods"] = [{"key": en["key"], "方法": en["title"], "内容": en["claim"],
                        "状态": learned.STATUS_ZH[en["status"]]} for en in method_entries]
    return pack


def evidence_keys(pack: dict) -> set[str]:
    keys = {"spy", "style", "breadth", "macro"}
    for t in pack["targets"].values():
        keys |= {k for k in t if k != "名称"}
    return keys


def resolve(o: "Outlook", pack: dict) -> "Outlook":
    """Map the ways the model points at evidence ('targets.XLE.est_XLE', 'context.macro') to the key it means."""
    known = evidence_keys(pack)

    def one(k: str) -> str:
        parts = [x for x in k.strip().split(".") if x]
        return next((x for x in reversed(parts) if x in known), k)

    return o.model_copy(update={"theses": [t.model_copy(update={"evidence": list(dict.fromkeys(one(k) for k in t.evidence))})
                                           for t in o.theses]})


class Thesis(BaseModel):
    target: str = Field(description="targets 里的 ETF 代码")
    stance: Literal["偏多", "偏空"]
    horizon: Literal[60, 120] = Field(description="持有期（交易日）")
    method: str = Field(description="methods 里的 key")
    title: str = Field(description="≤ 20 字")
    mechanism: str = Field(description="2–3 句：通过什么渠道影响盈利或估值，为什么价格还没有完全反映")
    evidence: list[str] = Field(description="引用的证据 key；至少一条是基本面、预期、评级、新闻或宏观，不能只有价格")
    falsification: list[str] = Field(description="1–3 条：出现什么情况说明判断错了")
    confidence: Literal["low", "medium", "high"]


class Outlook(BaseModel):
    drivers: str = Field(description="2–3 句：当前是谁在推动市场（行业、风格、宽度），只描述，不预测")
    theses: list[Thesis] = Field(description="0–3 条行业或主题判断；证据不够时宁可少写或不写")
    no_thesis_reason: str | None = Field(default=None, description="一条判断都没有时写原因")


PROMPT = """你是一名买方行业研究员，任务是基于 Python 计算好的证据，写每周的行业前景判断。

证据包：
- spy、style、breadth：大盘、风格（多空组合收益，组合在窗口开始时构建）和市场宽度，用来描述"当前是谁在推动市场"。
- targets：每个行业或主题 ETF 的 price_（价格）、fund_（研究池公司的收入和资本开支，按申报时点）、est_（EPS 预期修正）、
  rating_（评级变化）、news_（近期新闻）。
- macro：利率、美元、油价、VIX、信用利差。
- methods：判断方法（知识库条目）。每条判断必须选一个方法，并按这个方法的逻辑使用证据。
- open_theses：还在持有期内的判断。这些标的本周不要再写。

要求：
1. drivers：2–3 句，描述现在是谁在推动市场，只描述，不预测。
2. theses：最多 3 条。优先找扩散慢的信息：多家公司同时出现的收入、资本开支、预期修正趋势，政策和宏观传导。
   不追单条新闻；价格动量本身不算理由。每条判断都要引用至少一条非价格证据。
3. 写清机制：通过什么渠道影响盈利或估值，为什么价格还没有完全反映。标的要和方法的逻辑一致：
   例如资本开支周期说的是"下游花钱、上游受益"，判断应写在受益的上游行业，而不是花钱的行业。
4. 证伪条件写成可观察的方向变化（如"EPS 预期修正转为下调"、"油价跌回 20 个交易日前的水平"），
   或引用证据包里已有的数值；不要自己定新的数字阈值。
5. 证据不够时宁可不写，并在 no_thesis_reason 里说明原因。

硬性规则：数字只能原样引用证据包里的数字，不要自己计算；不编造事实；新闻写"据报道"；中文、专业、简洁；不下达买卖指令。"""


def validate(o: Outlook, pack: dict) -> list[str]:
    issues = []
    texts = [o.drivers, o.no_thesis_reason or ""] + [x for t in o.theses for x in (t.title, t.mechanism, *t.falsification)]
    bad = unsupported_numbers("\n".join(texts), allowed_numbers(pack))
    if bad:
        issues.append(f"以下数字不在证据包中：{', '.join(sorted(set(bad)))}")
    if len(o.theses) > MAX_THESES:
        issues.append(f"最多 {MAX_THESES} 条判断")
    known, method_keys = evidence_keys(pack), {m["key"] for m in pack["methods"]}
    open_ = pack["open_theses"] if isinstance(pack["open_theses"], dict) else {}
    seen = set()
    for t in o.theses:
        if t.target not in pack["targets"]:
            issues.append(f"{t.target} 不在可选标的里")
        if t.target in open_:
            issues.append(f"{t.target} 还有持有期内的判断（{open_[t.target]['日期']}），本周不要再写")
        if t.target in seen:
            issues.append(f"{t.target} 写了不止一条")
        seen.add(t.target)
        if t.method not in method_keys:
            issues.append(f"{t.target} 的方法 {t.method} 不在 methods 里")
        unknown = [k for k in t.evidence if k not in known]
        if unknown:
            issues.append(f"{t.target} 引用了不存在的证据：{', '.join(unknown)}")
        if not any(k.startswith(SLOW_PREFIXES) for k in t.evidence if k in known):
            issues.append(f"{t.target} 只引用了价格类证据；至少要有一条基本面、预期、评级、新闻或宏观证据")
        if not t.falsification:
            issues.append(f"{t.target} 没有写证伪条件")
    if not o.theses and not o.no_thesis_reason:
        issues.append("没有判断时要写 no_thesis_reason")
    return issues


async def _macro_now(asof: date) -> dict:
    return research.macro_now(await research._macro(asof - timedelta(days=60)))


async def generate(p=None) -> dict:
    """Weekly: drivers + theses; stores the brief and records the theses in the ledger."""
    from scout.pipeline import load_panel

    p = p if p is not None else await load_panel(lookback_days=PANEL_DAYS)
    asof = p.dates[-1]
    drv = drivers(p)
    entries = await learned.current()
    method_entries = methods(entries)
    prior = await db.fetch_all("SELECT ticker, asof, stance, payload FROM scout.judgments WHERE kind = 'sector_thesis' "
                               "AND asof >= :d", {"d": asof - timedelta(days=200)})
    pack = build_pack(drv, await sector_research(asof), await sector_news.by_sector(), await _macro_now(asof),
                      method_entries, open_theses(prior, asof))
    settings = get_scout_settings()
    out, history = None, []
    if settings.openai_api_key and method_entries:
        from openai import AsyncOpenAI

        client = AsyncOpenAI(api_key=settings.openai_api_key, timeout=120)
        messages = [{"role": "system", "content": PROMPT}, {"role": "user", "content": json.dumps(pack, ensure_ascii=False, default=str)}]
        for attempt in (1, 2):
            resp = await client.beta.chat.completions.parse(model=settings.scout_insight_model, messages=messages,
                                                            response_format=Outlook, temperature=0.2)
            parsed = resolve(resp.choices[0].message.parsed, pack)
            issues = validate(parsed, pack)
            history.append({"attempt": attempt, "issues": issues})
            if not issues:
                out = parsed
                break
            messages += [{"role": "assistant", "content": parsed.model_dump_json()},
                         {"role": "user", "content": "校验未通过，请修正后完整重写：\n" + "\n".join(issues)}]
    record = {"asof": str(asof), "drivers": drv, "pack": pack, "outlook": out.model_dump() if out else None,
              "validation": history, "model": settings.scout_insight_model if out else None}
    await db.execute("INSERT INTO scout.briefs (kind, asof, content, payload) VALUES ('sector_outlook', :d, :c, CAST(:p AS JSONB))",
                     {"d": asof, "c": render_text(record), "p": db.jsonb(record)})
    if out:
        by_key = {en["key"]: en for en in method_entries}
        for t in out.theses:
            await ledger.record("sector_thesis", t.target, asof, ledger.STANCE[t.stance], lens=t.method, sector_etf=EQUAL_WEIGHT,
                                source_ref="sector_outlook", model=record["model"], payload={**t.model_dump(), "headline": t.title})
            en = by_key.get(t.method)
            if en and en["status"] == "hypothesis":
                await learned.write({"key": en["key"], "status": "forward", "evidence": [{
                    "source": "ledger", "date": str(asof),
                    "summary": "开始前向记录：按本方法写的行业判断记入判断台账（sector_thesis），60 / 120 个交易日后对照 SPY 和等权 S&P 500（RSP）打分。"}]},
                    "system", f"首次使用（{t.target}，{asof}）")
                by_key[t.method] = {**en, "status": "forward"}
    return {"asof": str(asof), "theses": len(out.theses) if out else 0, "passed": out is not None,
            "issues": history[-1]["issues"] if history and not out else []}


async def latest() -> dict | None:
    row = await db.fetch_one("SELECT payload, created_at FROM scout.briefs WHERE kind = 'sector_outlook' ORDER BY id DESC LIMIT 1")
    return {**row["payload"], "created_at": row["created_at"]} if row else None


def drivers_lines(drv: dict) -> list[str]:
    etfs = drv["etfs"]
    rel = sorted(((etf, v.get("rel_20")) for etf, v in etfs.items() if etf in universe.SECTOR_ETFS.values() and v.get("rel_20") is not None),
                 key=lambda x: -x[1])
    lines = []
    if rel:
        lines.append("20 日相对 SPY 最强：" + "、".join(f"{ETF_ZH.get(e, e)} {_pct(r)}" for e, r in rel[:3])
                     + "；最弱：" + "、".join(f"{ETF_ZH.get(e, e)} {_pct(r)}" for e, r in rel[-2:]))
    st = drv["styles"].get(20) or drv["styles"].get("20") or {}     # int keys in memory, str after JSONB
    if st:
        lines.append("20 日风格：" + "；".join(f"{STYLE_ZH[k].split('（')[0]} {_pct(st.get(k))}" for k in STYLE_ZH if st.get(k) is not None))
    b = drv["breadth"]
    lines.append(f"宽度：站上 50 日线 {b['above_50'] * 100:.0f}%，站上 200 日线 {b['above_200'] * 100:.0f}%")
    return lines


def render_text(record: dict) -> str:
    lines = [f"🧭 行业前景（数据日期 {record['asof']}）", *drivers_lines(record["drivers"])]
    o = record.get("outlook")
    if not o:
        lines.append("本周的 AI 判断没有通过校验，只保留市场驱动的描述。")
        return "\n".join(lines)
    lines.append(f"驱动：{o['drivers']}")
    for t in o["theses"]:
        lines.append(f"• {ETF_ZH.get(t['target'], t['target'])}（{t['target']}）{t['stance']}，{t['horizon']} 个交易日：{t['title']}"
                     f"——{t['mechanism']} 证伪：{'；'.join(t['falsification'])}")
    if not o["theses"]:
        lines.append(f"本周没有新判断：{o.get('no_thesis_reason') or ''}")
    lines.append("判断方法都在前向验证中（未验证），满 30 个样本后才看校准。")
    return "\n".join(lines)
