"""The weekly market report (user's request 2026-11-04): market → sectors → stocks, every outside fact from the facts
store (verified, or unverified without numbers), every number from Scout's data or a verified fact.

1. Market: what moved US stocks this week and the bull / bear debate for the next one to three months (a web search
   through `external_facts`, subject MACRO), read against Scout's own market data (state, market filter, index and
   style returns, rates, dollar, oil, VIX, credit), the events of the next two weeks (`event_calendar`), and a
   direction for SPY over 20 sessions — scored in the ledger on SPY's own return (kind `view`, ticker SPY).
2. Sectors: one line for each of the 11 sectors from Scout's rotation / heat / flow / breadth data, plus full views
   (`viewdesk.write`) on the strongest and the weakest.
3. Stocks: full views on the holdings, the watchlist names that moved most on their own, and the most-reported
   names of the week.
One Telegram message with the summary; everything in brief `weekly_view` and on `/views`.
"""
from __future__ import annotations

import json
import logging
import time
from datetime import date, timedelta
from typing import Literal

from pydantic import BaseModel, Field

from scout import db, external_facts, ledger, universe, viewdesk
from scout.config import get_scout_settings
from scout.thesis import allowed_numbers, unsupported_numbers

logger = logging.getLogger("scout.weekly_view")

INDEXES = (("SPY", "标普 500"), ("QQQ", "纳指 100"), ("IWM", "罗素 2000 小盘"), ("RSP", "标普 500 等权"), ("TLT", "长期美债"), ("GLD", "黄金"))
HOT_NAMES = 3
CALENDAR_DAYS = 14

MARKET_PROMPT = """Today is {today}. Summarize what drove US stocks over the past week ({since} to {today}): the main moves and
their reported causes (rates, the Fed, economic data, earnings, AI investment, politics and policy, geopolitics). Then list the
strongest bull and bear arguments for US stocks over the next one to three months, from different kinds of sources (major news,
strategists and analysts as reported, independent research, investor community discussion). Use reports published between
{since} and {today}, with outlet and date for each."""


class Driver(BaseModel):
    driver: str = Field(description="本周的一个主导因素，几个字")
    explanation: str = Field(description="白话：发生了什么、为什么影响股市")
    fact_ids: list[int] = Field(description="支撑它的外部事实 id；只用 Scout 数据说明时为空")
    fields: list[str] = Field(description="用到的 Scout 市场数据字段")


class MarketArgument(BaseModel):
    claim: str
    source_kind: Literal["新闻媒体", "分析师", "独立研究", "投资者社区", "官方"]
    fact_ids: list[int] = Field(description="至少一个外部事实 id")
    check: Literal["Scout 数据一致", "部分一致", "Scout 数据矛盾", "无法用 Scout 数据核对"]
    check_note: str


class SectorLine(BaseModel):
    etf: str
    line: str = Field(description="一句白话：现状和最关键的数据，以及可能的原因（有外部事实就引用，没有就只写数据）")


class MarketWeek(BaseModel):
    week: str = Field(description="这周市场发生了什么，2–3 句白话")
    drivers: list[Driver]
    bull: list[MarketArgument]
    bear: list[MarketArgument]
    explained: list[viewdesk.Explained] = Field(description="2–4 个最关键的 Scout 市场数据，白话解读")
    sectors: list[SectorLine]
    ahead: str = Field(description="未来两周的关键事件和看点（来自 calendar）")
    stance: Literal["看涨", "中性", "看跌"] = Field(description="未来 20 个交易日 SPY 本身的涨跌方向")
    confidence: Literal["低", "中", "高"]
    confidence_reason: str
    wrong_if: str = Field(description="什么情况说明这个方向判断错了，具体可观察")
    for_you: str = Field(description="对用户组合意味着什么：核心仓（大盘 ETF，按大盘过滤管理）和卫星仓（个股，有上限）")


PROMPT = """你在给没有金融背景的投资者写每周市场观点。证据包：market（Scout 自己的市场数据，每项有 字段、名称、数值、含义）、
sectors（11 个板块的 Scout 数据）、facts（外部事实：已核对 = Scout 打开来源找到了原文；未核对 = 来源打不开，数字已隐去）、
calendar（未来两周的事件）、portfolio（用户的组合结构）、prediction（预测市场对宏观主题和大事的概率，市场价格，不是 Scout 的判断）。

要求：
1. week 和 drivers 写本周发生了什么、为什么；原因只能来自 facts（写明来源）或 Scout 数据，不要用你自己的知识补充新闻。
2. bull / bear 各 1–4 条，只用 facts，fact_ids 必填；只有未核对事实支撑的写成"有报道称"，不带数字；每条用 market 数据核对：
   论点的每个部分都被数据支持才是"Scout 数据一致"；只有一部分能核对（另一部分是对方的专有指标、预测或 Scout 没有的数据）写"部分一致"，
   check_note 说清哪部分核对了、哪部分没有；关于估值高低、估值压缩的论点用 valuation.* 核对（分清价格下跌和盈利变化）。
   报道的日期晚于 Scout 数据的日期时，check_note 写明两边各自的日期，不要用较早的数据否定较晚的报道。
3. explained 选 2–4 个最关键的 market 数据，按"是什么 → 通常意味着什么 → 对你意味着什么"写，专业词用半句话解释。
4. sectors 覆盖全部 11 个板块各一条（etf 用证据里的代码）。
4b. 在 week、drivers 或 ahead 里引用 prediction 中最重要的一两项概率及一周变化，写明来源（Polymarket / Kalshi）；不要把概率写成确定的事。
5. stance 是 SPY 未来 20 个交易日本身的涨跌方向，会被记账打分；证据不足时选"中性"。confidence 写"高"需要多条论点被 Scout 数据证实、
   且没有被否定的关键论点。
6. for_you 结合 portfolio：大盘过滤的状态、卫星仓是否超限，不给买卖指令。
7. 数字只能原样引用证据包里的数值，不要自己计算。中文。"""


def _pct(x: float | None, digits: int = 1) -> str | None:
    return None if x is None else f"{x * 100:+.{digits}f}%"


async def market_items(asof: date) -> list[dict]:
    """Scout's own market data as evidence items (字段, 名称, 数值, 含义)."""
    from scout import market_filter, opportunities, research

    items = []
    eod = await db.fetch_one("SELECT payload FROM scout.briefs WHERE kind = 'eod' ORDER BY id DESC LIMIT 1")
    market = ((eod or {}).get("payload") or {}).get("market") or {}
    if market:
        items.append({"字段": "market.state", "名称": "大盘状态", "数值": opportunities.state_text(market, detail=True),
                      "含义": "Scout 按 SPY 的长短期趋势和市场宽度（站上 50 日线的股票比例）判断的阶段"})
    mf = await market_filter.current(asof)
    if mf:
        items.append({"字段": "market.filter", "名称": "大盘过滤", "数值": market_filter.text(mf),
                      "含义": "SPY 月末收盘高于 10 个月均线为正常；低于时规则上不新买入，核心仓转现金"})
    rows = await db.fetch_all("SELECT ticker, date, close FROM scout.bars_daily WHERE ticker = ANY(:t) AND date >= :d ORDER BY date",
                              {"t": [t for t, _ in INDEXES], "d": asof - timedelta(days=45)})
    closes: dict[str, list[float]] = {}
    for r in rows:
        closes.setdefault(r["ticker"], []).append(r["close"])
    rets: dict[str, dict[int, float]] = {}
    for t, name in INDEXES:
        c = closes.get(t) or []
        for n in (5, 20):
            if len(c) > n:
                rets.setdefault(t, {})[n] = c[-1] / c[-1 - n] - 1
                items.append({"字段": f"{t}.ret_{n}d", "名称": f"{name}（{t}）近 {n} 个交易日", "数值": _pct(rets[t][n]),
                              "含义": "涨跌幅（价格，不含分红）"})
    for a, b, label, meaning in (("SPY", "RSP", "SPY 相对等权 RSP", "正 = 大市值公司更强，上涨集中在少数大公司；负 = 普通股票更强"),
                                 ("IWM", "SPY", "小盘 IWM 相对 SPY", "正 = 小盘股更强，通常意味着风险偏好较高或利率预期下行"),
                                 ("QQQ", "SPY", "科技成长 QQQ 相对 SPY", "正 = 科技成长更强")):
        if 20 in rets.get(a, {}) and 20 in rets.get(b, {}):
            items.append({"字段": f"style.{a.lower()}_{b.lower()}_20d", "名称": f"{label}（20 个交易日）",
                          "数值": _pct(rets[a][20] - rets[b][20]), "含义": meaning})
    macro = research.macro_now(await research._macro(asof - timedelta(days=60)))
    meaning = {"10 年期美债": "长期利率；上升通常压低成长股估值", "2 年期美债": "反映市场对美联储短期利率的预期",
               "美元指数": "美元走强通常不利于海外收入多的公司和大宗商品", "WTI 原油": "影响能源公司利润和通胀预期",
               "VIX": "标普期权隐含的未来 30 天波动；越高表示市场越紧张", "高收益债利差": "垃圾债相对国债的额外利率；扩大表示信用风险担忧上升"}
    live = await market_close(asof)
    for label, v in macro.items():
        text = f"{v['最新']}（20 个交易日变化 {v['20 个交易日变化']}，{v['日期']}，FRED）"
        y = live.get(label)
        if y and y["日期"] > v["日期"]:
            text = f"{y['最新']}（20 个交易日变化 {y['20 个交易日变化']}，{y['日期']}，Yahoo 收盘；FRED 最新到 {v['日期']}）"
        items.append({"字段": f"macro.{label}", "名称": label, "数值": text, "含义": meaning.get(label, "")})
    try:
        from scout import valuation

        items += await valuation.items(asof)
    except Exception as exc:             # the valuation is extra; the report still has the market data
        logger.warning("basket valuation failed: %s", exc)
    return items


LIVE = (("^TNX", "10 年期美债"), ("^VIX", "VIX"))


async def market_close(asof: date) -> dict[str, dict]:
    """The 10-year yield and VIX at Yahoo's last close: FRED's copies arrive a day or two later, which left the report
    unable to check same-day news (2026-11-07: Scout's 10-year stopped at 11-05, the rise reported for 11-06)."""
    from scout.data.yahoo import YahooProvider

    try:
        bars = await YahooProvider().daily_bars([s for s, _ in LIVE], asof - timedelta(days=45), asof)
    except Exception as exc:
        logger.warning("yahoo rates failed: %s", exc)
        return {}
    out = {}
    for sym, label in LIVE:
        c = bars.get(sym)
        if c is not None and len(c) > 20:
            c = c["close"]
            out[label] = {"最新": f"{c.iloc[-1]:.2f}", "20 个交易日变化": f"{c.iloc[-1] - c.iloc[-21]:+.2f}", "日期": c.index[-1].isoformat()}
    return out


async def sector_items() -> list[dict]:
    from scout import sectors

    _, rows = await sectors.latest("sector")
    out = []
    for r in sorted(rows, key=lambda r: -(r.get("rel_20") or -9)):
        c = sectors.card_context(r)
        out.append({"字段": f"{r['key']}.sector", "ETF": r["key"], "板块": c["板块"], "热度": c["热度"], "轮动象限": c["轮动象限"],
                    "上周象限": c["上周象限"], "资金流向（日线代理）": c["资金流向"],
                    "站上 50 日线比例": _pct(c["站上 50 日线比例"], 0)[1:] if c["站上 50 日线比例"] is not None else None,
                    "20 日相对 SPY": _pct(c["20 日相对 SPY"])})
    return out


async def portfolio_items() -> dict:
    from scout import levels, portfolio

    held = await portfolio.trades(open_only=True)
    sat = portfolio.satellite(await portfolio.get_settings())
    return {"核心仓": sorted({t["ticker"] for t in held if levels.is_core(t["ticker"])}) or "无",
            "卫星仓": sorted({t["ticker"] for t in held if not levels.is_core(t["ticker"])}) or "无",
            **({"卫星仓占账户": f"{sat['pct'] * 100:.1f}%（上限 {sat['cap_pct'] * 100:.0f}%）",
                "卫星仓超限": "是" if sat["over"] else "否"} if sat else {})}


def validate(m: MarketWeek, pack: dict) -> list[str]:
    issues = []
    texts = [m.week, m.ahead, m.confidence_reason, m.wrong_if, m.for_you, *[f"{d.driver} {d.explanation}" for d in m.drivers],
             *[f"{a.claim} {a.check_note}" for a in m.bull + m.bear], *[s.line for s in m.sectors],
             *[f"{x.what} {x.value} {x.usually} {x.for_you}" for x in m.explained]]
    bad = unsupported_numbers("\n".join(texts), allowed_numbers(pack))
    if bad:
        issues.append(f"以下数字不在证据包中：{', '.join(sorted(set(bad)))}")
    ids = {x["id"] for part in pack["facts"].values() if isinstance(part, list) for x in part}
    if any(not a.fact_ids or not set(a.fact_ids) <= ids for a in m.bull + m.bear) or \
            any(not set(d.fact_ids) <= ids for d in m.drivers):
        issues.append("论点和驱动因素的 fact_ids 只能填 facts 里存在的 id（论点至少一个）")
    known = {x["字段"] for x in pack["market"]}
    unknown = sorted({f for d in m.drivers for f in d.fields} - known) + sorted({x.field for x in m.explained} - known)
    if unknown:
        issues.append(f"引用了不存在的市场数据字段：{', '.join(unknown[:8])}")
    if not m.bull or not m.bear:
        issues.append("bull 和 bear 都至少一条")
    if not 2 <= len(m.explained) <= 4:
        issues.append("explained 要写 2–4 项")
    want = {s["ETF"] for s in pack["sectors"]}
    if sorted(s.etf for s in m.sectors) != sorted(want):
        issues.append(f"sectors 要覆盖这些板块各一条：{', '.join(sorted(want))}")
    confirmed = sum(a.check == "Scout 数据一致" for a in m.bull + m.bear)
    if m.confidence == "高" and (confirmed < 2 or any(a.check == "Scout 数据矛盾" for a in m.bull + m.bear)):
        issues.append("confidence 写「高」需要至少两条论点被 Scout 数据证实、且没有论点被否定")
    return issues


async def odds_pack() -> dict:
    """Prediction markets for the report: the macro topics and the week's largest changes."""
    from scout import prediction

    try:
        macro = await prediction.board(topic="macro")
        other = [e for e in await prediction.board() if e["topic"] != "macro"]
    except Exception as exc:
        logger.warning("prediction odds failed: %s", exc)
        return {}
    def swing(e):
        return max((abs((o["prob"] or 0) - o["before"]) for o in e["outcomes"] if o.get("before") is not None), default=0)
    return {"宏观主题": prediction.pack_rows(macro), "本周变化最大": prediction.pack_rows(sorted(other, key=swing, reverse=True)[:5])}


async def market_week(today: date) -> dict | None:
    """Section 1 and the sector lines: search → facts → one structured call → checks → ledger."""
    from scout import event_calendar, responses

    settings = get_scout_settings()
    since = today - timedelta(days=7)
    await external_facts.collect(MARKET_PROMPT.format(today=today, since=since), ["MACRO"], "weekly_market_search",
                                 model=settings.scout_agent_model, today=today)
    rows = await external_facts.recall(["MACRO"], days=10, today=today)
    cal = [event_calendar.event_row(e) for e in await event_calendar.load(today, today + timedelta(days=CALENDAR_DAYS))
           if e["importance"] == "高" or e["source"] != "ff"]
    pack = {"asof": today.isoformat(), "market": await market_items(today), "sectors": await sector_items(),
            "facts": external_facts.pack_rows(rows), "calendar": cal, "portfolio": await portfolio_items(),
            "prediction": await odds_pack()}
    out, tries = None, []
    if rows and settings.openai_api_key:
        messages = [{"role": "system", "content": PROMPT}, {"role": "user", "content": json.dumps(pack, ensure_ascii=False, default=str)}]
        for attempt in (1, 2):
            try:
                parsed = await responses.parse(None, settings.scout_agent_model, messages, MarketWeek, "weekly_market")
            except Exception as exc:
                tries.append([f"LLM 调用失败：{type(exc).__name__}"])
                break
            issues = validate(parsed, pack)
            tries.append(issues)
            if not issues:
                out = parsed
                break
            messages += [{"role": "assistant", "content": parsed.model_dump_json()},
                         {"role": "user", "content": "校验未通过，请修正后完整重写：\n" + "\n".join(issues)}]
    facts = {str(r["id"]): {"label": external_facts.label(r), "url": r["url"], "status": r["status"]} for r in rows}
    record = {"asof": today.isoformat(), "market": out.model_dump() if out else None, "validation": tries, "pack": pack,
              "facts": facts, "model": settings.scout_agent_model}
    if out:
        await ledger.record("view", universe.BENCHMARK, today, {"看涨": 1, "中性": 0, "看跌": -1}[out.stance], lens=out.confidence,
                            model=settings.scout_agent_model, payload={"summary": out.week, "horizon": 20, "scope": "market"})
    return record


async def hot_names(today: date, exclude: set[str]) -> list[str]:
    """The names with the most direct news this week, among the liquid universe."""
    from scout import premarket

    liquid = await premarket._tickers_ranked_by_liquidity()
    rows = await db.fetch_all(
        "SELECT ticker, count(DISTINCT url) AS n FROM public.ticker_news_library WHERE datetime_utc >= :s AND relevance = 'direct' "
        "AND ticker = ANY(:t) GROUP BY ticker ORDER BY n DESC LIMIT 30",
        {"s": int(time.time()) - 7 * 86400, "t": list(liquid)})
    return [r["ticker"] for r in rows if r["ticker"] not in exclude and not universe.SECTOR_ETFS.get(r["ticker"])][:HOT_NAMES]


async def run(today: date | None = None, push: bool = True) -> dict:
    today = today or date.today()
    try:
        market = await market_week(today)
    except Exception as exc:
        logger.warning("weekly market section failed: %s", exc)
        market = {"market": None, "error": type(exc).__name__}
    subjects = await viewdesk.subjects(today)
    names = {t for t, _ in subjects}
    subjects += [(t, None) for t in await hot_names(today, names)]
    views, failed = [], []
    for ticker, sector in subjects:
        try:
            rec = await viewdesk.write(ticker, today, sector)
        except Exception as exc:
            logger.warning("view on %s failed: %s", ticker, exc)
            rec = None
        if rec and rec.get("view"):
            views.append({"ticker": ticker, "name": rec["name"], "kind": "sector" if sector else "stock", "view": rec["view"]})
        else:
            failed.append(ticker)
    report = {**market, "views": views, "failed": failed}
    text = render(report)
    await db.execute("INSERT INTO scout.briefs (kind, asof, content, payload) VALUES ('weekly_view', :d, :c, CAST(:p AS JSONB))",
                     {"d": today, "c": text, "p": db.jsonb(report)})
    if push:
        from scout import telegram

        await telegram.send(telegram.esc(text))
    return {"market": bool(report.get("market")), "views": len(views), "failed": failed}


ICON = {"看涨": "🟢", "中性": "⚪", "看跌": "🔴", "看多": "🟢", "看空": "🔴"}
HEAT = {"热": "🔥", "温": "🌤", "冷": "🧊"}


def render(r: dict) -> str:
    lines = [f"📊 本周市场观点 · {r.get('asof', '')}（外部事实据报道，已核对的才带数字；方向记入判断台账）"]
    m, facts = r.get("market"), r.get("facts") or {}

    def src(ids: list[int]) -> str:
        return "；".join(dict.fromkeys(facts[str(i)]["label"] for i in ids if str(i) in facts))

    if m:
        lines += ["", "一、市场", m["week"], "本周主导因素："]
        lines += [f"· {d['driver']}：{d['explanation']}" + (f"（{src(d['fact_ids'])}）" if d["fact_ids"] else "") for d in m["drivers"]]
        lines.append("看涨的理由：")
        lines += [f"· {a['claim']}（{src(a['fact_ids'])}）→ {a['check']}" for a in m["bull"]]
        lines.append("看跌的理由：")
        lines += [f"· {a['claim']}（{src(a['fact_ids'])}）→ {a['check']}" for a in m["bear"]]
        lines.append("Scout 的数据怎么读：")
        lines += [f"· {x['what']}：{x['value']}。{x['usually']}；对你：{x['for_you']}" for x in m["explained"]]
        lines += [f"{ICON[m['stance']]} 未来 20 个交易日 SPY：{m['stance']}（把握{m['confidence']}）——{m['confidence_reason']}",
                  f"什么情况说明错了：{m['wrong_if']}", f"未来两周：{m['ahead']}", f"对你的组合：{m['for_you']}"]
        heat = {s["ETF"]: s.get("热度") for s in (r.get("pack") or {}).get("sectors", [])}
        lines += ["", "二、板块"]
        lines += [f"{HEAT.get(heat.get(s['etf']), '·')} {s['etf']}：{s['line']}" for s in m["sectors"]]
    else:
        lines += ["", "一、市场：本周没有写出通过校验的市场观点（外部事实不足或校验未通过）"]
    sec = [v for v in r.get("views", []) if v["kind"] == "sector"]
    if sec:
        lines.append("深入看：")
        lines += [f"{ICON[v['view']['stance']]} {v['ticker']} {v['view']['stance']}（{v['view']['horizon']} 日，把握{v['view']['confidence']}）："
                  f"{v['view']['summary']}" for v in sec]
    stocks = [v for v in r.get("views", []) if v["kind"] == "stock"]
    if stocks:
        lines += ["", "三、个股（持仓、自选股里变化大的、本周报道最多的）"]
        lines += [f"{ICON[v['view']['stance']]} {v['ticker']} {v['view']['stance']}（{v['view']['horizon']} 日，把握{v['view']['confidence']}）："
                  f"{v['view']['summary']}" for v in stocks]
    if r.get("failed"):
        lines.append(f"未生成：{'、'.join(r['failed'])}")
    lines.append("每条论点的来源、核对和白话解读见 Dashboard「多来源观点」，或问助手。")
    return "\n".join(lines)


async def latest() -> dict | None:
    row = await db.fetch_one("SELECT payload FROM scout.briefs WHERE kind = 'weekly_view' ORDER BY id DESC LIMIT 1")
    return row["payload"] if row else None
