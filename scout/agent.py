"""Q&A agent: read-only tool calling over Scout data (Telegram + dashboard)."""
from __future__ import annotations

import json
import logging
import re
from datetime import date
from typing import Any, Awaitable, Callable

from scout import context, db, knowledge, universe
from scout.config import get_scout_settings
from scout.features import SNAPSHOT_FEATURES

logger = logging.getLogger("scout.agent")

MAX_ROUNDS = 8
HISTORY_HOURS = 24
HISTORY_MESSAGES = 20

SYSTEM_PROMPT = f"""你是 Scout，一位美股研究助理，服务于一位有经验的个人投资者（单人账户）。今天是 {{today}}。
你只能通过工具读取 Scout 数据库里的数据；Scout 从不下单，你也不下达买卖指令。

关于 Scout 和这位用户（回答时以这些为前提）：
- 账户分核心仓（指数，在 Scout 之外）和卫星仓（「我的交易」里记录的个股，上限为账户的一定比例）。
- 持仓计划（2026-10-31 用户决定）：收盘低于买入以来最高收盘的 80% → 下一个交易日开盘卖出全部；不分批、不设目标、不设持有期限。
- 大盘过滤：SPY 月末收盘低于 10 个月均值时为避险，避险期不新买入，已有持仓不因此卖出。
- 仓位按 20% 止损和单笔风险计算，再受单只上限、波动上限和卫星仓剩余额度约束。
- 规则和选股方法都要过检验关（预登记、扣成本、相对同日其他股票、t ≥ 3、前后两半都为正）；知识库里"否定"的方法经检验无效，
  不能当作买入理由。机会卡来自规则，未通过检验的规则的卡片只作观察。
- 同一细分行业亏损卖出后 10 个交易日内再买入，会被标记（冷却期）。

工具怎么用：
- 问到"我的持仓 / 我的 X / 止损 / 该不该卖"先调 get_holdings；问到复盘、执行纪律调 get_trade_journal。
- 问到某只股票：get_snapshot（技术与涨跌拆分）、get_price_map（支撑阻力、买点、风险档位）、get_ai_insight（最近一次 AI 解读）、
  get_fundamentals（财报、估值、预期、持股、期权）、get_events（异动及原因）、get_news、get_earnings，按问题选。
- 问到某个方法有没有用、某条规则是否通过：get_knowledge。问到 Scout 过去判断准不准：get_ledger。
- 可以并行调用多个工具；需要就多调几次，宁可多查一步，也不要凭印象回答。

规则：
- 回答里的数字必须来自工具结果，原样引用（工具已按中文格式化）；查不到就说查不到，不要编造价格、新闻或财报数据。
- 指标带有 名称、数值、含义 和 读数（数据字典的标准解读），解读时以读数为准。
- 解释涨跌先拆成三段：大盘（market_ret_*）、行业（sector_ret_*）、公司自身（own_ret_*）；idio_ret_* 只扣了大盘、仍含行业。
- 行情是日线收盘数据，不是实时报价；注明数据日期。
- 给出条件和依据（"如果……说明……"），不要说"建议买入 / 卖出"；涉及卖出时说明持仓计划怎么规定。
- 中文，简洁具体；纯文本（不要 Markdown 表格、标题或 ** 加粗），适合 Telegram；不要出现英文字段名。
screen 可排序的字段：{", ".join(SNAPSHOT_FEATURES)}。"""

TICKER = {"type": "object", "properties": {"ticker": {"type": "string"}}, "required": ["ticker"]}
NONE = {"type": "object", "properties": {}}
TOOLS = [
    {"name": "get_holdings", "description": "用户当前持仓：成本、股数、最新收盘、盈亏、系统止损及依据、是否有待执行的卖出信号、买入理由，"
                                            "以及卫星仓占用、大盘过滤状态和账户风险设置", "parameters": NONE},
    {"name": "get_trade_journal", "description": "已平仓交易：盈亏、执行检查（按计划 / 提前 / 拖延、避险期买入、冷却期买入）和汇总",
     "parameters": {"type": "object", "properties": {"limit": {"type": "integer", "minimum": 1, "maximum": 50}}}},
    {"name": "get_snapshot", "description": "某只股票/ETF 最新一日的特征快照（含大盘 / 行业 / 公司自身的涨跌拆分）、所属行业、是否在 watchlist、最近的机会卡",
     "parameters": TICKER},
    {"name": "get_price_map", "description": "价位图：支撑 / 阻力、多空分界与情景、回调 / 突破买点（区间、失效位）、正常波动范围、风险档位、期权隐含波动和分析师目标价",
     "parameters": TICKER},
    {"name": "get_ai_insight", "description": "该股票最近一次 AI 解读（多视角）：结论、归因、各视角判断与要点、关键风险、观察点、失效条件和数据日期",
     "parameters": TICKER},
    {"name": "get_fundamentals", "description": "基本面与研究数据：财报趋势、估值（相对历史 / 行业）、预期与评级变化、财报反应、持股与做空、期权、宏观敏感度",
     "parameters": TICKER},
    {"name": "get_events", "description": "近 60 个交易日的异动（扣除大盘后 ≥ 3σ、涨跌 ≥ 8% 或 3 倍放量）及已归因的原因",
     "parameters": TICKER},
    {"name": "get_price_history", "description": "最近 N 个交易日的日线（日期、开高低收、成交量）",
     "parameters": {"type": "object", "properties": {"ticker": {"type": "string"},
                    "days": {"type": "integer", "minimum": 5, "maximum": 120}}, "required": ["ticker"]}},
    {"name": "get_news", "description": "与该股票相关的新闻（标题、来源、摘要、时间），默认最近 3 天，最长可查 90 天历史",
     "parameters": {"type": "object", "properties": {"ticker": {"type": "string"},
                    "days": {"type": "integer", "minimum": 1, "maximum": 90}}, "required": ["ticker"]}},
    {"name": "get_earnings", "description": "该股票最近一次和下一次财报（日期、EPS 预期/实际）", "parameters": TICKER},
    {"name": "get_knowledge", "description": "Scout 知识库：各规则、选股方法和研究结论的检验状态（已确认 / 前向验证中 / 回测通过 / 假设 / 否定）和最新证据；可按关键词过滤",
     "parameters": {"type": "object", "properties": {"query": {"type": "string", "description": "关键词，可选，如 动量、财报、突破"}}}},
    {"name": "get_ledger", "description": "判断台账：Scout 过去的判断（AI 解读、机会卡、行业热度）事后对不对；给 ticker 时返回该股的历次判断",
     "parameters": {"type": "object", "properties": {"ticker": {"type": "string"}}}},
    {"name": "list_opportunities", "description": "规则触发的机会卡列表（标明规则是否通过检验）",
     "parameters": {"type": "object", "properties": {
         "status": {"type": "string", "enum": ["active", "closed", "all"]},
         "ticker": {"type": "string"}, "limit": {"type": "integer", "minimum": 1, "maximum": 30}}}},
    {"name": "get_scoreboard", "description": "各规则的实盘表现与历史基准对比", "parameters": NONE},
    {"name": "get_market_overview", "description": "最新收盘的大盘状态、主要指数与行业/主题 ETF 表现、大盘过滤状态",
     "parameters": NONE},
    {"name": "screen", "description": "按某个特征对最新一日的股票排序筛选（例如 rs_pct 最高、ret_5d 最低）",
     "parameters": {"type": "object", "properties": {
         "sort_by": {"type": "string", "enum": list(SNAPSHOT_FEATURES)},
         "descending": {"type": "boolean"},
         "sector": {"type": "string", "description": "GICS 行业英文名，可选"},
         "watchlist_only": {"type": "boolean"},
         "limit": {"type": "integer", "minimum": 1, "maximum": 30}}, "required": ["sort_by"]}},
]


def _p(x: float | None, digits: int = 1) -> str:
    return "—" if x is None else f"{x * 100:+.{digits}f}%"


def _usd(x: float | None) -> str:
    return "—" if x is None else f"${x:,.0f}"


async def _latest_feature_date() -> date | None:
    row = await db.fetch_one("SELECT max(date) AS d FROM scout.features_daily")
    return row["d"] if row else None


async def get_snapshot(ticker: str) -> dict:
    t = universe.yahoo_symbol(ticker)
    feat = await db.fetch_one(
        "SELECT date, payload FROM scout.features_daily WHERE ticker = :t ORDER BY date DESC LIMIT 1", {"t": t}
    )
    inst = await db.fetch_one("SELECT name, sector, industry, kind FROM scout.instruments WHERE ticker = :t", {"t": t})
    watch = await db.fetch_one("SELECT theme FROM scout.watchlist WHERE ticker = :t AND active", {"t": t})
    opp = await db.fetch_one(
        "SELECT id, asof, primary_detector, status, score, entry_lo, entry_hi, stop, target1 "
        "FROM scout.opportunities WHERE ticker = :t ORDER BY asof DESC LIMIT 1", {"t": t}
    )
    if not feat and not inst:
        return {"error": f"{t} 不在 Scout 的标的池中，可用 /watch add {t} 加入"}
    from scout import knowledge, structure

    bars = await db.fetch_all(
        "SELECT * FROM (SELECT date, open, high, low, close, volume FROM scout.bars_daily WHERE ticker = :t "
        "ORDER BY date DESC LIMIT 260) x ORDER BY date", {"t": t},
    )
    values = {**((feat or {}).get("payload") or {}), **structure.compute(bars)}
    readings = [knowledge.evidence(k, v).as_dict() for k, v in values.items()
                if k in knowledge.features() and v is not None]
    return {"ticker": t, "instrument": inst, "watchlist_theme": (watch or {}).get("theme"),
            "features_date": feat and feat["date"], "indicators": readings, "latest_opportunity": opp}


async def get_price_history(ticker: str, days: int = 30) -> dict:
    t = universe.yahoo_symbol(ticker)
    rows = await db.fetch_all(
        "SELECT date, open, high, low, close, volume FROM scout.bars_daily WHERE ticker = :t "
        "ORDER BY date DESC LIMIT :n", {"t": t, "n": min(max(days, 5), 120)}
    )
    return {"ticker": t, "bars": [
        [r["date"].isoformat(), round(r["open"], 2), round(r["high"], 2), round(r["low"], 2), round(r["close"], 2), int(r["volume"])]
        for r in reversed(rows)
    ], "columns": ["date", "open", "high", "low", "close", "volume"]}


async def get_news(ticker: str, days: int = 3) -> dict:
    t = universe.yahoo_symbol(ticker)
    days = min(max(int(days), 1), 90)
    limit = context.MAX_HEADLINES if days <= 3 else 20
    return {"ticker": t, "days": days, "news": await context.load_news(t, hours=days * 24, limit=limit)}


async def get_earnings(ticker: str) -> dict:
    t = universe.yahoo_symbol(ticker)
    today = date.today()
    events = (await context.earnings_map(today, days_back=120, days_ahead=90)).get(t, [])
    return {"ticker": t, **context.earnings_summary(events, today)} if events else {"ticker": t, "note": "无财报日历数据"}


async def list_opportunities(status: str = "active", ticker: str | None = None, limit: int = 10) -> dict:
    where = {"active": "status IN ('new', 'triggered')", "closed": "status NOT IN ('new', 'triggered')", "all": "true"}[status]
    params: dict[str, Any] = {"n": limit}
    if ticker:
        where += " AND ticker = :t"
        params["t"] = universe.yahoo_symbol(ticker)
    rows = await db.fetch_all(
        f"SELECT o.id, o.ticker, o.asof, o.primary_detector, o.detectors, o.status, o.score, o.entry_lo, o.entry_hi, "
        f"o.stop, o.target1, o.horizon_days, o.thesis->>'headline' AS headline, oc.return_pct, oc.excess_pct "
        f"FROM scout.opportunities o LEFT JOIN scout.opportunity_outcomes oc ON oc.opportunity_id = o.id "
        f"WHERE {where} ORDER BY o.asof DESC, o.score DESC LIMIT :n", params,
    )
    from scout import learned

    quiet = learned.held_back(await learned.current())
    return {"opportunities": [{**r, "规则检验": "未通过检验，只作观察" if r["primary_detector"] in quiet else "未被否定"} for r in rows]}


async def get_scoreboard() -> dict:
    from scout.tracker import scoreboard

    return {"scoreboard": await scoreboard()}


async def get_market_overview() -> dict:
    brief = await db.fetch_one("SELECT asof, payload FROM scout.briefs WHERE kind = 'eod' ORDER BY asof DESC, id DESC LIMIT 1")
    d = await _latest_feature_date()
    rows = await db.fetch_all(
        "SELECT f.ticker, i.name, f.payload FROM scout.features_daily f JOIN scout.instruments i USING (ticker) "
        "WHERE i.kind = 'etf' AND f.date = :d ORDER BY f.ticker", {"d": d},
    ) if d else []
    # pre-formatted like the other tools, so the model quotes "-1.8%" instead of converting 0.0177 itself
    etfs = [{"代码": r["ticker"], "名称": r["name"], **{knowledge.zh(k): knowledge.fmt(k, (r["payload"] or {}).get(k))
                                                     for k in ("ret_1d", "ret_5d", "ret_20d")}} for r in rows]
    from scout import market_filter

    def pct_of(k: str, v: Any) -> Any:
        if isinstance(v, float) and k.endswith(("_1d", "_5d", "_20d", "_63d")):
            return knowledge.fmt("ret_1d", v)
        if isinstance(v, list) and k.endswith("_20d"):                 # [[ETF, return], ...] leaders / laggards
            return [[x[0], knowledge.fmt("ret_1d", x[1])] for x in v if isinstance(x, (list, tuple)) and len(x) == 2]
        return v

    market = {k: pct_of(k, v) for k, v in ((brief or {}).get("payload", {}).get("market") or {}).items()}
    return {"date": d, "market": market, "etfs": etfs,
            "大盘过滤": market_filter.text(await market_filter.current())}


async def screen(sort_by: str, descending: bool = True, sector: str | None = None,
                 watchlist_only: bool = False, limit: int = 10) -> dict:
    if sort_by not in SNAPSHOT_FEATURES:
        return {"error": "unknown feature"}
    d = await _latest_feature_date()
    if not d:
        return {"error": "no features yet"}
    where = ["f.date = :d", "i.kind = 'stock'", f"f.payload->>'{sort_by}' IS NOT NULL"]
    params: dict[str, Any] = {"d": d, "n": limit}
    if sector:
        where.append("i.sector = :s")
        params["s"] = sector
    if watchlist_only:
        where.append("f.ticker IN (SELECT ticker FROM scout.watchlist WHERE active)")
    order = "DESC" if descending else "ASC"
    rows = await db.fetch_all(
        f"SELECT f.ticker, i.name, i.sector, f.payload FROM scout.features_daily f JOIN scout.instruments i USING (ticker) "
        f"WHERE {' AND '.join(where)} ORDER BY (f.payload->>'{sort_by}')::float {order} LIMIT :n", params,
    )
    keep = dict.fromkeys(("close", "ret_1d", "ret_5d", "ret_20d", "ret_63d", "rs_pct", "vol_ratio", "dist_52w_high", sort_by))
    # Pre-formatted with Chinese names, as get_snapshot does: the model quotes "99%", not 0.9924.
    return {"date": d, "rows": [
        {"代码": r["ticker"], "名称": r["name"], "行业": r["sector"],
         **{knowledge.zh(k): knowledge.fmt(k, r["payload"].get(k)) for k in keep}} for r in rows
    ]}


async def get_holdings() -> dict:
    from scout import market_filter, portfolio, position_risk

    settings = await portfolio.get_settings()
    rows = []
    for t in await portfolio.trades(open_only=True):
        m, pl = portfolio.metrics(t), t.get("plan") or {}
        adh = position_risk.adherence(t, await portfolio.sells(t["id"]), date.today())
        thesis = pl.get("thesis") or {}
        rows.append({
            "代码": t["ticker"], "名称": t.get("name"), "行业": universe.SECTOR_ZH.get(t.get("sector"), t.get("sector")),
            "细分行业": t.get("industry"), "买入日": str(t["opened_on"]), "成本": f"{t['entry_price']:.2f}",
            "股数": f"{m['remaining']:g} / {t['shares']:g}", "最新收盘": f"{m['price']:.2f}" if m.get("price") else "—",
            "收盘日期": str(t.get("last_date")), "盈亏": _usd(m.get("pnl")), "盈亏幅度": _p(m.get("pnl_pct")),
            "系统止损": f"{t['stop']:.2f}" if t.get("stop") else "—", "止损依据": pl.get("stop_now_basis") or pl.get("stop_basis"),
            "距止损": _p(m.get("to_stop")), "待执行的卖出信号": (adh.get("signal") or {}).get("text") or "无",
            "买入时大盘避险": bool(pl.get("market_off_at_buy")),
            "同行业冷却期内买入": (pl.get("after_loss_same_industry") or {}).get("ticker"),
            "买入理由": thesis.get("reason") or t.get("note"), "证伪条件": thesis.get("falsification"),
            "来源": thesis.get("source")})
    sat = portfolio.satellite(settings)
    return {"持仓": rows, "大盘过滤": market_filter.text(await market_filter.current()),
            "卫星仓": ({"市值": _usd(sat["used"]), "占账户": f"{sat['pct'] * 100:.1f}%", "上限": f"{sat['cap_pct'] * 100:.0f}%",
                      "剩余额度": _usd(sat["room"])} if sat else "未设置账户规模"),
            "账户设置": {"账户规模": _usd(settings.get("account_size")), "单笔风险": f"{settings['risk_pct'] * 100:.1f}%",
                     "单只仓位上限": f"{settings['max_position_pct'] * 100:.0f}%", "单只月波动上限": f"{settings['vol_budget_pct'] * 100:.1f}%"}}


async def get_trade_journal(limit: int = 20) -> dict:
    from scout import portfolio

    closed = (await portfolio.trades(open_only=False))[:limit]
    per, summary = await portfolio.journal_adherence(closed)
    rows = []
    for t in closed:
        m, a = portfolio.metrics(t), per.get(t["id"]) or {}
        rows.append({"代码": t["ticker"], "买入日": str(t["opened_on"]), "平仓日": str(t["closed_on"]), "成本": f"{t['entry_price']:.2f}",
                     "平均卖出": f"{t['exit_price']:.2f}" if t.get("exit_price") else "—", "盈亏": _usd(m.get("pnl")),
                     "盈亏幅度": _p(m.get("pnl_pct")), "执行": a.get("verdict"), "卖出记录": [x["text"] for x in a.get("sells") or []],
                     "避险期买入": bool(a.get("against_filter")), "冷却期内买入": bool(a.get("after_loss")), "备注": t.get("note")})
    return {"已平仓": rows, "执行汇总": summary, "整体": portfolio.journal_summary(closed)}


async def get_price_map(ticker: str) -> dict:
    from scout import levels, position_risk

    t = universe.yahoo_symbol(ticker)
    pm = await levels.get_map(t) or {}
    if not pm:
        return {"error": f"{t} 没有价位图（不在标的池或数据不足）"}
    return {"代码": t, "数据日期": pm.get("asof"), "收盘": pm.get("close"), "趋势": pm.get("trend"),
            "支撑": [f"{x['label']} {x['price']:.2f}" for x in pm.get("supports", [])[:4]],
            "阻力": [f"{x['label']} {x['price']:.2f}" for x in pm.get("resistances", [])[:4]],
            "多空分界": pm.get("pivot"), "偏好情景": pm.get("preferred"), "备选情景": pm.get("alternative"),
            "买点": [{"名称": e["name"], "买入区间": f"{e['zone_lo']:.2f}–{e['zone_hi']:.2f}", "买点失效": f"{e['stop']:.2f}",
                    "上方阻力 1": f"{e['target1']:.2f}", "提示": e.get("warning")} for e in pm.get("entries", [])],
            "正常波动范围": [f"{r['label']}：±1σ {r['lo1']:.2f}–{r['hi1']:.2f}，±2σ {r['lo2']:.2f}–{r['hi2']:.2f}" for r in pm.get("move_range") or []],
            "风险档位": position_risk.tier_text(position_risk.tier(pm.get("vol_pct"))),
            "期权隐含波动": pm.get("expected_move"), "分析师目标价": pm.get("analyst_targets"), "备注": pm.get("note"),
            "说明": "价位是参考，不是经过回放检验的规则信号；持仓按回落 20% 管理，不按这里的失效位卖出"}


async def get_ai_insight(ticker: str) -> dict:
    from scout import insight

    t = universe.yahoo_symbol(ticker)
    rec = await insight.latest(t, max_age_days=365)      # any age: the data date says how old it is
    if not rec:
        return {"note": f"{t} 还没有 AI 解读（在 Dashboard 个股页生成）"}
    ins = rec.get("insight") or {}
    return {"代码": t, "数据日期": str(rec.get("asof")), "来源": rec.get("source"),
            "结论": ins.get("headline"), "驱动力": ins.get("driver"), "归因": ins.get("attribution"),
            "矛盾": ins.get("conflicts"), "关键风险": ins.get("key_risk"), "观察点": ins.get("watch"), "失效条件": ins.get("invalidation"),
            "置信度": ins.get("confidence"), "各视角": [{"视角": lv.get("lens"), "判断": lv.get("verdict"), "要点": lv.get("points")}
                                                 for lv in ins.get("lenses") or []]}


async def get_fundamentals(ticker: str) -> dict:
    from scout import fundamentals, research

    t = universe.yahoo_symbol(ticker)
    feat = await db.fetch_one("SELECT date, payload FROM scout.features_daily WHERE ticker = :t ORDER BY date DESC LIMIT 1", {"t": t})
    inst = await db.fetch_one("SELECT sector, kind FROM scout.instruments WHERE ticker = :t", {"t": t}) or {}
    if not feat:
        return {"error": f"{t} 没有数据"}
    asof = feat["date"]
    fund = await fundamentals.get(t) if (inst.get("kind") or "stock") == "stock" else {}
    events_ = (await context.earnings_map(asof, days_back=120, days_ahead=90)).get(t, [])
    rs = await research.build(t, asof, feat["payload"] or {}, inst.get("sector"),
                              context.earnings_summary(events_, asof) if events_ else {}, fund)
    values = {**fund, **rs["values"]}
    return {"代码": t, "数据日期": str(asof),
            "指标": [knowledge.evidence(k, v).as_dict() for k, v in values.items() if k in knowledge.features() and v is not None]}


async def get_events(ticker: str) -> dict:
    from scout import event_cause, events

    t = universe.yahoo_symbol(ticker)
    inst = await db.fetch_one("SELECT sector FROM scout.instruments WHERE ticker = :t", {"t": t}) or {}
    evs = await events.recent(t, universe.SECTOR_ETFS.get(inst.get("sector") or ""))
    causes = await event_cause.cached(t, [x["date"] for x in evs])
    return {"代码": t, "异动": [{"日期": x["date"], "当日涨跌": knowledge.fmt("event_ret", x["ret"]),
                              "扣除大盘后": knowledge.fmt("event_idio", x["idio"]), "板块当日": knowledge.fmt("event_sector_ret", x.get("sector_ret")),
                              "原因类型": (causes.get(x["date"]) or {}).get("cause_type"),
                              "原因": (causes.get(x["date"]) or {}).get("summary") or "尚未归因"} for x in evs]}


async def get_knowledge(query: str | None = None) -> dict:
    from scout import learned

    zh = {"confirmed": "已确认", "forward": "前向验证中", "backtested": "回测通过、待前向验证", "hypothesis": "假设（已预登记）",
          "rejected": "否定：经检验无效", "retired": "已失效"}
    rows = []
    for en in await learned.current():
        text = f"{en['key']} {en.get('title') or ''} {en.get('claim') or ''}"
        if query and query.lower() not in text.lower():
            continue
        ev = (en.get("evidence") or [])[-1:] or [{}]
        rows.append({"key": en["key"], "标题": en.get("title"), "状态": zh.get(en["status"], en["status"]), "类型": en.get("kind"),
                     "结论": (en.get("claim") or "")[:160], "最新证据": (ev[0].get("summary") or "")[:300]})
    return {"条目": rows[:40], "共": len(rows)}


async def get_ledger(ticker: str | None = None) -> dict:
    from scout import ledger

    out: dict = {"校准（20 日）": [{**r, "说明": "样本足够" if r["enough"] else f"样本不足 {ledger.MIN_SAMPLES}，不能下结论"}
                                 for r in await ledger.calibration(20)]}
    if ticker:
        t = universe.yahoo_symbol(ticker)
        out["该股历次 AI 判断"] = await ledger.history(t, date.today(), limit=6)
    return out


HANDLERS: dict[str, Callable[..., Awaitable[dict]]] = {
    "get_holdings": get_holdings, "get_trade_journal": get_trade_journal, "get_price_map": get_price_map,
    "get_ai_insight": get_ai_insight, "get_fundamentals": get_fundamentals, "get_events": get_events,
    "get_knowledge": get_knowledge, "get_ledger": get_ledger,
    "get_snapshot": get_snapshot, "get_price_history": get_price_history, "get_news": get_news,
    "get_earnings": get_earnings, "list_opportunities": list_opportunities, "get_scoreboard": get_scoreboard,
    "get_market_overview": get_market_overview, "screen": screen,
}


def _round(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 4)
    if isinstance(value, dict):
        return {k: _round(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_round(v) for v in value]
    return value


async def call_tool(name: str, args: dict) -> str:
    handler = HANDLERS.get(name)
    if handler is None:
        return json.dumps({"error": f"unknown tool {name}"})
    try:
        result = await handler(**args)
    except Exception as exc:
        logger.warning("tool %s failed: %s", name, exc)
        result = {"error": repr(exc)[:300]}
    return db.jsonb(_round(result))[:12000]


async def answer(question: str, channel: str = "telegram", remember: bool = True, trace: list | None = None) -> str:
    """Tool-using answer through /v1/responses (reasoning + function tools). `remember=False` neither reads nor
    writes chat history; `trace`, when given, collects each tool call with its output (for eval_agent)."""
    from scout import responses

    settings = get_scout_settings()
    if not settings.openai_api_key:
        return "问答功能需要配置 OPENAI_API_KEY。"
    history = await db.fetch_all(
        f"SELECT role, content FROM scout.chat_messages WHERE channel = :c AND created_at > now() - interval '{HISTORY_HOURS} hours' "
        "ORDER BY id DESC LIMIT :n", {"c": channel, "n": HISTORY_MESSAGES},
    ) if remember else []
    body = {"model": settings.scout_agent_model, "reasoning": {"effort": settings.scout_agent_reasoning},
            "instructions": SYSTEM_PROMPT.replace("{today}", date.today().isoformat()),
            "tools": [responses.as_tool(t) for t in TOOLS],
            "input": [{"role": r["role"], "content": r["content"]} for r in reversed(history)] + [{"role": "user", "content": question}]}
    text = "抱歉，这个问题没能在工具调用轮次内完成，请换个更具体的问法。"
    for _ in range(MAX_ROUNDS):
        data = await responses.create(body, timeout=settings.scout_agent_timeout_s)
        calls = responses.function_calls(data)
        if not calls:
            text = responses.output_text(data).strip() or text
            break
        outputs = []
        for c in calls:
            try:
                args = json.loads(c.get("arguments") or "{}")
            except json.JSONDecodeError:
                args = {}
            out = await call_tool(c["name"], args)
            if trace is not None:
                trace.append({"tool": c["name"], "args": args, "output": out})
            outputs.append({"type": "function_call_output", "call_id": c["call_id"], "output": out})
        body = {**body, "previous_response_id": data["id"], "input": outputs}
    if remember:
        await db.execute_many(
            "INSERT INTO scout.chat_messages (channel, role, content) VALUES (:c, :r, :t)",
            [{"c": channel, "r": "user", "t": question}, {"c": channel, "r": "assistant", "t": text}],
        )
    if channel != "telegram":
        return text
    from scout.telegram import esc

    # Models often emit **bold** despite the plain-text instruction; render it.
    return re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", esc(text))
