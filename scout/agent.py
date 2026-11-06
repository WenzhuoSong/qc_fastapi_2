"""Q&A agent: read-only tool calling over Scout data (Telegram + dashboard)."""
from __future__ import annotations

import json
import logging
import re
from datetime import date, timedelta
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
- 问到某个方法有没有用、某条规则是否通过：get_knowledge；问到前向验证、检验结果、研究进度：get_research。问到 Scout 过去判断准不准：get_ledger。
- 问到大盘、板块轮动、哪个行业强弱：get_market_overview、get_sectors；问到 Scout 的行业观点：get_outlook。
  板块"趋势"按 get_sectors 的趋势确认状态回答，并说明周期（短期 10 日 / 中期 20 日 / 较长 63 日）：确认只说明走势不像偶然，不说会延续。
- 问到今天 / 盘前要注意什么：get_briefs（premarket / eod / market_focus / weekly）；本周数据和财报、未来 30 天的美联储会议、选举、政策、行业和市场结构事件（含事前解读和事后复盘）：get_calendar。
- 问到机会卡 / 今天有什么机会：list_opportunities（会标明规则是否通过检验）；问到新闻对上下游、竞争对手的影响：get_propagation；问到某条规则在什么板块 / 市况下更好、胜率更高的组合：get_conditional_rates。
- 想全面了解一只股票（"X 怎么样""帮我分析 X"）：调 get_evidence，按下面"多视角证据"的写法逐个视角讲。
- 问到某件事发生的可能性、市场怎么押注（美联储加息 / 降息、就业数据、衰退、关税、停摆、选举、地缘）：get_prediction_markets，
  写明来源、概率、一周变化和成交量，说明这是市场价格不是 Scout 的判断。
- 问到某只股票、板块或大盘"会不会涨""下周 / 下个月怎么样"：get_norms，回答历史上涨比例、十次里较差的一次和正常范围，并说明 Scout 不能预测方向
  （用各种指标检验过，都不比历史比例更准：weekly_direction_v1、weekly_up_v2、sector_forecast_v1）；不要给出方向判断。
- 问到行业景气、行业核心数据（炼油差价、油价、天然气、利率对公用事业、银行利差、房贷利率等）或持仓为什么跟着行业动：get_industry_kpis。
- 问到分析师评级 / 目标价变化、公司公告、合同订单、监管诉讼、政策这类 Scout 自己没有的外部信息：先查 get_external_facts（外部信息库，
  每条有来源、日期和是否核对过原文），没有再用 research_company 检索；只有"已核对"的事实能引用数字，"未核对"的只能说"有报道称"。
- 问到"各方怎么看 X""市场上对 X 怎么说""多空分歧"：get_views（没有近期观点再用 research_views）；逐条讲看多 / 看空论点、
  来源和日期、Scout 数据是证实还是否定；关键数据按"是什么 → 通常意味着什么 → 对你意味着什么"讲；最后说明这只是观点，方向记入判断台账。
- 问到"从各个策略看这只股票行不行""哪些股票被多个策略选中"：get_strategy_view；逐个策略说是否选中、排第几，并强调每个策略的检验状态
  （只有前向验证中 / 已确认的有经过检验的依据；否定的经检验无效；描述性筛选只说明现状）。
- 问到公司的业务和行业现状（主要产品、客户、是不是某行业的主要供应商、行业需求是否旺盛、竞争格局）：先调 get_company_profile
  （公司自己年报里写的，带原文引用；"其他公司年报提到它"能看出它是谁的供应商），年报没写或需要最新情况时再用 research_company 联网检索。
  年报里的内容写成"据 X 年报（日期）"；
  这是外部报道：每条事实写明"据外部报道（未经核实）"和日期，并在回答末尾列出来源；再用 get_evidence 里的财务数据（收入增速与加速度、
  毛利率、预期修正、财报反应）和 get_outlook / get_sectors 的行业数据核对：报道说"需求旺盛"，就看收入和预期是否真的在加速、上调，
  一致就说一致，不一致就指出来。外部报道里的数字要标明来自报道，不能当成 Scout 计算的数字。
- 用户说想买 / 打算买 / 准备卖 / 要不要卖某只股票时：同时调 check_trade_idea（和记录交易时同一套检查）和 get_evidence（完整证据包），
  需要时再查 get_holdings、get_price_map、get_strategy_view（各策略是否选中）、research_company（业务和行业现状）。回答分四部分：
  1. 合不合用户的规则：逐条列出检查结果（卫星仓、大盘过滤、冷却期、建议股数、规则卡是否被否定、财报），不符合的放在前面；
  2. 多视角证据：按 get_evidence 的视角逐个过一遍，相近的可以合并成这几组——趋势与价格结构（趋势、支撑阻力）、动量与相对强度、
     波动与风险、大盘与行业（三段拆分、板块轮动）、事件与财报（催化、异动及原因、财报）、基本面（趋势、估值、预期与评级）、
     资金与情绪（持股与做空、内部人、期权）、宏观。每组写：这组整体偏多 / 偏空 / 中性；最关键的 2–4 个数字；用一句白话说明这些数字是什么意思、
     为什么重要（用证据里"—"后面的含义）；没有数据就写"缺少数据"。然后单独一段写"各视角之间的矛盾"（例如基本面改善但公司自身走势跑输），
     并引用 Scout 过去对这只股票的判断和各视角的历史可靠度（样本不足要说明）；
  3. 用户的理由站不站得住：和数据对照（例如说"板块强"，要看行业贡献和公司自身是否一致）；没有经过检验的方法要说明"未经检验"；
  4. 如果要做：建议写下的证伪条件和验证期、需要提前知道的事件；卖出时说明持仓计划怎么规定、这样卖算不算提前离场。
  不要给出"买"或"卖"的结论，最后一句说明决定由用户做。
- 问到符合某种条件的股票（内部人买入、毛利高、接近新高、7 天内财报、B0 打分）：get_screens；按单个指标排序：screen；某个规则信号成交了没有、还在等回踩吗：get_screens(screen_id="board")。
- 问到自选股、今天的提醒：get_watchlist；数据更新到哪天、某个任务有没有跑：get_data_status。
- 可以并行调用多个工具；需要就多调几次，宁可多查一步，也不要凭印象回答。

规则：
- 回答里的数字必须来自工具结果，原样引用（工具已按中文格式化）；查不到就说查不到，不要编造价格、新闻或财报数据。
- 只按这一次工具结果说话：不要说自己"之前漏掉了 / 之前说错了"，除非对话里能看到你之前的原话确实和工具结果矛盾；
  工具里有某笔记录，就直接说有（含日期），不要推测它是刚补上的。
- 指标带有 名称、数值、含义 和 读数（数据字典的标准解读），解读时以读数为准。
- 解释涨跌先拆成三段：大盘（market_ret_*）、行业（sector_ret_*）、公司自身（own_ret_*）；idio_ret_* 只扣了大盘、仍含行业。
- 行情是日线收盘数据，不是实时报价；注明数据日期。
- 给出条件和依据（"如果……说明……"），不要说"建议买入 / 卖出"；涉及卖出时说明持仓计划怎么规定。
- 中文，简洁具体；纯文本（不要 Markdown 表格、标题或 ** 加粗），适合 Telegram；不要出现英文字段名。
screen 可排序的字段：{", ".join(SNAPSHOT_FEATURES)}。"""

TICKER = {"type": "object", "properties": {"ticker": {"type": "string"}}, "required": ["ticker"]}
NONE = {"type": "object", "properties": {}}
# One registry for what the model sees (name, description, parameters), what runs, and which dashboard pages
# the tool covers (tests check every page has at least one tool, so the chat can answer what the dashboard shows).
TOOLS: list[dict] = []
HANDLERS: dict[str, Callable[..., Awaitable[dict]]] = {}
PAGES: dict[str, list[str]] = {}


def tool(name: str, description: str, parameters: dict | None = None, pages: tuple[str, ...] = ()):
    def register(fn: Callable[..., Awaitable[dict]]) -> Callable[..., Awaitable[dict]]:
        TOOLS.append({"name": name, "description": description, "parameters": parameters or NONE})
        HANDLERS[name] = fn
        for page in pages:
            PAGES.setdefault(page, []).append(name)
        return fn
    return register


def _p(x: float | None, digits: int = 1) -> str:
    return "—" if x is None else f"{x * 100:+.{digits}f}%"


def _usd(x: float | None) -> str:
    return "—" if x is None else f"${x:,.0f}"


async def _latest_feature_date() -> date | None:
    row = await db.fetch_one("SELECT max(date) AS d FROM scout.features_daily")
    return row["d"] if row else None


@tool('get_snapshot',
      '某只股票/ETF 最新一日的特征快照（含大盘 / 行业 / 公司自身的涨跌拆分）、所属行业、是否在 watchlist、最近的机会卡',
      TICKER,
      pages=('/ticker',))
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


@tool('get_price_history',
      '最近 N 个交易日的日线（日期、开高低收、成交量）',
      {'type': 'object',
       'properties': {'ticker': {'type': 'string'}, 'days': {'type': 'integer', 'minimum': 5, 'maximum': 120}},
       'required': ['ticker']},
      pages=('/ticker',))
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


@tool('get_news',
      '与该股票相关的新闻（标题、来源、摘要、时间），默认最近 3 天，最长可查 90 天历史',
      {'type': 'object',
       'properties': {'ticker': {'type': 'string'}, 'days': {'type': 'integer', 'minimum': 1, 'maximum': 90}},
       'required': ['ticker']},
      pages=('/ticker',))
async def get_news(ticker: str, days: int = 3) -> dict:
    t = universe.yahoo_symbol(ticker)
    days = min(max(int(days), 1), 90)
    limit = context.MAX_HEADLINES if days <= 3 else 20
    return {"ticker": t, "days": days, "news": await context.load_news(t, hours=days * 24, limit=limit)}


@tool('get_earnings',
      '该股票最近一次和下一次财报（日期、EPS 预期/实际）',
      TICKER,
      pages=('/ticker',))
async def get_earnings(ticker: str) -> dict:
    t = universe.yahoo_symbol(ticker)
    today = date.today()
    events = (await context.earnings_map(today, days_back=120, days_ahead=90)).get(t, [])
    return {"ticker": t, **context.earnings_summary(events, today)} if events else {"ticker": t, "note": "无财报日历数据"}


@tool('list_opportunities',
      '规则触发的机会卡列表（标明规则是否通过检验）',
      {'type': 'object',
       'properties': {'status': {'type': 'string', 'enum': ['active', 'closed', 'all']},
                      'ticker': {'type': 'string'},
                      'limit': {'type': 'integer', 'minimum': 1, 'maximum': 30}}},
      pages=('/', '/ideas'))
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


@tool('get_scoreboard',
      '各规则的实盘表现与历史基准对比',
      pages=('/scoreboard',))
async def get_scoreboard() -> dict:
    from scout.tracker import scoreboard

    return {"scoreboard": await scoreboard()}


@tool('get_market_overview',
      '最新收盘的大盘状态、主要指数与行业/主题 ETF 表现、大盘过滤状态',
      pages=('/', '/market'))
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


@tool("screen", "按某个特征对最新一日的股票排序筛选（例如 rs_pct 最高、ret_5d 最低）",
      {"type": "object", "properties": {
          "sort_by": {"type": "string", "enum": list(SNAPSHOT_FEATURES)},
          "descending": {"type": "boolean"},
          "sector": {"type": "string", "description": "GICS 行业英文名，可选"},
          "watchlist_only": {"type": "boolean"},
          "limit": {"type": "integer", "minimum": 1, "maximum": 30}}, "required": ["sort_by"]},
      pages=("/screens",))
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


@tool('get_holdings',
      '用户当前持仓：成本、股数、最新收盘、盈亏、系统止损及依据、是否有待执行的卖出信号、买入理由，组合风险（波动、分散度、风险贡献、风险档位），'
      '以及卫星仓占用、大盘过滤状态和账户风险设置',
      pages=('/', '/trades'))
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
    risk: dict = {}
    open_rows = await portfolio.trades(open_only=True)
    if open_rows:
        hr = await portfolio.holdings_risk(open_rows, settings.get("account_size"))
        v = hr.get("vol") or {}
        risk = {"组合年化波动": _usd(v.get("usd")), "一个月正常波动（1σ）": _usd(v.get("month_usd")),
                "占账户（月）": f"{v['month_pct_account'] * 100:.1f}%" if v.get("month_pct_account") else "—",
                "分散度": f"{v['diversification']:.2f}（1 = 完全不分散，越大分散越好）" if v.get("diversification") else "—",
                "风险贡献": [f"{c['ticker']} {c['share'] * 100:.0f}%" for c in v.get("contrib") or []],
                "风险档位": {tk: f"第 {t['tier']} 档（{t['label']}）" for tk, t in (hr.get("tiers") or {}).items() if t},
                "说明": f"按最近 {v.get('sessions', 63)} 个交易日的收益和相关性计算"}
    return {"持仓": rows, "组合风险": risk, "大盘过滤": market_filter.text(await market_filter.current()),
            "卫星仓": ({"市值": _usd(sat["used"]), "占账户": f"{sat['pct'] * 100:.1f}%", "上限": f"{sat['cap_pct'] * 100:.0f}%",
                      "剩余额度": _usd(sat["room"])} if sat else "未设置账户规模"),
            "账户设置": {"账户规模": _usd(settings.get("account_size")), "单笔风险": f"{settings['risk_pct'] * 100:.1f}%",
                     "单只仓位上限": f"{settings['max_position_pct'] * 100:.0f}%", "单只月波动上限": f"{settings['vol_budget_pct'] * 100:.1f}%"}}


@tool('get_trade_journal',
      '已平仓交易：盈亏、执行检查（按计划 / 提前 / 拖延、避险期买入、冷却期买入）和汇总',
      {'type': 'object', 'properties': {'limit': {'type': 'integer', 'minimum': 1, 'maximum': 50}}},
      pages=('/trades',))
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


@tool('get_price_map',
      '价位图：支撑 / 阻力、多空分界与情景、回调 / 突破买点（区间、失效位）、正常波动范围、风险档位、期权隐含波动和分析师目标价',
      TICKER,
      pages=('/ticker',))
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


@tool('get_ai_insight',
      '该股票最近一次 AI 解读（多视角）：结论、归因、各视角判断与要点、关键风险、观察点、失效条件和数据日期',
      TICKER,
      pages=('/ticker',))
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


@tool('get_fundamentals',
      '基本面与研究数据：财报趋势、估值（相对历史 / 行业）、预期与评级变化、财报反应、持股与做空、期权、宏观敏感度',
      TICKER,
      pages=('/ticker',))
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


@tool('get_events',
      '近 60 个交易日的异动（扣除大盘后 ≥ 3σ、涨跌 ≥ 8% 或 3 倍放量）及已归因的原因',
      TICKER,
      pages=('/ticker',))
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


@tool('get_knowledge',
      'Scout 知识库：各规则、选股方法和研究结论的检验状态（已确认 / 前向验证中 / 回测通过 / 假设 / 否定）和最新证据；可按关键词过滤',
      {'type': 'object', 'properties': {'query': {'type': 'string', 'description': '关键词，可选，如 动量、财报、突破'}}},
      pages=('/knowledge',))
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


@tool('get_ledger',
      '判断台账：Scout 过去的判断（AI 解读、机会卡、行业热度）事后对不对；给 ticker 时返回该股的历次判断',
      {'type': 'object', 'properties': {'ticker': {'type': 'string'}}},
      pages=('/ledger',))
async def get_ledger(ticker: str | None = None) -> dict:
    from scout import ledger

    out: dict = {"校准（20 日）": [{**r, "说明": "样本足够" if r["enough"] else f"样本不足 {ledger.MIN_SAMPLES}，不能下结论"}
                                 for r in await ledger.calibration(20)]}
    if ticker:
        t = universe.yahoo_symbol(ticker)
        out["该股历次 AI 判断"] = await ledger.history(t, date.today(), limit=6)
    return out




BRIEFS = {"premarket": "盘前简报", "eod": "收盘汇总（市场、板块、持仓、自选股变化、机会）", "market_focus": "市场关注点", "weekly": "周报"}
STUDIES = ("gate", "rule_benchmarks", "exit_vs_hold", "exit_redeploy", "exit_study", "hold_cap", "horizon_profile",
           "osap_replication", "sharadar_recheck", "level_study", "insight_eval", "agent_eval")


@tool("get_briefs", "Scout 最近一期的简报全文：盘前简报（隔夜行情、持仓盘前位置、财报、经济数据、新闻）、收盘扫描简报、市场关注点或周报",
      {"type": "object", "properties": {"kind": {"type": "string", "enum": list(BRIEFS)}}, "required": ["kind"]},
      pages=("/", "/market"))
async def get_briefs(kind: str) -> dict:
    row = await db.fetch_one("SELECT asof, content, created_at FROM scout.briefs WHERE kind = :k ORDER BY id DESC LIMIT 1", {"k": kind})
    if not row:
        return {"note": f"还没有{BRIEFS.get(kind, kind)}"}
    return {"简报": BRIEFS.get(kind, kind), "数据日期": str(row["asof"]), "生成时间": str(row["created_at"])[:16],
            "全文": (row["content"] or "")[:8000]}


@tool("get_sectors", "板块全景：每个行业和主题 ETF 的热度、轮动象限（领先 / 转弱 / 落后 / 改善）及上周象限、相对 SPY、"
                     "趋势确认（最新 5 日、短期 10 日、中期 20 日、较长 63 日：确认 / 初现 / 噪音，确认度、走势质量、成分股参与度）、"
                     "站上 50 日线比例、净新高、资金流、新闻关注度", pages=("/market",))
async def get_sectors() -> dict:
    from scout import sector_news, sector_trend
    from scout import sectors as sec

    asof, rows = await sec.latest()
    if asof is None:
        return {"note": "还没有板块数据"}
    news = await sector_news.by_sector()

    def row(r: dict) -> dict:
        n = news.get(r.get("key"), {})
        return {"名称": r.get("name"), "ETF": r.get("key"), "热度": r.get("heat_label"), "热度分": r.get("heat"),
                "轮动象限": sec.QUADRANTS.get(r.get("quadrant"), r.get("quadrant")),
                "上周象限": sec.QUADRANTS.get(r.get("quadrant_prev"), r.get("quadrant_prev")),
                "20 日相对 SPY": _p(r.get("rel_20")), "站上 50 日线": _p(r.get("pct_above_50"), 0).lstrip("+"),
                "净新高": r.get("net_new_highs"), "资金流 CMF": r.get("cmf_20"), "成交额热度": r.get("rel_dollar_vol"),
                "新闻关注度": sec.attention_label(n.get("news_attention")),
                "趋势确认": [{"周期": sector_trend.NAMES[c["h"]], "状态": c["label"] + ("（震荡）" if c.get("choppy") else ""),
                             "相对 SPY": _p(c.get("rel")), "确认度": knowledge.fmt("trend_z", c.get("z")),
                             "走势质量": knowledge.fmt("trend_er", c.get("er")),
                             **({"成分股跑赢 SPY": _p(c["breadth"], 0).lstrip("+")} if c.get("breadth") is not None else {}),
                             **({"未确认原因": c["missing"]} if c.get("missing") else {})} for c in r.get("trend") or []],
                "轮动速度（相对过去一年）": r.get("velocity_ratio")}

    sectors = sorted((r for r in rows if r.get("kind") == "sector"), key=lambda r: -(r.get("heat") or 0))
    return {"数据日期": str(asof), "行业": [row(r) for r in sectors], "主题": [row(r) for r in rows if r.get("kind") == "theme"],
            "趋势确认的含义": "确认 = 相对 SPY 的走势超过该板块平时相对波动的 2 倍、成分股过半同向、相邻周期同向；初现 = 1–2 倍或条件不全；"
                         "噪音 = 1 倍以内。只说明不像偶然，不预测延续：板块轮动在 Scout 检验 6 次未通过。"}


@tool("get_outlook", "每周行业展望：当前市场驱动、AI 的行业判断（目标 ETF、方向、期限、依据方法、证伪条件）以及历次行业判断事后的打分",
      pages=("/outlook",))
async def get_outlook() -> dict:
    from scout import outlook

    rec = await outlook.latest()
    rows = await db.fetch_all(
        "SELECT j.ticker, j.asof, j.stance, j.lens, j.payload->>'headline' AS headline, o60.excess_spy AS x60, o60.hit AS hit60 "
        "FROM scout.judgments j LEFT JOIN scout.judgment_outcomes o60 ON o60.judgment_id = j.id AND o60.horizon = 60 "
        "WHERE j.kind = 'sector_thesis' ORDER BY j.asof DESC, j.id DESC LIMIT 15")
    out = rec.get("outlook") if rec else None
    return {"数据日期": (rec or {}).get("asof"), "市场驱动": (out or {}).get("drivers"),
            "本周判断": (out or {}).get("theses") or (out or {}).get("no_thesis_reason") or "没有判断",
            "历次判断": [{"日期": str(r["asof"]), "目标": r["ticker"], "方向": {1: "偏多", 0: "中性", -1: "偏空"}.get(r["stance"]),
                       "判断": r["headline"], "60 日相对 SPY": _p(r["x60"]) if r["x60"] is not None else "未到期",
                       "60 日是否符合": None if r["hit60"] is None else ("符合" if r["hit60"] else "不符合")} for r in rows]}


@tool("get_screens", "预设筛选：不给 screen_id 时列出全部筛选（规则、技术信号、事件、基本面与事件角度、B0 动量打分、信号看板）和今天的数量；"
                     "给 screen_id 时返回符合条件的股票；screen_id='board' 返回每条规则信号的阶段（候选 / 触发 / 完成）",
      {"type": "object", "properties": {"screen_id": {"type": "string", "description": "筛选 id，先不带参数调用一次查看可用 id"}}},
      pages=("/screens",))
async def get_screens(screen_id: str | None = None) -> dict:
    from scout import b0, learned
    from scout import screens as sc

    asof, rows = await sc.snapshot()
    if asof is None:
        return {"note": "还没有每日快照"}
    quiet = learned.held_back(await learned.current())
    angle_asof, angle_rows = await sc.angle_snapshot()
    if not screen_id:
        listing = [{"id": x.id, "名称": x.title, "分组": x.group, "条件": list(x.criteria),
                    "规则检验": ("未通过检验" if x.detector.name in quiet else "未被否定") if x.detector is not None else "描述性筛选"}
                   for x in sc.all_screens()]
        listing += [{"id": x.id, "名称": x.title, "分组": "基本面与事件", "条件": list(x.criteria),
                     "今天数量": len(sc.run_angle(x, angle_rows))} for x in sc.ANGLE_SCREENS]
        listing.append({"id": "b0", "名称": "B0 动量打分", "分组": "我的策略", "条件": ["B0 自己的动量排名和大盘过滤（只作信息）"]})
        listing.append({"id": "board", "名称": "信号看板", "分组": "信号阶段",
                        "条件": ["每条规则信号所处的阶段：候选区（等回到买入区）、触发区（已成交）、完成区（已结束或作废）"]})
        return {"数据日期": str(asof), "筛选": listing}
    if screen_id == "board":
        from scout import signal_board

        board = await signal_board.load()
        return {"数据日期": str(board["asof"]), "说明": "价位以前向检验跟踪的那笔为准；「否定」的规则只做记录，不是买入理由；只记录规则表现，不是推荐",
                "信号": [{"阶段": signal_board.STAGES[r["stage"]], "代码": r["ticker"], "规则": r["label"], "规则检验": r["verdict"],
                        "信号日": str(r["asof"]), "买入区": f"{r['entry_lo']:.2f}–{r['entry_hi']:.2f}", "止损": r["stop"],
                        "目标": r["target1"], "现价": r["close"], "状态": r["text"]} for r in board["rows"][:40]]}
    if screen_id == "b0":
        rk = await b0.latest_b0_rank()
        return rk or {"note": "B0 打分未开启（SCOUT_B0_SCORE 未设置）"}
    angle = next((x for x in sc.ANGLE_SCREENS if x.id == screen_id), None)
    if angle:
        found = sc.run_angle(angle, angle_rows)
        return {"筛选": angle.title, "数据日期": str(angle_asof), "条件": list(angle.criteria),
                "股票": [{"代码": r["ticker"], "名称": r.get("name"), "数值": knowledge.fmt(angle.key, r.get(angle.key))} for r in found]}
    current = next((x for x in sc.all_screens() if x.id == screen_id), None)
    if current is None:
        return {"error": f"没有 id 为 {screen_id} 的筛选"}
    found = await sc.run(current, asof, rows)
    return {"筛选": current.title, "数据日期": str(asof), "条件": list(current.criteria),
            "规则检验": ("未通过检验，只作观察" if current.detector.name in quiet else "未被否定") if current.detector is not None else "描述性筛选",
            "股票": [{"代码": r["ticker"], "名称": r.get("name"), "收盘": r.get("close"), "1 日": _p(r.get("ret_1d")),
                    "20 日": _p(r.get("ret_20d")), "相对强度": _p(r.get("rs_pct"), 0).lstrip("+")} for r in found[:20]]}


@tool("get_research", "研究进度：正在前向验证的规则 / 方法（开始日期、完成的信号数、目前的超额、离确认还差什么）、"
                      "最近的检验关记录（通过与否）和最近的研究报告摘要", pages=("/knowledge", "/scoreboard"))
async def get_research() -> dict:
    from scout import forward, learned

    entries = await learned.current()
    progress = []
    for en in forward.under_test(entries):
        try:
            progress.append({"key": en["key"], "标题": en.get("title"), "状态": en["status"], **await forward.report(en, date.today())})
        except Exception as exc:
            progress.append({"key": en["key"], "error": repr(exc)[:120]})
    runs = await db.fetch_all("SELECT key, family, passed, ran_at FROM scout.gate_runs ORDER BY ran_at DESC LIMIT 15")
    studies = await db.fetch_all("SELECT DISTINCT ON (kind) kind, asof, content FROM scout.briefs WHERE kind = ANY(:k) "
                                 "ORDER BY kind, id DESC", {"k": list(STUDIES)})
    return {"前向验证": progress,
            "检验关记录": [{"规格": r["key"], "族": r["family"], "结果": "通过" if r["passed"] else "未通过", "日期": str(r["ran_at"])[:10]}
                      for r in runs],
            "研究报告": [{"类型": r["kind"], "日期": str(r["asof"]), "摘要": (r["content"] or "")[:600]} for r in studies],
            "说明": "本地数据（Sharadar 等）上的研究结论记在知识库条目和研究文档里，用 get_knowledge 查"}


@tool("get_calendar", "日历：本周美国经济数据（美东 / 北京时间、重要性、预期、前值）；未来 30 天的美联储会议、选举、政策与地缘、"
      "行业事件和市场结构事件（外部报道的标明未经核实），以及事前解读（是什么、为什么重要、可能情形、历史上市场怎么动、对持仓的影响）"
      "和事后复盘；持仓、自选和大市值股票的财报日期", pages=("/", "/calendar"))
async def get_calendar() -> dict:
    from scout import premarket

    econ = await context.economic_calendar()
    today = date.today()
    rows = await db.fetch_all("SELECT ticker, event_date, payload FROM scout.market_events WHERE event_type = 'earnings' "
                              "AND event_date BETWEEN :a AND :b ORDER BY event_date", {"a": today, "b": today + timedelta(days=7)})
    major = await premarket._tickers_ranked_by_liquidity()
    mine = {r["ticker"] for r in await db.fetch_all("SELECT ticker FROM scout.trades WHERE closed_on IS NULL "
                                                    "UNION SELECT ticker FROM scout.watchlist WHERE active")}
    hour = {"bmo": "盘前", "amc": "盘后"}
    from scout import event_calendar

    events = []
    for e in await event_calendar.load(today - timedelta(days=3), today + timedelta(days=30)):
        if e["source"] == "ff":
            continue
        notes = {n["note"]: n["payload"] for n in await db.fetch_all(
            "SELECT DISTINCT ON (note) note, payload FROM scout.event_notes WHERE event_key = :k ORDER BY note, created_at DESC",
            {"k": e["key"]})}
        events.append({**event_calendar.event_row(e),
                       **({"事前解读": notes["preview"]["preview"]} if "preview" in notes else {}),
                       **({"事后复盘": notes["review"]["review"]} if "review" in notes else {})})
    return {"事件": events,
            "经济数据": [{"事件": e["title_zh"], "日期": e["date"], "美东": e["time_et"], "北京": e["time_bj"], "重要性": e["impact"],
                       "预期": e.get("forecast"), "前值": e.get("previous")} for e in econ],
            "财报": [{"代码": r["ticker"], "日期": str(r["event_date"]), "时间": hour.get((r["payload"] or {}).get("hour"), "未定"),
                    "身份": "持仓或自选" if r["ticker"] in mine else "大市值"}
                   for r in rows if r["ticker"] in mine or r["ticker"] in major][:40]}


@tool("get_watchlist", "自选股：每只的主题、最新收盘和涨跌、相对强度，以及最近两天 Scout 推送过的盘中提醒", pages=("/watchlist",))
async def get_watchlist() -> dict:
    rows = await db.fetch_all(
        "SELECT w.ticker, w.theme, f.date, f.payload FROM scout.watchlist w LEFT JOIN LATERAL (SELECT date, payload FROM scout.features_daily "
        "WHERE ticker = w.ticker ORDER BY date DESC LIMIT 1) f ON true WHERE w.active ORDER BY w.theme, w.ticker")
    alerts = await db.fetch_all("SELECT kind, ticker, payload, created_at FROM scout.alerts WHERE created_at > now() - interval '2 days' "
                                "ORDER BY created_at DESC LIMIT 30")
    return {"自选": [{"代码": r["ticker"], "主题": r["theme"], "日期": str(r["date"]), "收盘": (r["payload"] or {}).get("close"),
                    "1 日": _p((r["payload"] or {}).get("ret_1d")), "20 日": _p((r["payload"] or {}).get("ret_20d")),
                    "相对强度": _p((r["payload"] or {}).get("rs_pct"), 0).lstrip("+")} for r in rows],
            "最近提醒": [{"类型": a["kind"], "代码": a["ticker"], "时间": str(a["created_at"])[:16],
                      "内容": (a["payload"] or {}).get("text") if isinstance(a["payload"], dict) else None} for a in alerts]}


@tool("get_data_status", "数据状态：各数据源（行情、SEC 财报与文件、内部人、预期与评级、持股、期权、宏观）更新到哪天、覆盖多少股票、"
                         "最近的数据质量问题、各定时任务最近一次运行、存储占用和 AI 用量（估计费用）", pages=("/data",))
async def get_data_status() -> dict:
    from scout import research_data

    from scout import llm_usage

    st = await research_data.status()
    usage = await llm_usage.totals(14)
    ins = await db.fetch_one("SELECT max(filed) AS last, count(DISTINCT ticker) AS tickers, count(*) AS n FROM scout.insider_trades")
    loads = await db.fetch_one("SELECT max(period) AS p FROM scout.data_loads WHERE source = 'sec_insiders'")
    return {"内部人交易（SEC Form 4 数据集，按季度发布）": {"最新申报日": str((ins or {}).get("last")), "股票数": (ins or {}).get("tickers"),
                                              "记录数": (ins or {}).get("n"), "已载入的季度": (loads or {}).get("p")},
            "AI 用量（估算，近 14 天）": {"合计": f"${usage['total_usd']:.2f}", "每天": usage["days"]},
            "快照": st.get("snapshots"), "财务": st.get("financials"), "文件": st.get("filings"), "评级变动": st.get("actions"),
            "宏观": st.get("macro"), "质量问题": st.get("issues"), "任务运行": [{"任务": r["job"], "状态": r["status"],
                                                                     "开始": str(r["started_at"])[:16]} for r in st.get("runs") or []],
            "存储": st.get("storage")}


@tool("check_trade_idea", "交易前检查（用户说想买 / 打算卖时先调用）：与记录交易时完全相同的逐项检查——是否在买点区间、回落 20% 的止损和亏损金额、"
                          "建议股数、买入后卫星仓、大盘过滤、风险档位、同行业冷却期、是否来自未通过检验的规则卡、自己和同行业大公司的财报、"
                          "大盘 / 行业 / 公司自身的涨跌拆分、同一细分行业已有持仓；卖出时对照持仓计划（是否提前离场）。没给价格用最近收盘，没给股数用建议股数",
      {"type": "object", "properties": {
          "ticker": {"type": "string"}, "side": {"type": "string", "enum": ["buy", "sell"]},
          "price": {"type": "number", "description": "打算的成交价，可选"},
          "shares": {"type": "number", "description": "打算的股数，可选；卖出全部可不填"}}, "required": ["ticker", "side"]},
      pages=("/trades",))
async def check_trade_idea(ticker: str, side: str, price: float | None = None, shares: float | None = None) -> dict:
    from scout import portfolio, trade_intake
    from scout.market_calendar import us_equity_market_status

    t = universe.yahoo_symbol(ticker)
    last = await db.fetch_one("SELECT date, close FROM scout.bars_daily WHERE ticker = :t ORDER BY date DESC LIMIT 1", {"t": t})
    if not last:
        return {"error": f"{t} 不在 Scout 的标的池（没有行情）"}
    day = trade_intake.trade_day({"date": None}, date.fromisoformat(us_equity_market_status().market_date))
    px = price or last["close"]
    notes = [] if price else [f"没给价格，按最近收盘 {last['close']:.2f}（{last['date']}）计算"]
    if side == "sell":
        held = [x for x in await portfolio.trades(open_only=True) if x["ticker"] == t]
        if not held:
            return {"error": f"没有 {t} 的持仓"}
        p = {"ticker": t, "side": "sell", "price": px, "shares": shares, "all_shares": not shares}
        return {"代码": t, "说明": notes, "检查": await trade_intake.sell_checks(p, day)}
    if not shares:
        settings = await portfolio.get_settings()
        f = await db.fetch_one("SELECT payload FROM scout.features_daily WHERE ticker = :t ORDER BY date DESC LIMIT 1", {"t": t})
        sz = portfolio.size(px, portfolio.holding_stop(px), settings, ((f or {}).get("payload") or {}).get("vol_63")) or {}
        shares = sz.get("shares") or 0
        notes.append(f"没给股数，按你的设置建议的 {shares:g} 股计算" if shares else "按你的设置建议股数为 0（额度或上限已用完），以下按 1 股计算")
        shares = shares or 1
    p = {"ticker": t, "side": "buy", "price": px, "shares": shares}
    return {"代码": t, "说明": notes, "检查": await trade_intake.buy_checks(p, day),
            "提示": "以上是对照用户自己规则的检查，数字由 Python 计算；结论由用户决定"}


@tool("get_evidence", "某只股票的完整证据包（和 AI 解读用的是同一份）：14 个视角——趋势、支撑阻力、动量与相对强度、波动与风险、大盘与行业、"
                      "催化、异动、财报、基本面趋势、估值、预期与评级、持股与做空、期权、宏观——每个数字都带名称、含义和标准读数；"
                      "另有 Scout 的规则读数、新闻、异动原因、相关知识库结论、Scout 过去对它的判断和各视角的历史可靠度。全面分析或交易前评估时用",
      TICKER, pages=("/ticker",))
async def get_evidence(ticker: str) -> dict:
    from scout import insight

    facts = await insight.gather(universe.yahoo_symbol(ticker))
    if not facts:
        return {"error": f"{ticker} 不在 Scout 的标的池（没有数据）"}
    pack = insight.build_pack(facts)
    lenses = []
    for lv in pack["lenses"]:
        items = [f"{x['名称']}：{x['数值']}" + (f"（{x['读数']}）" if x.get("读数") else "") + f" — {x['含义']}"
                 for x in lv["evidence"]]
        lenses.append({"视角": lv["title"], "证据": items or ["这个视角没有数据"]})
    return {"代码": pack["ticker"], "名称": pack["name"], "类型": pack["type"], "数据日期": pack["data_date"],
            "各视角": lenses, "背景": pack["context"],
            "说明": "数值已按中文格式化，原样引用；'—' 后是这个指标的含义，向用户解释时用它；没有数据的视角要说明缺失"}


@tool("get_strategy_view", "策略视角：给 ticker 时，逐个检查 Scout 的每个策略（5 条规则及候选规则、技术信号筛选、基本面与事件角度排名、B0 动量打分、"
                           "7 天内财报）今天是否选中这只股票、排第几，并标明每个策略的检验状态；不给 ticker 时，列出今天被最多策略同时选中的股票",
      {"type": "object", "properties": {"ticker": {"type": "string", "description": "可选；不填则扫描全部股票"}}},
      pages=("/screens",))
async def get_strategy_view(ticker: str | None = None) -> dict:
    from scout import b0, learned
    from scout import screens as sc

    asof, rows = await sc.snapshot()
    if asof is None:
        return {"note": "还没有每日快照"}
    entries = await learned.current()
    zh = {"confirmed": "已确认", "forward": "前向验证中", "backtested": "回测通过", "hypothesis": "假设", "rejected": "否定（检验无效）",
          "retired": "已失效"}
    by_rule = {r: zh.get(en["status"], en["status"]) for en in entries for r in [learned.rule_of(en)] if r}
    by_key = {en["key"]: zh.get(en["status"], en["status"]) for en in entries}
    picks: dict[str, list[str]] = {}                       # ticker → strategies selecting it today
    detail: list[dict] = []
    t = universe.yahoo_symbol(ticker) if ticker else None
    for x in sc.all_screens():
        if x.detector is not None:            # full membership (the screens page shows at most 40 rows)
            names = [r["ticker"] for r in await db.fetch_all(
                "SELECT ticker FROM scout.signal_hits WHERE asof = :d AND detector = :det ORDER BY score DESC",
                {"d": asof, "det": x.detector.name})]
        elif x is sc.EARNINGS_SCREEN:
            names = [r["ticker"] for r in await db.fetch_all(
                "SELECT ticker FROM scout.market_events WHERE event_type = 'earnings' AND event_date BETWEEN :a AND :b",
                {"a": asof, "b": asof + timedelta(days=7)})]
        else:
            names = [r["ticker"] for r in rows if x.keep(r)]
        status = (by_rule.get(x.detector.name, "未登记检验") if x.detector is not None else "描述性筛选（没有检验）")
        for n in names:
            picks.setdefault(n, []).append(f"{x.title}（{status}）")
        if t:
            detail.append({"策略": x.title, "分组": x.group, "检验状态": status, "今天选中": t in names,
                           "条件": list(x.criteria), "选中数": len(names)})
    angle_asof, angle_rows = await sc.angle_snapshot()
    for x in sc.ANGLE_SCREENS:
        ranked = sorted((r for r in angle_rows if r.get(x.key) is not None), key=lambda r: r[x.key], reverse=x.descending)
        top = {r["ticker"] for r in ranked[:max(1, len(ranked) // 10)]}
        status = by_key.get(x.strategy, "未登记检验")
        for n in top:
            picks.setdefault(n, []).append(f"{x.title}前 10%（{status}）")
        if t:
            pos = next((i for i, r in enumerate(ranked, 1) if r["ticker"] == t), None)
            val = next((r[x.key] for r in ranked if r["ticker"] == t), None)
            detail.append({"策略": x.title, "分组": "基本面与事件", "检验状态": status, "今天选中（前 10%）": t in top,
                           "排名": f"{pos} / {len(ranked)}" if pos else "没有数据", "数值": knowledge.fmt(x.key, val), "条件": list(x.criteria)})
    rk = await b0.latest_b0_rank()
    if rk:
        order = [r.get("ticker") for r in rk.get("rows") or []]
        for n in order[:10]:
            picks.setdefault(n, []).append("B0 动量打分前 10（只作信息）")
        if t:
            detail.append({"策略": "B0 动量打分", "分组": "我的策略", "检验状态": "用户自己的策略（只作信息）",
                           "排名": f"{order.index(t) + 1} / {len(order)}" if t in order else "不在名单"})
    if t:
        return {"代码": t, "数据日期": str(asof), "各策略": detail, "被选中的策略": picks.get(t, []),
                "说明": "只有检验状态为'前向验证中'或'已确认'的策略有经过检验的依据；'否定'表示经检验无效，'描述性筛选'只说明现在符合什么条件"}
    top = sorted(picks.items(), key=lambda kv: -len(kv[1]))[:15]
    return {"数据日期": str(asof), "被最多策略选中的股票": [{"代码": k, "策略数": len(v), "策略": v} for k, v in top],
            "说明": "被多个策略选中不等于有优势：先看每个策略的检验状态"}


@tool("get_company_profile", "公司画像与产业链（来自公司自己的年报 10-K / 20-F 业务部分，每条都有原文引用，按申报日保存）：业务分部与收入、"
                             "终端市场、点名的客户（含披露的收入占比）、供应商 / 代工厂、竞争对手、合作方、竞争优势、业务风险、明确未披露的信息；"
                             "以及其他公司年报里提到它的关系（谁把它列为供应商 / 客户 / 竞争对手），和这些关联公司的近期走势与财报日期",
      TICKER, pages=("/ticker",))
async def get_company_profile(ticker: str) -> dict:
    from scout import company_profile

    t = universe.yahoo_symbol(ticker)
    p = await company_profile.latest(t)
    if not p:
        return {"note": f"{t} 还没有公司画像（没有找到 10-K / 20-F 业务部分，或还没处理）；可以用 research_company 联网检索"}
    prof = p["profile"] or {}

    def items(key: str) -> list[str]:
        return [f"{x.get('name') or x.get('point')}：{x.get('detail') or ''}（原文：{x['quote'][:200]}）" for x in prof.get(key) or []]

    zh = {"customer": "客户", "supplier": "供应商", "competitor": "竞争对手", "partner": "合作方"}
    near = await company_profile.neighbours(t)
    rows = await db.fetch_all("SELECT f.ticker, f.payload FROM scout.features_daily f WHERE f.ticker = ANY(:t) "
                              "AND f.date = (SELECT max(date) FROM scout.features_daily)", {"t": list(near)}) if near else []
    earn = {r["ticker"]: str(r["event_date"]) for r in await db.fetch_all(
        "SELECT DISTINCT ON (ticker) ticker, event_date FROM scout.market_events WHERE event_type = 'earnings' AND ticker = ANY(:t) "
        "AND event_date >= CURRENT_DATE ORDER BY ticker, event_date", {"t": list(near)})} if near else {}
    return {"代码": t, "来源": f"{p['form']}（{p['filed']} 申报）", "原文链接": p["url"], "概述": prof.get("summary"),
            "业务分部": items("segments"), "终端市场": items("end_markets"), "客户": items("customers"), "供应商": items("suppliers"),
            "竞争对手": items("competitors"), "合作方": items("partners"), "竞争优势": items("advantages"), "业务风险": items("business_risks"),
            "年报未披露": prof.get("not_disclosed"),
            "其他公司年报提到它": [f"{r['ticker']} {REL_BY.get(r['relation'], r['relation'])}（{r['filed']}，原文：{(r['quote'] or '')[:160]}）"
                            for r in p["named_by"]],
            "关联公司近况": [{"代码": r["ticker"], "关系": "、".join(near[r["ticker"]]), "20 日": _p((r["payload"] or {}).get("ret_20d")),
                          "20 日公司自身": _p((r["payload"] or {}).get("own_ret_20d")), "下次财报": earn.get(r["ticker"], "未知")} for r in rows],
            "说明": "年报一年一份，说的是申报时的情况；最新的需求和订单变化看季度财务数据（get_evidence）和 research_company；"
                    f"关系类型：{zh}"}


REL_BY = {"customer": "在年报里把它列为客户（它向对方采购，对方是它的供应商）", "supplier": "在年报里把它列为供应商（它向对方供货，对方是它的客户）",
          "competitor": "在年报里把它列为竞争对手", "partner": "在年报里把它列为合作方"}


@tool("get_propagation", "产业链传导提示：最近的新闻（关于某公司的重要消息）对它的客户、供应商、竞争对手、合作方可能的利好 / 利空推断，"
                         "含渠道、年报原文依据、当天关联公司的走势；给 ticker 时只看与它相关的（它是消息来源或受影响方）。推断，未经检验",
      {"type": "object", "properties": {"ticker": {"type": "string"}, "days": {"type": "integer", "minimum": 1, "maximum": 60}}},
      pages=("/",))
async def get_propagation(ticker: str | None = None, days: int = 14) -> dict:
    from scout import propagation

    zh = propagation.REL_ZH
    rows = await propagation.recent(ticker, days)
    return {"说明": "推断，未经检验；每条都记入判断台账，5 / 20 / 60 个交易日后对照 SPY 和行业打分（用 get_ledger 看校准）",
            "提示": [{"日期": str(r["asof"]), "消息来源": r["source_ticker"], "新闻": r["headline"], "受影响": r["target_ticker"],
                    "关系": f"{r['target_ticker']} 是 {r['source_ticker']} 的{zh.get(r['relation'], r['relation'])}",
                    "方向": "利好" if r["impact"] > 0 else "利空", "渠道": r["mechanism"], "置信度": r["confidence"],
                    "年报依据": (r["relation_quote"] or "")[:200],
                    "当天走势": {k: _p(v) for k, v in (r["reaction"] or {}).items() if v is not None}} for r in rows]}


@tool("get_conditional_rates", "规则在不同板块 × 市况下的历史条件统计（收缩估计，预登记、只跑一次）：每格笔数、扣费后上涨比例与同日随机股票对照、"
                               "相对随机股票的收缩超额和 90% 区间。不给 detector 时返回各规则整体的统计",
      {"type": "object", "properties": {"detector": {"type": "string", "description": "规则名（机会卡的 primary_detector）"},
                                        "sector": {"type": "string"}, "regime": {"type": "string", "enum": ["uptrend", "mixed", "downtrend"]}}},
      pages=("/scoreboard",))
async def get_conditional_rates(detector: str | None = None, sector: str | None = None, regime: str | None = None) -> dict:
    from scout import conditional_rates as cr

    note = ("历史统计（当前成分股，有幸存者偏差），未经前向验证。2026-11-03 的结论：细分到板块 × 市况后，"
            "各组合之间的差异没有超出抽样误差，组合不比规则整体提供更多信息；看规则整体即可")
    if detector and (sector or regime):
        return {"说明": note, "所在组合": cr.text(await cr.for_signal(detector, sector, regime))}
    where, params = ("detector = :d AND layer <> 'vol'", {"d": detector}) if detector else ("layer = 'all'", {})
    rows = await db.fetch_all(f"SELECT * FROM scout.conditional_rates WHERE run_on = (SELECT max(run_on) FROM scout.conditional_rates) "
                              f"AND {where} ORDER BY layer = 'all' DESC, raw_edge DESC NULLS LAST LIMIT 60", params)
    return {"说明": note, "组合": [{"规则": r["detector"], "层": r["layer"], "统计": cr.text(r)} for r in rows]}


@tool("get_prediction_markets", "预测市场（Polymarket / Kalshi，只读）对事件的概率：美联储议息、就业 / CPI 等数据、选举和政策、宏观主题（衰退、"
                                "美联储路径、政府停摆、关税、伊朗、俄乌、油价、台海）、和持仓公司相关的市场，以及每日扫描发现的大事；"
                                "每项有各结果的概率、一周前的概率、成交量，和最近的大幅变化及原因",
      {"type": "object", "properties": {"topic": {"type": "string", "description": "可选：calendar | macro | holding | discovery"},
                                        "query": {"type": "string", "description": "可选：按关键词筛选（中文或英文），如 美联储、衰退、PBF"}}})
async def get_prediction_markets(topic: str | None = None, query: str | None = None) -> dict:
    from scout import prediction

    every = await prediction.board()
    events = [e for e in every if not topic or e["topic"] == topic] or every
    note = None
    if query:                          # any word of the query in the Chinese or English title, sectors or link (all topics if needed)
        words = [w for w in re.split(r"[\s,，、/]+", query.lower()) if len(w) >= 2]
        hay = lambda e: " ".join([e["title"] or "", e["title_en"] or "", " ".join(e["sectors"] or []), e["link"] or ""]).lower()
        hit = [e for e in events if any(w in hay(e) for w in words)] or [e for e in every if any(w in hay(e) for w in words)]
        if hit:
            events = hit
        else:
            events, note = every, f"没有标题含「{query}」的市场，下面是全部跟踪中的市场，请自己挑相关的"
    recent = await db.fetch_all("SELECT payload, created_at FROM scout.alerts WHERE kind = 'pm_move' AND created_at > now() - interval '7 days' "
                                "ORDER BY created_at DESC LIMIT 10")
    if not events and not recent:
        return {"note": "没有符合条件的预测市场（只跟踪成交量 ≥ 100 万的市场）"}
    return {"市场": [{**r, "主题": prediction.TOPIC_NAME.get(e["link"], e["topic"]), "链接": e["url"]}
                    for e in events[:25] for r in prediction.pack_rows([e])],
            "近 7 天的大幅变化": [{"日期": str(r["created_at"])[:10], "说明": prediction.move_line(r["payload"])} for r in recent],
            "说明": (note + "。" if note else "") + "预测市场价格大致可看作市场认为的概率，不是 Scout 的判断；引用时写明来源和成交量"}


@tool("get_norms", "历史常态：任意股票或 ETF（含 SPY、板块）未来 1 周 / 1 个月 / 3 个月通常会怎样——历史上涨比例、中位涨跌、"
                   "十次里较差 / 较好的一次（10% / 90% 分位）、跌 5% 以上的比例、按最近 63 日波动的正常范围；"
                   "问\"会不会涨\"\"下个月怎么样\"时用它，不是预测", TICKER)
async def get_norms(ticker: str) -> dict:
    from scout import norms

    t = universe.yahoo_symbol(ticker)
    return norms.agent_view(t, await norms.for_ticker(t))


@tool("get_industry_kpis", "持仓或任意股票所在行业的核心指标（免费数据）：例如炼油的 3-2-1 炼油差价、油气的原油和天然气价格、公用事业的长期利率、"
                           "银行的利差和信用利差、住宅建筑的房贷利率和新屋开工、电商的零售销售；每项有最新值、20 期变化、一年分位、含义，"
                           "盘中有期货的给出实时变化；没有免费数据的行业会说明缺什么", TICKER)
async def get_industry_kpis(ticker: str) -> dict:
    from scout import industry_kpis
    from scout.data.yahoo import latest_quotes

    t = universe.yahoo_symbol(ticker)
    inst = await db.fetch_one("SELECT industry FROM scout.instruments WHERE ticker = :t", {"t": t}) or {}
    quotes = await latest_quotes(industry_kpis.live_symbols({t: inst.get("industry")}), date.today())
    k = await industry_kpis.for_ticker(t, quotes)
    if not k:
        return {"note": f"{t} 所在行业（{inst.get('industry') or '未知'}）还没有登记核心指标"}
    return {"行业": k["industry"], "细分行业": k["gics"], "缺少的数据": k["missing"] or "无",
            "指标": [{"名称": m["name"], "单位": m["unit"], "含义": m["meaning"], "读数": m["text"]} for m in k["metrics"]],
            "代表 ETF": k["proxies"], "说明": "描述性指标，不是买卖信号；FRED / EIA 日度数据通常滞后约一周，期货实时值来自 Yahoo"}


@tool("get_external_facts", "外部信息库：联网检索时找到、Scout 无法从自己的数据里得到的事实（分析师评级和目标价、业绩指引、合同订单、"
                            "监管诉讼、并购、管理层、政策、宏观、行业），每条有来源、日期、原文摘录，以及 Scout 是否打开来源核对过原文",
      {"type": "object", "properties": {"subject": {"type": "string", "description": "股票或板块 ETF 代码；宏观、政策、市场整体用 MACRO"},
                                        "days": {"type": "integer", "description": "回看天数，默认 30"}},
       "required": ["subject"]})
async def get_external_facts(subject: str, days: int = 30) -> dict:
    from scout import external_facts

    s = "MACRO" if subject.upper() in ("MACRO", "宏观", "市场") else universe.yahoo_symbol(subject)
    rows = await external_facts.recall([s], days=min(max(days, 1), 365))
    if not rows:
        return {"note": f"外部信息库里近 {days} 天没有 {s} 的事实；可以用 research_company 或 research_views 检索"}
    return {**external_facts.pack_rows(rows), "对象": s}


@tool("get_views", "多来源观点：各类来源（新闻、分析师、公司自身、独立研究、投资者社区）近 30 天对一只股票或板块的看多 / 看空论点，"
                   "每条用 Scout 数据核对（一致 / 矛盾 / 无法核对），关键数据的白话解读，成立条件、证伪条件、把握，以及记入判断台账的方向；"
                   "不给 ticker 时列出最近的全部观点。只返回已有的，没有时用 research_views 现写",
      {"type": "object", "properties": {"ticker": {"type": "string", "description": "可选；不填时还返回本周市场观点（市场、11 个板块、方向）"}}},
      pages=("/views",))
async def get_views(ticker: str | None = None) -> dict:
    from scout import viewdesk

    t = universe.yahoo_symbol(ticker) if ticker else None
    recs = await viewdesk.latest(t, limit=3 if t else 20)
    if not recs:
        return {"note": f"{'这只' if t else '最近'}还没有多来源观点；可以用 research_views 现写一份"}
    if not t:
        from scout import weekly_view

        rep = await weekly_view.latest()
        m = (rep or {}).get("market")
        return {**({"本周市场观点": {**m, "日期": rep["asof"], "来源": rep.get("facts")}} if m else {}),
                "观点": [{"代码": r["ticker"], "日期": r["asof"], "方向": r["view"]["stance"], "期限": r["view"]["horizon"],
                        "把握": r["view"]["confidence"], "结论": r["view"]["summary"]} for r in recs]}
    return {"观点": [{**r["view"], "日期": r["asof"], "来源构成": r["mix"], "来源": r.get("sources", [])[:10],
                    "说明": f"论点{viewdesk.EXTERNAL}；方向记入判断台账（kind view），get_ledger 可查过去的命中情况"} for r in recs]}


@tool("research_views", "现写一份多来源观点（联网检索各类来源的看多 / 看空论点，用 Scout 数据逐条核对，白话解读，记入判断台账）；"
                        "同一天同一只复用。用户问'各方怎么看''市场上怎么说''多空分歧'且 get_views 没有近期观点时用", TICKER, pages=("/views",))
async def research_views(ticker: str) -> dict:
    from scout import viewdesk

    rec = await viewdesk.write(universe.yahoo_symbol(ticker))
    if rec is None:
        return {"error": f"{ticker} 不在 Scout 的标的池（没有数据）"}
    if not rec.get("view"):
        return {"error": "这次没能写出通过校验的观点（" + ("没有检索结果" if rec.get("source") == "none" else "未通过校验") + "）"}
    return {**rec["view"], "来源构成": rec["mix"], "来源": rec.get("sources", [])[:10], "说明": f"论点{viewdesk.EXTERNAL}"}


@tool("research_company", "联网检索公司和行业的真实现状（外部报道，未经核实）：主要产品和收入构成、主要客户与供应商、在行业里的地位和份额、"
                          "竞争格局、行业需求趋势和最新指引、主要风险；可以带一个具体问题。结果附来源和日期，同一天同一问题会复用缓存",
      {"type": "object", "properties": {"ticker": {"type": "string"},
                                        "question": {"type": "string", "description": "可选的具体问题，例如'它是不是英伟达 HBM 的主要供应商'"}},
       "required": ["ticker"]}, pages=("/ticker",))
async def research_company(ticker: str, question: str | None = None) -> dict:
    from scout import responses

    t = universe.yahoo_symbol(ticker)
    inst = await db.fetch_one("SELECT name, sector, industry FROM scout.instruments WHERE ticker = :t", {"t": t}) or {}
    q = (question or "").strip()
    cached = await db.fetch_one("SELECT payload FROM scout.briefs WHERE kind = 'company_research' AND asof = CURRENT_DATE "
                                "AND payload->>'ticker' = :t AND payload->>'question' = :q ORDER BY id DESC LIMIT 1", {"t": t, "q": q})
    if cached:
        return cached["payload"]["result"]
    name = inst.get("name") or t
    prompt = (f"Research {name} ({t}), {inst.get('industry') or inst.get('sector') or 'US-listed company'}, as of {date.today().isoformat()}. "
              "Cover, with dates and the source of each fact: (1) main products / segments and their share of revenue; "
              "(2) major customers and suppliers, and customer concentration; (3) its position in the industry: rank, market share, "
              "whether it is a key supplier to the industry leaders; (4) competitors and how it compares; (5) the current demand trend of its "
              "end markets and the latest company guidance; (6) the main risks. Distinguish reported facts from estimates and analyst opinion; "
              "say plainly when something is not disclosed. Answer in Chinese."
              + (f" Specific question: {q}" if q else ""))
    from scout import external_facts

    settings = get_scout_settings()
    found = await external_facts.collect(prompt, [t], "company_research", model=settings.scout_agent_model)
    facts = found["facts"]
    result = {"代码": t, "检索日期": date.today().isoformat(), "标注": "据外部报道（未经核实）",
              "内容": found["text"][:12000], "来源": [{"标题": x["title"], "url": x["url"]} for x in found["sources"][:12]],
              "外部事实": external_facts.pack_rows([f for f in facts if f["status"] in ("verified", "unreachable")]),
              "核对": {k: sum(f["status"] == k for f in facts) for k in ("verified", "unreachable", "mismatch")},
              "说明": "外部报道，不是 Scout 的数据。'内容'是检索模型写的总结，其中的数字只有出现在'外部事实·已核对'里的才能引用（Scout 打开来源找到了原文）；"
                      "未核对的只能说'有报道称'。引用时写明来源和日期，并用 get_evidence 的财务数据和 get_outlook 的行业数据核对"}
    await db.execute("INSERT INTO scout.briefs (kind, asof, content, payload) VALUES ('company_research', CURRENT_DATE, :c, CAST(:p AS JSONB))",
                     {"c": result["内容"][:4000], "p": db.jsonb({"ticker": t, "question": q, "result": result})})
    return result


def _round(value: Any) -> Any:
    if isinstance(value, float):
        return round(value, 4)
    if isinstance(value, dict):
        return {k: _round(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_round(v) for v in value]
    return value


LIMITS = {"get_evidence": 60000, "get_briefs": 20000, "get_strategy_view": 20000, "research_company": 16000}      # characters of tool output passed to the model


async def call_tool(name: str, args: dict) -> str:
    handler = HANDLERS.get(name)
    if handler is None:
        return json.dumps({"error": f"unknown tool {name}"})
    try:
        result = await handler(**args)
    except Exception as exc:
        logger.warning("tool %s failed: %s", name, exc)
        result = {"error": repr(exc)[:300]}
    return db.jsonb(_round(result))[:LIMITS.get(name, 12000)]


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
        data = await responses.create(body, timeout=settings.scout_agent_timeout_s, purpose="agent")
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
