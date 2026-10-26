"""Scout web service: Telegram webhook + dashboard. `uvicorn scout.web.app:app`."""
from __future__ import annotations

import asyncio
import logging
import secrets
from contextlib import asynccontextmanager
from datetime import date, timedelta

from fastapi import Depends, FastAPI, Form, Header, HTTPException, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from scout import db, telegram, universe
from scout.config import get_scout_settings
from scout.features import SNAPSHOT_FEATURES
from scout.web import views
from scout.web.views import e, layout, opp_card, signed, table, tlink

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger("scout.web")
security = HTTPBasic()
_background: set[asyncio.Task] = set()


@asynccontextmanager
async def lifespan(_: FastAPI):
    try:
        await db.apply_migrations()
    except Exception as exc:  # web must still boot to report health
        logger.error("migrations failed at startup: %s", exc)
    yield
    await db.get_engine().dispose()


app = FastAPI(title="scout", version="1.0.0", lifespan=lifespan)


def require_auth(credentials: HTTPBasicCredentials = Depends(security)) -> str:
    s = get_scout_settings()
    if not s.scout_dashboard_user or not s.scout_dashboard_password:
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "dashboard auth not configured")
    ok = secrets.compare_digest(credentials.username, s.scout_dashboard_user) and secrets.compare_digest(
        credentials.password, s.scout_dashboard_password
    )
    if not ok:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid credentials", {"WWW-Authenticate": "Basic"})
    return credentials.username


@app.get("/health")
async def health() -> dict:
    last = await db.fetch_one(
        "SELECT job, status, started_at, finished_at FROM scout.job_runs ORDER BY id DESC LIMIT 1"
    )
    return {"status": "ok", "service": "scout", "last_job": last}


@app.post("/telegram")
async def telegram_webhook(
    request: Request, x_telegram_bot_api_secret_token: str | None = Header(default=None)
) -> dict:
    secret = get_scout_settings().scout_tg_webhook_secret
    if not secret or not secrets.compare_digest(x_telegram_bot_api_secret_token or "", secret):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "bad secret")
    update = await request.json()
    # Answer Telegram immediately; LLM answers can take longer than its timeout.
    task = asyncio.create_task(telegram.handle_update(update))
    _background.add(task)
    task.add_done_callback(_background.discard)
    return {"ok": True}


# ─────────────────────────── dashboard ───────────────────────────

@app.get("/", response_class=HTMLResponse)
async def today(_: str = Depends(require_auth)) -> str:
    brief = await db.fetch_one("SELECT asof, content, payload FROM scout.briefs WHERE kind = 'eod' ORDER BY asof DESC, id DESC LIMIT 1")
    active = await db.fetch_all(
        "SELECT * FROM scout.opportunities WHERE status IN ('new', 'triggered') ORDER BY asof DESC, score DESC LIMIT 24"
    )
    events = await db.fetch_all(
        "SELECT ev.event_type, ev.event_date, ev.price, o.ticker, o.primary_detector FROM scout.opportunity_events ev "
        "JOIN scout.opportunities o ON o.id = ev.opportunity_id ORDER BY ev.event_date DESC, ev.id DESC LIMIT 12"
    )
    market = ((brief or {}).get("payload") or {}).get("market") or {}
    regime_zh = {"uptrend": "上升趋势", "mixed": "震荡", "downtrend": "下降趋势"}
    head = ""
    if market:
        leaders = "，".join(f"{tlink(k)} {signed(v)}" for k, v in market.get("sector_leaders_20d") or [])
        laggards = "，".join(f"{tlink(k)} {signed(v)}" for k, v in market.get("sector_laggards_20d") or [])
        head = f"""<div class="card"><h3>收盘 {e(brief['asof'])} · 大盘 {e(regime_zh.get(market.get('regime'), market.get('regime')))}</h3>
{tlink('SPY')} 当日 {signed(market.get('spy_ret_1d'))} · 20 日 {signed(market.get('spy_ret_20d'))} · {tlink('QQQ')} 20 日 {signed(market.get('qqq_ret_20d'))}
· {tlink('IWM')} 20 日 {signed(market.get('iwm_ret_20d'))} · {tlink('SMH')} 20 日 {signed(market.get('smh_ret_20d'))}
<div class="small">领涨板块：{leaders}　落后板块：{laggards}</div></div>"""
        head += await etf_tables()
    else:
        head = '<div class="card">还没有收盘扫描结果。运行 <code>python -m scout.jobs.eod</code> 后这里会出现内容。</div>'
    ev_rows = [[e(x["event_date"]), f'<a href="/ticker/{e(x["ticker"])}">{e(x["ticker"])}</a>', e(x["primary_detector"]),
                e(telegram.STATUS_LABELS.get(x["event_type"], x["event_type"])), f"{x['price']:.2f}" if x["price"] else "—"]
               for x in events]
    body = head + "<h2>活跃机会</h2>"
    body += f'<div class="grid">{"".join(opp_card(o) for o in active)}</div>' if active else "<p class='muted'>暂无活跃机会。</p>"
    if ev_rows:
        body += "<h2>最近状态变化</h2>" + table(["日期", "代码", "规则", "事件", "价格"], ev_rows)
    return layout("今日", "today", body)


@app.get("/ideas", response_class=HTMLResponse)
async def ideas(status_: str = "all", detector: str = "", _: str = Depends(require_auth)) -> str:
    where, params = ["true"], {}
    if status_ == "active":
        where.append("o.status IN ('new', 'triggered')")
    elif status_ == "closed":
        where.append("o.status NOT IN ('new', 'triggered')")
    if detector:
        where.append("o.primary_detector = :d")
        params["d"] = detector
    rows = await db.fetch_all(
        f"SELECT o.*, oc.return_pct, oc.excess_pct FROM scout.opportunities o "
        f"LEFT JOIN scout.opportunity_outcomes oc ON oc.opportunity_id = o.id "
        f"WHERE {' AND '.join(where)} ORDER BY o.asof DESC, o.score DESC LIMIT 300", params,
    )
    filters = " · ".join(
        f'<a href="/ideas?status_={k}&detector={e(detector)}">{v}</a>' for k, v in (("all", "全部"), ("active", "活跃"), ("closed", "已结束"))
    )
    trs = [[
        e(o["asof"]), f'<a href="/ticker/{e(o["ticker"])}">{e(o["ticker"])}</a>',
        f'<a href="/ideas?status_={e(status_)}&detector={e(o["primary_detector"])}">{e(o["primary_detector"])}</a>',
        f"{o['score']:.2f}", e(telegram.STATUS_LABELS.get(o["status"], o["status"])),
        f"{o['entry_lo']:.2f}–{o['entry_hi']:.2f}", f"{o['stop']:.2f}", f"{o['target1']:.2f}",
        signed(o.get("return_pct")), signed(o.get("excess_pct")), e((o.get("thesis") or {}).get("headline")),
    ] for o in rows]
    body = f"<h2>机会看板</h2><p class='small'>{filters}</p>" + table(
        ["日期", "代码", "规则", "评分", "状态", "入场", "止损", "目标", "收益", "超额", "标题"], trs
    )
    return layout("机会看板", "ideas", body)


async def _latest_etf_features() -> list[dict]:
    rows = await db.fetch_all(
        "SELECT f.ticker, f.payload FROM scout.features_daily f JOIN scout.instruments i USING (ticker) "
        "WHERE i.kind = 'etf' AND f.date = (SELECT max(date) FROM scout.features_daily)"
    )
    return [{"ticker": r["ticker"], **(r["payload"] or {})} for r in rows]


async def etf_tables() -> str:
    """Home-page sector and theme ETF tables, every row links to its detail page."""
    rows = await _latest_etf_features()
    if not rows:
        return ""
    sector_label = {etf: universe.SECTOR_ZH.get(s, s) for s, etf in universe.SECTOR_ETFS.items()}
    sectors = sorted((r for r in rows if r["ticker"] in sector_label), key=lambda r: r.get("ret_20d") or 0, reverse=True)
    themes = sorted((r for r in rows if r["ticker"] in universe.THEME_ETFS and r["ticker"] not in ("SPY", "QQQ", "IWM", "RSP")),
                    key=lambda r: r.get("ret_20d") or 0, reverse=True)
    return (
        '<div class="grid" style="margin-top:12px">'
        f'<div class="card"><h3>板块表现 <span class="small muted">按 20 日排序，点击查看成分股</span></h3>{views.sector_rows(sectors, sector_label)}</div>'
        f'<div class="card"><h3>主题与资产 ETF</h3>{views.sector_rows(themes, universe.THEME_ETFS)}</div>'
        "</div>"
    )


async def sector_members_card(etf: str) -> str:
    sector = next((s for s, x in universe.SECTOR_ETFS.items() if x == etf), None)
    if not sector:
        return ""
    rows = await db.fetch_all(
        "SELECT f.ticker, i.name, f.payload FROM scout.features_daily f JOIN scout.instruments i USING (ticker) "
        "WHERE i.kind = 'stock' AND i.sector = :s AND f.date = (SELECT max(date) FROM scout.features_daily)",
        {"s": sector},
    )
    members = [{"ticker": r["ticker"], "name": r["name"], **(r["payload"] or {})} for r in rows if (r["payload"] or {}).get("ret_20d") is not None]
    if not members:
        return ""
    members.sort(key=lambda m: m["ret_20d"], reverse=True)
    above50 = [m for m in members if m.get("sma50") and m["close"] > m["sma50"]]

    def tr(m: dict) -> list[str]:
        return [tlink(m["ticker"]), e((m.get("name") or "")[:24]), signed(m.get("ret_1d")), signed(m.get("ret_20d")),
                views.pct(m.get("rs_pct"), 0)]

    head = ["代码", "名称", "1 日", "20 日", "RS 分位"]
    return (
        f'<div class="card"><h3>板块成分股 · {e(universe.SECTOR_ZH.get(sector, sector))}</h3>'
        f'<div class="small muted">共 {len(members)} 只，站上 50 日线 {len(above50)} 只（{views.pct(len(above50) / len(members), 0)}）</div>'
        f"<h4>20 日最强</h4>{table(head, [tr(m) for m in members[:10]])}"
        f"<h4>20 日最弱</h4>{table(head, [tr(m) for m in members[-5:][::-1]])}</div>"
    )


@app.post("/ticker/{ticker}/ai")
async def ticker_ai(ticker: str, _: str = Depends(require_auth)):
    from scout import insight

    t = universe.yahoo_symbol(ticker)
    try:
        rec = await insight.generate(t)
    except Exception as exc:
        logger.exception("ticker AI analysis failed")
        return HTMLResponse(layout(t, "", f"<p>{e(telegram.friendly_error(exc))}</p><p>{tlink(t, '返回 ' + t)}</p>"), status_code=502)
    if rec is None:
        return HTMLResponse(layout(t, "", f"<p>{e(t)} 还没有特征数据。</p>"), status_code=404)
    return RedirectResponse(f"/ticker/{t}#insight", status_code=303)


@app.get("/ticker")
async def ticker_lookup(t: str = "", _: str = Depends(require_auth)):
    return RedirectResponse(f"/ticker/{universe.yahoo_symbol(t)}" if t else "/", status_code=302)


@app.get("/ticker/{ticker}", response_class=HTMLResponse)
async def ticker_page(ticker: str, _: str = Depends(require_auth)) -> str:
    from scout.analysis import scout_view
    from scout.context import load_news

    t = universe.yahoo_symbol(ticker)
    inst = await db.fetch_one("SELECT * FROM scout.instruments WHERE ticker = :t", {"t": t})
    bars = await db.fetch_all(
        "SELECT * FROM (SELECT date, open, high, low, close, volume FROM scout.bars_daily WHERE ticker = :t "
        "ORDER BY date DESC LIMIT 260) x ORDER BY date", {"t": t},
    )
    feat = await db.fetch_one("SELECT date, payload FROM scout.features_daily WHERE ticker = :t ORDER BY date DESC LIMIT 1", {"t": t})
    opps = await db.fetch_all(
        "SELECT o.*, oc.return_pct FROM scout.opportunities o LEFT JOIN scout.opportunity_outcomes oc ON oc.opportunity_id = o.id "
        "WHERE o.ticker = :t ORDER BY o.asof DESC LIMIT 20", {"t": t},
    )
    news = await load_news(t)
    nxt = await db.fetch_one(
        "SELECT event_date FROM scout.market_events WHERE ticker = :t AND event_type = 'earnings' AND event_date >= :d "
        "ORDER BY event_date LIMIT 1", {"t": t, "d": date.today()},
    )
    from scout import insight, structure as price_structure

    ai = await insight.latest(t)
    if not inst and not bars:
        return layout(t, "", f"<p>{e(t)} 不在标的池中。可以在 Watchlist 页加入。</p>")
    latest = opps[0] if opps else None
    # Plan to draw: the latest card if it is active or closed within ~2 months
    # (so entry/exit markers of recent trades stay visible).
    plan_opp = latest if latest and (latest["status"] in ("new", "triggered")
                                     or (date.today() - latest["asof"]).days <= 60) else None
    plan = None
    if plan_opp:
        plan = {k: plan_opp.get(k) for k in ("entry_lo", "entry_hi", "stop", "target1", "target2", "current_stop",
                                             "asof", "triggered_on", "closed_on")}
    payload = (feat or {}).get("payload") or {}
    pct_keys = {"ret_1d", "ret_5d", "ret_20d", "ret_63d", "ret_252d", "atr_pct", "dist_52w_high", "gap_pct",
                "rs_pct", "sector_rs_pct", "sector_rel_20", "market_ret_1d", "idio_ret_1d", "idio_ret_5d", "idio_ret_20d",
                "bb_pct", "vol_20", "max_dd_60"}
    def fmt(k: str, v: float) -> str:
        if k in pct_keys:
            return signed(v)
        return f"{v:,.0f}" if k == "dollar_vol20" else f"{v:.2f}"

    from scout import knowledge

    feat_rows = [
        [f"<span title='{e((knowledge.features().get(k) or {}).get('definition', ''))}'>{e(knowledge.zh(k))}</span>",
         fmt(k, payload[k]), e(knowledge.reading(k, payload[k]) or "")]
        for k in SNAPSHOT_FEATURES if k in payload
    ]
    news_html = "".join(
        f"<li><a href='{e(n.get('url'))}' target='_blank' rel='noopener'>{e(n['headline'])}</a> "
        f"<span class='small muted'>{e(n.get('source'))} {e((n.get('published') or '')[:16])}</span>"
        f"{'<br><span class=small>' + e(n['summary']) + '</span>' if n.get('summary') else ''}</li>"
        for n in news
    ) or "<li class='muted'>近 72 小时无新闻。</li>"
    name = " · ".join(x for x in ((inst or {}).get("name"), (inst or {}).get("sector")) if x)
    body = f"<h2>{e(t)} <span class='muted small'>{e(name)}</span></h2>"
    struct = price_structure.compute(bars)
    side = views.trade_plan(plan_opp, payload.get("close")) if plan_opp else views.key_levels(payload, struct)
    body += f"<div class='chartrow'>{views.chart_script(bars, plan, struct)}{side}</div>"
    from scout import sectors as sector_mod

    headline, lines = scout_view(
        payload, is_stock=(inst or {}).get("kind", "stock") == "stock",
        next_earnings=(nxt or {}).get("event_date"), latest_opp=latest, today=date.today(),
        structure=struct, sector_ctx=await sector_mod.context_for((inst or {}).get("sector")),
    )
    body += '<div class="grid" style="margin-top:12px">'
    body += f'<div id="scout">{views.scout_panel(t, headline, lines, ai, bool(get_scout_settings().openai_api_key))}</div>'
    if ai:
        body += views.insight_card(ai)
    body += f'<div class="card"><h3>新闻</h3><ul class="tight">{news_html}</ul></div>'
    if latest:
        body += opp_card(latest)
    body += await sector_members_card(t)
    body += (f'<details class="card"><summary>全部特征 <span class="small muted">{e((feat or {}).get("date"))}</span></summary>'
             f'<p class="small muted">鼠标悬停指标名可查看定义（数据字典 scout/knowledge/features.yaml）</p>'
             f'{table(["指标", "值", "读数"], feat_rows)}</details>')
    body += "</div>"
    if opps:
        body += "<h2>历史机会</h2>" + table(
            ["日期", "规则", "状态", "评分", "收益"],
            [[e(o["asof"]), e(o["primary_detector"]), e(telegram.STATUS_LABELS.get(o["status"], o["status"])),
              f"{o['score']:.2f}", signed(o.get("return_pct"))] for o in opps],
        )
    return layout(t, "", body, head=views.CHART_HEAD)


@app.get("/market", response_class=HTMLResponse)
async def market_page(view: str = "sectors", _: str = Depends(require_auth)) -> str:
    import pandas as pd

    from scout import knowledge, market_focus, sector_news
    from scout import sectors as sec

    asof, rows = await sec.latest()
    if asof is None:
        return layout("市场全景", "market", "<p class='muted'>还没有板块数据，收盘扫描后会生成。</p>")
    sector_rows = sorted((r for r in rows if r["kind"] == "sector"), key=lambda r: -(r.get("heat") or 0))
    theme_rows = [r for r in rows if r["kind"] == "theme"]
    news = await sector_news.by_sector()
    focus = await market_focus.latest()
    hit_rows = await db.fetch_all(
        "SELECT h.ticker, h.detector, h.score, i.sector FROM scout.signal_hits h JOIN scout.instruments i USING (ticker) "
        "WHERE h.asof = (SELECT max(asof) FROM scout.signal_hits) AND (h.payload->>'live')::boolean ORDER BY h.score DESC")

    # ── AI focus ──
    if focus:
        f = focus["focus"]
        names = {r["key"]: r["name"] for r in rows}
        themes = "".join(
            f"<li><b>{e(t['title'])}</b>：{e(t['summary'])} "
            + "".join(f"<a class='chip' href='#{e(k)}'>{e(names.get(k, k))}</a>" for k in t["sectors"]) + "</li>"
            for t in f["themes"])
        source = f"{focus.get('model')} · 校验通过" if focus["source"] == "llm" else "未通过校验，显示规则读数"
        focus_html = (f"<div class='card'><h3>市场关注点 <span class='small muted'>{e(focus['asof'])} · {e(source)}</span></h3>"
                      f"<div class='headline'>{e(f['headline'])}</div><p class='small'>{e(f['regime_view'])}</p><ol>{themes}</ol>"
                      f"<p class='small'><b>关注</b>：{e('；'.join(f['watch']))}<br><b>风险</b>：{e('；'.join(f['risks']))}</p>"
                      f"<form method='post' action='/market/focus'><button>重新生成</button></form></div>")
    else:
        focus_html = ("<div class='card'><h3>市场关注点</h3><p class='muted small'>还没有生成。</p>"
                      "<form method='post' action='/market/focus'><button>生成</button></form></div>")

    # ── rotation graph ──
    tabs = " ".join(f"<a class='chip' style='{'background:var(--accent);color:#fff' if view == v else ''}' href='/market?view={v}'>{lbl}</a>"
                    for v, lbl in (("sectors", "行业板块"), ("themes", "主题 ETF")))
    rrg_rows = sector_rows if view != "themes" else theme_rows
    rrg_html = (f"<div class='card'><h3>相对轮动图 {tabs}</h3>{views.rrg_svg(rrg_rows)}"
                "<p class='small muted'>横轴：相对 SPY 的强度是否高于自身 50 日平均；纵轴：这种相对强度是在加速还是减速。"
                "通常按 改善 → 领先 → 转弱 → 落后 顺时针轮动；线条是最近 8 周的轨迹，圆点是本周。（JdK RRG 的开放近似）</p></div>")

    # ── heat table ──
    cols = [("rel_20", "相对SPY"), ("pct_above_50", "站上50日线"), ("net_new_highs", "净新高"), ("updown_vol_20", "涨跌量比"),
            ("cmf_20", "CMF"), ("rel_dollar_vol", "成交额热度"), ("dollar_vol_share", "成交占比")]
    ranks = {k: pd.Series([r.get(k) for r in sector_rows], dtype="float64").rank(pct=True).tolist() for k, _ in cols}
    body_rows = []
    for i, r in enumerate(sector_rows):
        n = news.get(r["key"], {})
        att = sec.attention_label(n.get("news_attention"))
        move = f"{sec.QUADRANTS.get(r.get('quadrant_prev'))}→" if r.get("quadrant_prev") and r["quadrant_prev"] != r.get("quadrant") else ""
        cells = [f"<td><a href='#{e(r['key'])}'>{e(r['name'])}</a> <span class='small muted'>{e(r['key'])}</span></td>",
                 f"<td>{sec.HEAT_ICON.get(r.get('heat_label'), '')} {e(r.get('heat_label'))} <span class='small muted'>{(r.get('heat') or 0):.2f}</span></td>",
                 f"<td style='color:{QUAD.get(r.get('quadrant'), '')}'>{e(move)}{e(sec.QUADRANTS.get(r.get('quadrant')))}</td>"]
        cells += [views.heat_cell(r.get(k), ranks[k][i], knowledge.fmt(k, r.get(k))) for k, _ in cols]
        cells += [f"<td>{e(r.get('flow_label'))}</td>",
                  f"<td>{e(att)}{' ' + knowledge.fmt('news_attention', n.get('news_attention')) if n.get('news_attention') else ''}</td>",
                  f"<td>{knowledge.fmt('tone_net', n.get('tone_net')) if n.get('tone_net') is not None else '—'}</td>",
                  f"<td>{sum(1 for h in hit_rows if h['sector'] == r.get('sector'))}</td>"]
        body_rows.append("<tr>" + "".join(cells) + "</tr>")
    head = "".join(f"<th>{h}</th>" for h in ["板块", "热度", "轮动", *[c[1] for c in cols], "资金流向", "新闻关注度", "新闻情绪", "规则命中"])
    table_html = (f"<div class='card'><h3>板块热力表 <span class='small muted'>数据日期 {e(asof)}</span></h3>"
                  f"<div class='scroll'><table><thead><tr>{head}</tr></thead><tbody>{''.join(body_rows)}</tbody></table></div>"
                  "<p class='small muted'>颜色是板块之间的相对高低。涨跌量比、CMF 和资金流向都是根据日线成交量推算的代理指标，不是真实的买卖单；"
                  "资金流向只有在 CMF 和涨跌量比方向一致时才写成流入或流出。热度 = 相对 SPY、相对强度比、动量、宽度、涨跌量比、成交额热度六项排名的平均。</p></div>")

    # ── sector detail cards ──
    members = await db.fetch_all(
        "SELECT f.ticker, i.name, i.sector, f.payload FROM scout.features_daily f JOIN scout.instruments i USING (ticker) "
        "WHERE f.date = (SELECT max(date) FROM scout.features_daily) AND i.kind = 'stock' "
        "AND (f.payload->>'dollar_vol20')::float >= 20000000")
    cards = []
    for r in sector_rows:
        k = r["key"]
        n = news.get(k, {})
        top = sorted((m for m in members if m["sector"] == r.get("sector") and m["payload"].get("rs_pct") is not None),
                     key=lambda m: -m["payload"]["rs_pct"])[:5]
        top_html = "".join(f"<li>{tlink(m['ticker'])} <span class='small muted'>{e((m['name'] or '')[:20])}</span> "
                           f"20 日 {signed(m['payload'].get('ret_20d'))} · RS {views.pct(m['payload'].get('rs_pct'), 0)}</li>" for m in top)
        hits = [h for h in hit_rows if h["sector"] == r.get("sector")][:5]
        hits_html = "".join(f"<a class='chip' href='/ticker/{e(h['ticker'])}'>{e(h['ticker'])} · {e(h['detector'])}</a>" for h in hits) or "<span class='muted small'>今日无</span>"
        news_html = "".join(f"<li><a href='{e(h['url'])}' target='_blank' rel='noopener'>{e(h['headline'])}</a> "
                            f"<span class='small muted'>{e(h['source'])} {e(h['published'])} · {e(', '.join(h['tickers']))}</span></li>"
                            for h in n.get("headlines", [])[:3]) or "<li class='muted'>近 3 天没有可用新闻</li>"
        cards.append(
            f"<div class='card' id='{e(k)}'><h3>{sec.HEAT_ICON.get(r.get('heat_label'), '')} {e(r['name'])} {tlink(k)} "
            f"<span class='small muted'>{e(sec.QUADRANTS.get(r.get('quadrant')))} · 20 日 {signed(r.get('ret_20d'))}</span></h3>"
            f"<div class='small'>站上 50 日线 {views.pct(r.get('pct_above_50'), 0)} · 资金 {e(r.get('flow_label'))} · "
            f"成交额热度 {knowledge.fmt('rel_dollar_vol', r.get('rel_dollar_vol'))} · 新闻 {n.get('news_7d', 0)} 条/7 日</div>"
            f"<div class='small muted' style='margin-top:6px'>头条</div><ul class='tight'>{news_html}</ul>"
            f"<div class='small muted'>最强成分股</div><ul class='tight'>{top_html}</ul>"
            f"<div class='small muted'>今日规则命中</div><div>{hits_html}</div></div>")

    wraps = await sector_news.market_wraps()
    macro = await sector_news.macro_headlines(8)
    mnews = "".join(f"<li><a href='{e(x['url'])}' target='_blank' rel='noopener'>{e(x['headline'])}</a> "
                    f"<span class='small muted'>{e(x.get('source'))} {e(x.get('published'))}</span></li>" for x in wraps + macro)
    body = (f"<h2>市场全景</h2><div class='duo'>{focus_html}{rrg_html}</div>{table_html}"
            f"<h2>板块详情</h2><div class='grid'>{''.join(cards)}</div>"
            f"<h2>全市场新闻</h2><div class='card'><ul class='tight'>{mnews or '<li class=muted>暂无</li>'}</ul></div>")
    return layout("市场全景", "market", body)


QUAD = {"leading": "var(--up)", "weakening": "var(--warn)", "lagging": "var(--down)", "improving": "var(--accent)"}


@app.post("/market/focus")
async def market_focus_regenerate(_: str = Depends(require_auth)):
    from scout import market_focus

    try:
        await market_focus.generate()
    except Exception as exc:
        logger.exception("market focus failed")
        return HTMLResponse(layout("市场全景", "market", f"<p>{e(telegram.friendly_error(exc))}</p>"), status_code=502)
    return RedirectResponse("/market", status_code=303)


@app.get("/screens", response_class=HTMLResponse)
async def screens_page(s: str = "", _: str = Depends(require_auth)) -> str:
    from scout import base_rates as br_mod
    from scout import screens as sc

    asof, rows = await sc.snapshot()
    if asof is None:
        return layout("策略筛选", "screens", "<p class='muted'>还没有每日快照。</p>")
    hit_counts = {r["detector"]: r["n"] for r in await db.fetch_all(
        "SELECT detector, count(*) AS n FROM scout.signal_hits WHERE asof = :d GROUP BY detector", {"d": asof})}
    sig_counts = sc.counts(rows)
    earn = await db.fetch_one(
        "SELECT count(*) AS n FROM scout.market_events WHERE event_type = 'earnings' AND event_date BETWEEN :a AND :b",
        {"a": asof, "b": asof + timedelta(days=7)})
    screens = sc.all_screens()

    def count_of(x) -> int:
        if x.detector is not None:
            return hit_counts.get(x.detector.name, 0)
        return (earn or {}).get("n", 0) if x.id == "earnings_week" else sig_counts.get(x.id, 0)

    current = next((x for x in screens if x.id == s), None) or next(
        (x for x in screens if x.detector is not None and x.detector.live and count_of(x)), screens[0])
    nav = ""
    for group in ("规则", "技术信号", "事件"):
        chips = "".join(
            f"<a class='chip' style='{'background:var(--accent);color:#fff' if x.id == current.id else ''}' "
            f"href='/screens?s={e(x.id)}'>{e(x.title)} {count_of(x)}</a>"
            for x in screens if x.group == group)
        nav += f"<div style='margin:4px 0'><span class='small muted'>{group}：</span>{chips}</div>"
    found = await sc.run(current, asof, rows)

    crit = "".join(f"<li>{e(c)}</li>" for c in current.criteria)
    stats_html = ""
    if current.detector is not None:
        st = sc.rule_stats(await br_mod.load(), current.detector.name)
        a = st["all"]
        pos = " · ".join(f"{e(lbl)}：相对持有 {signed(x.get('avg_edge_vs_hold'))}（{x.get('n', 0)} 次）"
                         for lbl, x in st["positions"] if x.get("n"))
        stats_html = (f"<p class='small'>历史回放（扣除成本，对比一直持有）：共 {a.get('n', 0)} 次，扣费胜率 {views.pct(a.get('win_rate_net'), 0)}，"
                      f"相对持有 {signed(a.get('avg_edge_vs_hold'))}<br>按位置：{pos or '—'}</p>"
                      + ("" if current.detector.live else "<p class='small warn'>候选规则：只做回放统计，不进入机会看板和推送。</p>"))
    else:
        stats_html = "<p class='small muted'>描述性筛选：只说明「现在符合什么条件」，没有历史优势统计。</p>"
    note = f"<p class='small'>{e(current.note)}</p>" if current.note else ""

    def pos_chip(r: dict) -> str:
        why = r.get("why") or {}
        if why.get("chase_risk"):
            return "<span class='chip' style='background:var(--down);color:#fff'>追高风险</span>"
        b = br_mod.position_bucket(r.get("dist_52w_high"))
        return f"<span class='chip'>{e(b[1].split('（')[0].replace('距 52 周高点 ', '距高点 '))}</span>" if b else ""

    head = ["代码", "名称", "行业", "收盘", "1 日", "20 日", "距高点", "RS", "量比", "位置"]
    extra_head = ["评分"] if current.detector is not None else (["财报日"] if current.id == "earnings_week" else [])
    table_rows = []
    for r in found:
        cells = [tlink(r["ticker"]), e((r.get("name") or "")[:22]), e(universe.SECTOR_ZH.get(r.get("sector") or "", r.get("sector") or "")),
                 f"{r['close']:.2f}" if r.get("close") else "—", signed(r.get("ret_1d")), signed(r.get("ret_20d")),
                 signed(r.get("dist_52w_high")), views.pct(r.get("rs_pct"), 0),
                 f"{r['vol_ratio']:.1f}" if r.get("vol_ratio") is not None else "—", pos_chip(r)]
        if current.detector is not None:
            cells.append(f"{r.get('score', 0):.2f}")
        elif current.id == "earnings_week":
            cells.append(f"{e(r.get('earnings_date'))} {e(r.get('earnings_hour') or '')}")
        table_rows.append(cells)
    body = (f"<h2>策略筛选 <span class='small muted'>数据日期 {e(asof)} · 只含日均成交额 ≥ $20M 的个股</span></h2>{nav}"
            f"<div class='card' style='margin-top:12px'><h3>{e(current.title)}</h3><ul class='tight'>{crit}</ul>{stats_html}{note}</div>"
            + (table(head + extra_head, table_rows) if table_rows else "<p class='muted'>今天没有符合条件的股票。</p>"))
    return layout("策略筛选", "screens", body)


@app.get("/scoreboard", response_class=HTMLResponse)
async def scoreboard_page(_: str = Depends(require_auth)) -> str:
    from scout.detectors import registry
    from scout.tracker import scoreboard

    live = {r["detector"]: r for r in await scoreboard()}
    rates = await db.fetch_all("SELECT detector, regime, stats, computed_at FROM scout.detector_base_rates")
    by = {(r["detector"], r["regime"]): r["stats"] for r in rates}
    computed = max((r["computed_at"] for r in rates), default=None)
    rows = []
    for d in registry():
        lv = live.get(d.name) or {}
        tag = "" if d.live else " <span class='chip'>候选 · 仅回放</span>"
        cells = [f"<b>{e(d.label)}</b>{tag}<br><span class='small muted'>{e(d.name)} · {d.horizon_days} 日</span>"]
        for regime in ("all", "uptrend", "mixed", "downtrend"):
            s = by.get((d.name, regime)) or {}
            cells.append(
                f"n={s.get('n', 0)}<br>扣费胜率 {views.pct(s.get('win_rate_net', s.get('win_rate')), 0)}<br>"
                f"相对持有 {signed(s.get('avg_edge_vs_hold'))}<br>跑赢持有 {views.pct(s.get('beat_hold_rate'), 0)}<br>"
                f"<span class='small muted'>对 SPY（扣费）{views.pct(s.get('avg_net_excess'), 1, True)}</span><br>"
                f"<span class='small muted'>达标 {views.pct(s.get('target_rate'), 0)} · 止损 {views.pct(s.get('stop_rate'), 0)}</span>"
                if s.get("n") else "—"
            )
        cells.append(
            f"共 {lv.get('total', 0)} · 活跃 {lv.get('active', 0)} · 结束 {lv.get('closed', 0)}<br>"
            f"扣费胜率 {views.pct(lv.get('win_rate_net'), 0)} · 扣费超额 {signed(lv.get('avg_net_excess'))}<br>"
            f"<span class='small'>👍 {lv.get('thumbs_up', 0)} 👎 {lv.get('thumbs_down', 0)}</span>"
        )
        rows.append(cells)
    body = (
        "<h2>规则记分板</h2><p class='small muted'>历史基准：当前成分股 3 年日线回放（含幸存者偏差），入场为信号后 3 日内首次触及入场区间，"
        "止损优先判定；每笔扣除 10 个基点往返成本。<b>相对持有</b> = 扣费收益 − 同期按该股平均漂移一直持有的收益，"
        f"≤ 0 表示择时没有比直接持有更好。计算于 {e(computed)}。</p>"
        + table(["规则", "历史·全部", "历史·上升", "历史·震荡", "历史·下降", "实盘"], rows)
        + "<h3>规则说明</h3><ul>" + "".join(f"<li><b>{e(d.label)}</b>：{e(d.description)}</li>" for d in registry()) + "</ul>"
    )
    return layout("记分板", "scoreboard", body)


@app.get("/watchlist", response_class=HTMLResponse)
async def watchlist_page(_: str = Depends(require_auth)) -> str:
    rows = await universe.load_watchlist()
    d = await db.fetch_one("SELECT max(date) AS d FROM scout.features_daily")
    feats = {}
    if d and d["d"]:
        for r in await db.fetch_all(
            "SELECT ticker, payload FROM scout.features_daily WHERE date = :d AND ticker IN "
            "(SELECT ticker FROM scout.watchlist WHERE active)", {"d": d["d"]}
        ):
            feats[r["ticker"]] = r["payload"]
    trs = []
    for r in rows:
        f = feats.get(r["ticker"], {})
        trs.append([
            f'<a href="/ticker/{e(r["ticker"])}">{e(r["ticker"])}</a>', e(r["theme"]),
            f"{f['close']:.2f}" if "close" in f else "—", signed(f.get("ret_1d")), signed(f.get("ret_5d")),
            signed(f.get("ret_20d")), views.pct(f.get("rs_pct"), 0), signed(f.get("dist_52w_high")),
            f'<form method="post" action="/watchlist/remove"><input type="hidden" name="ticker" value="{e(r["ticker"])}">'
            f'<button style="background:var(--down)">移除</button></form>',
        ])
    form = ('<form method="post" action="/watchlist/add" class="card" style="display:flex;gap:8px;flex-wrap:wrap">'
            '<input name="ticker" placeholder="代码" required size="8"><input name="theme" placeholder="主题（可选）">'
            '<button>加入</button></form>')
    body = "<h2>Watchlist</h2>" + form + table(
        ["代码", "主题", "收盘", "1 日", "5 日", "20 日", "RS 分位", "距 52 周高点", ""], trs
    )
    return layout("Watchlist", "watchlist", body)


@app.post("/watchlist/add")
async def watchlist_add(ticker: str = Form(...), theme: str = Form(""), _: str = Depends(require_auth)):
    await universe.add_watch(ticker, theme or None)
    return RedirectResponse("/watchlist", status_code=303)


@app.post("/watchlist/remove")
async def watchlist_remove(ticker: str = Form(...), _: str = Depends(require_auth)):
    await universe.remove_watch(ticker)
    return RedirectResponse("/watchlist", status_code=303)


@app.get("/chat", response_class=HTMLResponse)
async def chat_page(_: str = Depends(require_auth)) -> str:
    rows = await db.fetch_all(
        "SELECT role, content, created_at, channel FROM scout.chat_messages ORDER BY id DESC LIMIT 40"
    )
    msgs = "".join(
        f'<div class="card" style="margin-bottom:8px;{"border-color:var(--accent)" if r["role"] == "user" else ""}">'
        f'<div class="small muted">{"你" if r["role"] == "user" else "Scout"} · {e(r["channel"])} · {e(str(r["created_at"])[:16])}</div>'
        f'<pre>{e(r["content"])}</pre></div>'
        for r in reversed(rows)
    )
    form = ('<form method="post" action="/chat" class="card" style="display:flex;gap:8px">'
            '<textarea name="q" rows="2" style="flex:1" placeholder="例如：最近 5 天 watchlist 里谁最弱？为什么？" required></textarea>'
            '<button>提问</button></form>')
    return layout("问答", "chat", "<h2>问答</h2>" + msgs + form)


@app.post("/chat")
async def chat_ask(q: str = Form(...), _: str = Depends(require_auth)):
    from scout.agent import answer

    await answer(q, channel="dashboard")
    return RedirectResponse("/chat", status_code=303)
