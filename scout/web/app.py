"""Scout web service: Telegram webhook + dashboard. `uvicorn scout.web.app:app`."""
from __future__ import annotations

import asyncio
import json
import logging
import re
import secrets
from contextlib import asynccontextmanager
from datetime import date, timedelta

from fastapi import Depends, FastAPI, Form, Header, HTTPException, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from scout import db, event_cause, fundamentals, learned, opportunities, portfolio, premarket, telegram, universe
from scout import events as events_mod
from scout.config import get_scout_settings
from scout.features import SNAPSHOT_FEATURES
from scout.market_calendar import us_equity_market_status
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
        await learned.seed()
    except Exception as exc:  # web must still boot to report health
        logger.error("migrations failed at startup: %s", exc)
    yield
    await db.get_engine().dispose()


app = FastAPI(title="scout", version="1.0.0", lifespan=lifespan)


@app.middleware("http")
async def timing(request, call_next):
    """Server-Timing (total / db) on every response, and a log line for slow pages."""
    import time

    stats = {"n": 0, "s": 0.0, "max": (0.0, "")}
    token = db.QUERY_STATS.set(stats)
    t0 = time.perf_counter()
    try:
        response = await call_next(request)
    finally:
        db.QUERY_STATS.reset(token)
    total = time.perf_counter() - t0
    response.headers["Server-Timing"] = f"total;dur={total * 1000:.0f}, db;dur={stats['s'] * 1000:.0f};desc=\"{stats['n']} queries\""
    if total > 2:
        logger.info("slow %s %s: %.1fs, %d queries %.1fs, slowest %.2fs %s", request.method, request.url.path, total,
                    stats["n"], stats["s"], stats["max"][0], stats["max"][1])
    return response


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
        "SELECT o.*, i.sector, i.industry FROM scout.opportunities o LEFT JOIN scout.instruments i USING (ticker) "
        "WHERE o.status IN ('new', 'triggered') ORDER BY o.asof DESC, o.score DESC LIMIT 24"
    )
    events = await db.fetch_all(
        "SELECT ev.event_type, ev.event_date, ev.price, o.ticker, o.primary_detector FROM scout.opportunity_events ev "
        "JOIN scout.opportunities o ON o.id = ev.opportunity_id ORDER BY ev.event_date DESC, ev.id DESC LIMIT 12"
    )
    market = ((brief or {}).get("payload") or {}).get("market") or {}
    head = ""
    if market:
        leaders = "，".join(f"{tlink(k)} {signed(v)}" for k, v in market.get("sector_leaders_20d") or [])
        laggards = "，".join(f"{tlink(k)} {signed(v)}" for k, v in market.get("sector_laggards_20d") or [])
        head = f"""<div class="card"><h3>收盘 {e(brief['asof'])} · 大盘 {e(opportunities.state_text(market))}</h3>
<div class="small muted">{e(opportunities.state_text(market, detail=True))}</div>
{tlink('SPY')} 当日 {signed(market.get('spy_ret_1d'))} · 20 日 {signed(market.get('spy_ret_20d'))} · {tlink('QQQ')} 20 日 {signed(market.get('qqq_ret_20d'))}
· {tlink('IWM')} 20 日 {signed(market.get('iwm_ret_20d'))} · {tlink('SMH')} 20 日 {signed(market.get('smh_ret_20d'))}
<div class="small">领涨板块：{leaders}　落后板块：{laggards}</div></div>"""
        head += await etf_tables()
    else:
        head = '<div class="card">还没有收盘扫描结果。运行 <code>python -m scout.jobs.eod</code> 后这里会出现内容。</div>'
    ev_rows = [[e(x["event_date"]), f'<a href="/ticker/{e(x["ticker"])}">{e(x["ticker"])}</a>', e(x["primary_detector"]),
                e(telegram.STATUS_LABELS.get(x["event_type"], x["event_type"])), f"{x['price']:.2f}" if x["price"] else "—"]
               for x in events]
    pre = await premarket.latest()
    current = bool(pre) and pre["asof"] == date.fromisoformat(us_equity_market_status().market_date)
    settings = await portfolio.get_settings()
    live = [o for o in active if views.is_live(o["primary_detector"])]
    demoted = [o for o in active if not views.is_live(o["primary_detector"])]
    held = await portfolio.trades(open_only=True)
    body = views.premarket_card(pre, current) + head
    if held:
        body += "<h2>我的持仓</h2>" + views.trades_table(held, closable=False)
    warn = portfolio.concentration(
        [{"ticker": o["ticker"], "sector": universe.SECTOR_ZH.get(o.get("sector"), o.get("sector")), "industry": o.get("industry")}
         for o in live + held])
    if warn:
        body += "<div class='card small warn' style='margin:12px 0'>⚠️ 集中度：" + "<br>".join(e(w) for w in warn) + "</div>"
    body += "<h2>活跃机会</h2>"
    body += (f'<div class="grid">{"".join(opp_card(o, settings) for o in live)}</div>' if live
             else "<p class='muted'>暂无活跃机会。</p>")
    if demoted:
        body += (f"<details style='margin-top:12px'><summary class='muted'>降为候选的规则仍在跟踪的机会（{len(demoted)}）"
                 f"——只为积累实盘记录，不建议据此开仓</summary><div class='grid' style='margin-top:8px'>"
                 f"{''.join(opp_card(o) for o in demoted)}</div></details>")
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

    head = ["代码", "名称", "1 日", "20 日", "相对强度分位（3–12 月）"]
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


@app.post("/ticker/{ticker}/events")
async def ticker_events(ticker: str, _: str = Depends(require_auth)):
    """Research the cause of recent material abnormal days (web search; ~30 s)."""
    from scout.insight import material

    t = universe.yahoo_symbol(ticker)
    inst = await db.fetch_one("SELECT name, sector FROM scout.instruments WHERE ticker = :t", {"t": t}) or {}
    evs = await events_mod.recent(t, universe.SECTOR_ETFS.get(inst.get("sector") or ""))
    done = await event_cause.cached(t, [x["date"] for x in evs])
    todo = [x for x in evs if x["date"] not in done and material(x)][:3]
    await asyncio.gather(*(event_cause.explain(t, inst.get("name") or t, x) for x in todo), return_exceptions=True)
    return RedirectResponse(f"/ticker/{t}", status_code=303)


@app.get("/ticker")
async def ticker_lookup(t: str = "", _: str = Depends(require_auth)):
    return RedirectResponse(f"/ticker/{universe.yahoo_symbol(t)}" if t else "/", status_code=302)


async def _side_panel(t: str, opp: dict | None, f: dict, struct: dict, bars: list[dict]) -> str:
    """Next to the chart: the active rule plan (with its lifecycle) or reference levels, plus the user's holding."""
    from scout import levels
    from scout.data.yahoo import live_quote
    from scout.market_calendar import us_equity_market_status

    try:
        live = await asyncio.wait_for(live_quote(t), timeout=8)
    except Exception:                       # the page must render on the last close when Yahoo is slow
        live = None
    today = date.fromisoformat(us_equity_market_status().market_date)
    settings = await portfolio.get_settings()
    active = opp if opp and opp["status"] in ("new", "triggered") else None
    tracking = ""
    if active:
        trig = await db.fetch_one("SELECT price FROM scout.opportunity_events WHERE opportunity_id = :i AND event_type = 'triggered'",
                                  {"i": active["id"]})
        tracking = views.trade_plan(active, f.get("close"), None, live=live, today=today,
                                    entry_price=(trig or {}).get("price"))
    try:
        pm = await levels.get_map(t) or {}
    except Exception:
        logger.exception("price map for %s failed", t)
        pm = {}
    panel = views.price_panel(pm, live, settings, opp=active, today=today, tracking_html=tracking, ticker=t)
    held = [x for x in await portfolio.trades(open_only=True) if x["ticker"] == t]
    if held:
        panel += views.holding_card(held, live, f.get("close"), today,
                                    {x["id"]: await portfolio.sells(x["id"]) for x in held})
    return f"<div>{panel}</div>"


async def _research_card(t: str, inst: dict, feat: dict) -> str:
    """The data behind the fundamental / expectations / ownership / options / macro lenses."""
    from scout import context, research

    asof = feat["date"]
    fund = await fundamentals.get(t) if inst.get("kind", "stock") == "stock" else {}
    events_ = (await context.earnings_map(asof, days_back=120, days_ahead=90)).get(t, [])
    rs = await research.build(t, asof, feat["payload"] or {}, inst.get("sector"),
                              context.earnings_summary(events_, asof) if events_ else {}, fund)
    return views.research_card(rs, {**fund, **rs["values"]})


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
    side = await _side_panel(t, plan_opp, payload, struct, bars)
    body += f"<div class='chartrow'>{views.chart_script(bars, plan, struct)}{side}</div>"
    from scout import sectors as sector_mod

    headline, lines = scout_view(
        payload, is_stock=(inst or {}).get("kind", "stock") == "stock",
        next_earnings=(nxt or {}).get("event_date"), latest_opp=latest, today=date.today(),
        structure=struct, sector_ctx=await sector_mod.context_for((inst or {}).get("sector")),
    )
    body += '<div class="grid" style="margin-top:12px">'
    body += f'<div id="scout">{views.scout_panel(t, headline, lines, ai, bool(get_scout_settings().openai_api_key))}</div>'
    body += f'<div class="card"><h3>新闻</h3><ul class="tight">{news_html}</ul></div>'
    evs = await events_mod.recent(t, universe.SECTOR_ETFS.get((inst or {}).get("sector") or ""))
    above_200 = (payload["close"] > payload["sma200"]) if payload.get("close") and payload.get("sma200") else None
    body += views.events_card(t, evs, await event_cause.cached(t, [x["date"] for x in evs]),
                              await events_mod.load_base_rates(), above_200)
    if feat:
        try:                                # a slow data source must not break the page
            body += await asyncio.wait_for(_research_card(t, inst or {}, feat), timeout=20)
        except Exception as exc:
            logger.warning("research data for %s unavailable: %s", t, exc)
    if latest:
        body += opp_card(latest)
    body += await sector_members_card(t)
    body += (f'<details class="card"><summary>全部特征 <span class="small muted">{e((feat or {}).get("date"))}</span></summary>'
             f'<p class="small muted">鼠标悬停指标名可查看定义（数据字典 scout/knowledge/features.yaml）</p>'
             f'{table(["指标", "值", "读数"], feat_rows)}</details>')
    body += "</div>"
    if ai:      # full width below the grid, so the Scout panel does not sit alone in a row
        body += f"<div style='margin-top:12px'>{views.insight_card(ai)}</div>"
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
    rrg_html = f"<div class='card'><h3>相对轮动图 {tabs}</h3>{views.rrg_panel(rrg_rows)}</div>"

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
        "SELECT count(DISTINCT ticker) AS n FROM scout.market_events WHERE event_type = 'earnings' "
        "AND event_date BETWEEN :a AND :b AND ticker = ANY(:t)",
        {"a": asof, "b": asof + timedelta(days=7), "t": [r["ticker"] for r in rows]})
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
    from scout import b0

    b0_rank = await b0.latest_b0_rank()
    on = s == "b0"
    nav += (f"<div style='margin:4px 0'><span class='small muted'>我的策略：</span><a class='chip' "
            f"style='{'background:var(--accent);color:#fff' if on else ''}' href='/screens?s=b0'>B0 动量打分 "
            f"{len((b0_rank or {}).get('rows') or [])}</a></div>")
    angle_asof, angle_rows = await sc.angle_snapshot()
    nav += ("<div style='margin:4px 0'><span class='small muted'>基本面与事件：</span>" + "".join(
        f"<a class='chip' style='{'background:var(--accent);color:#fff' if x.id == s else ''}' href='/screens?s={e(x.id)}'>"
        f"{e(x.title)} {len(sc.run_angle(x, angle_rows))}</a>" for x in sc.ANGLE_SCREENS) + "</div>")
    if on:
        return layout("策略筛选", "screens", f"<h2>策略筛选</h2>{nav}" + views.b0_rank_panel(b0_rank))
    angle = next((x for x in sc.ANGLE_SCREENS if x.id == s), None)
    if angle:
        entry = await learned.get(angle.strategy)
        return layout("策略筛选", "screens", f"<h2>策略筛选</h2>{nav}"
                      + views.angle_screen_panel(angle, sc.run_angle(angle, angle_rows), angle_asof, entry))
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
    from scout import forward
    from scout.detectors import registry
    from scout.tracker import scoreboard

    live = {r["detector"]: r for r in await scoreboard()}
    rates = await db.fetch_all("SELECT detector, regime, stats, computed_at FROM scout.detector_base_rates")
    legacy: dict[str, dict] = {}
    for r in rates:
        legacy.setdefault(r["detector"], {})[r["regime"]] = r["stats"]
    computed = max((r["computed_at"] for r in rates), default=None)
    fair_row = await db.fetch_one("SELECT asof, payload FROM scout.briefs WHERE kind = 'rule_benchmarks' ORDER BY id DESC LIMIT 1")
    fair = ((fair_row or {}).get("payload") or {}).get("detectors") or {}
    entries = await learned.current()
    held = learned.held_back(entries)
    by_rule = {}
    for en in entries:                    # the entry that is the verdict on a rule (its spec's rule first)
        rule = learned.rule_of(en)
        if rule and (rule not in by_rule or en.get("spec")):
            by_rule[rule] = en
    order = {"confirmed": 0, "forward": 1, "backtested": 2, "hypothesis": 3, "rejected": 4, "retired": 5}
    rows = []
    for d in registry():
        en = by_rule.get(d.name)
        fw = await forward.report(en, date.today()) if en and en["status"] in ("forward", "confirmed") else None
        rows.append({"det": d, "entry": en, "held_back": d.name in held, "fair": fair.get(d.name), "live": live.get(d.name),
                     "forward": fw, "legacy": legacy.get(d.name, {})})
    rows.sort(key=lambda r: (order.get((r["entry"] or {}).get("status"), 9), not r["det"].live))
    return views.scoreboard_page(rows, (fair_row or {}).get("asof"), computed)


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
        ["代码", "主题", "收盘", "1 日", "5 日", "20 日", "相对强度分位（3–12 月）", "距 52 周高点", ""], trs
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


def _bold(escaped: str) -> str:
    """Render the **bold** the model sometimes emits (input is already HTML-escaped)."""
    return re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", escaped)


@app.get("/chat", response_class=HTMLResponse)
async def chat_page(_: str = Depends(require_auth)) -> str:
    rows = await db.fetch_all(
        "SELECT role, content, created_at, channel FROM scout.chat_messages ORDER BY id DESC LIMIT 40"
    )
    msgs = "".join(
        f'<div class="card" style="margin-bottom:8px;{"border-color:var(--accent)" if r["role"] == "user" else ""}">'
        f'<div class="small muted">{"你" if r["role"] == "user" else "Scout"} · {e(r["channel"])} · {e(str(r["created_at"])[:16])}</div>'
        f'<pre>{_bold(e(r["content"]))}</pre></div>'
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


# ── My trades ──

def _num(x: str | None) -> float | None:
    try:
        return float(str(x).replace(",", "").replace("$", "").strip()) if x not in (None, "") else None
    except ValueError:
        return None


@app.get("/trades", response_class=HTMLResponse)
async def trades_page(opp: str | None = None, t: str | None = None, plan: str | None = None,
                      _: str = Depends(require_auth)) -> str:
    from scout import levels

    settings = await portfolio.get_settings()
    open_rows, closed_rows = await portfolio.trades(open_only=True), await portfolio.trades(open_only=False)
    pre = {}
    if t and plan and not opp:
        pm = await levels.get_map(universe.yahoo_symbol(t)) or {}
        en = next((x for x in pm.get("entries") or [] if x["name"] == plan), None)
        if en:
            mid = (en["zone_lo"] + en["zone_hi"]) / 2
            sz = portfolio.size(mid, en["stop"], settings, pm.get("vol")) or {}
            pre = {"ticker": universe.yahoo_symbol(t), "entry": f"{mid:.2f}", "stop": f"{en['stop']:.2f}",
                   "target": f"{en['target1']:.2f}", "shares": str(sz.get("shares") or ""), "from": plan}
    if opp:
        o = await db.fetch_one("SELECT * FROM scout.opportunities WHERE id = :id", {"id": opp})
        if o:
            stop = o.get("current_stop") or o["stop"]
            mid = (o["entry_lo"] + o["entry_hi"]) / 2
            sz = portfolio.size(mid, stop, settings, portfolio.opp_vol(o)) or {}
            pre = {"ticker": o["ticker"], "opp": o["id"], "entry": f"{mid:.2f}", "stop": f"{stop:.2f}",
                   "target": f"{o['target1']:.2f}", "shares": str(sz.get("shares") or "")}
    acct = settings.get("account_size")
    settings_html = f"""<details class="card" {'' if acct else 'open'}><summary>账户与风险设置
<span class="small muted">{'账户 $' + format(acct, ',.0f') + f" · 单笔风险 {settings['risk_pct'] * 100:.1f}% · 单只上限 {settings['max_position_pct'] * 100:.0f}% · 单只月波动上限 {settings['vol_budget_pct'] * 100:.1f}%" if acct else '未设置：卡片只显示每 $1,000 风险对应的股数'}</span></summary>
<form method="post" action="/trades/settings" style="display:flex;gap:8px;flex-wrap:wrap;margin-top:8px;align-items:end">
<label class="small">账户规模（美元）<br><input name="account_size" value="{e(acct or '')}" placeholder="如 100000" size="12"></label>
<label class="small">单笔风险 %<br><input name="risk_pct" value="{settings['risk_pct'] * 100:g}" size="5"></label>
<label class="small">单只仓位上限 %<br><input name="max_position_pct" value="{settings['max_position_pct'] * 100:g}" size="5"></label>
<label class="small">单只月波动上限 %<br><input name="vol_budget_pct" value="{settings['vol_budget_pct'] * 100:g}" size="5"></label>
<button>保存</button></form>
<p class="small muted">仓位 = 账户 × 单笔风险 ÷（入场 − 止损），再按两个上限截断：单只仓位上限，以及波动上限——
一只股票一个月的正常波动（1σ，按过去 63 日波动）带来的盈亏不超过账户的这个比例，波动越大仓位越小
（《股票趋势技术分析》第 42 章的"综合杠杆"思路；风险研究中按过去波动倒数加权的组合夏普 0.57，等权 0.49，回撤也更小）。只用于计算，Scout 不会下单。</p></details>"""
    add_html = f"""<div class="card" style="margin-top:12px"><h3>记录一笔入场</h3>
<form method="post" action="/trades/add" style="display:flex;gap:8px;flex-wrap:wrap;align-items:end">
<input type="hidden" name="opportunity_id" value="{e(pre.get('opp', ''))}">
<label class="small">代码<br><input name="ticker" value="{e(pre.get('ticker', ''))}" size="7" required></label>
<label class="small">日期<br><input name="opened_on" type="date" value="{date.today()}" required></label>
<label class="small">成交价<br><input name="entry_price" value="{e(pre.get('entry', ''))}" size="8" required></label>
<label class="small">股数<br><input name="shares" value="{e(pre.get('shares', ''))}" size="6" required></label>
<label class="small">备注<br><input name="note" size="20"></label><button>记录</button></form>
<p class="small muted">只需填代码、日期、成交价和股数。止损和止盈由系统设置：按成交时的价位图给出初始止损、目标 1 / 2，
之后每天收盘后自动更新（不分批：到目标 1 后止损改用吊灯止损，只升不降，跌破时卖出全部，最长持有 60 个交易日），与个股页「我的持仓」一致，变动会推送到 Telegram。</p>
{'<p class="small muted">已按' + ('价位参考的「' + e(pre["from"]) + '」' if pre.get("from") else '机会卡') + '预填买入区间中点和建议股数，请改成你的实际成交。</p>' if pre else ''}</div>"""
    risk = sum(portfolio.metrics(t).get("open_risk") or 0 for t in open_rows)
    risk_line = (f"<p class='small'>持仓 {len(open_rows)} 笔 · 全部触发止损的潜在亏损约 ${risk:,.0f}"
                 + (f"（账户的 {risk / acct * 100:.1f}%）" if acct else "") + "</p>") if open_rows else ""
    if open_rows:
        risk_line += views.holdings_risk_html(await portfolio.holdings_risk(open_rows, acct), acct)
    adh, adh_summary = await portfolio.journal_adherence(closed_rows)
    warn = portfolio.concentration([{"ticker": t["ticker"], "sector": universe.SECTOR_ZH.get(t.get("sector"), t.get("sector")),
                                     "industry": t.get("industry")} for t in open_rows])
    warn_html = ("<p class='small warn'>⚠️ " + "<br>".join(e(w) for w in warn) + "</p>") if warn else ""
    summ = portfolio.journal_summary(closed_rows)
    summ_html = (f"<p class='small'>已平仓 {summ['n']} 笔 · 胜率 {summ['win_rate'] * 100:.0f}% · 累计盈亏 ${summ['total_pnl']:+,.0f}"
                 + (f" · 平均 {summ['avg_r']:+.2f}R" if summ.get("avg_r") is not None else "") + "</p>") if summ["n"] else ""
    summ_html += views.adherence_html(adh_summary)
    body = (f"<h2>我的交易</h2>{settings_html}{add_html}<h2>持仓</h2>{risk_line}{warn_html}{views.trades_table(open_rows)}"
            f"<h2>已平仓</h2>{summ_html}{views.trades_table(closed_rows, adherence=adh)}"
            "<p class='small muted'>现价为最近一次收盘价（Scout 标的池之外的代码没有行情）。这里记录的是你实际做了什么，"
            "与规则记分板对照，可以看出执行是否偏离规则。</p>")
    return layout("我的交易", "trades", body)


@app.post("/trades/settings")
async def trades_settings(account_size: str = Form(""), risk_pct: str = Form("1"), max_position_pct: str = Form("20"),
                          vol_budget_pct: str = Form("2"), _: str = Depends(require_auth)):
    risk, cap, vb = _num(risk_pct), _num(max_position_pct), _num(vol_budget_pct)
    await portfolio.save_settings({
        "account_size": _num(account_size),
        "risk_pct": min(max((risk or 1) / 100, 0.001), 0.05),          # 0.1%–5% per trade
        "max_position_pct": min(max((cap or 20) / 100, 0.01), 1.0),
        "vol_budget_pct": min(max((vb or 2) / 100, 0.002), 0.2),       # 0.2%–20% of the account per month (1σ)
    })
    return RedirectResponse("/trades", status_code=303)


@app.post("/trades/add")
async def trades_add(ticker: str = Form(...), opened_on: str = Form(...), entry_price: str = Form(...), shares: str = Form(...),
                     note: str = Form(""), opportunity_id: str = Form(""),
                     _: str = Depends(require_auth)):
    entry, qty = _num(entry_price), _num(shares)
    if not entry or not qty or entry <= 0 or qty <= 0:
        raise HTTPException(status_code=400, detail="成交价和股数必须是正数")
    tid = await portfolio.add_trade({"ticker": universe.yahoo_symbol(ticker), "opened_on": date.fromisoformat(opened_on),
                                     "entry_price": entry, "shares": qty, "note": note, "opportunity_id": opportunity_id or None})
    row = await db.fetch_one("SELECT * FROM scout.trades WHERE id = :id", {"id": tid})
    try:                                    # stop and target are the system's from the start
        await portfolio.manage_trade(row)
    except Exception:
        logger.exception("managing new trade %s failed", tid)
    return RedirectResponse("/trades", status_code=303)


@app.post("/trades/{trade_id}/sell")
async def trades_sell(trade_id: int, price: str = Form(...), sold_on: str = Form(...), shares: str = Form(""),
                      back: str = Form("/trades"), _: str = Depends(require_auth)):
    try:
        await portfolio.add_sell(trade_id, date.fromisoformat(sold_on), _num(price) or 0, _num(shares))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return RedirectResponse(back if back.startswith("/") and not back.startswith("//") else "/trades", status_code=303)


@app.post("/trades/sells/{sell_id}/delete")
async def trades_sell_delete(sell_id: int, _: str = Depends(require_auth)):
    await portfolio.delete_sell(sell_id)
    return RedirectResponse("/trades", status_code=303)


@app.post("/trades/{trade_id}/delete")
async def trades_delete(trade_id: int, _: str = Depends(require_auth)):
    await portfolio.delete_trade(trade_id)
    return RedirectResponse("/trades", status_code=303)


async def _angles_overview() -> str:
    """Each analysis angle: source, how far back it goes, the point-in-time rule, how it is validated and the verdict so far."""
    from scout import angles, ledger
    from scout.detectors import registry
    from scout.insight import LENS_TITLES

    entries = {en["key"]: en for en in await learned.current()}
    rules = {learned.rule_of(en): en for en in entries.values() if learned.rule_of(en)}
    cal = {}
    for r in await ledger.calibration(20):
        if r["kind"] == "insight_lens":
            cal[r["lens"]] = cal.get(r["lens"], 0) + r["n"]
    rows = []
    for a in angles.ANGLES:
        table_name, col = a["history"]
        h = await db.fetch_one(f"SELECT min({col}) AS lo, max({col}) AS hi, count(*) AS n FROM {table_name}")
        depth = f"{h['lo']} 至 {h['hi']}（{h['n']:,} 行）" if h and h["lo"] else "—"
        if a["strategies"] == "rule":
            tested = [(d.label, rules.get(d.name)) for d in registry()]
        else:
            tested = [((entries.get(k) or {}).get("title", k), entries.get(k)) for k in a["strategies"]]
        verdicts = "<br>".join(f"{e(name)}：<b>{e(learned.STATUS_ZH.get(en['status'], en['status']) if en else '未检验')}</b>"
                               for name, en in tested) or "—"
        fwd = "；".join(f"{e(LENS_TITLES.get(l, l))} {cal.get(l, 0)} 个" for l in a["lenses"]) or "—"
        rows.append([f"<b>{e(a['name'])}</b>", e(a["source"]), e(depth), e(a["pit"]),
                     "回测（检验关）+ 前向" if a["path"] == "backtest" else "只能前向记录", verdicts, fwd])
    return ("<h2>分析角度</h2><p class='small muted'>每个角度的数据来源、历史深度、按时点使用的规则、验证方式和目前的结论。"
            "有干净历史的走检验关（和同一天随机选的股票比，前后两半都赢、t ≥ 3），再前向验证；"
            "没有历史的（期权、新闻、AI 判断）从现在起记入判断台账，满 30 个样本后看校准。</p>"
            + table(["角度", "来源", "历史", "按时点规则", "验证方式", "选股规则的检验结论", "AI 视角已打分样本（20 日）"], rows))


@app.get("/data", response_class=HTMLResponse)
async def data_page(_: str = Depends(require_auth)) -> str:
    """Health of the research data: coverage, freshness, quality findings, storage."""
    from scout import research_data

    st = await research_data.status()
    used, cap = st["storage"]["used_gb"], st["storage"]["volume_gb"]
    kinds = {"estimates": "预期与目标价（Yahoo）", "ownership": "持仓与做空（Yahoo）", "options": "期权快照（Yahoo，盘中 15:30–16:00）"}
    rows = [[e(kinds.get(r["kind"], r["kind"])), e(r["last"]), str(r["tickers"]), f"{r['rows']:,}"] for r in st["snapshots"]]
    for label, r in (("季度财务（SEC EDGAR）", st["financials"]), ("SEC 文件索引", st["filings"]), ("评级变动（Yahoo）", st["actions"])):
        rows.append([e(label), e(str(r["last"])[:16] if r["last"] else "—"), str(r["tickers"]), f"{r['rows']:,}"])
    macro = " · ".join(f"{e(m['series'])} {e(m['last'])}" for m in st["macro"]) or "—"
    issue_rows = [[e(i["asof"]), e(i["check_name"]), str(i["n"]), e(", ".join((i["tickers"] or [])[:12]))] for i in st["issues"]]
    run_rows = [[e(r["job"]), e(r["status"]), e(str(r["started_at"])[:16]),
                 e(json.dumps(r["summary"], ensure_ascii=False, default=str)[:180])] for r in st["runs"]]
    angles_html = await _angles_overview()
    body = (angles_html + f"<h2>数据状态</h2><p class='small muted'>研究池 {st['pool']} 只（持仓 ∪ 活跃机会 ∪ 自选 ∪ 成交额前 100）。"
            f"所有快照按日期追加保存，不覆盖历史。</p>"
            f"<p class='small'>数据库已用 <b>{used:.2f} GB</b> / {cap:g} GB（{used / cap:.0%}），超过 80% 会推送扩容提醒。</p>"
            + table(["数据", "最近", "覆盖股票", "累计行数"], rows)
            + f"<p class='small'>宏观（FRED）最新日期：{macro}</p>"
            + "<h2>数据质量（近 14 天）</h2>" + (table(["日期", "检查", "次数", "涉及"], issue_rows) if issue_rows
                                                else "<p class='muted small'>没有发现问题。</p>")
            + "<h2>最近任务</h2>" + table(["任务", "状态", "开始（UTC）", "摘要"], run_rows))
    return layout("数据状态", "data", body)


@app.get("/ledger", response_class=HTMLResponse)
async def ledger_page(h: int = 20, kind: str = "", _: str = Depends(require_auth)) -> str:
    """Calibration of every kind of judgment, and the judgments themselves for spot checks."""
    from scout import ledger
    from scout.insight import LENS_TITLES

    h = h if h in ledger.HORIZONS else 20
    tabs = " ".join(f"<a class='chip' style='{'background:var(--accent);color:#fff' if x == h else ''}' href='/ledger?h={x}'>{x} 日</a>"
                    for x in ledger.HORIZONS)
    titles = {**{en["key"]: en["title"] for en in await learned.current()}, **LENS_TITLES}     # lens or thesis method
    cal_rows = []
    for r in await ledger.calibration(h):
        name = titles.get(r["lens"], r["lens"] or "—")
        enough = r["enough"]
        cal_rows.append([e(ledger.KIND_ZH.get(r["kind"], r["kind"])), e(name), e(ledger.STANCE_ZH[r["stance"]]), str(r["n"]),
                         f"{r['hit_rate'] * 100:.0f}%" if enough else f"<span class='muted'>{r['hit_rate'] * 100:.0f}%（样本不足）</span>",
                         signed(r["median_excess"]) if enough else "<span class='muted'>—</span>"])
    where = "WHERE j.kind = :k" if kind in ledger.KIND_ZH else ""
    recent = await db.fetch_all(
        f"SELECT j.*, o20.excess_spy AS x20, o20.hit AS hit20, o5.excess_spy AS x5 FROM scout.judgments j "
        f"LEFT JOIN scout.judgment_outcomes o20 ON o20.judgment_id = j.id AND o20.horizon = 20 "
        f"LEFT JOIN scout.judgment_outcomes o5 ON o5.judgment_id = j.id AND o5.horizon = 5 "
        f"{where} ORDER BY j.asof DESC, j.id DESC LIMIT 60", {"k": kind})
    judg_rows = []
    for j in recent:
        p = j["payload"] or {}
        what = p.get("headline") or ("；".join(p.get("points") or [])[:80]) or (f"热度 {p.get('heat')}" if j["kind"] == "sector_heat" else "")
        lens = titles.get(j["lens"], j["lens"])
        rating = {1: "👍", -1: "👎"}.get(j["human_rating"], "")
        form = (f"<form method='post' action='/ledger/{j['id']}/rate' style='display:inline'>"
                f"<button name='r' value='1' style='padding:0 6px'>👍</button><button name='r' value='-1' style='padding:0 6px;background:var(--down)'>👎</button></form>")
        judg_rows.append([e(j["asof"]), f"<a href='/ticker/{e(j['ticker'])}'>{e(j['ticker'])}</a>",
                          e(ledger.KIND_ZH.get(j["kind"], j["kind"])) + (f"<div class='small muted'>{e(lens)}</div>" if lens else ""),
                          e(ledger.STANCE_ZH[j["stance"]]), e(what), signed(j["x5"]) if j["x5"] is not None else "—",
                          (signed(j["x20"]) + (" ✓" if j["hit20"] else " ✗" if j["hit20"] is False else "")) if j["x20"] is not None else "未到期",
                          rating or form])
    kinds = " ".join(f"<a class='chip' href='/ledger?h={h}&kind={k}'>{e(v)}</a>" for k, v in ledger.KIND_ZH.items())
    rated = await db.fetch_one("SELECT count(*) AS n FROM scout.judgments WHERE human_rating IS NOT NULL "
                               "AND made_at >= date_trunc('month', now())")
    body = (f"<h2>判断台账 {tabs}</h2><p class='small muted'>每个判断记录当时的数据日期和价格；{h} 个交易日后与 SPY 比较。"
            f"偏多 = 跑赢，偏空 = 跑输，中性 = 相对 SPY 在 ±{ledger.NEUTRAL_BAND[h] * 100:.1f}% 以内。"
            f"少于 {ledger.MIN_SAMPLES} 个样本的只显示，不参与任何调整。</p>"
            + (table(["类别", "视角 / 规则", "判断", "样本", "命中率", "相对 SPY 中位数"], cal_rows) if cal_rows
               else "<p class='muted small'>还没有到期的判断：最早的判断在 5 个交易日后开始打分。</p>")
            + f"<h2>最近的判断</h2><p class='small'>{kinds}　本月人工抽检 {rated['n']}/10（判断写得是否合理，与结果无关）</p>"
            + table(["日期", "代码", "类别", "判断", "内容", "5 日相对 SPY", "20 日相对 SPY", "抽检"], judg_rows))
    return layout("判断台账", "ledger", body)


@app.post("/ledger/{judgment_id}/rate")
async def ledger_rate(judgment_id: int, r: int = Form(...), _: str = Depends(require_auth)):
    await db.execute("UPDATE scout.judgments SET human_rating = :r WHERE id = :i", {"r": 1 if r > 0 else -1, "i": judgment_id})
    return RedirectResponse("/ledger", status_code=303)


@app.get("/knowledge", response_class=HTMLResponse)
async def knowledge_page(_: str = Depends(require_auth)) -> str:
    entries = await learned.current()
    rows = await db.fetch_all("SELECT key, version, status, changed_by, note, created_at FROM scout.knowledge "
                              "ORDER BY key, version DESC")
    versions: dict[str, list[dict]] = {}
    for r in rows:
        versions.setdefault(r["key"], []).append(r)
    runs = {r["key"]: r for r in await db.fetch_all(
        "SELECT DISTINCT ON (key) key, backtest, critique, passed, ran_at FROM scout.gate_runs ORDER BY key, id DESC")}
    from scout import forward

    fwd = {en["key"]: await forward.report(en, date.today()) for en in entries if en["status"] in ("forward", "confirmed")}
    return views.knowledge_page(entries, versions, date.today(), runs, fwd)


@app.post("/knowledge/{key}/status")
async def knowledge_status(key: str, status_: str = Form(..., alias="status"), note: str = Form(""),
                           _: str = Depends(require_auth)):
    """The user moves an entry along the gate (the only way to `confirmed`)."""
    if status_ == "confirmed":            # G4 only after the forward record meets the agreed bar
        from scout import forward

        entry = await learned.get(key)
        rep = await forward.report(entry, date.today()) if entry else {"ready": False, "criteria": []}
        if not rep["ready"]:
            missing = "；".join(f"{c['name']} {c['value']}（要求 {c['bar']}）" for c in rep["criteria"] if not c["ok"])
            raise HTTPException(status.HTTP_400_BAD_REQUEST, f"前向验证还没有达标，不能确认：{missing}")
    try:
        await learned.move(key, status_, "user", note)
    except learned.KnowledgeError as exc:
        raise HTTPException(status.HTTP_400_BAD_REQUEST, str(exc)) from exc
    return RedirectResponse(f"/knowledge#{key}", status_code=303)



@app.get("/outlook", response_class=HTMLResponse)
async def outlook_page(_: str = Depends(require_auth)) -> str:
    from scout import outlook

    rec = await outlook.latest()
    methods = {en["key"]: en for en in await learned.current()}
    rows = await db.fetch_all(
        "SELECT j.*, o20.excess_spy AS x20, o20.hit AS hit20, o60.excess_spy AS x60, o60.hit AS hit60, "
        "o120.excess_spy AS x120, o120.hit AS hit120 FROM scout.judgments j "
        "LEFT JOIN scout.judgment_outcomes o20 ON o20.judgment_id = j.id AND o20.horizon = 20 "
        "LEFT JOIN scout.judgment_outcomes o60 ON o60.judgment_id = j.id AND o60.horizon = 60 "
        "LEFT JOIN scout.judgment_outcomes o120 ON o120.judgment_id = j.id AND o120.horizon = 120 "
        "WHERE j.kind = 'sector_thesis' ORDER BY j.asof DESC, j.id DESC LIMIT 30")
    theses = [{**r, "method_title": (methods.get(r["lens"]) or {}).get("title"),
               "method_status": learned.STATUS_ZH.get((methods.get(r["lens"]) or {}).get("status"), "")} for r in rows]
    return views.outlook_page(rec, theses)
