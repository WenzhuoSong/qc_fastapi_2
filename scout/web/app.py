"""Scout web service: Telegram webhook + dashboard. `uvicorn scout.web.app:app`."""
from __future__ import annotations

import asyncio
import logging
import secrets
from contextlib import asynccontextmanager
from datetime import date

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


AI_PROMPT = (
    "为 {t} 写一段 Scout 分析（150–250 字，纯文本）：先判断近期走势是个股自身还是大盘带动（用 idio/market 拆分），"
    "再说趋势与相对强度、近期新闻或财报催化、主要风险，最后给出「值得关注的条件」和「失效条件」。只用工具返回的数据，注明数据日期。"
)


@app.post("/ticker/{ticker}/ai")
async def ticker_ai(ticker: str, _: str = Depends(require_auth)):
    from scout.agent import answer

    t = universe.yahoo_symbol(ticker)
    feat = await db.fetch_one("SELECT max(date) AS d FROM scout.features_daily WHERE ticker = :t", {"t": t})
    try:
        text = await answer(AI_PROMPT.format(t=t), channel="dashboard", remember=False)
    except Exception as exc:
        logger.exception("ticker AI analysis failed")
        return HTMLResponse(layout(t, "", f"<p>{e(telegram.friendly_error(exc))}</p><p>{tlink(t, '返回 ' + t)}</p>"), status_code=502)
    await db.execute(
        "INSERT INTO scout.briefs (kind, asof, content, payload) VALUES ('ticker_ai', :d, :c, CAST(:p AS JSONB))",
        {"d": (feat or {}).get("d") or date.today(), "c": text, "p": db.jsonb({"ticker": t})},
    )
    return RedirectResponse(f"/ticker/{t}#scout", status_code=303)


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
    ai = await db.fetch_one(
        "SELECT content, created_at FROM scout.briefs WHERE kind = 'ticker_ai' AND payload->>'ticker' = :t "
        "AND asof >= COALESCE((SELECT max(date) FROM scout.features_daily WHERE ticker = :t), CURRENT_DATE) "
        "ORDER BY id DESC LIMIT 1", {"t": t},
    )
    if not inst and not bars:
        return layout(t, "", f"<p>{e(t)} 不在标的池中。可以在 Watchlist 页加入。</p>")
    latest = opps[0] if opps else None
    levels = None
    if latest and latest["status"] in ("new", "triggered"):
        levels = {k: latest[k] for k in ("entry_lo", "entry_hi", "stop", "target1", "target2")}
    payload = (feat or {}).get("payload") or {}
    pct_keys = {"ret_1d", "ret_5d", "ret_20d", "ret_63d", "ret_252d", "atr_pct", "dist_52w_high", "gap_pct",
                "rs_pct", "sector_rs_pct", "sector_rel_20", "market_ret_1d", "idio_ret_1d", "idio_ret_5d", "idio_ret_20d",
                "bb_pct", "vol_20", "max_dd_60"}
    def fmt(k: str, v: float) -> str:
        if k in pct_keys:
            return signed(v)
        return f"{v:,.0f}" if k == "dollar_vol20" else f"{v:.2f}"

    feat_rows = [[e(views.FEATURE_ZH.get(k, k)), fmt(k, payload[k])] for k in SNAPSHOT_FEATURES if k in payload]
    news_html = "".join(
        f"<li><a href='{e(n.get('url'))}' target='_blank' rel='noopener'>{e(n['headline'])}</a> "
        f"<span class='small muted'>{e(n.get('source'))} {e((n.get('published') or '')[:16])}</span>"
        f"{'<br><span class=small>' + e(n['summary']) + '</span>' if n.get('summary') else ''}</li>"
        for n in news
    ) or "<li class='muted'>近 72 小时无新闻。</li>"
    name = " · ".join(x for x in ((inst or {}).get("name"), (inst or {}).get("sector")) if x)
    body = f"<h2>{e(t)} <span class='muted small'>{e(name)}</span></h2>"
    body += views.chart_script(bars, levels)
    headline, lines = scout_view(
        payload, is_stock=(inst or {}).get("kind", "stock") == "stock",
        next_earnings=(nxt or {}).get("event_date"), latest_opp=latest, today=date.today(),
    )
    body += '<div class="grid" style="margin-top:12px">'
    body += f'<div id="scout">{views.scout_panel(t, headline, lines, ai, bool(get_scout_settings().openai_api_key))}</div>'
    body += f'<div class="card"><h3>新闻</h3><ul class="tight">{news_html}</ul></div>'
    if latest:
        body += opp_card(latest)
    body += await sector_members_card(t)
    body += (f'<details class="card"><summary>全部特征 <span class="small muted">{e((feat or {}).get("date"))}</span></summary>'
             f'{table(["特征", "值"], feat_rows)}</details>')
    body += "</div>"
    if opps:
        body += "<h2>历史机会</h2>" + table(
            ["日期", "规则", "状态", "评分", "收益"],
            [[e(o["asof"]), e(o["primary_detector"]), e(telegram.STATUS_LABELS.get(o["status"], o["status"])),
              f"{o['score']:.2f}", signed(o.get("return_pct"))] for o in opps],
        )
    return layout(t, "", body, head=views.CHART_HEAD)


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
