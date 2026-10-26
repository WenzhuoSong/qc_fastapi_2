"""End-of-day and weekly pipelines."""
from __future__ import annotations

import logging
from datetime import date, datetime, timedelta

from scout import bars, base_rates, context, db, opportunities, sectors, structure, telegram, thesis, tracker, universe
from scout.config import get_scout_settings
from scout.data.provider import get_provider
from scout.features import SNAPSHOT_FEATURES, FeaturePanel, build_panel
from scout.market_calendar import previous_us_equity_trading_day, us_equity_market_status

logger = logging.getLogger("scout.pipeline")

PANEL_LOOKBACK_DAYS = 420          # ≥ 252 sessions + 200-day SMA warm-up headroom
MAX_PER_DETECTOR = 3               # keep one rule (e.g. a whole sector turning) from flooding the cards


async def prepare(full_backfill: bool = False) -> dict:
    """Migrations, universe and bars. Idempotent."""
    settings = get_scout_settings()
    out: dict = {"migrations": await db.apply_migrations()}
    out["watchlist_seeded"] = await universe.seed_watchlist()
    count = await db.fetch_one("SELECT count(*) AS n FROM scout.instruments WHERE kind = 'stock'")
    if full_backfill or not count or count["n"] < 400:
        out["instruments"] = await universe.refresh_instruments()
    await universe.ensure_watchlist_instruments()
    tickers = [r["ticker"] for r in await universe.load_scan_instruments()]
    provider = get_provider()
    if full_backfill:
        out["bars"] = await bars.backfill(provider, tickers, settings.scout_history_years)
    else:
        out["bars"] = await bars.refresh(provider, tickers, settings.scout_history_years)
    return out


async def load_panel(lookback_days: int | None = PANEL_LOOKBACK_DAYS) -> FeaturePanel:
    settings = get_scout_settings()
    instruments = {r["ticker"]: r for r in await universe.load_scan_instruments()}
    since = date.today() - timedelta(days=lookback_days) if lookback_days else None
    frames = await bars.load(list(instruments), since=since)
    return build_panel(frames, instruments, settings.scout_min_price, settings.scout_min_dollar_volume)


STRUCTURE_FIELDS = ("swing_trend", "swing_break", "support", "dist_support", "resistance", "dist_resistance", "updown_vol_20")


def panel_bars(p: FeaturePanel, ticker: str, asof: date, n: int = structure.LOOKBACK) -> list[dict]:
    """One ticker's last `n` sessions up to `asof`, as bar dicts (for scout.structure)."""
    end = p.dates.get_loc(asof) + 1
    cols = {k: p[k][ticker].to_numpy()[max(0, end - n):end] for k in ("open", "high", "low", "close", "volume")}
    dates = p.dates[max(0, end - n):end]
    return [
        {"date": d, **{k: float(cols[k][i]) for k in cols}}
        for i, d in enumerate(dates) if cols["close"][i] == cols["close"][i]   # skip NaN
    ]


async def store_features(p: FeaturePanel, asof: date) -> int:
    """Daily snapshot: SNAPSHOT_FEATURES plus price structure (support/resistance, swings)."""
    def rows():
        for ticker in p["close"].columns:
            payload = {k: round(v, 6) for k, v in p.row(ticker, asof, SNAPSHOT_FEATURES).items()}
            if "close" not in payload:
                continue
            s = structure.compute(panel_bars(p, ticker, asof))
            payload.update({k: (round(s[k], 6) if isinstance(s[k], float) else s[k]) for k in STRUCTURE_FIELDS if k in s})
            yield {"t": ticker, "d": asof, "p": db.jsonb(payload)}

    return await db.execute_many(
        "INSERT INTO scout.features_daily (ticker, date, payload) VALUES (:t, :d, CAST(:p AS JSONB)) "
        "ON CONFLICT (ticker, date) DO UPDATE SET payload = EXCLUDED.payload",
        rows(),
    )


async def ensure_base_rates(force: bool = False) -> dict:
    rates = {} if force else await base_rates.load()
    if rates:
        return rates
    logger.info("computing base rates from full history")
    full = await load_panel(lookback_days=None)
    rates = base_rates.compute(full)
    await base_rates.store(rates)
    return rates


async def store_signal_hits(p: FeaturePanel, rates: dict, asof: date) -> int:
    """Every hit of every rule today (live and candidate), with its explanation —
    the strategy screener shows these; the opportunity board keeps the Top N."""
    from scout.detectors import registry

    rows = []
    for det in registry(include_candidates=True):
        for cand in opportunities.scan(p, rates, asof, detectors=[det]):
            rows.append({"d": asof, "det": det.name, "t": cand.ticker, "s": cand.score, "p": db.jsonb({
                "why": opportunities.why_block(cand),
                "levels": {"entry_lo": cand.levels.entry_lo, "entry_hi": cand.levels.entry_hi, "stop": cand.levels.stop,
                           "target1": cand.levels.target1},
                "live": det.live,
            })})
    await db.execute("DELETE FROM scout.signal_hits WHERE asof = :d", {"d": asof})
    return await db.execute_many(
        "INSERT INTO scout.signal_hits (asof, detector, ticker, score, payload) VALUES (:d, :det, :t, :s, CAST(:p AS JSONB))",
        rows,
    )


def select_top(candidates: list, n: int, per_detector: int = MAX_PER_DETECTOR) -> list:
    """Highest scores first, at most `per_detector` cards from any one rule."""
    picked, counts = [], {}
    for c in candidates:
        if counts.get(c.primary.name, 0) >= per_detector:
            continue
        picked.append(c)
        counts[c.primary.name] = counts.get(c.primary.name, 0) + 1
        if len(picked) >= n:
            break
    return picked


def expected_session(now: datetime | None = None) -> tuple[date | None, str]:
    """The session whose close an EOD scan covers; None on weekends/holidays."""
    status = us_equity_market_status(now)
    today = date.fromisoformat(status.market_date)
    if not status.is_trading_day:
        return None, status.reason
    if status.reason == "after_close":
        return today, "after_close"
    return previous_us_equity_trading_day(today), "before_close"


def days_to_next_earnings(earnings: dict[str, list[dict]], asof: date) -> dict[str, int]:
    out = {}
    for t, events in earnings.items():
        nxt = [e["date"] for e in events if e["date"] >= asof]
        if nxt:
            out[t] = (nxt[0] - asof).days
    return out


async def run_eod(push: bool = True, force: bool = False) -> dict:
    """After-close scan. Skips market holidays; a session is scanned and pushed
    once — `force` rescans (e.g. to regenerate cards) without the guards."""
    settings = get_scout_settings()
    session, reason = expected_session()
    if session is None:
        if not force:
            return {"skipped": reason}
        session = previous_us_equity_trading_day(date.today())
    summary: dict = {"session": session.isoformat(), "prepare": await prepare()}
    p = await load_panel()
    asof: date = p.dates[-1]
    summary["asof"] = asof.isoformat()
    if asof < session:
        raise RuntimeError(f"bar for {session} not available yet (latest {asof}); data source may be lagging")
    summary["features"] = await store_features(p, asof)
    sector_rows = sectors.compute(p, asof)
    summary["sectors"] = await sectors.store(asof, sector_rows)
    sector_ctx = {r["sector"]: sectors.card_context(r) for r in sector_rows if r["kind"] == "sector"}

    transitions = await tracker.update_all()
    summary["transitions"] = [t.__dict__ for t in transitions]
    already = await db.fetch_one(
        "SELECT 1 AS x FROM scout.briefs WHERE kind = 'eod' AND asof = :d LIMIT 1", {"d": asof}
    )
    if already and not force:
        summary["skipped_scan"] = "session already scanned"
        return summary

    summary["earnings_rows"] = await context.refresh_earnings()
    earnings = await context.earnings_map(asof)
    rates = await ensure_base_rates()
    watch = {r["ticker"]: r["theme"] for r in await universe.load_watchlist()}

    candidates = opportunities.scan(
        p, rates, asof,
        recent=await opportunities.recent_keys(asof),
        watchlist=watch,
        feedback_weights=await opportunities.feedback_weights(),
        days_to_earnings=days_to_next_earnings(earnings, asof),
    )
    summary["candidates"] = len(candidates)
    summary["signal_hits"] = await store_signal_hits(p, rates, asof)
    market = opportunities.market_context(p, asof)
    saved = []
    for c in select_top(candidates, settings.scout_top_n):
        c.context = {
            "news": await context.load_news(c.ticker),
            "earnings": context.earnings_summary(earnings.get(c.ticker, []), asof),
            "sector_context": sector_ctx.get(c.sector),
        }
        ev = opportunities.evidence(c, market)
        th, source = await thesis.write_thesis(ev)
        await opportunities.save(c, ev, th.model_dump(), source)
        saved.append(c.id)
    summary["saved"] = saved

    focus = None
    try:                               # an LLM hiccup must not fail the scan
        from scout import market_focus

        focus = await market_focus.generate()
        summary["market_focus"] = focus["source"] if focus else None
    except Exception as exc:
        logger.warning("market focus failed: %s", exc)
        summary["market_focus"] = f"failed: {type(exc).__name__}"

    brief = render_eod_brief(asof, market, candidates, transitions, saved, sector_rows, focus)
    await db.execute(
        "INSERT INTO scout.briefs (kind, asof, content, payload) VALUES ('eod', :d, :c, CAST(:p AS JSONB))",
        {"d": asof, "c": brief, "p": db.jsonb({"market": market, "saved": saved, "candidates": len(candidates)})},
    )
    if push:
        await telegram.send(brief)
        pushed = 0
        for opp_id in saved:
            o = await telegram.load_opportunity(opp_id)
            if o and o["score"] >= settings.scout_push_threshold and pushed < settings.scout_max_push_cards:
                if await telegram.push_card(o):
                    pushed += 1
        summary["pushed"] = pushed
    return summary


def render_eod_brief(asof: date, market: dict, candidates, transitions, saved: list[str],
                     sector_rows: list[dict] | None = None, focus: dict | None = None) -> str:
    pct = telegram.pct
    lines = [
        f"🌇 <b>Scout 收盘扫描 {asof}</b>",
        f"大盘：{telegram.esc(opportunities.state_text(market, detail=True))}\nSPY 当日 {pct(market.get('spy_ret_1d'), 1, True)}，"
        f"20 日 {pct(market.get('spy_ret_20d'), 1, True)} | QQQ 20 日 {pct(market.get('qqq_ret_20d'), 1, True)} | "
        f"SMH 20 日 {pct(market.get('smh_ret_20d'), 1, True)}",
    ]
    rotation = sectors.rotation_line(sector_rows or [])
    if rotation:
        lines.append(rotation)
    elif market.get("sector_leaders_20d"):
        lines.append("领涨板块(20日)：" + "，".join(f"{k} {pct(v, 1, True)}" for k, v in market["sector_leaders_20d"]))
        lines.append("落后板块(20日)：" + "，".join(f"{k} {pct(v, 1, True)}" for k, v in market["sector_laggards_20d"]))
    if focus and focus.get("focus"):
        f = focus["focus"]
        lines.append(f"\n🧭 <b>市场关注点</b>：{telegram.esc(f['headline'])}")
        lines += [f"{i}. {telegram.esc(t['title'])}" for i, t in enumerate(f.get("themes", []), 1)]
        lines.append("（/sectors 查看板块详情）")
    if transitions:
        icon = {"triggered": "▶️", "hit_target": "🎯", "stopped": "🛑", "trailed": "🏁", "expired": "⌛", "expired_untriggered": "⌛"}
        lines.append("\n<b>机会状态变化</b>")
        for t in transitions:
            ret = f" {pct(t.return_pct, 1, True)}" if t.return_pct is not None else ""
            lines.append(f"{icon.get(t.new, '•')} {t.ticker} → {telegram.STATUS_LABELS.get(t.new, t.new)}{ret}")
    if candidates:
        by_det: dict[str, int] = {}
        for c in candidates:
            by_det[c.primary.label] = by_det.get(c.primary.label, 0) + 1
        lines.append(f"\n今日触发 {len(candidates)} 个候选（{'，'.join(f'{k} {v}' for k, v in by_det.items())}），"
                     f"入选 {len(saved)} 张卡片：")
        chosen = set(saved)
        for c in candidates:
            if c.id in chosen:
                lines.append(f"• <b>{c.ticker}</b> {c.primary.label} {c.score:.2f}")
    else:
        lines.append("\n今日没有规则触发。")
    return "\n".join(lines)


async def run_weekly(push: bool = True) -> dict:
    summary: dict = {"prepare": await prepare(full_backfill=True)}
    rates = await ensure_base_rates(force=True)
    summary["base_rates"] = {d: b.get("all", {}).get("n", 0) for d, b in rates.items()}
    report = await render_weekly_report()
    await db.execute(
        "INSERT INTO scout.briefs (kind, asof, content) VALUES ('weekly', :d, :c)", {"d": date.today(), "c": report}
    )
    if push:
        await telegram.send(report)
    return summary


async def render_weekly_report() -> str:
    since = date.today() - timedelta(days=7)
    closed = await db.fetch_all(
        "SELECT o.ticker, o.primary_detector, oc.outcome, oc.return_pct, oc.excess_pct "
        "FROM scout.opportunity_outcomes oc JOIN scout.opportunities o ON o.id = oc.opportunity_id "
        "WHERE oc.closed_on >= :d ORDER BY oc.closed_on", {"d": since},
    )
    new = await db.fetch_one("SELECT count(*) AS n FROM scout.opportunities WHERE asof >= :d", {"d": since})
    pct = telegram.pct
    lines = [f"🗓 <b>Scout 周报 {date.today()}</b>", f"本周新机会 {new['n'] if new else 0} 个，结束 {len(closed)} 个。"]
    for r in closed:
        lines.append(
            f"• {r['ticker']} {r['primary_detector']} → {telegram.STATUS_LABELS.get(r['outcome'], r['outcome'])} "
            f"{pct(r['return_pct'], 1, True)}（超额 {pct(r['excess_pct'], 1, True)}）"
        )
    _, sector_rows = await sectors.latest("sector")
    rotation = sectors.rotation_line(sector_rows)
    if rotation:
        lines.append(f"\n本周{rotation}")
    health = base_rates.rule_health(await base_rates.load())
    if health:
        lines.append("\n<b>规则体检</b>\n" + "\n".join(telegram.esc(x) for x in health))
    lines.append("")
    lines.append(await telegram.stats_text())
    return "\n".join(lines)

