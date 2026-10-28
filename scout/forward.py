"""Forward test (v3 P3, gate G3): the rules that passed the backtest, recorded from now on.

After every close, each knowledge entry under test (a rule spec in status `backtested` or
`forward`) gets:
- its rule's signals of that session, with the levels the rule gives that day;
- for each signal, up to `gate.MAX_TRIES` selection-control legs: random other stocks of the
  same session with the same level formula, drawn with a fixed seed per (entry, date, stock).
  As in the backtest, the control is the mean of the first `gate.DRAWS` legs that fill.
Every row is resolved with `scout.simulate`, the replay's simulator; nothing stored is
recomputed or tuned later. An entry moves to `forward` when its record starts.

Ranking strategies (`spec.type: rank`) are recorded in `scout.forward_ranks` instead: on the
record's first session and then every `every` sessions, the ranker's picks and every other
eligible stock of that day, each held from the next open to the close `hold` sessions later
(the backtest's rule). A pick's control is the mean of every other stock of its day.

Report: per signal, its return minus its control; mean and t on monthly means. The entry is
ready for the user's decision when the record spans ≥ 6 months, has ≥ 30 complete signals,
the edge is positive, and it is not significantly below the backtest (t of forward − backtest
> −2). Only the user confirms (`/knowledge`).
"""
from __future__ import annotations

import logging
import zlib
from datetime import date

import numpy as np
import pandas as pd

from scout import bars as bar_store
from scout import base_rates, db, gate, learned
from scout.detectors import Levels, registry
from scout.simulate import Bar, simulate
from scout.universe import BENCHMARK

logger = logging.getLogger("scout.forward")

MIN_T_VS_BACKTEST = -2.0
ACTIVE = ("pending", "open")
FILLED = ("open", "hit_target", "stopped", "trailed", "expired")


def under_test(entries: list[dict]) -> list[dict]:
    """Rule entries with a spec that passed the backtest and are not yet decided."""
    return [en for en in entries if en.get("spec") and en["status"] in ("backtested", "forward")
            and en["spec"].get("primary_control") == "select"]


def leg_rng(key: str, asof: date, ticker: str) -> np.random.Generator:
    return np.random.default_rng([gate.SEED, zlib.crc32(key.encode()), asof.toordinal(), zlib.crc32(ticker.encode())])


def _levels(det, p, ticker: str, asof: date) -> Levels | None:
    try:
        lv = det.levels(p.row(ticker, asof))
    except KeyError:
        return None
    return lv if lv.valid() else None


def _row(key: str, det, role: str, ticker: str, leg: str, draw: int, asof: date, lv: Levels) -> dict:
    return {"key": key, "det": det.name, "role": role, "t": ticker, "leg": leg, "draw": draw, "d": asof,
            "lo": lv.entry_lo, "hi": lv.entry_hi, "stop": lv.stop, "t1": lv.target1, "t2": lv.target2}


def new_rows(p, entry: dict, asof: date, recent: set[str]) -> list[dict]:
    """Today's signals of the entry's rule and their control legs. `recent` = tickers with a
    signal of this entry inside the replay's cooldown (one sample per stock per 10 sessions)."""
    det = next(d for d in registry(include_candidates=True) if d.name == entry["spec"]["detector"])
    mask = det.mask(p)
    if asof not in mask.index:
        return []
    today = mask.loc[asof].fillna(False)
    m = gate.Market(p)
    i = p.dates.get_loc(asof)
    spec = entry["spec"]
    pool = [m.stocks[k] for k in np.flatnonzero(m.valid[i])]
    rows = []
    for ticker in sorted(today[today].index):
        if ticker in recent or ticker not in m.col or not gate.applies(spec, m.regime[i], m.vol[i], m.sector.get(ticker)):
            continue
        lv = _levels(det, p, ticker, asof)
        if not lv:
            continue
        rows.append(_row(entry["key"], det, "signal", ticker, ticker, 0, asof, lv))
        rng, others, draw = leg_rng(entry["key"], asof, ticker), [t for t in pool if t != ticker], 0
        for _ in range(gate.MAX_TRIES):
            if not others:
                break
            leg = others[int(rng.integers(len(others)))]
            c = _levels(det, p, leg, asof)
            if c:
                draw += 1
                rows.append(_row(entry["key"], det, "select", ticker, leg, draw, asof, c))
    return rows


async def record(p, asof: date) -> dict:
    """Store today's forward rows for every entry under test; start the record if needed."""
    entries = under_test(await learned.current())
    out = {}
    cooldown_from = p.dates[max(0, p.dates.get_loc(asof) - base_rates.COOLDOWN_SESSIONS + 1)]
    for en in entries:
        if en["status"] == "backtested":
            note = "每次调仓记录选中的股票和全部候选股" if is_rank(en) else "每日记录信号和选股对照"
            en = await learned.move(en["key"], "forward", "system", f"开始前向验证（{asof} 收盘起{note}）")
        try:
            out[en["key"]] = await (record_rank(p, en, asof) if is_rank(en) else record_signals(p, en, asof, cooldown_from))
        except Exception as exc:          # one entry must not stop the others' records
            logger.warning("forward record %s failed: %r", en["key"], exc)
            out[en["key"]] = f"failed: {type(exc).__name__}"
    return out


async def record_signals(p, en: dict, asof: date, cooldown_from: date) -> int:
    """Today's signals of a detector rule and their control legs; returns the signals stored."""
    recent = {r["ticker"] for r in await db.fetch_all(
        "SELECT DISTINCT ticker FROM scout.forward_trades WHERE key = :k AND role = 'signal' AND asof >= :d AND asof < :t",
        {"k": en["key"], "d": cooldown_from, "t": asof})}
    rows = new_rows(p, en, asof, recent)
    if rows:
        await db.execute_many(
            "INSERT INTO scout.forward_trades (key, detector, role, ticker, leg_ticker, draw, asof, entry_lo, entry_hi, stop, "
            "target1, target2) VALUES (:key, :det, :role, :t, :leg, :draw, :d, :lo, :hi, :stop, :t1, :t2) "
            "ON CONFLICT (key, role, ticker, asof, draw) DO NOTHING", rows)
    return sum(r["role"] == "signal" for r in rows)


def is_rank(entry: dict) -> bool:
    return (entry.get("spec") or {}).get("type") == "rank"


def sessions_since(p, last: date, asof: date) -> int:
    ts = pd.to_datetime(p.dates)
    return int(((ts > pd.Timestamp(last)) & (ts <= pd.Timestamp(asof))).sum())


async def rank_rows(p, entry: dict, asof: date) -> list[dict]:
    """The ranker's picks and the rest of the eligible pool after the close of `asof`."""
    from scout import rankers

    spec = entry["spec"]
    a, stocks = rankers.arrays(p)
    i = p.dates.get_loc(asof)
    a["angle"] = await rankers.prepare(spec, p, stocks, days=[i])
    pool = rankers._universe(a, i)
    picks = set(rankers.RANKERS[spec["ranker"]](a, i, float(spec.get("top_frac", 0.1))))
    regime, vol = context(p, i)
    return [{"key": entry["key"], "d": asof, "t": stocks[j], "role": "pick" if j in picks else "pool", "hold": int(spec["hold"]),
             "counted": j not in picks or gate.applies(spec, regime, vol, (p.sectors or {}).get(stocks[j]))}
            for j in pool.tolist()]


def context(p, i: int) -> tuple[str, str]:
    """Market regime and volatility state after the close of day i, as the backtest sees them."""
    return str(p.regime.reindex(p.dates).iloc[i]), str(gate.vol_states(p["close"][BENCHMARK].to_numpy())[i])


async def record_rank(p, entry: dict, asof: date) -> int:
    """Store a rebalance on the record's first session and every `every` sessions after; returns the picks stored."""
    if asof not in p.dates:
        return 0
    row = await db.fetch_one("SELECT max(asof) AS d FROM scout.forward_ranks WHERE key = :k", {"k": entry["key"]})
    last = (row or {}).get("d")
    if last is not None and sessions_since(p, last, asof) < int(entry["spec"]["every"]):
        return 0
    rows = await rank_rows(p, entry, asof)
    if rows:
        await db.execute_many(
            "INSERT INTO scout.forward_ranks (key, asof, ticker, role, hold, counted) VALUES (:key, :d, :t, :role, :hold, :counted) "
            "ON CONFLICT (key, asof, ticker) DO NOTHING", rows)
    return sum(r["role"] == "pick" and r["counted"] for r in rows)


def resolve_rank(p, r: dict) -> dict | None:
    """Next open → close `hold` sessions after the rebalance, from one price series; None while running."""
    ts = pd.to_datetime(p.dates)
    if pd.Timestamp(r["asof"]) < ts[0]:
        return None                                   # older than the panel: wait for a longer one
    after = np.flatnonzero(ts > pd.Timestamp(r["asof"]))
    if len(after) < r["hold"]:
        return None
    a, b = after[0], after[r["hold"] - 1]
    o, c, spy_o, spy_c = (p[f][col].iloc[k] if col in p[f].columns else np.nan
                          for f, col, k in (("open", r["ticker"], a), ("close", r["ticker"], b), ("open", BENCHMARK, a), ("close", BENCHMARK, b)))
    out = {"id": r["id"], "ed": p.dates[a], "xd": p.dates[b], "ret": None, "spy": None, "s": "missing"}
    if np.isfinite(o) and np.isfinite(c) and o > 0:
        out.update(ret=float(c / o - 1), s="closed")
    if np.isfinite(spy_o) and np.isfinite(spy_c) and spy_o > 0:
        out["spy"] = float(spy_c / spy_o - 1)
    return out


async def update_ranks(p) -> int:
    rows = await db.fetch_all("SELECT id, asof, ticker, hold FROM scout.forward_ranks WHERE state = 'pending'")
    done = [x for x in (resolve_rank(p, r) for r in rows) if x]
    if done:
        await db.execute_many("UPDATE scout.forward_ranks SET state = :s, entry_date = :ed, exit_date = :xd, ret = :ret, "
                              "spy_ret = :spy WHERE id = :id", done)
    return len(done)


def rank_pairs(rows: list[dict]) -> list[dict]:
    """Complete picks: closed, with the mean of every other closed stock of the same rebalance as control."""
    by_day: dict[date, list[dict]] = {}
    pending = {r["asof"] for r in rows if r["state"] == "pending"}
    for r in rows:
        if r["state"] == "closed" and r["ret"] is not None:
            by_day.setdefault(r["asof"], []).append(r)
    out = []
    for d, rs in by_day.items():
        if d in pending:
            continue
        total, n = sum(r["ret"] for r in rs), len(rs)
        for r in rs:
            if r["role"] == "pick" and r.get("counted", True) and n > 1:
                control = (total - r["ret"]) / (n - 1)
                out.append({"date": d, "ticker": r["ticker"], "ret": r["ret"], "select": control, "diff": r["ret"] - control,
                            "spy": r["spy_ret"]})
    return out


def _bars_after(frame: pd.DataFrame | None, after: date) -> list[Bar]:
    if frame is None or frame.empty:
        return []
    sub = frame[frame.index > after]
    return [Bar(d, r["open"], r["high"], r["low"], r["close"]) for d, r in sub.iterrows()]


async def update() -> int:
    """Resolve open rows with the bars stored so far."""
    rows = await db.fetch_all("SELECT * FROM scout.forward_trades WHERE state = ANY(:s)", {"s": list(ACTIVE)})
    if not rows:
        return 0
    dets = {d.name: d for d in registry(include_candidates=True)}
    frames = await bar_store.load(sorted({r["leg_ticker"] for r in rows} | {BENCHMARK}), since=min(r["asof"] for r in rows))
    spy = frames.get(BENCHMARK)
    changed = 0
    for r in rows:
        det = dets.get(r["detector"])
        if det is None:
            continue
        lv = Levels(r["entry_lo"], r["entry_hi"], r["stop"], r["target1"], r["target2"] or r["target1"])
        res = simulate(_bars_after(frames.get(r["leg_ticker"]), r["asof"]), lv, det.horizon_days, det.trigger_window, det.exit)
        if res.state == r["state"] and res.entry_date == r["entry_date"]:
            continue
        spy_ret = None
        if res.closed and res.entry_date and res.exit_date and spy is not None \
                and res.entry_date in spy.index and res.exit_date in spy.index:
            spy_ret = float(spy.at[res.exit_date, "close"] / spy.at[res.entry_date, "open"] - 1)
        await db.execute(
            "UPDATE scout.forward_trades SET state = :s, entry_date = :ed, entry_price = :ep, exit_date = :xd, exit_price = :xp, "
            "ret = :ret, spy_ret = :spy, updated_at = now() WHERE id = :id",
            {"s": res.state, "ed": res.entry_date, "ep": res.entry_price, "xd": res.exit_date if res.closed else None,
             "xp": res.exit_price if res.closed else None, "ret": res.return_pct if res.closed else None, "spy": spy_ret,
             "id": r["id"]})
        changed += 1
    return changed


# ── report ──────────────────────────────────────────────────────────────

def pairs(rows: list[dict]) -> list[dict]:
    """Complete signals: the signal closed with a fill, and its control known (every leg past its
    trigger window, the first DRAWS filled legs closed)."""
    legs: dict[tuple, list[dict]] = {}
    signals = []
    for r in rows:
        if r["role"] == "signal":
            signals.append(r)
        else:
            legs.setdefault((r["ticker"], r["asof"]), []).append(r)
    out = []
    for s in signals:
        if s["state"] not in FILLED or s["state"] == "open" or s["ret"] is None:
            continue
        ls = sorted(legs.get((s["ticker"], s["asof"]), []), key=lambda x: x["draw"])
        if any(x["state"] == "pending" for x in ls):
            continue
        filled = [x for x in ls if x["state"] in FILLED][: gate.DRAWS]
        if not filled or any(x["ret"] is None for x in filled):
            continue
        control = float(np.mean([x["ret"] for x in filled]))
        out.append({"date": s["asof"], "ticker": s["ticker"], "ret": s["ret"], "select": control, "diff": s["ret"] - control,
                    "spy": s["spy_ret"]})
    return out


def months_between(a: date, b: date) -> float:
    return (b - a).days / 30.44


def evaluate(complete: list[dict], started: date | None, today: date, backtest_edge: float | None) -> dict:
    mean, t, months = gate.monthly_t([c["date"] for c in complete], [c["diff"] for c in complete])
    t_bt = None
    if backtest_edge is not None and complete:
        _, t_bt, _ = gate.monthly_t([c["date"] for c in complete], [c["diff"] - backtest_edge for c in complete])
    span = months_between(started, today) if started else 0.0
    crit = [
        {"name": "记录时长", "value": f"{span:.1f} 个月", "bar": f"≥ {gate.FORWARD_MIN_MONTHS} 个月", "ok": span >= gate.FORWARD_MIN_MONTHS},
        {"name": "完成的信号", "value": len(complete), "bar": f"≥ {gate.FORWARD_MIN_SAMPLES}", "ok": len(complete) >= gate.FORWARD_MIN_SAMPLES},
        {"name": "相对选股对照的超额", "value": None if mean is None else round(mean, 5), "bar": "> 0", "ok": (mean or 0) > 0},
        {"name": "不显著低于回测", "value": None if t_bt is None else round(t_bt, 2), "bar": f"t > {MIN_T_VS_BACKTEST:.0f}",
         "ok": t_bt is not None and t_bt > MIN_T_VS_BACKTEST},
    ]
    return {"started": str(started) if started else None, "months": round(span, 1), "n": len(complete), "edge": mean, "t": t,
            "signal_months": months, "backtest_edge": backtest_edge, "t_vs_backtest": t_bt, "criteria": crit,
            "ready": all(c["ok"] for c in crit)}


async def started_on(key: str, table: str = "forward_trades") -> date | None:
    row = await db.fetch_one(f"SELECT min(asof) AS d FROM scout.{table} WHERE key = :k", {"k": key})
    return (row or {}).get("d")


async def backtest_edge(entry: dict) -> float | None:
    row = await db.fetch_one("SELECT backtest FROM scout.gate_runs WHERE key = :k AND passed ORDER BY id DESC LIMIT 1",
                             {"k": entry["key"]})
    k = (entry.get("spec") or {}).get("primary_control", "select")
    return (((row or {}).get("backtest") or {}).get("summary") or {}).get(f"vs_{k}", {}).get("edge")


async def report(entry: dict, today: date) -> dict:
    if is_rank(entry):
        rows = await db.fetch_all("SELECT * FROM scout.forward_ranks WHERE key = :k", {"k": entry["key"]})
        out = evaluate(rank_pairs(rows), await started_on(entry["key"], "forward_ranks"), today, await backtest_edge(entry))
        out["signals"] = sum(r["role"] == "pick" and r.get("counted", True) for r in rows)
        out["open_signals"] = sum(r["role"] == "pick" and r.get("counted", True) and r["state"] == "pending" for r in rows)
        return out
    rows = await db.fetch_all("SELECT * FROM scout.forward_trades WHERE key = :k", {"k": entry["key"]})
    out = evaluate(pairs(rows), await started_on(entry["key"]), today, await backtest_edge(entry))
    out["signals"] = sum(r["role"] == "signal" for r in rows)
    out["open_signals"] = sum(r["role"] == "signal" and r["state"] in ACTIVE for r in rows)
    return out
