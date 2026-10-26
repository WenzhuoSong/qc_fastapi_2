"""Scan → rank → opportunity records.

Everything numeric on a card is computed here in Python. The LLM only writes
prose around this evidence (see scout.thesis).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date, timedelta

import numpy as np

from scout import base_rates, db, knowledge
from scout.detectors import Detector, Levels, registry
from scout.features import SNAPSHOT_FEATURES, FeaturePanel
from scout.universe import BENCHMARK, SECTOR_ETFS

logger = logging.getLogger("scout.opportunities")

COOLDOWN_DAYS = 14                 # calendar days before the same ticker × detector re-fires
EARNINGS_PENALTY_DAYS = 5
EARNINGS_PENALTY = 0.15
WATCHLIST_BONUS = 0.05
EVENT_DETECTORS = {"power_gap"}    # detectors that are themselves event-driven


@dataclass
class Candidate:
    ticker: str
    asof: date
    regime: str
    primary: Detector
    detectors: list[str]
    levels: Levels
    features: dict[str, float]
    base_rate: dict
    score: float
    score_parts: dict[str, float]
    sector: str | None = None
    why: list[dict] = field(default_factory=list)          # primary rule's conditions with actual values
    position_rate: dict | None = None                      # same rule, same distance-from-high bucket
    theme: str | None = None
    context: dict = field(default_factory=dict)

    @property
    def id(self) -> str:
        return f"{self.asof:%Y%m%d}-{self.ticker}-{self.primary.name}"


def _clip(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    return float(min(max(x, lo), hi))


def score_candidate(
    det: Detector, detectors: list[str], levels: Levels, feat: dict, stats: dict,
    *, feedback_weight: float = 1.0, in_watchlist: bool = False, days_to_earnings: int | None = None,
) -> tuple[float, dict[str, float]]:
    parts = {
        "edge": _clip(0.5 + base_rates.shrunk_edge(stats) / 0.04),
        "rs": _clip(feat.get("rs_pct", 0.5)),
        "confluence": _clip((len(detectors) - 1) * 0.5),
        "risk_reward": _clip((levels.risk_reward - 1.0) / 2.0),
    }
    score = 0.45 * parts["edge"] + 0.25 * parts["rs"] + 0.15 * parts["confluence"] + 0.15 * parts["risk_reward"]
    if det.kind == "watch":
        score *= 0.9
    if (
        days_to_earnings is not None
        and 0 <= days_to_earnings <= EARNINGS_PENALTY_DAYS
        and det.name not in EVENT_DETECTORS
    ):
        score -= EARNINGS_PENALTY
        parts["earnings_penalty"] = -EARNINGS_PENALTY
    if in_watchlist:
        score += WATCHLIST_BONUS
        parts["watchlist_bonus"] = WATCHLIST_BONUS
    score *= feedback_weight
    parts["feedback_weight"] = feedback_weight
    return round(_clip(score), 4), parts


def scan(
    p: FeaturePanel,
    rates: dict,
    asof: date | None = None,
    *,
    recent: set[tuple[str, str]] | None = None,
    watchlist: dict[str, str] | None = None,
    feedback_weights: dict[str, float] | None = None,
    days_to_earnings: dict[str, int] | None = None,
    detectors: list[Detector] | None = None,
) -> list[Candidate]:
    asof = asof or p.dates[-1]
    regime = str(p.regime.loc[asof])
    recent = recent or set()
    watchlist = watchlist or {}
    feedback_weights = feedback_weights or {}
    days_to_earnings = days_to_earnings or {}

    hits: dict[str, list[Detector]] = {}
    for det in detectors or registry(include_candidates=False):
        row = det.mask(p).loc[asof]
        for ticker in row.index[row.fillna(False).to_numpy(dtype=bool)]:
            if (ticker, det.name) not in recent:
                hits.setdefault(ticker, []).append(det)

    out: list[Candidate] = []
    for ticker, dets in hits.items():
        full_row = p.row(ticker, asof)
        dist = full_row.get("dist_52w_high")
        stats_by_det = {d.name: base_rates.pick(rates, d.name, regime) for d in dets}
        pos_by_det = {d.name: base_rates.pick_position(rates, d.name, dist) for d in dets}
        # The edge used for ranking: same rule at the same distance from the high
        # when that bucket has enough history, else the rule in this market regime.
        edge_by_det = {n: pos_by_det[n] or stats_by_det[n] for n in stats_by_det}
        # Primary = actionable setup with the best measured edge.
        primary = max(dets, key=lambda d: (d.kind == "setup", base_rates.shrunk_edge(edge_by_det[d.name])))
        levels = primary.levels(full_row)
        if not levels.valid():
            continue
        feat = {k: full_row[k] for k in SNAPSHOT_FEATURES if k in full_row}
        names = [d.name for d in dets]
        score, parts = score_candidate(
            primary, names, levels, feat, edge_by_det[primary.name],
            feedback_weight=feedback_weights.get(primary.name, 1.0),
            in_watchlist=ticker in watchlist,
            days_to_earnings=days_to_earnings.get(ticker),
        )
        out.append(Candidate(
            ticker=ticker, asof=asof, regime=regime, primary=primary, detectors=names,
            levels=levels, features=feat, base_rate=stats_by_det[primary.name],
            score=score, score_parts=parts, sector=p.sectors.get(ticker), theme=watchlist.get(ticker),
            why=primary.explain(full_row), position_rate=pos_by_det[primary.name],
        ))
    out.sort(key=lambda c: c.score, reverse=True)
    return out


def market_context(p: FeaturePanel, asof: date) -> dict:
    c = p["close"]
    def ret(t: str, n: int) -> float | None:
        frame = p.f.get(f"ret_{n}d")
        if frame is None or t not in frame.columns:
            return None
        v = frame.at[asof, t]
        return None if np.isnan(v) else round(float(v), 4)

    sectors = {
        etf: ret(etf, 20) for etf in SECTOR_ETFS.values() if etf in c.columns
    }
    ranked = sorted(((k, v) for k, v in sectors.items() if v is not None), key=lambda kv: kv[1], reverse=True)
    return {
        "date": asof.isoformat(),
        "regime": str(p.regime.loc[asof]),
        "spy_ret_1d": ret(BENCHMARK, 1),
        "spy_ret_20d": ret(BENCHMARK, 20),
        "qqq_ret_20d": ret("QQQ", 20),
        "iwm_ret_20d": ret("IWM", 20),
        "smh_ret_20d": ret("SMH", 20),
        "sector_leaders_20d": ranked[:3],
        "sector_laggards_20d": ranked[-3:][::-1],
    }


def evidence(c: Candidate, market: dict) -> dict:
    f = c.features
    lv = c.levels
    return {
        "ticker": c.ticker,
        "asof": c.asof.isoformat(),
        "sector": c.sector,
        "theme": c.theme,
        "setup": {"primary": c.primary.name, "label": c.primary.label, "rule": c.primary.description,
                  "all_detectors": c.detectors, "horizon_days": c.primary.horizon_days},
        "levels": {"entry_lo": lv.entry_lo, "entry_hi": lv.entry_hi, "stop": lv.stop,
                   "target1": lv.target1, "target2": lv.target2, "risk_reward": round(lv.risk_reward, 2),
                   "stop_distance_pct": round(lv.stop / lv.entry_mid - 1, 4)},
        "price": {k: round(f[k], 4) for k in (
            "close", "ret_1d", "ret_5d", "ret_20d", "ret_63d", "ret_252d", "dist_52w_high",
            "vol_ratio", "atr_pct", "rsi14", "rsi2", "gap_pct", "rs_pct", "sector_rs_pct", "sector_rel_20",
            "beta_60", "corr_60", "market_ret_1d", "idio_ret_1d", "idio_ret_5d", "idio_ret_20d",
            "zscore_20", "bb_pct", "vol_20", "max_dd_60",
        ) if k in f},
        "readings": {knowledge.zh(k): r for k in f if (r := knowledge.reading(k, f[k]))},
        "base_rate": c.base_rate,
        "why": why_block(c),
        "market": market,
        **c.context,
    }


SCORE_WEIGHTS = {"edge": 0.45, "rs": 0.25, "confluence": 0.15, "risk_reward": 0.15}
SCORE_LABELS = {"edge": "规则历史优势", "rs": "相对强度", "confluence": "多规则共振", "risk_reward": "盈亏比",
                "earnings_penalty": "财报临近", "watchlist_bonus": "在 watchlist 中", "feedback_weight": "反馈权重"}


def why_block(c: Candidate) -> dict:
    """Everything needed to explain the pick: conditions, score breakdown, position."""
    parts = []
    for key, weight in SCORE_WEIGHTS.items():
        v = c.score_parts.get(key)
        if v is not None:
            parts.append({"key": key, "label": SCORE_LABELS[key], "value": round(v, 3), "weight": weight,
                          "contribution": round(v * weight, 3)})
    adjustments = [{"key": k, "label": SCORE_LABELS[k], "value": c.score_parts[k]}
                   for k in ("earnings_penalty", "watchlist_bonus") if k in c.score_parts]
    fw = c.score_parts.get("feedback_weight", 1.0)
    if fw != 1.0:
        adjustments.append({"key": "feedback_weight", "label": SCORE_LABELS["feedback_weight"], "value": fw})
    if c.primary.kind == "watch":
        adjustments.append({"key": "watch", "label": "观察类规则 ×0.9", "value": 0.9})
    dist = c.features.get("dist_52w_high")
    bucket = base_rates.position_bucket(dist)
    pos = c.position_rate or {}
    edge = base_rates.edge_of(pos) if pos else None
    return {
        "rule": c.primary.label,
        "conditions": c.why,
        "other_rules": [d for d in c.detectors if d != c.primary.name],
        "score": c.score,
        "score_parts": parts,
        "adjustments": adjustments,
        "edge_basis": "同规则、同位置" if pos else f"同规则、{c.base_rate.get('regime', 'all')} 市场状态",
        "position": {
            "bucket": bucket[0] if bucket else None,
            "label": bucket[1] if bucket else "未知",
            "dist_52w_high": dist,
            "n": pos.get("n"),
            "edge_vs_hold": edge,
            "win_rate_net": pos.get("win_rate_net"),
        },
        "chase_risk": bool(bucket and bucket[0] == "near_high" and edge is not None and edge < 0),
    }


async def recent_keys(asof: date) -> set[tuple[str, str]]:
    rows = await db.fetch_all(
        "SELECT ticker, primary_detector, detectors FROM scout.opportunities WHERE asof > :start AND asof < :d",
        {"d": asof, "start": asof - timedelta(days=COOLDOWN_DAYS)},
    )
    keys = set()
    for r in rows:
        for det in r["detectors"] or [r["primary_detector"]]:
            keys.add((r["ticker"], det))
    return keys


async def feedback_weights() -> dict[str, float]:
    """Per-detector weight in [0.8, 1.2] from 👍/👎, shrunk toward 1 with few votes."""
    rows = await db.fetch_all(
        """
        SELECT o.primary_detector AS det,
               sum(CASE WHEN f.action IN ('up', 'star') THEN 1 ELSE 0 END) AS pos,
               sum(CASE WHEN f.action IN ('down', 'dismiss') THEN 1 ELSE 0 END) AS neg
        FROM scout.feedback f JOIN scout.opportunities o ON o.id = f.opportunity_id
        GROUP BY o.primary_detector
        """
    )
    out = {}
    for r in rows:
        pos, neg = int(r["pos"] or 0), int(r["neg"] or 0)
        total = pos + neg
        if total:
            out[r["det"]] = round(1.0 + 0.2 * (pos - neg) / (total + 5), 3)
    return out


async def save(c: Candidate, ev: dict, thesis: dict | None, thesis_source: str) -> None:
    lv = c.levels
    await db.execute(
        """
        INSERT INTO scout.opportunities (
            id, ticker, asof, primary_detector, detectors, direction, score, status,
            entry_lo, entry_hi, stop, target1, target2, horizon_days, risk_reward, regime,
            base_rate, evidence, thesis, thesis_source)
        VALUES (:id, :ticker, :asof, :primary, CAST(:dets AS JSONB), 'long', :score, 'new',
            :elo, :ehi, :stop, :t1, :t2, :h, :rr, :regime,
            CAST(:br AS JSONB), CAST(:ev AS JSONB), CAST(:th AS JSONB), :ts)
        ON CONFLICT (id) DO UPDATE SET
            score = EXCLUDED.score, detectors = EXCLUDED.detectors, regime = EXCLUDED.regime,
            entry_lo = EXCLUDED.entry_lo, entry_hi = EXCLUDED.entry_hi, stop = EXCLUDED.stop,
            target1 = EXCLUDED.target1, target2 = EXCLUDED.target2, risk_reward = EXCLUDED.risk_reward,
            base_rate = EXCLUDED.base_rate, evidence = EXCLUDED.evidence, thesis = EXCLUDED.thesis,
            thesis_source = EXCLUDED.thesis_source, updated_at = now()
        """,
        {
            "id": c.id, "ticker": c.ticker, "asof": c.asof, "primary": c.primary.name,
            "dets": db.jsonb(c.detectors), "score": c.score,
            "elo": lv.entry_lo, "ehi": lv.entry_hi, "stop": lv.stop, "t1": lv.target1, "t2": lv.target2,
            "h": c.primary.horizon_days, "rr": round(lv.risk_reward, 3), "regime": c.regime,
            "br": db.jsonb(c.base_rate), "ev": db.jsonb(ev), "th": db.jsonb(thesis), "ts": thesis_source,
        },
    )
