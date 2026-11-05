"""Weekly direction, pre-registered in `scout/knowledge/external_specs/weekly_direction_v1.yaml` (one run, local only).

SPY and the SPDR sector ETFs, decided at each week's last close: will next week's return be positive? Features from
data up to the decision close only (price, volatility, daily-bar flow proxies, member breadth, VIX / Baa spread /
curve, same-month seasonality), standardised and imputed with training statistics. M0 = training base rate,
M1 = L2 logistic (C 0.1), M2 = LightGBM (fixed configuration); refit every January on weeks whose outcome is known.
Scored out of sample: log loss vs M0 (weekly Newey-West t), high-confidence weeks (p ≥ 0.65), calibration, and
holding only when the model is above the base rate vs always holding.
"""
from __future__ import annotations

import math
from pathlib import Path

import numpy as np
import pandas as pd

from scout import cross_asset as ca
from scout import risk_warning as rw

KEY = "weekly_direction_v1"
FAMILY = "short_term_reversal"
ASSETS = ("SPY", "XLB", "XLE", "XLF", "XLI", "XLK", "XLP", "XLU", "XLV", "XLY", "XLRE", "XLC")
BREADTH_KEY = {"SPY": "TOP500"}
SHARADAR = Path("exports/sharadar/parquet/funds.parquet")
VOL_WINDOW, VOL_MIN = 756, 504
FLOW_WINDOW, FLOW_MIN = 1260, 756
SEASON_MIN_YEARS = 5
TRAIN_MIN_YEARS = 4
C = 0.1
LGB_PARAMS = {"objective": "binary", "num_leaves": 15, "min_data_in_leaf": 100, "learning_rate": 0.03,
              "feature_fraction": 0.8, "bagging_fraction": 0.8, "bagging_freq": 1, "seed": 7, "verbose": -1,
              "deterministic": True, "force_row_wise": True}
LGB_ROUNDS = 300
HIGH_P = 0.65
HIGH_GAP = 0.05
MIN_PER_YEAR = 4
T_BAR = 3.55
CAL_SLOPE = (0.5, 1.5)
NW_LAG = 4
COST, COST_HIGH = 0.0005, 0.0020
FEATURES = ("r1w", "r4w", "r12w", "r52w", "rel1w", "rel4w", "rel12w", "dist200", "vol_pct", "vol21",
            "cmf", "udv", "cmf_pct", "udv_pct", "breadth", "breadth_chg4w", "vix", "vix_chg4w", "baa_z", "curve", "season")


# ── data ──

def load_prices() -> dict[str, pd.DataFrame]:
    f = pd.read_parquet(SHARADAR, columns=["ticker", "date", "closeadj", "high", "low", "close", "volume"],
                        filters=[("ticker", "in", list(ASSETS))])
    f["date"] = pd.to_datetime(f["date"])
    rf = ca.french_daily_rf()
    out = {}
    for t, g in f.groupby("ticker"):
        g = g.set_index("date").sort_index()
        out[t] = g[g.index <= rf.index[-1]]
    return out


async def load_macro() -> dict[str, pd.Series]:
    from datetime import date

    from scout.data import fred

    out = {}
    for sid in ("VIXCLS", "BAA10Y", "T10Y2Y"):
        rows = await fred.series(sid, date(1990, 1, 1), date(2026, 10, 28))
        out[sid] = pd.Series({pd.Timestamp(d): v for d, v in rows}, dtype=float).sort_index()
    return out


def trailing_pct(x: pd.Series, window: int, min_periods: int) -> pd.Series:
    return rw.trailing_share_le(x, window, min_periods)


def daily_features(g: pd.DataFrame, spy: pd.Series, breadth: pd.Series | None, macro: dict[str, pd.Series]) -> pd.DataFrame:
    """Per-day features for one asset (each row uses data up to that day; macro values from the previous day)."""
    c = g["closeadj"]
    idx = c.index
    lr = np.log(c).diff()
    rng = (g["high"] - g["low"]).replace(0, np.nan)
    mfv = (((g["close"] - g["low"]) - (g["high"] - g["close"])) / rng).fillna(0) * g["volume"]
    cmf = mfv.rolling(20).sum() / g["volume"].rolling(20).sum()
    up = c.diff() > 0
    udv = g["volume"].where(up, 0).rolling(20).sum() / g["volume"].where(~up & (c.diff() < 0), 0).rolling(20).sum().replace(0, np.nan)
    vol21 = lr.rolling(21).std() * math.sqrt(252)
    sp = spy.reindex(idx)
    f = pd.DataFrame(index=idx)
    for n, d in ((1, 5), (4, 20), (12, 60), (52, 252)):
        f[f"r{n}w"] = c / c.shift(d) - 1
    for n, d in ((1, 5), (4, 20), (12, 60)):
        f[f"rel{n}w"] = f[f"r{n}w"] - (sp / sp.shift(d) - 1)
    f["dist200"] = c / c.rolling(200).mean() - 1
    f["vol21"] = vol21
    f["vol_pct"] = trailing_pct(vol21, VOL_WINDOW, VOL_MIN)
    f["cmf"], f["udv"] = cmf, udv
    f["cmf_pct"] = trailing_pct(cmf, FLOW_WINDOW, FLOW_MIN)
    f["udv_pct"] = trailing_pct(udv, FLOW_WINDOW, FLOW_MIN)
    b = breadth.reindex(idx) if breadth is not None else pd.Series(np.nan, index=idx)
    f["breadth"], f["breadth_chg4w"] = b, b - b.shift(20)
    vix = macro["VIXCLS"].reindex(idx, method="ffill").shift(1)
    f["vix"], f["vix_chg4w"] = vix, vix - vix.shift(20)
    baa = macro["BAA10Y"].dropna()
    baa_z = ((baa - baa.rolling(252, min_periods=252).mean()) / baa.rolling(252, min_periods=252).std())
    f["baa_z"] = baa_z.reindex(idx, method="ffill").shift(1)
    f["curve"] = macro["T10Y2Y"].reindex(idx, method="ffill").shift(1)
    return f


def week_ends(index: pd.DatetimeIndex) -> pd.DatetimeIndex:
    """Last trading day of each week (the final, possibly incomplete, week excluded)."""
    s = pd.Series(index, index=index)
    last = s.groupby(index.to_period("W-FRI")).max()
    return pd.DatetimeIndex(last.iloc[:-1].to_numpy())


def seasonality(c: pd.Series, decision: pd.Timestamp, target_end: pd.Timestamp, monthly: pd.Series) -> float:
    """Mean return of the target week's calendar month in earlier years (≥ 5 years), else NaN."""
    m = target_end.month
    past = monthly[(monthly.index.month == m) & (monthly.index.year < decision.year)]
    return float(past.mean()) if len(past) >= SEASON_MIN_YEARS else np.nan


def build(prices: dict[str, pd.DataFrame], members: pd.DataFrame, macro: dict[str, pd.Series]) -> pd.DataFrame:
    """Rows = (decision week end, asset): features, target y, next-week return and next-week RF."""
    spy = prices["SPY"]["closeadj"]
    ends = week_ends(spy.index)
    rf = ca.french_daily_rf()
    rf_cum = np.log1p(rf).cumsum()
    rows = []
    for a in ASSETS:
        if a not in prices:
            continue
        g = prices[a]
        bkey = BREADTH_KEY.get(a, a)
        feats = daily_features(g, spy, members[bkey] if bkey in members.columns else None, macro)
        c = g["closeadj"]
        monthly = c.resample("ME").last().pct_change().dropna()
        e = ends[(ends >= c.index[0])]
        fe = feats.reindex(e)
        nxt = pd.Series(e[1:].append(pd.DatetimeIndex([pd.NaT])), index=e)
        ce = c.reindex(e)
        fe["ret_next"] = ce.shift(-1) / ce - 1
        fe["spy_next"] = spy.reindex(e).shift(-1) / spy.reindex(e) - 1
        rc = rf_cum.reindex(e, method="ffill")
        fe["rf_next"] = np.expm1(rc.shift(-1) - rc)
        fe["season"] = [seasonality(c, d, n, monthly) if pd.notna(n) else np.nan for d, n in zip(e, nxt)]
        fe["asset"] = a
        fe["target_end"] = nxt.to_numpy()
        rows.append(fe)
    d = pd.concat(rows)
    d.index.name = "date"
    d = d.reset_index()
    d = d[d["ret_next"].notna()]
    d["y"] = (d["ret_next"] > 0).astype(int)
    return d


# ── models ──

def prepare(train: pd.DataFrame, test: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    """Standardise with training mean / std; missing → training median (before standardising) plus a flag."""
    cols = list(FEATURES)
    med = train[cols].median()
    mu = train[cols].fillna(med).mean()
    sd = train[cols].fillna(med).std().replace(0, 1).fillna(1)

    def mat(x: pd.DataFrame) -> np.ndarray:
        miss = x[cols].isna().to_numpy(float)
        z = ((x[cols].fillna(med) - mu) / sd).fillna(0.0).to_numpy(float)
        return np.hstack([z, miss])
    return mat(train), mat(test)


def fit_predict(train: pd.DataFrame, test: pd.DataFrame) -> dict[str, np.ndarray]:
    from sklearn.linear_model import LogisticRegression
    import lightgbm as lgb

    Xtr, Xte = prepare(train, test)
    y = train["y"].to_numpy()
    m1 = LogisticRegression(C=C, max_iter=2000).fit(Xtr, y)
    booster = lgb.train(LGB_PARAMS, lgb.Dataset(Xtr, label=y), num_boost_round=LGB_ROUNDS)
    return {"m0": np.full(len(test), y.mean()), "m1": m1.predict_proba(Xte)[:, 1], "m2": booster.predict(Xte)}


def walk_forward(d: pd.DataFrame) -> pd.DataFrame:
    start = d.loc[(d["asset"] == "SPY") & d["cmf_pct"].notna(), "date"].min()
    d = d[d["date"] >= start]
    first_year = start.year + TRAIN_MIN_YEARS + (0 if start.month == 1 else 1)
    out = []
    for yr in range(first_year, d["date"].dt.year.max() + 1):
        cut = pd.Timestamp(f"{yr}-01-01")
        train = d[pd.to_datetime(d["target_end"]) < cut]
        test = d[d["date"].dt.year == yr]
        if test.empty:
            continue
        p = fit_predict(train, test)
        out.append(test[["date", "asset", "y", "ret_next", "spy_next", "rf_next"]].assign(**p))
    return pd.concat(out, ignore_index=True)


# ── evaluation ──

def nw_t(x: pd.Series, lags: int = NW_LAG) -> float | None:
    v = x.dropna().to_numpy()
    n = len(v)
    if n < 30:
        return None
    u = v - v.mean()
    s = u @ u / n
    for L in range(1, lags + 1):
        s += 2 * (1 - L / (lags + 1)) * (u[L:] @ u[:-L]) / n
    return float(v.mean() / math.sqrt(s / n)) if s > 0 else None


def logloss(y: np.ndarray, p: np.ndarray) -> np.ndarray:
    p = np.clip(p, 1e-6, 1 - 1e-6)
    return -(y * np.log(p) + (1 - y) * np.log(1 - p))


def calibration(y: np.ndarray, p: np.ndarray) -> dict:
    from sklearn.linear_model import LogisticRegression

    lp = np.log(np.clip(p, 1e-6, 1 - 1e-6) / (1 - np.clip(p, 1e-6, 1 - 1e-6)))
    slope = float(LogisticRegression(C=1e6, max_iter=1000).fit(lp.reshape(-1, 1), y).coef_[0, 0])
    q = pd.qcut(p, 10, labels=False, duplicates="drop")
    dec = pd.DataFrame({"p": p, "y": y, "q": q}).groupby("q").agg(pred=("p", "mean"), actual=("y", "mean"), n=("y", "size"))
    return {"slope": slope, "deciles": dec.round(4).to_dict(orient="records")}


def score(o: pd.DataFrame, model: str) -> dict:
    o = o.assign(gain=logloss(o["y"].to_numpy(), o["m0"].to_numpy()) - logloss(o["y"].to_numpy(), o[model].to_numpy()),
                 bgain=(o["m0"] - o["y"]) ** 2 - (o[model] - o["y"]) ** 2)
    weekly = o.groupby("date")["gain"].mean()
    hi = o[o[model] >= HIGH_P]
    hw = (hi["y"] - hi["m0"]).groupby(hi["date"]).mean()
    years = (o["date"].max() - o["date"].min()).days / 365.25
    assets_per_week = o.groupby("date").size().mean()
    return {"obs": int(len(o)), "logloss": float(logloss(o["y"].to_numpy(), o[model].to_numpy()).mean()),
            "logloss_m0": float(logloss(o["y"].to_numpy(), o["m0"].to_numpy()).mean()),
            "gain": float(weekly.mean()), "t": nw_t(weekly), "brier_gain": float(o["bgain"].mean()),
            "hit": float(((o[model] > 0.5) == (o["y"] == 1)).mean()), "p_sd": float(o[model].std()),
            "high": {"n": int(len(hi)), "per_asset_year": float(len(hi) / (assets_per_week * years)) if years else None,
                     "up_share": float(hi["y"].mean()) if len(hi) else None, "m0": float(hi["m0"].mean()) if len(hi) else None,
                     "gap": float((hi["y"] - hi["m0"]).mean()) if len(hi) else None, "t": nw_t(hw) if len(hw) else None,
                     "next_ret": float(hi["ret_next"].mean()) if len(hi) else None,
                     "rel_spy": float((hi["ret_next"] - hi["spy_next"])[hi["asset"] != "SPY"].mean()) if len(hi) else None}}


def economic(o: pd.DataFrame, model: str, cost: float) -> pd.DataFrame:
    """Weekly equal-weight book: hold an asset next week when the model is above the base rate, else cash."""
    o = o.sort_values(["asset", "date"]).copy()
    o["held"] = o[model] > o["m0"]
    prev = o.groupby("asset")["held"].shift(1)
    o["switch"] = (o["held"] != prev) & ~(prev.isna() & ~o["held"])     # first week: a cost only if bought
    o["r"] = np.where(o["held"], o["ret_next"], o["rf_next"]) - cost * o["switch"]
    strat = o.groupby("date")["r"].mean()
    hold = o.groupby("date")["ret_next"].mean()
    rf = o.groupby("date")["rf_next"].mean()
    return pd.DataFrame({"strat": strat, "hold": hold, "rf": rf, "held": o.groupby("date")["held"].mean()})


def perf(r: pd.Series, rf: pd.Series) -> dict:
    ex = r - rf
    nav = (1 + r).cumprod()
    return {"cagr": float(nav.iloc[-1] ** (52 / len(r)) - 1), "sharpe": float(ex.mean() / ex.std() * math.sqrt(52)),
            "max_dd": float((nav / nav.cummax() - 1).min())}


def evaluate(o: pd.DataFrame) -> dict:
    mid = o["date"].min() + (o["date"].max() - o["date"].min()) / 2
    halves = {"all": o, "first": o[o["date"] < mid], "second": o[o["date"] >= mid]}
    res: dict = {"mid": str(mid.date()), "range": [str(o["date"].min().date()), str(o["date"].max().date())],
                 "base_up": float(o["y"].mean())}
    for m in ("m1", "m2"):
        res[m] = {h: score(x, m) for h, x in halves.items()}
        res[m]["calibration"] = calibration(o["y"].to_numpy(), o[m].to_numpy())
        econ = {}
        for cost, tag in ((COST, ""), (COST_HIGH, "_cost_high")):
            e = economic(o, m, cost)
            econ["strat" + tag] = {h: perf(x["strat"], x["rf"]) for h, x in (("all", e), ("first", e[e.index < mid]), ("second", e[e.index >= mid]))}
            if not tag:
                econ["hold"] = {h: perf(x["hold"], x["rf"]) for h, x in (("all", e), ("first", e[e.index < mid]), ("second", e[e.index >= mid]))}
                econ["held_share"] = float(e["held"].mean())
        res[m]["economic"] = econ
        a, f, s = res[m]["all"], res[m]["first"], res[m]["second"]
        checks = {
            "prob_t": (a["t"] or 0) >= T_BAR,
            "prob_halves": f["gain"] > 0 and s["gain"] > 0,
            "high_gap": (a["high"]["gap"] or 0) >= HIGH_GAP,
            "high_t": (a["high"]["t"] or 0) >= T_BAR,
            "high_halves": (f["high"]["gap"] or 0) > 0 and (s["high"]["gap"] or 0) > 0,
            "high_count": (a["high"]["per_asset_year"] or 0) >= MIN_PER_YEAR,
            "sharpe_halves": all(econ["strat"][h]["sharpe"] >= econ["hold"][h]["sharpe"] for h in ("first", "second")),
            "calibration": CAL_SLOPE[0] <= res[m]["calibration"]["slope"] <= CAL_SLOPE[1],
        }
        res[m]["decision"] = {"checks": checks, "pass": all(checks.values())}
    return res


def run(macro: dict[str, pd.Series]) -> dict:
    prices = load_prices()
    members = rw.load_members()
    d = build(prices, members, macro)
    o = walk_forward(d)
    return {"key": KEY, "rows": int(len(d)), "oos_rows": int(len(o)), **evaluate(o)}


# ── report ──

def _p(x: float | None, d: int = 1) -> str:
    return "—" if x is None else f"{x * 100:+.{d}f}%"


def report_text(res: dict) -> str:
    L = [f"周度方向检验 {res['key']}：样本外 {res['range']}（{res['oos_rows']} 个资产-周），中点 {res['mid']}，下周上涨基础比例 {res['base_up'] * 100:.1f}%"]
    for m, name in (("m1", "M1 逻辑回归"), ("m2", "M2 LightGBM")):
        r = res[m]
        L.append(f"\n【{name}】")
        for h in ("all", "first", "second"):
            x = r[h]
            hi = x["high"]
            L.append(f"  {h}: 对数损失 {x['logloss']:.4f} vs 基础 {x['logloss_m0']:.4f}（每周改善 {x['gain']:+.5f}，t {x['t'] or 0:.2f}），命中 {x['hit'] * 100:.1f}%，概率离散度 {x['p_sd']:.3f}；"
                     f"高置信（≥65%）{hi['n']} 次（每资产每年 {hi['per_asset_year'] or 0:.1f}），实际上涨 {(hi['up_share'] or 0) * 100:.1f}% vs 基础 {(hi['m0'] or 0) * 100:.1f}%"
                     f"（差 {_p(hi['gap'])}，t {hi['t'] or 0:.2f}），下周平均 {_p(hi['next_ret'], 2)}，板块相对 SPY {_p(hi['rel_spy'], 2)}")
        c = r["calibration"]
        L.append(f"  校准斜率 {c['slope']:.2f}；十分组 预测→实际：" + " ".join(f"{x['pred'] * 100:.0f}→{x['actual'] * 100:.0f}" for x in c["deciles"]))
        e = r["economic"]
        for k in ("strat", "strat_cost_high", "hold"):
            v = e[k]
            L.append(f"  {k}: 年化 {_p(v['all']['cagr'])} 夏普 {v['all']['sharpe']:.2f}（前 {v['first']['sharpe']:.2f} / 后 {v['second']['sharpe']:.2f}） 最大回撤 {_p(v['all']['max_dd'])}")
        L.append(f"  持有时间比例 {e['held_share'] * 100:.0f}%")
        L.append(f"  判定：{'通过' if r['decision']['pass'] else '不通过'} {r['decision']['checks']}")
    return "\n".join(L)
