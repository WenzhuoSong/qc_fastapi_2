"""Stage 5 (pre-registered in `scout/knowledge/external_specs/ml_v5.yaml`), discovery only, top 1000:
(A) ridge v2 with the missing feature groups (peer, own history, sector aggregates, context × stock
crosses incl. dispersion and macro), (B) a tabular network with PLR numeric and categorical embeddings,
single- and multi-task, (C) ridge with per-sector coefficients shrunk towards the shared ones. Every
model is compared with the stage-4 baseline ridge_sec_u1000 by the same increment rule.
"""
from __future__ import annotations

import json

import numpy as np
import pandas as pd

from scout import sharadar_research as sr
from scout.ml import baselines as B
from scout.ml import features as F
from scout.ml import gbdt
from scout.ml import operators as op
from scout.ml import stage4

UNTIL = B.DISCOVERY[1]
STORE = "features_v5"
BASE_CS_SECN = ["mom_12_1", "bm", "gp_assets", "vol_63"]             # already sector-neutral in store v1
PEER = [c for c in F.PRICE + F.FUND + F.EVENTS if c not in BASE_CS_SECN]
INDUSTRY_REL = ["ret_21", "ret_63", "mom_12_1", "bm", "ep", "gp_assets"]
INDUSTRY_MIN = 5
OWN = [c for c in F.FUND if c != "log_mcap"] + ["vol_63", "log_dv21"]
OWN_WINDOW, OWN_MIN = 260, 20
DISPERSION = ["ret_21", "ret_63", "mom_12_1"]
MACRO = ["baa10y", "baa10y_chg63", "dgs10_chg63"]
STOCK_X = gbdt.INTERACT                                            # mom_12_1, vol_63, log_mcap, bm
GROUPS = ("peer", "own", "sector", "ctx_x")
RIDGE_RUNS = ("full", *GROUPS)
LAMBDA_MULT = (1.0, 10.0, 100.0, 1000.0)
BASELINE = "ridge_sec_u1000"
NEW_CTX = "cx5_"                       # stage-5 context columns (kept apart from the stage-1 design's ctx_)


# ── feature groups ────────────────────────────────────────────────────────────

def industry_relative(raw: pd.DataFrame) -> pd.DataFrame:
    """Value minus the date's industry median where the industry has ≥ INDUSTRY_MIN members, else NaN."""
    g = raw.groupby(["date", "industry"], sort=False)
    size = g["ticker"].transform("size")
    out = pd.DataFrame(index=raw.index)
    for c in INDUSTRY_REL:
        rel = raw[c] - g[c].transform("median")
        out[f"{c}_indrel"] = rel.where(size >= INDUSTRY_MIN)
    return out


def own_history(raw: pd.DataFrame, cols: list[str] = OWN) -> pd.DataFrame:
    """z-score of each value against the stock's own earlier samples (up to OWN_WINDOW, at least OWN_MIN)."""
    order = raw.sort_values(["ticker", "date"]).index
    s = raw.loc[order]
    out = pd.DataFrame(index=order)
    for c in cols:
        prev = s.groupby("ticker", sort=False)[c].shift(1)
        roll = prev.groupby(s["ticker"], sort=False).rolling(OWN_WINDOW, min_periods=OWN_MIN)
        mean = roll.mean().reset_index(level=0, drop=True)
        std = roll.std().reset_index(level=0, drop=True)
        out[f"{c}_own"] = ((s[c] - mean) / std.replace(0, np.nan)).clip(-5, 5)
    return out.loc[raw.index]


def sector_aggregates(raw: pd.DataFrame) -> pd.DataFrame:
    g = raw.groupby(["date", "sector"], sort=False)
    above = (raw["gap_sma200"] > 0).astype(float).where(raw["gap_sma200"].notna())
    return pd.DataFrame({
        "sec_mom_med": g["mom_12_1"].transform("median"),
        "sec_above200": above.groupby([raw["date"], raw["sector"]]).transform("mean"),
        "sec_bm_med": g["bm"].transform("median"),
        "sec_sales_med": g["sales_growth"].transform("median"),
        "sec_ear_med": g["ear"].transform("median"),
        "sec_ret63_std": g["ret_63"].transform("std"),
    }, index=raw.index)


def dispersion(raw: pd.DataFrame) -> pd.DataFrame:
    """Per date: cross-sectional std of the winsorized raw values (one value per date)."""
    g = raw.groupby("date", sort=False)
    return pd.DataFrame({f"disp_{c}": g[c].transform(lambda s: op.winsorize(s).std()) for c in DISPERSION},
                        index=raw.index)


def macro_frame(fred: dict, dates: pd.DatetimeIndex) -> pd.DataFrame:
    """Per trading date, yesterday's values (known before today's close)."""
    def s(sid):
        return pd.Series({pd.Timestamp(d): v for d, v in fred.get(sid, [])}, dtype=float).sort_index() \
            .reindex(dates, method="ffill").shift(1)
    baa, d10 = s("BAA10Y"), s("DGS10")
    return pd.DataFrame({"baa10y": baa, "baa10y_chg63": baa - baa.shift(63), "dgs10_chg63": d10 - d10.shift(63)},
                        index=dates)


def future_vol(p: sr.Prices, rows: np.ndarray, cols: np.ndarray, n: int = 21) -> np.ndarray:
    """Std of daily log returns over sessions i+1 .. i+n (NaN with < 15 valid returns or beyond the data)."""
    with np.errstate(all="ignore"):
        lr = np.diff(np.log(np.where(p.closeadj > 0, p.closeadj, np.nan)), axis=0, prepend=np.nan)
    out = np.full(len(rows), np.nan)
    T = len(lr)
    for k, (i, j) in enumerate(zip(rows, cols)):
        if i + n < T:
            w = lr[i + 1:i + n + 1, j]
            w = w[np.isfinite(w)]
            if len(w) >= 15:
                out[k] = w.std()
    return out


def build(fred: dict) -> pd.DataFrame:
    """Stage-5 extras for the store-v1 rows up to UNTIL, in store order; nothing later is read."""
    raw = pd.read_parquet(F.OUT / "raw_v1.parquet")
    raw["date"] = pd.to_datetime(raw["date"])
    raw = raw[raw["date"] <= pd.Timestamp(UNTIL)].reset_index(drop=True)
    parts = [raw[["date", "ticker"]]]
    peer = op.cross_sectional(raw, PEER, group="sector")
    parts.append(peer[[c for c in peer.columns if not c.endswith("_na")]].add_suffix("_peer"))
    ind = industry_relative(raw)
    parts.append(op.cross_sectional(pd.concat([raw[["date"]], ind], axis=1), list(ind.columns)))
    own = own_history(raw)
    parts.append(op.cross_sectional(pd.concat([raw[["date"]], own], axis=1), list(own.columns)))
    agg = sector_aggregates(raw)
    ranked = op.cross_sectional(pd.concat([raw[["date"]], agg], axis=1), list(agg.columns))
    parts.append(ranked[list(agg.columns)])
    ctx_new = dispersion(raw)
    dates = sr.trading_days()
    dates = dates[dates <= pd.Timestamp(UNTIL)]
    mf = macro_frame(fred, dates)
    for c in MACRO:
        ctx_new[c] = raw["date"].map(mf[c]).to_numpy()
    parts.append(op.context(pd.concat([raw[["date"]], ctx_new], axis=1), list(ctx_new.columns)).add_prefix(NEW_CTX))
    names = sorted(raw["ticker"].unique())
    p = sr.prices(names, dates)
    out = pd.concat(parts, axis=1)
    j = raw["ticker"].map(p.col)
    if j.isna().any():
        raise ValueError(f"{int(j.isna().sum())} rows without prices")
    out["fut_vol21"] = future_vol(p, raw["i"].to_numpy(), j.astype(int).to_numpy())
    return out


def load(n: int | None = None) -> pd.DataFrame:
    """Store-v1 rows up to UNTIL joined with the stage-5 extras (same order); rows with the main label of
    horizon n (any label when n is None)."""
    df = pd.read_parquet(F.OUT / "features_v1.parquet")
    df["date"] = pd.to_datetime(df["date"])
    for k in F.HORIZONS:
        df[f"y{k}_end"] = pd.to_datetime(df[f"y{k}_end"])
    df = df[df["date"] <= pd.Timestamp(UNTIL)].reset_index(drop=True)
    ext = pd.read_parquet(F.OUT / f"{STORE}.parquet")
    if not (len(ext) == len(df) and (ext["ticker"].to_numpy() == df["ticker"].to_numpy()).all()):
        raise ValueError("stage-5 extras are not aligned with store v1")
    df = pd.concat([df, ext.drop(columns=["date", "ticker"])], axis=1)
    keep = df[f"y{n}"].notna() if n else df["y21"].notna()
    return df[keep].reset_index(drop=True)


# ── designs ───────────────────────────────────────────────────────────────────

def context_levels(df: pd.DataFrame) -> dict[str, pd.Series]:
    """Every per-date input on a signed scale: context z-scores (old and new), regime and vol state."""
    lv = {c: df[c] for c in df.columns if c.startswith("ctx_") or c.startswith(NEW_CTX)}
    lv["regime"] = (df["regime"] - 1).clip(-1, 1)
    lv["vol_state"] = (df["vol_state"] * 2 - 1).clip(-1, 1)
    return lv


def group_columns(df: pd.DataFrame, group: str) -> list[str]:
    if group == "peer":
        return [f"{c}_peer" for c in PEER] + [c for c in df.columns if c.endswith("_indrel") or c.endswith("_indrel_na")]
    if group == "own":
        return [c for c in df.columns if c.endswith("_own") or c.endswith("_own_na")]
    if group == "sector":
        return [c for c in df.columns if c.startswith("sec_")]
    raise KeyError(group)


def design(df: pd.DataFrame, groups: tuple[str, ...]) -> tuple[np.ndarray, list[str]]:
    """Baseline ridge design plus the chosen groups; ctx_x replaces the two hand interactions with every
    context level × STOCK_X."""
    Xb, names = B.design(df)
    X = pd.DataFrame(Xb, columns=names, index=df.index)
    for g in groups:
        if g == "ctx_x":
            X = X.drop(columns=["mom_x_spy252", "mom_x_vix"])
            lv = context_levels(df)
            X = pd.concat([X, pd.DataFrame({f"{c}_x_{f}": lv[c] * df[f] for c in lv for f in STOCK_X}, index=df.index)], axis=1)
        else:
            X = pd.concat([X, df[group_columns(df, g)].astype("float64")], axis=1)
    return X.to_numpy(dtype=np.float64), list(X.columns)


def run_groups(run: str) -> tuple[str, ...]:
    return GROUPS if run == "full" else (run,)


def preds_path(key: str, n: int):
    return F.OUT / f"preds_v5_{key}_y{n}.parquet"


def _save(key: str, n: int, rows: pd.DataFrame, score) -> None:
    pd.DataFrame({"date": rows["date"].to_numpy(), "ticker": rows["ticker"].to_numpy(),
                  "score": np.asarray(score)}).to_parquet(preds_path(key, n), index=False)


def run_ridge(run: str, n: int) -> None:
    df = load(n)
    groups = run_groups(run)
    scored = B.walk_forward(df, n, alphas=gbdt.RIDGE_V2, name=f"v5_ridge_{run}", label=f"y{n}_sec",
                            design_fn=lambda d: design(d, groups))
    _save(f"ridge_{run}", n, scored, scored["ridge"])


# ── C: per-sector shrinkage ───────────────────────────────────────────────────

def shrink_fit(Z: np.ndarray, y: np.ndarray, sec: np.ndarray, k: int, alpha: float, lam: float) -> np.ndarray:
    """Coefficients (shared, then one block per sector code 0..k−1) of y ≈ Z (w + v_s), penalty
    alpha |w|² + lam Σ |v_s|²; Z standardized, y centred. Codes < 0 use the shared part only."""
    p = Z.shape[1]
    A = np.zeros((p * (k + 1), p * (k + 1)))
    b = np.zeros(p * (k + 1))
    for s in range(k):
        m = sec == s
        G, g = Z[m].T @ Z[m], Z[m].T @ y[m]
        sl = slice(p * (s + 1), p * (s + 2))
        A[:p, :p] += G
        A[:p, sl] = G
        A[sl, :p] = G
        A[sl, sl] = G + lam * np.eye(p)
        b[:p] += g
        b[sl] = g
    m = sec < 0
    A[:p, :p] += Z[m].T @ Z[m] + alpha * np.eye(p)
    b[:p] += Z[m].T @ y[m]
    return np.linalg.solve(A, b)


def shrink_predict(coef: np.ndarray, Z: np.ndarray, sec: np.ndarray) -> np.ndarray:
    p = Z.shape[1]
    W = coef.reshape(-1, p)
    out = Z @ W[0]
    ok = sec >= 0
    out[ok] += np.einsum("ij,ij->i", Z[ok], W[1:][sec[ok]])
    return out


def run_shrink(n: int) -> None:
    """Monthly versions as ridge v2: alpha chosen pooled on the inner validation, then lam = alpha × LAMBDA_MULT."""
    y, end = f"y{n}", f"y{n}_end"
    df = load(n)
    df["t"] = B.target(df, f"y{n}_sec").fillna(0.0)
    X, names = B.design(df)
    sectors = sorted(df["sector"].dropna().unique())
    sec = df["sector"].map({s: k for k, s in enumerate(sectors)}).fillna(-1).astype(int).to_numpy()
    tgt = df["t"].to_numpy()
    disc = gbdt._discovery(df, n)
    i_of_date = df.groupby("date")["i"].first()
    pred = pd.Series(np.nan, index=df.index)
    log = []
    for m in sorted(disc["date"].dt.to_period("M").unique()):
        rows = disc.index[disc["date"].dt.to_period("M") == m]
        cut = int(i_of_date[df.loc[rows, "date"].min()]) - n
        train = df.index[(df["i"] + n) <= cut]
        if len(train) < 10_000:
            continue
        tr_dates = df.loc[train, "date"]
        inner = tr_dates.max() - pd.DateOffset(months=B.INNER_MONTHS)
        fit_i = train[tr_dates <= inner - pd.Timedelta(days=int(n * 1.5))]
        val_i = train[tr_dates > inner]
        mu, sd = X[fit_i].mean(0), X[fit_i].std(0)
        sd[sd == 0] = 1.0
        Zf, Zv, yf = (X[fit_i] - mu) / sd, (X[val_i] - mu) / sd, tgt[fit_i] - tgt[fit_i].mean()
        v = df.loc[val_i, ["date", y]].copy()

        def val_ic(s):
            v["s"] = s
            return B.daily_ic(v, "s", y).mean()
        G, g = Zf.T @ Zf, Zf.T @ yf
        alpha = max(gbdt.RIDGE_V2, key=lambda a: val_ic(Zv @ np.linalg.solve(G + a * np.eye(len(g)), g)))
        lam_ic = {mult: val_ic(shrink_predict(shrink_fit(Zf, yf, sec[fit_i], len(sectors), alpha, alpha * mult), Zv, sec[val_i]))
                  for mult in LAMBDA_MULT}
        mult = max(lam_ic, key=lam_ic.get)
        mu, sd = X[train].mean(0), X[train].std(0)
        sd[sd == 0] = 1.0
        Zt = (X[train] - mu) / sd
        coef = shrink_fit(Zt, tgt[train] - tgt[train].mean(), sec[train], len(sectors), alpha, alpha * mult)
        pred.loc[rows] = shrink_predict(coef, (X[rows] - mu) / sd, sec[rows])
        log.append({"month": str(m), "alpha": alpha, "lam_mult": mult, "val_ic": {str(k): round(float(x), 5) for k, x in lam_ic.items()}})
    _save("shrink", n, disc, pred.loc[disc.index])
    (F.OUT / f"trials_v5_shrink_y{n}.json").write_text(json.dumps(log, indent=1))


# ── B: tabular network ────────────────────────────────────────────────────────

CATS = {"sector": 4, "industry": 8, "size_bucket": 4, "regime": 2, "vol_state": 2}
PLR_K, PLR_D = 8, 8
HIDDEN = (256, 128)
EPOCH_ROWS, BATCH = 400_000, 2048
FIRST_EPOCHS, LATER_EPOCHS, PATIENCE = 15, 5, 2
INNER_MONTHS = 12
SEED = 7
DNN_VARIANTS = {"single_21": (("y21_sec",), 21), "single_63": (("y63_sec",), 63),
                "multi": (("y21_sec", "y63_sec", "fut_vol21"), 63)}


def numeric_columns(df: pd.DataFrame) -> list[str]:
    """Network inputs: every numeric input of the full ridge design except one-hots and hand crosses
    (context levels go in directly), without the ordinals (embedded instead)."""
    cs = F.PRICE + F.FUND + F.EVENTS
    cols = cs + [f"{c}_na" for c in cs] + [c for c in df.columns if c.endswith("_secn")] + [f"{c}_chg" for c in F.TS_SOURCES]
    cols += [c for c in df.columns if c.startswith("ctx_") or c.startswith(NEW_CTX)] + F.SIGNALS
    cols += [c for g in ("peer", "own", "sector") for c in group_columns(df, g)]
    return list(dict.fromkeys(cols))


def category_codes(df: pd.DataFrame) -> np.ndarray:
    """(rows, len(CATS)) int64 codes; 0 = unknown, categories by sorted value."""
    out = []
    for c in CATS:
        if c in F.ORDINAL:
            out.append((df[c].astype(int) + 1).clip(0, len(F.ORDINAL[c])).to_numpy())
        else:
            cats = sorted(df[c].dropna().unique())
            out.append(df[c].map({v: k + 1 for k, v in enumerate(cats)}).fillna(0).astype(int).to_numpy())
    return np.stack(out, axis=1).astype(np.int64)


def category_sizes(df: pd.DataFrame) -> list[int]:
    return [len(F.ORDINAL[c]) + 1 if c in F.ORDINAL else df[c].nunique() + 1 for c in CATS]


def make_net(n_num: int, sizes: list[int], heads: int):
    import torch
    from torch import nn

    class PLR(nn.Module):
        def __init__(self):
            super().__init__()
            self.freq = nn.Parameter(torch.randn(n_num, PLR_K))
            self.w = nn.Parameter(torch.randn(n_num, 2 * PLR_K, PLR_D) / np.sqrt(2 * PLR_K))
            self.b = nn.Parameter(torch.zeros(n_num, PLR_D))

        def forward(self, x):
            z = 2 * np.pi * self.freq[None] * x[..., None]
            z = torch.cat([torch.sin(z), torch.cos(z)], dim=-1)
            return torch.relu(torch.einsum("bfk,fkd->bfd", z, self.w) + self.b).flatten(1)

    class Net(nn.Module):
        def __init__(self):
            super().__init__()
            self.plr = PLR()
            self.emb = nn.ModuleList([nn.Embedding(s, d) for s, d in zip(sizes, CATS.values())])
            d_in = n_num * PLR_D + sum(CATS.values())
            layers = []
            for h in HIDDEN:
                layers += [nn.Linear(d_in, h), nn.GELU(), nn.Dropout(0.1)]
                d_in = h
            self.body = nn.Sequential(*layers)
            self.out = nn.Linear(d_in, heads)

        def forward(self, x, c):
            e = [m(c[:, k]) for k, m in enumerate(self.emb)]
            return self.out(self.body(torch.cat([self.plr(x), *e], dim=1)))

    return Net()


def masked_mse(pred, target):
    import torch
    ok = torch.isfinite(target)
    return ((torch.where(ok, pred, 0.0) - torch.where(ok, target, 0.0)) ** 2).sum() / ok.sum().clamp(min=1)


def net_predict(model, X: np.ndarray, C: np.ndarray, rows: np.ndarray, dev) -> np.ndarray:
    import torch
    model.eval()
    out = []
    with torch.no_grad():
        for k in range(0, len(rows), 8 * BATCH):
            b = rows[k:k + 8 * BATCH]
            out.append(model(torch.from_numpy(X[b]).to(dev), torch.from_numpy(C[b]).to(dev)).float().cpu().numpy())
    return np.concatenate(out) if out else np.zeros((0, model.out.out_features))


def net_train(model, X, C, T, fit_i, val_i, val_frames, epochs: int, seed: int, dev) -> tuple[object, list]:
    """val_frames = [(head, frame with date and that head's main label)]; the score is the mean IC."""
    import torch
    torch.manual_seed(seed)
    model.to(dev)
    opt = torch.optim.AdamW(model.parameters(), lr=1e-3, weight_decay=1e-4)
    rng = np.random.default_rng(seed)
    best, best_state, stale, log = -np.inf, None, 0, []
    for e in range(epochs):
        model.train()
        pick = fit_i[rng.choice(len(fit_i), min(EPOCH_ROWS, len(fit_i)), replace=False)]
        loss_sum = 0.0
        for k in range(0, len(pick), BATCH):
            b = pick[k:k + BATCH]
            loss = masked_mse(model(torch.from_numpy(X[b]).to(dev), torch.from_numpy(C[b]).to(dev)),
                              torch.from_numpy(T[b]).to(dev))
            opt.zero_grad()
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            opt.step()
            loss_sum += float(loss) * len(b)
        p = net_predict(model, X, C, val_i, dev)
        ics = []
        for h, fr in val_frames:
            v = fr.copy()
            v["s"] = p[:, h]
            ics.append(float(B.daily_ic(v, "s", v.columns[1]).mean()))
        ic = float(np.mean(ics))
        log.append({"epoch": e, "loss": round(loss_sum / len(pick), 5), "val_ic": round(ic, 5)})
        if ic > best:
            best, best_state, stale = ic, {k: t.detach().clone() for k, t in model.state_dict().items()}, 0
        else:
            stale += 1
            if stale >= PATIENCE:
                break
    model.load_state_dict(best_state)
    return model, log


def run_dnn(variant: str) -> dict:
    import torch
    from scout.ml.sequence import device
    heads, purge = DNN_VARIANTS[variant]
    dev = device()
    df = load(None)
    T = np.stack([B.target(df, h).to_numpy() if h != "fut_vol21" else df.groupby("date")[h].transform(op.rank_pm1).to_numpy()
                  for h in heads], axis=1).astype(np.float32)
    num = numeric_columns(df)
    X = df[num].fillna(0.0).to_numpy(dtype=np.float32)
    C = category_codes(df)
    sizes = category_sizes(df)
    horizons = [int(h[1:3]) for h in heads if h != "fut_vol21"]
    score_rows = df.index[(df["date"] >= B.DISCOVERY[0])]
    i_of_date = df.groupby("date")["i"].first()
    pred = np.full((len(df), len(heads)), np.nan, dtype=np.float32)
    model, log = None, []
    for year in sorted(df.loc[score_rows, "date"].dt.year.unique()):
        rows = score_rows[df.loc[score_rows, "date"].dt.year == year].to_numpy()
        cut = int(i_of_date[df.loc[rows, "date"].min()]) - purge
        lab_ok = np.isfinite(T[:, 0]) if len(horizons) == 1 else df["y63"].notna().to_numpy()
        train_i = df.index[((df["i"] + purge) <= cut) & lab_ok]
        tr_dates = df.loc[train_i, "date"]
        inner = tr_dates.max() - pd.DateOffset(months=INNER_MONTHS)
        fit_i = train_i[tr_dates <= inner - pd.Timedelta(days=int(purge * 1.5))].to_numpy()
        val_i = train_i[tr_dates > inner].to_numpy()
        frames = [(k, df.loc[val_i, ["date", f"y{h}"]]) for k, h in enumerate(horizons)]
        first = model is None
        model = make_net(X.shape[1], sizes, len(heads)) if first else model
        model, ep = net_train(model, X, C, T, fit_i, val_i, frames, FIRST_EPOCHS if first else LATER_EPOCHS,
                              SEED + int(year), dev)
        pred[rows] = net_predict(model, X, C, rows, dev)
        torch.save(model.state_dict(), B.CKPT / f"v5_dnn_{variant}_{year}.pt")
        log.append({"year": int(year), "epochs": ep})
    for k, h in enumerate(horizons):
        disc = gbdt._discovery(df[df[f"y{h}"].notna()], h)
        _save(f"dnn_{variant}", h, disc, pred[disc.index, k])
    info = {"inputs": num, "training": log}
    (F.OUT / f"trials_v5_dnn_{variant}.json").write_text(json.dumps(info, indent=1))
    return info


# ── report ────────────────────────────────────────────────────────────────────

def keys(n: int) -> list[str]:
    return [BASELINE, *(f"ridge_{r}" for r in RIDGE_RUNS), "shrink", f"dnn_single_{n}", "dnn_multi"]


COMPARE = {  # score → the scores it is compared with (the first is the decision comparator)
    "ridge_full": [BASELINE], **{f"ridge_{g}": [BASELINE] for g in GROUPS}, "shrink": [BASELINE],
    "dnn_single": ["ridge_full", BASELINE], "dnn_multi": ["ridge_full", "dnn_single", BASELINE],
}


def report(n: int) -> dict:
    df = load(n)
    disc = gbdt._discovery(df, n)[["date", "ticker", "regime", f"y{n}"]].reset_index(drop=True)
    scores, scored = {}, disc.copy()
    for k in keys(n):
        path = stage4.preds_path(k, n) if k == BASELINE else preds_path(k, n)
        name = "dnn_single" if k.startswith("dnn_single") else k
        scores[name] = pd.read_parquet(path)[["date", "ticker", "score"]]
        scored = scored.merge(scores[name].rename(columns={"score": name}), on=["date", "ticker"], how="left")
    metrics = gbdt.evaluate(scored, n, tuple(scores))
    inc = {k: {b: stage4.increment(scored, n, k, b) for b in bases} for k, bases in COMPARE.items()}
    res = {"metrics": metrics, "increments": inc, "tilt": stage4.tilt_ir(scores)}
    (F.OUT / f"results_v5_y{n}.json").write_text(json.dumps(res, ensure_ascii=False, indent=2, default=str))
    return res


def report_text(results: dict) -> str:
    p = lambda x: "—" if x is None else f"{x:+.4f}"  # noqa: E731
    q = lambda x: "—" if x is None else f"{x:+.2%}"  # noqa: E731
    lines = [f"ML 第五阶段（挖掘期 {B.DISCOVERY[0][:7]} 至 {B.DISCOVERY[1][:7]}，前 1000；确认期未触碰）"]
    for n, r in results.items():
        lines.append(f"\n持有 {n} 个交易日：模型 | IC（t） | IC 前半 / 后半 | 前 10% 超额（t） | 倾斜组合 IR（t） | 候选 | 增量（相对 → 均值（t）成立否）")
        for k, v in r["metrics"].items():
            ic, td, tl = v["ic"], v["top_decile"], r["tilt"][k]
            inc = r["increments"].get(k, {})
            inc_s = "；".join(f"{b} → {p(s['all']['mean'])}（{s['all']['t_nw']}）{'成立' if s['holds'] else '不成立'}"
                             for b, s in inc.items() if s) or "基线"
            lines.append(f"{k} | {p(ic['all']['mean'])}（{ic['all']['t_nw']}） | {p(ic['first']['mean'])} / {p(ic['second']['mean'])} | "
                         f"{q(td['all']['mean'])}（{td['all']['t_nw']}） | {tl['ir']}（{tl['t']}） | {'是' if v['candidate'] else '否'} | {inc_s}")
    return "\n".join(lines)
