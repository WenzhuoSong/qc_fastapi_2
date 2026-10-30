"""Stage 3: sequence models (pre-registered in `scout/knowledge/external_specs/ml_v3.yaml`), discovery only.

Each (sample date, stock) row gets the 250 sessions up to its close as 50 five-day tokens of 7 daily
channels, gathered on the fly from (dates × stocks × channels) arrays. A third-order Markov chain over
weekly states, an LSTM and a Transformer encoder are trained on the sector-neutral label; each yearly
version uses only samples whose labels ended ≥ N sessions before its year. Fusion averages a model's
per-date rank with the stage-2b b4 score rescored from its saved quarterly versions.
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

WINDOW, PATCH = 250, 5
TOKENS = WINDOW // PATCH
CHANNELS = ["ret", "ret_ex", "dv_rel", "range", "gap", "spy", "valid"]
WARMUP = ("1999-01-01", "2003-12-31")
CLIP = 10.0
MARKOV_ORDER, MARKOV_PRIOR = 3, 1000.0
EPOCH_ROWS, BATCH = 300_000, 1024
FIRST_EPOCHS, LATER_EPOCHS, PATIENCE = 15, 5, 2
INNER_MONTHS = 12
SEED = 7
NETS = ("lstm", "attention")
MODELS = ("markov", *NETS)


# ── channels ────────────────────────────────────────────────────────────────

def channels(p: sr.Prices, spy: np.ndarray) -> np.ndarray:
    """(dates, stocks, 7) float32, unscaled; each day uses only that day and earlier."""
    with np.errstate(all="ignore"):
        lc = np.log(np.where(p.closeadj > 0, p.closeadj, np.nan))
        ret = np.clip(np.diff(lc, axis=0, prepend=np.nan), -1, 1)
        ls = np.log(np.where(spy > 0, spy, np.nan))
        spy_r = np.clip(np.diff(ls, prepend=np.nan), -1, 1)
        dv = np.where(p.close * p.volume > 0, p.close * p.volume, np.nan)
        prior = pd.DataFrame(dv).shift(1).rolling(63, min_periods=20).mean().to_numpy()
        dv_rel = np.log(dv / prior)
        rng = np.log(np.where((p.high > 0) & (p.low > 0), p.high / p.low, np.nan))
        open_adj = p.open * p.closeadj / p.close
        gap = np.log(open_adj / np.roll(p.closeadj, 1, axis=0))
        gap[0] = np.nan
    out = np.stack([ret, ret - spy_r[:, None], dv_rel, rng, gap, np.broadcast_to(spy_r[:, None], ret.shape),
                    np.isfinite(p.closeadj).astype(float)], axis=-1).astype(np.float32)
    return out


def scales(arr: np.ndarray, dates: pd.DatetimeIndex, cols: np.ndarray) -> np.ndarray:
    """Per-channel std over the warmup sessions of the stocks sampled in warmup (fixed afterwards)."""
    w = (dates >= WARMUP[0]) & (dates <= WARMUP[1])
    block = arr[w][:, np.unique(cols)]
    sd = np.nanstd(block.reshape(-1, block.shape[-1]), axis=0)
    sd[-1] = 1.0                                                     # the valid flag is not scaled
    sd[~np.isfinite(sd) | (sd == 0)] = 1.0
    return sd.astype(np.float32)


def scaled(arr: np.ndarray, sd: np.ndarray) -> np.ndarray:
    out = np.clip(arr / sd, -CLIP, CLIP)
    return np.nan_to_num(out, nan=0.0).astype(np.float32)


def gather(arr: np.ndarray, i: np.ndarray, j: np.ndarray) -> np.ndarray:
    """(rows, TOKENS, PATCH × channels): the WINDOW sessions ending at day i (inclusive); days before the
    data start are zeros (valid = 0)."""
    off = np.arange(-WINDOW + 1, 1)
    idx = i[:, None] + off[None, :]
    seq = arr[np.clip(idx, 0, None), j[:, None]]
    seq[idx < 0] = 0.0
    return seq.reshape(len(i), TOKENS, PATCH * arr.shape[-1])


# ── Markov chain ────────────────────────────────────────────────────────────

def weekly_states(arr: np.ndarray, breaks: np.ndarray) -> np.ndarray:
    """(dates, stocks) state 0–9 of the five sessions ending each day (−1 if any of them is missing):
    excess-return quintile (fixed warmup breakpoints) × dollar volume above / below its prior mean."""
    ex = pd.DataFrame(arr[..., 1]).rolling(PATCH, min_periods=PATCH).sum().to_numpy()
    dv = pd.DataFrame(arr[..., 2]).rolling(PATCH, min_periods=PATCH).mean().to_numpy()
    q = np.searchsorted(breaks, np.nan_to_num(ex, nan=0.0))
    s = q * 2 + (np.nan_to_num(dv, nan=0.0) > 0)
    return np.where(np.isfinite(ex) & np.isfinite(dv), s, -1).astype(np.int16)


def markov_breaks(arr: np.ndarray, rows: np.ndarray, cols: np.ndarray) -> np.ndarray:
    ex = pd.DataFrame(arr[..., 1]).rolling(PATCH, min_periods=PATCH).sum().to_numpy()[rows, cols]
    return np.nanquantile(ex, [0.2, 0.4, 0.6, 0.8])


def patterns(states: np.ndarray, i: np.ndarray, j: np.ndarray) -> np.ndarray:
    """Code of the last MARKOV_ORDER weekly states (newest first), −1 when any is missing."""
    code = np.zeros(len(i), dtype=np.int64)
    ok = np.ones(len(i), dtype=bool)
    for k in range(MARKOV_ORDER):
        ii = i - PATCH * k
        s = np.where(ii >= 0, states[np.clip(ii, 0, None), j], -1)
        ok &= s >= 0
        code = code * 10 + np.clip(s, 0, None)
    return np.where(ok, code, -1)


def markov_fit(codes: np.ndarray, t: np.ndarray) -> np.ndarray:
    cells = 10 ** MARKOV_ORDER
    ok = codes >= 0
    total = np.bincount(codes[ok], weights=t[ok], minlength=cells)
    count = np.bincount(codes[ok], minlength=cells)
    return total / (count + MARKOV_PRIOR)


def markov_predict(table: np.ndarray, codes: np.ndarray) -> np.ndarray:
    return np.where(codes >= 0, table[np.clip(codes, 0, None)], 0.0)


# ── nets ────────────────────────────────────────────────────────────────────

def device():
    import torch
    return torch.device("mps" if torch.backends.mps.is_available() else "cpu")


def make(name: str):
    import torch
    from torch import nn

    d_in = PATCH * len(CHANNELS)

    class Head(nn.Module):
        def __init__(self):
            super().__init__()
            self.net = nn.Sequential(nn.Linear(64, 32), nn.GELU(), nn.Linear(32, 1))

        def forward(self, h):
            return self.net(h).squeeze(-1)

    class LSTMNet(nn.Module):
        def __init__(self):
            super().__init__()
            self.embed = nn.Linear(d_in, 64)
            self.rnn = nn.LSTM(64, 64, num_layers=2, batch_first=True, dropout=0.1)
            self.head = Head()

        def forward(self, x):
            h, _ = self.rnn(self.embed(x))
            return self.head(h[:, -1])

    class AttnNet(nn.Module):
        def __init__(self):
            super().__init__()
            self.embed = nn.Linear(d_in, 64)
            self.pos = nn.Parameter(torch.zeros(1, TOKENS + 1, 64))
            self.cls = nn.Parameter(torch.zeros(1, 1, 64))
            layer = nn.TransformerEncoderLayer(64, 4, 128, dropout=0.1, batch_first=True, activation="gelu")
            self.enc = nn.TransformerEncoder(layer, 2)
            self.head = Head()
            nn.init.normal_(self.pos, std=0.02)

        def forward(self, x):
            h = torch.cat([self.cls.expand(len(x), -1, -1), self.embed(x)], dim=1) + self.pos
            return self.head(self.enc(h)[:, 0])

    return {"lstm": LSTMNet, "attention": AttnNet}[name]()


def predict(model, arr: np.ndarray, i: np.ndarray, j: np.ndarray, dev) -> np.ndarray:
    import torch
    model.eval()
    out = []
    with torch.no_grad():
        for k in range(0, len(i), 4 * BATCH):
            x = torch.from_numpy(gather(arr, i[k:k + 4 * BATCH], j[k:k + 4 * BATCH])).to(dev)
            out.append(model(x).float().cpu().numpy())
    return np.concatenate(out) if out else np.array([])


def train(model, arr: np.ndarray, fit: tuple, val: tuple, epochs: int, seed: int, dev) -> tuple[object, list]:
    """fit = (i, j, target); val = (i, j, frame with date and the main label). Keeps the epoch with the best
    validation IC; stops after PATIENCE epochs without improvement."""
    import torch
    torch.manual_seed(seed)
    model.to(dev)
    opt = torch.optim.AdamW(model.parameters(), lr=1e-3, weight_decay=1e-2)
    rng = np.random.default_rng(seed)
    fi, fj, ft = fit
    best, best_state, stale, log = -np.inf, None, 0, []
    for e in range(epochs):
        model.train()
        pick = rng.choice(len(fi), min(EPOCH_ROWS, len(fi)), replace=False)
        loss_sum = 0.0
        for k in range(0, len(pick), BATCH):
            b = pick[k:k + BATCH]
            x = torch.from_numpy(gather(arr, fi[b], fj[b])).to(dev)
            y = torch.from_numpy(ft[b].astype(np.float32)).to(dev)
            loss = torch.nn.functional.mse_loss(model(x), y)
            opt.zero_grad()
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            opt.step()
            loss_sum += float(loss) * len(b)
        v = val[2].copy()
        v["s"] = predict(model, arr, val[0], val[1], dev)
        ic = float(B.daily_ic(v, "s", v.columns[1]).mean())
        log.append({"epoch": e, "loss": round(loss_sum / len(pick), 5), "val_ic": round(ic, 5)})
        if ic > best:
            best, best_state, stale = ic, {k: t.detach().clone() for k, t in model.state_dict().items()}, 0
        else:
            stale += 1
            if stale >= PATIENCE:
                break
    model.load_state_dict(best_state)
    return model, log


# ── walk-forward ────────────────────────────────────────────────────────────

def load(n: int) -> tuple[pd.DataFrame, np.ndarray, np.ndarray]:
    """Discovery-period feature-store rows with labels for horizon n, the scaled channel array and the
    weekly Markov states; nothing after the discovery period is read."""
    df = pd.read_parquet(F.OUT / "features_v1.parquet")
    df["date"] = pd.to_datetime(df["date"])
    for k in F.HORIZONS:
        df[f"y{k}_end"] = pd.to_datetime(df[f"y{k}_end"])
    df = df[df["date"] <= pd.Timestamp(B.DISCOVERY[1])]
    df = df[df[f"y{n}"].notna()].reset_index(drop=True)
    dates = sr.trading_days()
    dates = dates[dates <= pd.Timestamp(B.DISCOVERY[1])]
    names = sorted(df["ticker"].unique())
    p = sr.prices(names, dates)
    funds = sr._read("funds", ["ticker", "date", "closeadj"], filters=[("ticker", "in", ["SPY"])])
    funds["date"] = pd.to_datetime(funds["date"])
    spy = funds.set_index("date")["closeadj"].reindex(dates).to_numpy(dtype=float)
    raw = channels(p, spy)
    df["j"] = df["ticker"].map(p.col).astype(int)
    warm = df["date"] <= pd.Timestamp(WARMUP[1])
    breaks = markov_breaks(raw, df.loc[warm, "i"].to_numpy(), df.loc[warm, "j"].to_numpy())
    states = weekly_states(raw, breaks)
    arr = scaled(raw, scales(raw, dates, df.loc[warm, "j"].to_numpy()))
    del raw
    return df, arr, states


def b4_scores(n: int) -> pd.DataFrame:
    """The stage-2b b4 predictions, rescored from its saved quarterly versions (same rows and matrix as
    `gbdt.run_2b`)."""
    import lightgbm as lgb
    df = pd.read_parquet(F.OUT / "features_v1.parquet")
    df["date"] = pd.to_datetime(df["date"])
    for k in F.HORIZONS:
        df[f"y{k}_end"] = pd.to_datetime(df[f"y{k}_end"])
    df = df[df["date"] <= pd.Timestamp(B.DISCOVERY[1])].reset_index(drop=True)
    df = df[df[f"y{n}"].notna()].reset_index(drop=True)
    X = gbdt.matrix_2b(df, gbdt.VARIANTS["b4_sector_label"])
    disc = gbdt._discovery(df, n)
    s = pd.Series(np.nan, index=disc.index)
    for (year, q), rows in disc.groupby([disc["date"].dt.year, (disc["date"].dt.month - 1) // 3]).groups.items():
        m = lgb.Booster(model_file=str(B.CKPT / f"b4_sector_label_y{n}_{year}Q{q + 1}.txt"))
        s.loc[rows] = m.predict(X.loc[rows])
    return pd.DataFrame({"date": disc["date"], "ticker": disc["ticker"], "b4": s})


def walk_forward(df: pd.DataFrame, arr: np.ndarray, states: np.ndarray, n: int) -> tuple[pd.DataFrame, dict]:
    import torch
    y, dev = f"y{n}", device()
    df = df.copy()
    df["t"] = B.target(df, f"y{n}_sec").fillna(0.0)
    disc = gbdt._discovery(df, n)
    i_of_date = df.groupby("date")["i"].first()
    I, J, T = df["i"].to_numpy(), df["j"].to_numpy(), df["t"].to_numpy()
    codes = patterns(states, I, J)
    preds = {k: pd.Series(np.nan, index=disc.index) for k in MODELS}
    nets, log = {}, {k: [] for k in MODELS}
    for year in sorted(disc["date"].dt.year.unique()):
        rows = disc.index[disc["date"].dt.year == year]
        cut = int(i_of_date[df.loc[rows, "date"].min()]) - n
        train_i = df.index[(df["i"] + n) <= cut]
        table = markov_fit(codes[train_i], T[train_i])
        preds["markov"].loc[rows] = markov_predict(table, codes[rows])
        np.save(B.CKPT / f"v3_markov_y{n}_{year}.npy", table)
        tr_dates = df.loc[train_i, "date"]
        inner = tr_dates.max() - pd.DateOffset(months=INNER_MONTHS)
        fit_i = train_i[tr_dates <= inner - pd.Timedelta(days=int(n * 1.5))]
        val_i = train_i[tr_dates > inner]
        for name in NETS:
            first = name not in nets
            model = make(name) if first else nets[name]
            model, ep = train(model, arr, (I[fit_i], J[fit_i], T[fit_i]), (I[val_i], J[val_i], df.loc[val_i, ["date", y]]),
                              FIRST_EPOCHS if first else LATER_EPOCHS, SEED + year, dev)
            nets[name] = model
            preds[name].loc[rows] = predict(model, arr, I[rows], J[rows], dev)
            torch.save(model.state_dict(), B.CKPT / f"v3_{name}_y{n}_{year}.pt")
            log[name].append({"year": int(year), "epochs": ep})
    out = disc.copy()
    for k in MODELS:
        out[k] = preds[k]
    return out, log


def fuse(scored: pd.DataFrame) -> pd.DataFrame:
    rank = lambda c: scored.groupby("date")[c].transform(op.rank_pm1)  # noqa: E731
    b4 = rank("b4")
    for k in MODELS:
        scored[f"fuse_{k}"] = (b4 + rank(k)) / 2
    return scored


def increment(scored: pd.DataFrame, n: int, score: str, base: str = "b4") -> dict:
    y = f"y{n}"
    d = B.daily_ic(scored, score, y) - B.daily_ic(scored, base, y)
    s = B.summary(d.dropna(), n)
    s["holds"] = bool(s and s["all"]["mean"] > 0 and s["all"]["t_nw"] is not None and s["all"]["t_nw"] >= 2
                      and s["first"]["mean"] > 0 and s["second"]["mean"] > 0)
    return s


def b4_path(n: int):
    return F.OUT / f"b4_scores_y{n}.parquet"


def run(n: int) -> dict:
    """Needs `b4_path(n)` from a separate process first (`ml_v3 b4 <n>`): LightGBM and torch each bring
    an OpenMP runtime, and on macOS torch deadlocks after LightGBM has run in the same process."""
    b4 = pd.read_parquet(b4_path(n))
    B.CKPT.mkdir(parents=True, exist_ok=True)
    df, arr, states = load(n)
    scored, log = walk_forward(df, arr, states, n)
    scored = scored.merge(b4, on=["date", "ticker"], how="left")
    scored = fuse(scored)
    scores = (*MODELS, "b4", *[f"fuse_{k}" for k in MODELS])
    metrics = gbdt.evaluate(scored, n, scores)
    res = {"metrics": metrics, "increments": {f"fuse_{k}": increment(scored, n, f"fuse_{k}") for k in MODELS},
           "training": log}
    scored[["date", "ticker", *scores]].to_parquet(F.OUT / f"preds_v3_y{n}.parquet")
    (F.OUT / f"results_v3_y{n}.json").write_text(json.dumps(res, ensure_ascii=False, indent=2, default=str))
    return res


def report_text(results: dict) -> str:
    p = lambda x: "—" if x is None else f"{x:+.4f}"  # noqa: E731
    q = lambda x: "—" if x is None else f"{x:+.2%}"  # noqa: E731
    lines = [f"ML 第三阶段 · 序列模型（挖掘期 {B.DISCOVERY[0][:7]} 至 {B.DISCOVERY[1][:7]}，每年一个版本，行业中性标签训练；确认期未触碰）"]
    for n, r in results.items():
        lines.append(f"\n持有 {n} 个交易日：模型 | IC（t） | IC 前半 / 后半 | 前 10% 超额（t） | 前半 / 后半 | 候选")
        for k, v in r["metrics"].items():
            ic, td = v["ic"], v["top_decile"]
            lines.append(f"{k} | {p(ic['all']['mean'])}（{ic['all']['t_nw']}） | {p(ic['first']['mean'])} / {p(ic['second']['mean'])} | "
                         f"{q(td['all']['mean'])}（{td['all']['t_nw']}） | {q(td['first']['mean'])} / {q(td['second']['mean'])} | "
                         f"{'是' if v['candidate'] else '否'}")
        lines.append("融合相对 b4 的 IC 增量：" + "；".join(
            f"{k} {p(v['all']['mean'])}（t {v['all']['t_nw']}，前半 {p(v['first']['mean'])} / 后半 {p(v['second']['mean'])}）"
            f"{'成立' if v['holds'] else '不成立'}" for k, v in r["increments"].items()))
        for name in NETS:
            eps = [len(x["epochs"]) for x in r["training"][name]]
            lines.append(f"{name} 每个版本训练轮数：{eps}")
    return "\n".join(lines)
