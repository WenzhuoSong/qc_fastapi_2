# Scout：美股机会观察器 架构与开发计划 v1

日期：2026-10-25
状态：P0–P1 已实现，P2/P3 部分实现（见第 12 节）

## 0. 一句话定位

Scout 是一个**给人用的美股机会发现与研究助手**：主动扫描市场、用确定性规则发现候选机会、
用 LLM 把证据组织成可执行的机会卡片，通过 Telegram 和 Dashboard 推送与问答，
并对每个机会做事后追踪打分。

它与 QuantConnect B0 **完全独立**：不输出权重，不发送任何指令，不读写 QC 执行链路。

### 明确不做（防止重走老路）

- 不自动交易，不产出组合权重，不对接任何 broker / QC command；
- 主路径上不做多 agent 辩论，不做层层许可闸门；
- LLM 不计算任何数字，只组织和解释 Python 算好的证据；
- 不追求覆盖所有机会，只追求推送的少而准、可检验。

---

## 1. 核心设计原则

1. **Python 发现，LLM 解释**：机会由确定性 detector 触发；LLM 只负责写 thesis、找反方证据、
   回答问题。所有数字来自 evidence，出卡前校验文本中的数字必须能在 evidence 中找到。
2. **每个机会都要能被证伪**：卡片必须带入场区间、失效条件（止损）、目标、持有周期。
   不能写成「值得关注」这种无法检验的结论。
3. **先有历史基准，再上线**：每个 detector 上线前先在 3 年日线上回放，
   卡片上显示「该形态历史 N 次，持有 X 日胜率 / 平均超额」。
4. **自动记分**：每张卡进入 lifecycle 追踪（触发 / 止盈 / 止损 / 过期），
   按 detector 汇总胜率和超额收益；连续表现不佳的 detector 降权或下线。
5. **注意力是稀缺资源**：Telegram 推送有每日上限和分级，其余放在 Dashboard 里。

---

## 2. 总体架构

```
┌──────────────────────── Ingest（数据层，Adapter 隔离供应商）───────────────────────┐
│ 行情: 日线 / 盘中快照      事件: 财报日历·EPS 惊喜·SEC 8-K/Form4    新闻: 复用现有管线 │
└──────────────┬──────────────────────────┬───────────────────────────┬──────────────┘
               ▼                          ▼                           ▼
┌──────────── Feature（Python，纯计算）────────────────────────────────────────────────┐
│ 趋势/动量/RS 排名/波动/量能 z-score/距 52w 高点/板块相对强度/财报历史跳空幅度 …        │
└──────────────┬─────────────────────────────────────────────────────────────────────┘
               ▼
┌──────────── Detectors（Python，可回放）──────────┐     ┌──── Base Rates（离线回放）────┐
│ breakout / pullback / PEAD / gap / RS 轮动 / …   │◄────│ 每个 detector 的历史胜率/超额 │
└──────────────┬───────────────────────────────────┘     └──────────────────────────────┘
               ▼ detector_hits
┌──────────── Ranker（Python）────────────────────┐
│ 去重 · 合并同票多信号 · 打分 · 每日 Top-N         │
└──────────────┬──────────────────────────────────┘
               ▼ candidates（附带 evidence bundle：数字 + 新闻 + 事件）
┌──────────── Thesis Writer（LLM）────────────────┐
│ thesis / 催化剂 / 反方证据 / 风险 → 结构化输出    │
│ + 数字校验器 + Python 模板兜底                    │
└──────────────┬──────────────────────────────────┘
               ▼ opportunities
┌──────────── Delivery ───────────────┐   ┌──── Tracker（Python）───────────────┐
│ Telegram 推送 / 简报 / 问答          │   │ 盘中/收盘检查入场·止损·目标·过期      │
│ Dashboard 看板 / 个股页 / 对话       │   │ → outcomes → detector scoreboard    │
└──────────────┬──────────────────────┘   └──────────────────────────────────────┘
               ▼
        用户反馈（👍/👎/关注/忽略）→ Ranker 权重
```

问答 agent（Telegram 自由文本 + Dashboard Chat）通过工具调用读取上面所有层的数据。

---

## 3. 数据层

### 3.1 Universe（三层）

| 层 | 规模 | 内容 | 用途 |
|---|---|---|---|
| Core Watchlist | 20–80 | 用户手动维护（`/watch add NVDA`） | 盘中监控、优先推送、新闻全量抓取 |
| Scan Universe | ~1000–1500 | S&P 500 + Nasdaq-100 + 流动性中盘股；过滤 价格 > $5、20 日均成交额 > $20M | 收盘后全量扫描 |
| Context | ~30 | SPY/QQQ/IWM、11 个行业 ETF、主题 ETF（SMH/XBI/KRE/ARKK…）、VIX、TLT、DXY 代理 | 市场与板块背景 |

Scan Universe 每周日刷新一次。

### 3.2 数据源（Adapter 隔离，可替换）

| 数据 | 首选（需付费） | 免费 / 兜底 |
|---|---|---|
| 日线 OHLCV（全市场） | Polygon (Massive) Stocks 套餐，含分组日线接口，一次请求拉全市场 | yfinance（仅限离线回放与研究） |
| 盘中快照 | Polygon 全市场 snapshot / Alpaca 行情 | Alpaca 免费 IEX 行情 |
| 财报日历 / EPS 惊喜 | Finnhub（已接入）/ FMP | Finnhub 免费层 |
| SEC 公告 / Form 4 | SEC EDGAR（免费，需带 User-Agent） | — |
| 分析师评级变动 | FMP / Finnhub 付费层 | 暂缓 |
| 新闻 | 复用 Finnhub + AlphaVantage + RSS 管线 | — |

关键约束：Finnhub 免费层每分钟 60 次请求，不可能每 2 小时对 1500 只票全量抓个股新闻。因此：
- 定时抓取覆盖 Core Watchlist、Context 和**当前活跃机会**的标的；
- 扫描出候选后，再**按需**抓取候选标的近 72 小时的新闻，组装进 evidence。

价格以官网为准，大致预算见第 8 节。所有供应商都实现同一个 `MarketDataProvider` 接口，
上层代码不出现供应商名字。

### 3.3 复用现有代码

| 现有模块 | 在 Scout 中的用途 |
|---|---|
| `cron/pre_fetch_news.py`、`services/finnhub_client.py`、`alphavantage_client.py`、`rss_fetcher.py`、`news_summarizer.py`、`news_evidence.py` | 新闻管线，扩展 ticker 范围 |
| `services/earnings_tracker.py` | 财报日历（universe 改为 Scout 的标的） |
| `services/market_feature_store.py` | 日线特征存储思路，迁到 `scout` schema |
| `services/sector_rotation.py`、`macro_regime_builder.py`、`transmission.py` | 市场背景 / 板块轮动上下文 |
| `services/signal_outcome_labeler.py`、`signal_ledger.py` | 机会结果打标签的思路（远期收益、MFE/MAE） |
| `services/similar_case_retrieval.py` | 卡片中的「历史相似情形」 |
| `agents/base_agent.py` | 工具调用循环（问答 agent） |
| `tools/notify_tools.py` | Telegram 发送与长文本切分 |

---

## 4. 机会引擎

### 4.1 Detector v1 清单（日线级，先做前 5 个）

| # | Detector | 触发逻辑（概要） | 默认持有周期 |
|---|---|---|---|
| D1 | `trend_breakout` | 收盘创 N 日新高，量能 z ≥ 2，处于 50/200 日均线上方，RS 排名前 20% | 10–20 日 |
| D2 | `pullback_in_uptrend` | 强趋势股（RS 前 20%）回撤到 20/50 日均线附近，缩量，RSI(2) 处于超卖 | 5–10 日 |
| D3 | `post_earnings_drift` | 财报 EPS/营收超预期 + 跳空 ≥ 1.5×历史平均财报波动 + 首日收在日内区间上半部 | 10–40 日 |
| D4 | `sector_leader_rotation` | 行业 ETF 的 RS 由弱转强（20 日 RS 斜率翻正），选取行业内 RS 最强的 1–3 只 | 10–20 日 |
| D5 | `unusual_volume_no_news` | 量能 z ≥ 3 且价格异动，但 48 小时内没有可匹配的新闻（信息缺口） | 3–10 日，偏观察 |
| D6 | `pre_earnings_setup` | 财报前 5 个交易日内，趋势强势，历史财报后漂移方向一致 | 事件驱动 |
| D7 | `insider_cluster_buy` | 30 天内 ≥ 3 位内部人公开市场买入（Form 4 code P） | 20–60 日 |
| D8 | `gap_and_hold` | 盘前/开盘跳空 ≥ 3%，有新闻催化，开盘 30 分钟后仍守住跳空 | 1–5 日（盘中） |

空头方向第一版只做「风险提示」（例如持仓或观察股出现破位、财报大幅低于预期），
不推「做空机会」，等多头 detector 的统计稳定之后再说。

每个 detector 是一个纯函数：
`detect(features: FeatureFrame, events: EventFrame, asof: date) -> list[DetectorHit]`，
因此在线扫描和离线回放共用同一份代码。

### 4.2 Base Rates（离线回放）

- 输入：3 年日线 + 当时可得的事件数据；
- 对每次触发计算：持有 5/10/20 日的绝对收益、相对 SPY 和行业 ETF 的超额、MFE/MAE、
  在失效条件之前先触及目标的比例；
- 输出 `scout.detector_base_rates`，按 detector × 市场状态（趋势/震荡/高波动）分桶；
- **已知偏差**：用当前成分股回放存在幸存者偏差，卡片上标注「历史基准偏乐观」。
  后续可接入历史成分股数据来修正。

### 4.3 Ranker

```
score = base_rate_edge          # 该 detector × 当前市场状态的历史超额（收缩到样本量）
      × confluence_bonus        # 同票多个 detector 同时触发
      × context_fit             # 板块/大盘背景是否顺风
      × liquidity_ok
      × feedback_weight         # 用户对该 detector 的 👍/👎 历史
      − event_risk_penalty      # 例如 2 天内有财报，而 detector 不是事件型
```

每天收盘扫描输出 Top-N（默认 8 张）进入卡片生成；score 超过阈值的才推送到 Telegram。

### 4.4 机会卡片（Opportunity Schema）

```json
{
  "id": "opp_20261025_NVDA_D1",
  "ticker": "NVDA",
  "detector": ["trend_breakout", "sector_leader_rotation"],
  "direction": "long",
  "asof": "2026-10-25",
  "setup": "放量突破 3 个月盘整区上沿",
  "thesis": "…（LLM，≤ 120 字）",
  "catalysts": [{"type": "earnings", "date": "2026-11-19"}],
  "evidence": [{"kind": "feature", "name": "volume_z", "value": 2.7}, {"kind": "news", "url": "…"}],
  "counter_evidence": ["…（LLM 反方证据）"],
  "entry_zone": [182.0, 186.5],
  "invalidation": 176.8,
  "targets": [198.0, 210.0],
  "horizon_days": 15,
  "risk_reward": 2.4,
  "base_rate": {"n": 143, "win_rate_10d": 0.58, "avg_excess_10d": 0.021, "regime": "trend"},
  "score": 0.71,
  "confidence": "medium",
  "status": "new"
}
```

入场区间、止损、目标由 Python 根据 ATR 和结构位计算，LLM 不改动这些数字。

### 4.5 Thesis Writer（LLM）

- 输入：evidence bundle（数字 + 新闻摘要 + 事件 + 市场背景 + base rate + 相似历史案例）；
- 输出：Pydantic 结构化 `thesis / catalysts / counter_evidence / key_risk / confidence_reason`；
- 校验：文本中出现的数字必须能在 evidence 中找到（容差匹配），否则重试一次，仍失败则用 Python 模板兜底；
- 模型：thesis 用轻量模型，Top-3 卡片额外做一次「反方审查」（复用 bear 思路，单次调用）；
- LLM 封装在 `scout/llm/` 的 provider 接口后面，模型可配置。

### 4.6 生命周期与追踪

```
new ──(价格进入 entry_zone)──► triggered ──► hit_target / stopped / expired
  └──(horizon 内未触发)──► expired_untriggered
  └──(用户忽略)──► dismissed（仍然记分，用于评估用户的过滤效果）
```

- 盘中（仅 Watchlist + 活跃机会）每 15 分钟检查一次，收盘后对全部活跃机会检查一次；
- 状态变化写入 `opportunity_events`，重要变化（触发、止损）推送 Telegram；
- 结束时写入 `opportunity_outcomes`：实际收益、超额、MFE/MAE、持有天数。

### 4.7 Detector 记分板

按 detector 汇总：上线后样本数、胜率、平均超额、与 base rate 的偏离、用户 👍 率。
规则：样本 ≥ 30 且滚动 8 周平均超额 ≤ 0 → 自动降为「仅 Dashboard，不推送」，在周报里提示人工复核。

---

## 5. 交付层

### 5.1 Telegram（使用一个新 bot，与 noble-flow 运维 bot 分开）

**推送节奏（ET）**

| 时间 | 内容 | 上限 |
|---|---|---|
| 08:45 | 盘前简报：隔夜要闻、今日财报/宏观日历、盘前异动、活跃机会状态 | 1 条 |
| 09:45–15:45 | 盘中告警：高分 `gap_and_hold`、活跃机会的触发/止损 | 每日 ≤ 5 条 |
| 17:45 | 收盘机会：当日 Top 机会卡（score 超过阈值） | ≤ 3 张 |
| 周六 10:00 | 周报：detector 记分板、本周机会结果、下周事件 | 1 条 |

晚上和周末是静默时段，只发周报。

**命令**

| 命令 | 功能 |
|---|---|
| `/ideas` | 当前活跃机会列表 |
| `/idea NVDA` | 对单票现场生成机会分析（即使没有 detector 触发） |
| `/why NVDA` | 解释今天的异动（价格 + 新闻 + 板块） |
| `/scan breakout` | 按指定 detector 即时扫描 |
| `/watch add/rm/list` | 维护 Core Watchlist |
| `/stats` | detector 记分板摘要 |
| 自由文本 | 进入问答 agent |

每张卡片带 inline 按钮：`👍 有用` `👎 没用` `⭐ 关注` `🙈 忽略`，点击写入 `feedback`。

### 5.2 问答 Agent

工具集（只读）：
`get_quote`、`get_bars`、`get_features`、`get_news`、`get_events`（财报/公告/内部人交易）、
`get_opportunities`、`get_detector_stats`、`get_market_context`、`run_detector(ticker|universe)`、
`compare_tickers`。

约束：回答中的数字必须来自工具结果；单轮最多 6 次工具调用；对话按 chat 保存，
可在 Dashboard 中查看。

### 5.3 Dashboard

技术选型：沿用现有 FastAPI + 服务端渲染，加 HTMX 做局部刷新，
图表用 TradingView lightweight-charts（CDN）。单用户，HTTP Basic Auth，与现有 monitor 保持一致。

| 页面 | 内容 |
|---|---|
| Today | 盘前简报、市场背景（指数/板块热力图）、今日新机会、活跃机会状态 |
| Opportunity Board | 全部机会，可按 detector/状态/时间筛选，可看结果 |
| Ticker | K 线 + 入场/止损/目标标注、特征、新闻时间线、该票的历史机会 |
| Scoreboard | detector 实盘 vs base rate、胜率、超额、反馈率 |
| Watchlist | 增删改，附简要状态 |
| Chat | 与 Telegram 共用同一个问答 agent |

---

## 6. 存储（Postgres，新建独立 schema `scout`）

| 表 | 说明 |
|---|---|
| `instruments` | 代码、名称、行业、市值、是否属于 scan universe |
| `watchlist` | Core Watchlist |
| `bars_daily` | 日线（3 年回填 + 每日增量） |
| `intraday_snapshots` | 盘中快照（保留 10 天） |
| `features_daily` | 每票每日特征 |
| `market_events` | 财报、EPS 惊喜、8-K、Form 4 |
| `detector_hits` | 原始触发（含被 ranker 过滤掉的） |
| `detector_base_rates` | 离线回放统计 |
| `opportunities` | 机会卡片 |
| `opportunity_events` | 生命周期事件 |
| `opportunity_outcomes` | 最终结果 |
| `feedback` | 用户反馈 |
| `briefs` | 每次简报的完整内容 |
| `chat_messages` | 问答记录 |
| `scout_job_runs` | 任务运行审计 |

新闻继续使用现有的 `ticker_news_library` / `ticker_news_archive`，Scout 只读。
Migration 用 SQL 文件放在 `scout/migrations/`，沿用现有习惯。

---

## 7. 代码与部署边界

### 7.1 代码位置

建议放在本仓库下新建顶层包 `scout/`，原因是可以直接复用新闻管线和 DB session。

```
scout/
  app.py              # FastAPI：/health、/telegram、dashboard、chat API
  config.py
  data/               # MarketDataProvider 接口 + polygon/alpaca/fmp/edgar 适配器
  universe.py
  features/
  detectors/          # 每个 detector 一个文件 + registry
  backtest/           # base rate 离线回放
  ranker.py
  thesis/             # LLM thesis writer + 数字校验 + 模板兜底
  tracker.py
  delivery/           # telegram 推送、命令、卡片渲染
  agent/              # 问答 agent + 工具
  dashboard/
  jobs/               # 各定时任务入口
  migrations/
tests/scout/
```

**Import fence 测试**：`scout/` 不得 import `services/execution_*`、`qc_command_sender`、
`api/observer`、`audit/` 等 QC 相关模块；noble-flow 也不 import `scout/`。
与现有 execution fence 测试保持同样的写法。

如果之后要彻底分仓，`scout/` 只依赖新闻管线和 `db/session`，很容易拆出去。

### 7.2 Railway 服务（新增，不改动 noble-flow 现有的 6–7 个服务）

| 服务 | 类型 | 命令 / 调度 |
|---|---|---|
| `scout-web` | 常驻 | `uvicorn scout.app:app`（Telegram webhook + Dashboard + Chat） |
| `scout-eod` | cron | 工作日 21:30 UTC：日线增量 → 特征 → 扫描 → 卡片 → 推送 |
| `scout-premarket` | cron | 工作日 12:30 UTC：盘前异动 + 日历 → 简报 |
| `scout-intraday` | cron | 工作日 13:45–19:45 UTC 每 15 分钟：Watchlist + 活跃机会检查 |
| `scout-weekly` | cron | 周六：universe 刷新、记分板、周报 |

UTC 时间按美东夏令时写，冬令时由 job 内部判断美东时间和交易日（复用 `market_calendar`）。
新闻继续由现有的 `cron-pre-fetch-news` 负责，只扩展它的 ticker 列表来源（读 `scout.watchlist` + 活跃机会）。

---

## 8. 成本粗估（每月）

| 项 | 估算 |
|---|---|
| 行情数据（Polygon/Massive 入门套餐，或 Alpaca 免费） | $0–80 |
| 基本面 / 分析师数据（FMP，可选） | $0–30 |
| LLM（每天约 10 张卡 + 2 份简报 + 问答） | $10–40 |
| Railway 新增服务 | $10–20 |

价格以各家官网为准；第一阶段可以先用免费数据源把链路跑通。

---

## 9. 开发计划

### Phase 0 — 地基（约 1 周）

- 建立 `scout/` 包骨架、`scout` schema migration、import fence 测试；
- 实现 `MarketDataProvider` 接口和首个适配器；回填 Scan Universe 3 年日线；
- universe builder；`features_daily` 计算；
- 新建 Telegram bot，部署 `scout-web`（先只有 `/health`）。

验收：一条命令即可算出任意日期全 universe 的特征；fence 测试通过。

### Phase 1 — 机会引擎 MVP（约 2 周）

- D1–D5 detectors + registry；
- base rate 离线回放 → `detector_base_rates`；
- ranker + 机会卡片（先用 Python 模板，再接入 LLM thesis + 数字校验）；
- `scout-eod` job + Telegram 收盘推送 + 反馈按钮 + `/ideas` `/idea` `/watch`。

验收：连续 5 个交易日每天产出 3–8 张卡，每张都有 base rate、入场/止损/目标；
LLM 文本数字校验失败率 < 5%。

### Phase 2 — 追踪与 Dashboard v1（约 1.5 周）

- tracker + lifecycle + outcomes；
- Dashboard：Today / Opportunity Board / Ticker / Scoreboard / Watchlist；
- `scout-weekly` 周报。

验收：任意一张历史卡片都能在 Dashboard 中看到完整生命周期和结果；记分板数据正确。

### Phase 3 — 问答 Agent（约 1.5 周）

- 只读工具集 + agent；Telegram 自由文本 + `/why` + Dashboard Chat；
- 对话持久化，数字来源校验。

验收：准备 20 个典型问题（异动解释、对比、财报前分析、某个 detector 的表现等）作为评测集，
回答正确率和「数字有出处」比例都 ≥ 90%。

### Phase 4 — 盘中与事件（约 2 周）

- 盘中快照适配器 + `scout-intraday`：活跃机会触发/止损告警、D8 `gap_and_hold`；
- 盘前简报 `scout-premarket`；
- D6 `pre_earnings_setup`、D7 `insider_cluster_buy`（接入 EDGAR Form 4）；
- 新闻管线扩展到 Watchlist + 活跃机会。

验收：盘中告警延迟 ≤ 20 分钟；每日推送总数 ≤ 10 条。

### Phase 5 — 持续迭代（滚动）

- 根据反馈和记分板调整 ranker 权重，执行 detector 的降级/下线规则；
- 分析师评级变动、期权隐含波动等新数据；
- 可选：只读引用 QC 持仓，在卡片上标注「B0 已持有」，避免重复暴露（仍然不向 QC 输出任何东西）；
- 修正幸存者偏差（接入历史成分股数据）。

### 总体验收指标（上线 8 周后复盘）

- 每日推送 ≤ 10 条，用户 👍 率 ≥ 50%；
- 至少 3 个 detector 实盘样本 ≥ 30，且 10 日平均超额 > 0；
- 机会卡片中可证伪字段（入场/止损/目标/周期）完整率 100%；
- 数据新鲜度：EOD 扫描在收盘后 2 小时内完成。

---

## 10. 主要风险

| 风险 | 缓解 |
|---|---|
| 复杂度失控、重走老路 | 「明确不做」清单；每个 Phase 只加验收标准中写明的东西 |
| 推送疲劳 | 每日上限 + 分级 + 静默时段 + 低分只进 Dashboard |
| LLM 编造数字或叙事 | 数字校验器、模板兜底、反方审查、evidence 必须带出处 |
| Detector 过拟合 | 参数先固定为常识值；样本外看实盘记分板；不针对回放结果做调参 |
| 历史基准偏乐观 | 卡片标注偏差；后续接入历史成分股数据 |
| 数据源限流或停服 | Adapter 隔离；免费源兜底；`scout_job_runs` 记录失败并告警 |

---

## 11. 待确认的开放决策

1. **数据预算**：先用免费源（Alpaca IEX + yfinance 回放 + Finnhub 免费层）跑通 Phase 0–2，
   还是一开始就买 Polygon/Massive？（建议：Phase 0–1 先用免费源，Phase 4 盘中功能上线前再决定。）
2. **代码位置**：本仓库下 `scout/`（建议），还是新开一个仓库。
3. **Railway**：放在同一个 project 下作为新服务（共用 Postgres），还是新建 project。
4. **初始 Watchlist**：请给出 20–50 只你关心的股票 / 主题。

---

## 12. 决策记录与实施状态（2026-10-25）

**已确认的决策**
- 数据：先用免费源跑通（yfinance 日线 + Finnhub 免费层 + Wikipedia 成分股）；
- 代码：放在本仓库 `scout/`，同时清理旧的交易流水线代码（agents、strategies、140+ 个 services、16 个 cron）；
- Railway：放在同一个 project，共用 Postgres（独立 `scout` schema）；
- 初始 Watchlist：按 2026-10 的热门主题选取 36 只（AI 算力/网络/存储、超大规模云、AI 软件、AI 电力与核能、能源、加密、量子、航天等），见 `scout/universe.py`。

**已实现**
- P0：`scout` schema 与 migration、数据接口与 yfinance 适配器、标的池（S&P 500 + S&P 400 + Nasdaq-100 + 主题 ETF，约 950 只）、3 年日线回填（约 70 万行，约 30 秒）、特征面板、隔离测试；
- P1：5 条规则（D1–D5；D3 用「放量跳空」代替财报数据，财报信息在实盘中通过 Finnhub 补充）、历史基准回放（按市场状态分桶）、排序器（含单规则上限）、LLM thesis + 数字校验 + 模板兜底、收盘扫描任务、Telegram 推送与反馈按钮；
- P2：生命周期追踪（与回放共用同一个模拟器）、记分板、Dashboard（今日 / 机会看板 / 个股页 K 线 / 记分板 / Watchlist / 问答）、周报任务；
- P3：问答 agent（8 个只读工具），Telegram 自由文本、`/why`、Dashboard 问答页。

**与设计的差异**
- 所有规则的止损距离入场中点至少 1 个 ATR（避免止损落在日内噪声里）；
- 首轮回放结果：放量异动、强势回调有小幅正超额；趋势突破接近 0；板块轮动、放量跳空为负。排序器已据此自动压低后两者的分数，没有为了好看去调参。

**未实现（P4 及以后）**
- 盘中监控与盘前简报（需要盘中数据源）、D6 财报前布局、D7 内部人买入（SEC Form 4）；
- 历史成分股修正幸存者偏差。
