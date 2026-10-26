# noble-flow 项目总览（Scout）

更新：2026-10-26

本文是 review 和后续开发的入口：系统现在是什么、线上怎么跑、代码在哪里、怎么扩展、还有哪些已知问题。
设计背景和取舍见 [scout_opportunity_observer_design_v1_zh.md](scout_opportunity_observer_design_v1_zh.md)。
旧交易系统里仍有复用价值的模块见 [legacy_reusable_catalog_zh.md](legacy_reusable_catalog_zh.md)（tag `legacy-trading-system`）。

---

## 1. 现在是什么

仓库里只有一个产品：**Scout 美股机会观察器**（`scout/`）。每个交易日收盘后扫描约 950 只美股和 ETF，用规则找机会，
数字由 Python 计算、文字由 LLM 撰写，生成机会卡推送到 Telegram，并追踪每张卡直到出结果；同时支持 Telegram 问答和 Dashboard。它从不交易。

另有一条共用的**新闻管线**（`cron/pre_fetch_news.py`）。它每 2 小时抓取 Finnhub、Alpha Vantage 和 RSS 新闻，写入新闻表，Scout 从这些表读取。

**已归档、不在代码树里的东西：**

| 内容 | 位置 |
|---|---|
| 旧 agentic 交易系统（10 阶段 LLM 流水线、执行与风控） | git tag `legacy-trading-system`；可复用部分见 `legacy_reusable_catalog_zh.md` |
| QC 观察器（QuantConnect 实盘的只读监控、审计、Dashboard；QC 自 2026-09-25 起停止推送，2026-10-26 下线） | git tag `qc-observer-final`；本地完整归档 `~/Work/trading/qc_observer_archive/`（代码快照、设计文档、数据库备份、Railway 配置、恢复步骤） |
| QC 策略研究文件 | `~/Work/trading/strategy_research_lab/imported/qc_fastapi_2_research_20261026/` |

---

## 2. 线上部署（Railway 项目 `noble-flow`）

代码服务从 GitHub `WenzhuoSong/qc_fastapi_2` 的 **`main` 分支自动部署**，共用一个 Dockerfile（默认启动 Scout web），各服务通过自己的启动命令区分。

| 服务 | 启动命令 | 调度（UTC） |
|---|---|---|
| `scout-web` | `uvicorn scout.web.app:app` | 常驻 · https://scout-web-production-39ab.up.railway.app |
| `scout-eod` | `python -m scout.jobs.eod` | `30 21 * * 1-5`（美东收盘后；休市日自动跳过） |
| `scout-weekly` | `python -m scout.jobs.weekly` | `0 14 * * 6`（周六） |
| `cron-pre-fetch-news` | `python -m cron.pre_fetch_news` | `50 */2 * * *`（24/7） |
| `main-service` | **不部署**（已断开 GitHub） | 只用来存放共享密钥 |
| `Postgres` | — | — |

### 环境变量策略

**共享密钥只存放在 `main-service` 一处**，其他服务一律用 `${{main-service.X}}` 引用。换 key 时只改 main-service，然后重新部署引用它的服务（Railway 在部署时解析引用）。

| 变量 | 存放位置 | 使用者 |
|---|---|---|
| `DATABASE_URL` | 引用 `${{Postgres.DATABASE_URL}}` | 全部 |
| `OPENAI_API_KEY`、`FINNHUB_API_KEY`、`ALPHAVANTAGE_API_KEY` | main-service | 新闻 cron、Scout |
| `TG_BOT_TOKEN`、`TG_CHAT_ID` | main-service | Scout 推送与任务告警、新闻 cron（@wenzhuo_qc_bot） |
| `DASHBOARD_USER`、`DASHBOARD_PASSWORD` | main-service | Scout Dashboard（`SCOUT_DASHBOARD_*` 引用它们） |
| `SCOUT_TG_WEBHOOK_SECRET` | scout-web | Telegram webhook 校验 |
| `WEBHOOK_SECRET`、`QC_OBSERVER_*`、`FASTAPI_EXECUTION_AUTHORITY` | main-service | 已归档的 QC 观察器（保留以便恢复，当前没有程序使用） |

可选的 Scout 调参变量（不设则用默认值）：`SCOUT_LLM_MODEL`（gpt-4o-mini）、`SCOUT_TOP_N`（8）、`SCOUT_PUSH_THRESHOLD`（0.6）、
`SCOUT_MAX_PUSH_CARDS`（3）、`SCOUT_MIN_PRICE`（5）、`SCOUT_MIN_DOLLAR_VOLUME`（2000 万）、`SCOUT_HISTORY_YEARS`（3）。
如果以后想给 Scout 单独开一个 bot，设置 `SCOUT_TG_BOT_TOKEN` / `SCOUT_TG_CHAT_ID` 即可覆盖 `TG_*`。

Telegram webhook：@wenzhuo_qc_bot → `scout-web/telegram`，带 secret token 校验。

---

## 3. Scout 怎么工作

### 3.1 数据流

```
                 ┌──────────── 每个交易日收盘后（scout-eod）────────────┐
 yfinance 日线 ─► bars_daily ─► FeaturePanel ─► 5 条规则 ─► 排序 ─► 证据包 ─► LLM 文字 ─► 机会卡 ─► Telegram
 Wikipedia 成分股 ─► instruments           │                         ▲   ▲                 │
 Finnhub 财报日历 ─► market_events ─────────┼─────────────────────────┘   │                 ▼
 新闻 cron ─► ticker_news_library ──────────┼─────────────────────────────┘          opportunities
                                            ▼                                              │
                               历史回放（同一套规则代码）◄── 生命周期追踪（同一个模拟器）◄──┘
                               detector_base_rates            opportunity_events / outcomes
```

### 3.2 一次收盘扫描（`scout/pipeline.py::run_eod`）

0. **交易日判断**（`scout/market_calendar.py`）：周末和美股休市日直接跳过；同一个交易日只扫描并推送一次（`--force` 可以重跑）。
1. **prepare**：执行 migration、补 watchlist、增量拉最近 10 天日线。如果检测到拆股或复权变化（重叠日收盘价偏差 > 2%），就重拉该股全部历史。
2. **load_panel**：读最近 420 天日线，构建宽表特征面板（日期 × 标的）。
3. 写入当天的 `features_daily` 快照，供 Dashboard 和问答使用。
4. **tracker.update_all**：推进所有活跃机会的状态（触发 / 达标 / 止损 / 到期）。
5. 刷新 Finnhub 财报日历；读取历史基准（没有就现算）。
6. **scan**：跑 5 条规则 → 打分 → 每条规则最多 3 张 → Top 8。
7. 每张卡：拉新闻和财报 → 组装证据包 → LLM 写文字（带数字校验，失败退回模板）→ 存库。
8. 推送：一条收盘简报，加最多 3 张评分 ≥ 0.6 的卡（带 👍👎⭐🙈 按钮）。

### 3.3 规则（`scout/detectors/rules.py`）与历史基准

基准口径：每笔扣除 **10 个基点往返成本**（`simulate.ROUND_TRIP_COST`），并和"**同期一直持有这只股票**"比较
（按该股平均日漂移计算同样天数的持有收益）。"相对持有 ≤ 0"表示择时并没有比直接持有更好；这个口径还能部分抵消幸存者偏差。

| 规则 | 中文名 | 状态 | 周期 | 样本 | 对 SPY（未扣费） | **扣费后相对持有** | 跑赢持有 |
|---|---|---|---|---|---|---|---|
| `trend_breakout` | 趋势突破 | 上线 | 20 日 | 627 | +0.3% | **−0.6%** | 36% |
| `pullback_in_uptrend` | 强势回调 | 上线 | 10 日 | 1161 | +1.0% | **+0.4%** | 46% |
| `power_gap` | 放量跳空 | 上线 | 20 日 | 317 | ≈ 0 | **−0.5%** | 40% |
| `sector_rotation` | 板块轮动龙头 | 上线 | 20 日 | 1708 | −0.6% | **−1.1%** | 37% |
| `volume_shock` | 放量异动 | 上线（watch） | 10 日 | 180 | +1.7% | **+1.8%** | 53% |
| `zscore_oversold` | z 分数超跌 | **候选（仅回放）** | 10 日 | 895 | — | **+0.4%** | 42% |

所有规则的止损距入场中点至少 1 个 ATR。`live=False` 的候选规则会参与回放并显示在记分板上，但不扫描、不推送。
`zscore_oversold`（2026-10-26 评估）：和强势回调有 34% 重叠，不重叠的部分 +0.48%，但在上升市只有 +0.11%、止损率 50%，
所以暂不上线，等积累更多震荡市样本后再评估。

### 3.4 评分（`scout/opportunities.py::score_candidate`）

```
score = 0.45·规则优势（扣费后相对持有，按样本量收缩） + 0.25·相对强度 + 0.15·多规则共振 + 0.15·盈亏比
        × 0.9（watch 类）  − 0.15（5 天内有财报且不是事件型规则）  + 0.05（在 watchlist 中）
        × 反馈权重（0.8–1.2，来自 👍👎）
```

### 3.4.1 卡片上的"大盘 vs 个股"

每只股票都计算 60 日 beta 和相关性（相对 SPY），当日涨跌拆成**大盘贡献**（前一日 beta × SPY 当日涨跌）和**个股自身**两部分，
同时给出 5 日和 20 日的个股自身累计涨跌。卡片、个股页、LLM 证据和问答（`/why`）都会用到。另外，风险画像特征也写入了每日快照：
20 日 z 分数、布林带位置、20 日年化波动率、60 日最大回撤。

### 3.5 LLM 的边界（`scout/thesis.py`）

- LLM 只写文字：标题、逻辑、催化剂、反方证据、风险、置信度。入场、止损、目标等数字全部由 Python 计算。
- 交给 LLM 的是格式化后的证据（百分比字符串），校验也按这份证据做。文字里出现证据中没有的数字，就带着错误重试一次；仍失败就退回 Python 模板。
- 催化剂只能是新闻或财报事件；历史基准要写成"该规则历史……"；样本 < 30 时置信度不能是 high。

---

## 4. 代码结构

```
qc_fastapi_2/
├── scout/                         ── Scout（约 3,500 行）
│   ├── config.py           52     ScoutSettings（环境变量）
│   ├── db.py              133     独立 async engine、SQL 辅助函数、apply_migrations、job_run 记账
│   ├── migrations/001_init.sql    scout schema（14 张表，幂等，每次启动时执行）
│   ├── universe.py        219     种子 watchlist（36 只）、主题和行业 ETF、Wikipedia 成分股
│   ├── data/
│   │   ├── provider.py     29     MarketDataProvider 协议（上层代码不出现数据供应商的名字）
│   │   └── yahoo.py        76     yfinance 实现（每批 150 只，失败重试一次）
│   ├── bars.py            117     日线存储：回填、增量、复权检测、读取（整批写入）
│   ├── features.py        185     FeaturePanel：收益、均线、ATR、量比、RSI、相对强度、行业相对强弱、beta 拆分、风险画像、市场状态
│   ├── detectors/
│   │   ├── __init__.py     81     Detector / Levels 契约，base_mask，r_targets
│   │   └── rules.py       165     5 条上线规则 + 1 条候选：mask(panel) 和 levels(row)，止损至少 1 ATR
│   ├── simulate.py        108     括号单模拟器（回放和实盘追踪共用）
│   ├── base_rates.py      200     历史回放（扣费、对比一直持有）→ 按市场状态分桶的统计
│   ├── opportunities.py   235     扫描、评分、证据包、冷却期、反馈权重、存库
│   ├── context.py         162     新闻（新闻库 + 归档 + Finnhub 按需拉取）、财报日历
│   ├── thesis.py          201     LLM 文字（格式化证据 + 数字校验）+ 模板兜底
│   ├── tracker.py         142     生命周期、结果记录、记分板
│   ├── telegram.py        284     卡片渲染、推送、命令、反馈回调、友好报错
│   ├── agent.py           232     问答 agent：8 个只读工具，最多 6 轮工具调用
│   ├── pipeline.py        236     run_eod / run_weekly / prepare / ensure_base_rates / 交易日判断 / 简报渲染
│   ├── market_calendar.py 162     美股交易日与节假日（从旧系统取回）
│   ├── web/app.py         292     FastAPI：/health、/telegram、Dashboard 6 个页面
│   ├── web/views.py       115     HTML / CSS（亮暗主题）/ K 线图（lightweight-charts）
│   └── jobs/              ...     _run.py（日志、job_runs、失败告警）、eod、weekly、backfill
│
├── cron/pre_fetch_news.py  683    ── 共用新闻管线（约 60 只标的，按 Finnhub 免费层限速）
├── services/                      新闻客户端：finnhub、alphavantage、rss；news_summarizer；cron_audit；retry_protocol
├── tools/notify_tools.py    81    新闻任务的 Telegram 告警
├── db/                            新闻表的 ORM（4 个模型）、session、2 个 SQL migration
├── constants.py             61    新闻覆盖的标的：17 只 ETF + Scout watchlist / 活跃机会
├── config.py                23    新闻管线的 Settings
├── tests/                         unittest，45 个测试
├── docs/                          本总览、Scout 设计、旧系统复用目录
└── Dockerfile / railway.toml      单镜像，默认启动 Scout web
```

### 数据库表

| schema | 表 | 写入方 | 说明 |
|---|---|---|---|
| `scout` | `instruments`、`watchlist` | universe | 标的池（约 950 只）与 watchlist |
| `scout` | `bars_daily`、`features_daily` | bars / pipeline | 3 年日线（约 71 万行）、每日特征快照（JSON） |
| `scout` | `market_events` | context | Finnhub 财报日历 |
| `scout` | `detector_base_rates` | base_rates | 规则 × 市场状态的历史统计 |
| `scout` | `opportunities`、`opportunity_events`、`opportunity_outcomes` | opportunities / tracker | 机会卡与完整生命周期 |
| `scout` | `feedback`、`chat_messages`、`briefs`、`job_runs` | telegram / agent / pipeline | 反馈、对话、简报、任务记账 |
| `public` | `ticker_news_library`（48 小时）、`ticker_news_archive`（2023-12 起约 2.7 万条）、`macro_news_cache`、`cron_run_log` | 新闻 cron | Scout 只读；问答 `get_news(days≤90)` 会读归档 |
| `public` | 17 张 `qc_*` 表和 1 个视图 | 无（已归档） | QC 观察器的历史数据，保留未删，另有备份 |

---

## 5. 开发与发布流程

```bash
# 本地
python -m unittest discover -s tests -t .
export DATABASE_URL=postgresql://user@localhost:5432/scout
python -m scout.jobs.backfill            # 约 1 分钟：标的池 + 3 年日线 + 历史基准 + 特征
python -m scout.jobs.eod --no-push --force  # 收盘扫描（不推送；--force 在周末或已扫过时也重跑）
SCOUT_DASHBOARD_USER=u SCOUT_DASHBOARD_PASSWORD=p uvicorn scout.web.app:app --port 8765
```

- **发布**：功能分支（当前是 `scout-rewrite`）→ 测试通过 → 推到 `main`，Railway 自动部署 4 个代码服务（main-service 已断开，不会被部署）。
- **Schema 变更**：新增 `scout/migrations/00N_xxx.sql`（必须幂等），服务启动和任务执行时会自动应用。
- **手动对生产跑任务**：用 `DATABASE_PUBLIC_URL` 在本地执行 `python -m scout.jobs.<name>`。本地经公网连生产库延迟较高，写入一定要批量。

---

## 6. 必须守住的约定

1. **隔离**：`scout/` 不得 import 任何交易或执行相关代码；新闻管线（`services/`、`cron/`、`db/`、`tools/`）不得 import `scout`，保证 Scout 出问题时新闻照常更新。由 `tests/test_scout_core.py::ScoutFenceTests` 保证。
2. **不交易**：仓库里没有、也不能有任何向 QC 或券商下单的代码路径。
3. **数字来自 Python**：LLM 只写文字，新增的 LLM 输出也要接入数字校验。
4. **不对回放结果调参**：历史基准是用来衡量规则的，不是优化目标；新规则的参数用常识值。
5. **回放和实盘同一套代码**：规则只写成 `mask` / `levels` 纯函数；追踪只用 `simulate.py`。
6. **日志不得含密钥**：httpx 的 INFO 日志已关闭（Telegram / Finnhub 的 URL 带 token），有回归测试覆盖。

---

## 7. 扩展指南

| 想做的事 | 改哪里 | 注意 |
|---|---|---|
| 加一条规则 | `scout/detectors/rules.py` 追加 `Detector` | 之后跑 backfill（或等周六）生成历史基准；没有基准的规则排序会偏低 |
| 加一个特征 | `scout/features.py::build_panel`；需要展示就加进 `SNAPSHOT_FEATURES` | 宽表计算，避免逐列赋值 |
| 换或加数据源 | `scout/data/` 新增一个实现 `MarketDataProvider` 的类，在 `get_provider()` 切换 | 上层代码不用改 |
| 加 Telegram 命令 | `scout/telegram.py::dispatch` + `HELP_TEXT` | 回复用 HTML，记得 `esc()` |
| 给问答加工具 | `scout/agent.py`：`TOOLS` 加 schema，`HANDLERS` 加实现 | 工具只读；结果会被截断到 12k 字符 |
| 加 Dashboard 页面 | `scout/web/app.py` 加路由（带 `require_auth`），导航在 `views.layout` | 服务端渲染，没有构建步骤 |
| 加定时任务 | `scout/jobs/<name>.py` 用 `_run.run()` 包装；Railway 新建 cron 服务 | 自动写 `job_runs`，失败会发 Telegram 告警 |
| 调整推送 / 评分 | 环境变量 `SCOUT_*` 或 `score_candidate` 权重 | 改权重前先看记分板数据 |

---

## 8. 当前状态与已知问题

**已上线（P0–P3）**：数据层、5 条规则、历史基准、排序、LLM 文字、收盘推送、反馈按钮、生命周期追踪、记分板、
Dashboard（今日 / 机会看板 / 个股 K 线 / 记分板 / Watchlist / 问答）、周报、Telegram 问答（新闻可查 90 天）、交易日判断。

| 问题 | 影响 | 建议 |
|---|---|---|
| 历史基准有幸存者偏差 | 基准偏乐观 | 接入历史成分股数据 |
| yfinance 是免费、非官方的数据源 | 偶尔会失败或延迟；收盘后才有数据 | 盘中功能上线前换成付费数据源（`MarketDataProvider` 已预留） |
| `power_gap` 和 `sector_rotation` 的历史超额 ≤ 0 | 这些卡评分低，一般不会推送 | 积累约 8 周实盘数据后，按记分板决定降级或下线 |
| `main-service` 这个名字已经名不副实（只存密钥） | 只是命名问题 | 以后可以改用 Railway 的环境级 Shared Variables，然后删掉它 |

---

## 9. 路线图（下一步候选）

1. **P4 盘中与事件**：盘中监控活跃机会的触发和止损、盘前简报、财报前布局规则（D6）、SEC Form 4 内部人集中买入（D7）。需要盘中数据源。
2. **反馈闭环**：每周按记分板和 👍👎 自动降级或提示规则；在 Dashboard 上展示反馈统计。
3. **问答增强**：同行业对比工具、财报历史跳空统计、"为什么没推这只"的解释。
4. **数据质量**：历史成分股、付费行情、期权隐含波动。
5. **QC 量化监控**：如果需要，从 `~/Work/trading/qc_observer_archive/` 恢复或参考它重新设计。

---

## 附：2026-10-25 数据库清理记录

旧交易系统的 30 张表（约 275 MB，169,468 行）已从生产库删除：Agent 分析与步骤日志、策略信号与结果、旧 QC 快照、
持仓因子、执行日志、记忆表等。新闻相关的三张表（`ticker_news_library`、`ticker_news_archive`、`macro_news_cache`）都保留。
删除的表里没有需要迁移的数据：`market_daily_features` 只多出 45 只旧策略用的杠杆 / 反向 / 债券 ETF，yfinance 随时可以重拉；
`macro_events_cache` 是过期缓存；`earnings_calendar` 和 `news_evidence_snapshots` 是空表。

- 备份：`~/Work/trading/backups/noble_flow_legacy_tables_20261025.dump`（58 MB，pg_dump custom 格式；
  已恢复到临时库校验，30 张表的行数与生产库完全一致）
- 恢复（只恢复某一张表时加 `-t 表名`）：
  ```bash
  /opt/homebrew/opt/postgresql@18/bin/pg_restore --no-owner -d "$DATABASE_PUBLIC_URL" ~/Work/trading/backups/noble_flow_legacy_tables_20261025.dump
  ```

## 附：2026-10-26 QC 观察器下线记录

- 代码：观察器的 18 个代码文件、8 个测试、观察器 migration 和 3 份契约文档已从仓库删除，另有 11 个未跟踪的设计文档和运维工具移出仓库。
  完整快照保存在 git tag `qc-observer-final` 和 `~/Work/trading/qc_observer_archive/code/`。
- Railway：`monitor`、`cron-morning-health`、`cron-post-market-report` 已删除，配置记录在归档的 `railway_services.json`。
  `main-service` 已断开 GitHub、停止部署，只保留变量；域名保留未删。
- 数据：17 张 `qc_*` 表保留在库里，同时备份到 `qc_observer_archive/db/qc_observer_tables_20261026.dump`（4,393 行）。
- 恢复步骤见 `~/Work/trading/qc_observer_archive/README.md`。
