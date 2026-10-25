# noble-flow 项目总览

更新：2026-10-25 · 线上版本：`main` @ `e689e1d`

本文是 review 和后续开发的入口：讲清楚系统现在是什么、线上怎么跑、代码在哪里、怎么扩展、还有哪些已知问题。
设计背景和取舍见 [scout_opportunity_observer_design_v1_zh.md](scout_opportunity_observer_design_v1_zh.md)。

---

## 1. 两个独立产品

| | **Scout**（主线） | **QC 观察器**（维持） |
|---|---|---|
| 定位 | 美股机会观察器：主动扫描、排序、解释、追踪机会，Telegram + Dashboard 推送和问答 | QuantConnect 实盘算法的只读伴随系统：接收事件、投影、审计、通知 |
| 代码 | `scout/` | `main.py`、`api/`、`audit/`、`dashboard/`、`services/qc_observer_*` |
| 与交易的关系 | 从不交易，不输出权重 | `execution_authority=none`，永久不向 QC 发指令 |

两者共用一个 Postgres（不同表 / schema）和一条新闻管线（`cron/pre_fetch_news.py`）。
彼此不 import 对方的代码，由测试强制保证（见 §6）。

---

## 2. 线上部署（Railway 项目 `noble-flow`）

所有代码服务都从 GitHub `WenzhuoSong/qc_fastapi_2` 的 **`main` 分支自动部署**，共用一个 Dockerfile，靠各自的启动命令区分。

| 服务 | 产品 | 启动命令 | 调度（UTC） |
|---|---|---|---|
| `scout-web` | Scout | `uvicorn scout.web.app:app` | 常驻 · https://scout-web-production-39ab.up.railway.app |
| `scout-eod` | Scout | `python -m scout.jobs.eod` | `30 21 * * 1-5`（美东收盘后，冬令时/夏令时都适用） |
| `scout-weekly` | Scout | `python -m scout.jobs.weekly` | `0 14 * * 6`（周六） |
| `main-service` | QC 观察器 | `uvicorn main:app`（Dockerfile 默认） | 常驻 · 接收 QC 事件 |
| `monitor` | QC 观察器 | `uvicorn dashboard.app:app` | 常驻 · 观察器 Dashboard |
| `cron-morning-health` | QC 观察器 | `python -m cron.morning_health` | `0 13 * * 1-5` |
| `cron-post-market-report` | QC 观察器 | `python -m cron.post_market_report` | `35 20 * * 1-5` |
| `cron-pre-fetch-news` | 共用 | `python -m cron.pre_fetch_news` | `50 */2 * * *`（24/7） |
| `Postgres` | 共用 | — | — |

### 环境变量策略

**共享密钥只存在 `main-service` 一处**，其他服务一律用 `${{main-service.X}}` 引用。换 key 时只改 main-service，然后重新部署引用它的服务（Railway 在部署时解析引用）。

| 变量 | 存放位置 | 使用者 |
|---|---|---|
| `DATABASE_URL` | 引用 `${{Postgres.DATABASE_URL}}` | 全部 |
| `OPENAI_API_KEY`、`FINNHUB_API_KEY`、`ALPHAVANTAGE_API_KEY` | main-service | 新闻 cron、Scout |
| `TG_BOT_TOKEN`、`TG_CHAT_ID` | main-service | 通知 cron、Scout（@wenzhuo_qc_bot） |
| `WEBHOOK_SECRET`、`QC_OBSERVER_*`、`FASTAPI_EXECUTION_AUTHORITY=none` | main-service | 观察器 |
| `DASHBOARD_USER/PASSWORD` | monitor | 观察器 Dashboard；Scout Dashboard 引用同一套 |
| `SCOUT_TG_WEBHOOK_SECRET` | scout-web | Telegram webhook 校验 |

可选的 Scout 调参变量（不设则用默认值）：`SCOUT_LLM_MODEL`（gpt-4o-mini）、`SCOUT_TOP_N`（8）、`SCOUT_PUSH_THRESHOLD`（0.6）、
`SCOUT_MAX_PUSH_CARDS`（3）、`SCOUT_MIN_PRICE`（5）、`SCOUT_MIN_DOLLAR_VOLUME`（2000 万）、`SCOUT_HISTORY_YEARS`（3）。
如果以后想给 Scout 单独开一个 bot，设置 `SCOUT_TG_BOT_TOKEN` / `SCOUT_TG_CHAT_ID` 即可覆盖 `TG_*`。

Telegram webhook：@wenzhuo_qc_bot → `scout-web/telegram`，带 secret token 校验。
运维通知是主动推送，不经过 webhook，所以两类消息共用一个 bot 互不影响。

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

1. **prepare**：执行 migration、补 watchlist、增量拉最近 10 天日线。如果检测到拆股或复权变化（重叠日收盘价偏差 > 2%），就重拉该股全部历史。
2. **load_panel**：读最近 420 天日线，构建宽表特征面板（日期 × 标的）。
3. 写入当天的 `features_daily` 快照，供 Dashboard 和问答使用。
4. **tracker.update_all**：推进所有活跃机会的状态（触发 / 达标 / 止损 / 到期）。
5. 刷新 Finnhub 财报日历；读取历史基准（没有就现算）。
6. **scan**：跑 5 条规则 → 打分 → 每条规则最多 3 张 → Top 8。
7. 每张卡：拉新闻和财报 → 组装证据包 → LLM 写文字（带数字校验，失败退回模板）→ 存库。
8. 推送：一条收盘简报，加最多 3 张评分 ≥ 0.6 的卡（带 👍👎⭐🙈 按钮）。

### 3.3 规则（`scout/detectors/rules.py`）与首轮历史基准

| 规则 | 中文名 | 类型 | 周期 | 历史样本 | 平均超额（vs SPY） |
|---|---|---|---|---|---|
| `trend_breakout` | 趋势突破 | setup | 20 日 | 627 | +0.3% |
| `pullback_in_uptrend` | 强势回调 | setup | 10 日 | 1161 | +1.0% |
| `power_gap` | 放量跳空 | setup | 20 日 | 317 | ≈ 0 |
| `sector_rotation` | 板块轮动龙头 | setup | 20 日 | 1708 | −0.6% |
| `volume_shock` | 放量异动 | watch | 10 日 | 180 | +1.7% |

所有规则的止损距入场中点至少 1 个 ATR。历史基准用当前成分股回放，存在幸存者偏差，卡片上有标注。

### 3.4 评分（`scout/opportunities.py::score_candidate`）

```
score = 0.45·历史超额（按样本量收缩） + 0.25·相对强度 + 0.15·多规则共振 + 0.15·盈亏比
        × 0.9（watch 类）  − 0.15（5 天内有财报且不是事件型规则）  + 0.05（在 watchlist 中）
        × 反馈权重（0.8–1.2，来自 👍👎）
```

### 3.5 LLM 的边界（`scout/thesis.py`）

- LLM 只写文字：标题、逻辑、催化剂、反方证据、风险、置信度。入场、止损、目标等数字全部由 Python 计算。
- 交给 LLM 的是格式化后的证据（百分比字符串），校验也按这份证据做。文字里出现证据中没有的数字，就带着错误重试一次；仍失败就退回 Python 模板。
- 催化剂只能是新闻或财报事件；历史基准要写成"该规则历史……"；样本 < 30 时置信度不能是 high。

---

## 4. 代码结构

```
qc_fastapi_2/
├── scout/                         ── Scout（约 3,000 行）
│   ├── config.py           52     ScoutSettings（环境变量）
│   ├── db.py              133     独立 async engine、SQL 辅助函数、apply_migrations、job_run 记账
│   ├── migrations/001_init.sql    scout schema（14 张表，幂等，每次启动执行）
│   ├── universe.py        219     种子 watchlist（36 只）、主题/行业 ETF、Wikipedia 成分股
│   ├── data/
│   │   ├── provider.py     29     MarketDataProvider 协议（上层不出现供应商名字）
│   │   └── yahoo.py        76     yfinance 实现（分批 150 只，失败重试一次）
│   ├── bars.py            117     日线存储：回填 / 增量 / 复权检测 / 读取
│   ├── features.py        155     FeaturePanel：收益、均线、ATR、量比、RSI、相对强度分位、行业相对强弱、市场状态
│   ├── detectors/
│   │   ├── __init__.py     81     Detector / Levels 契约，base_mask，r_targets
│   │   └── rules.py       142     5 条规则：mask(panel) 和 levels(row)
│   ├── simulate.py        108     括号单模拟器（回放和实盘追踪共用）
│   ├── base_rates.py      173     历史回放 → 按市场状态分桶的统计
│   ├── opportunities.py   235     扫描、评分、证据包、冷却期、反馈权重、存库
│   ├── context.py         151     新闻（共享新闻库 + Finnhub 按需拉取）、财报日历
│   ├── thesis.py          201     LLM 文字 + 数字校验 + 模板兜底
│   ├── tracker.py         142     生命周期、结果记录、记分板
│   ├── telegram.py        284     卡片渲染、推送、命令、反馈回调、友好报错
│   ├── agent.py           229     问答 agent：8 个只读工具，最多 6 轮工具调用
│   ├── pipeline.py        212     run_eod / run_weekly / prepare / ensure_base_rates / 简报渲染
│   ├── web/app.py         293     FastAPI：/health、/telegram、Dashboard 6 个页面
│   ├── web/views.py       115     HTML / CSS（亮暗主题）/ K 线图（lightweight-charts）
│   └── jobs/              ...     _run.py（日志、job_runs、失败告警）、eod、weekly、backfill
│
├── main.py                 57     ── QC 观察器 API 入口
├── api/observer.py        136     /api/observer/* 事件接收与状态
├── audit/                 575     只读审计 worker（决策 / 成交、目标 / 实际）
├── dashboard/             406     观察器 Dashboard（monitor 服务）
├── services/
│   ├── qc_observer_*.py  1391     事件接收、投影、健康度
│   ├── qc_webhook_auth.py  145    HMAC 请求校验
│   ├── execution_authority.py 73  执行围栏（永远 none）
│   ├── execution_policy.py 497    QC 标的角色表（仅被 constants.resolve_universe 使用）
│   ├── finnhub_client.py  291     ┐
│   ├── alphavantage_client.py 147 ├ 新闻管线（共用）
│   ├── rss_fetcher.py     154     │
│   ├── news_summarizer.py 167     ┘
│   ├── cron_audit.py      213     cron 运行记录
│   └── retry_protocol.py  292     重试工具
├── cron/
│   ├── pre_fetch_news.py  683     共用新闻 cron（覆盖 QC 标的 + Scout watchlist + 活跃机会）
│   ├── morning_health.py   39     观察器早间健康检查
│   └── post_market_report.py 81   观察器盘后报告
├── tools/notify_tools.py   81     运维 Telegram 发送
├── db/                            观察器 / 共用表的 ORM（models.py 含已冻结的旧表定义）+ SQL migrations
├── constants.py           115     新闻覆盖的标的池（resolve_universe）
├── config.py               34     观察器 Settings
├── tests/                         unittest，108 个测试
├── docs/                          设计文档、本总览、观察器契约
├── Dockerfile / railway.toml      单镜像，按启动命令区分服务
└── requirements.txt
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
| `public` | `ticker_news_library`、`macro_news_cache` | 新闻 cron | Scout 只读 |
| `public` | `qc_observer_*`、`qc_*` 投影 / 审计表 | 观察器 | Scout 不碰 |
| `public` | 旧交易系统的表（`agent_analysis`、`execution_log` 等） | 无 | 已冻结，暂不删除 |

---

## 5. 开发与发布流程

```bash
# 本地
python -m unittest discover -s tests -t .
export DATABASE_URL=postgresql://user@localhost:5432/scout
python -m scout.jobs.backfill            # 约 1 分钟：标的池 + 3 年日线 + 历史基准 + 特征
python -m scout.jobs.eod --no-push       # 收盘扫描（不推送）
SCOUT_DASHBOARD_USER=u SCOUT_DASHBOARD_PASSWORD=p uvicorn scout.web.app:app --port 8765
```

- **发布**：功能分支（当前是 `scout-rewrite`）→ 测试通过 → 推到 `main`，Railway 自动部署全部 8 个服务。
  合并会同时重新部署观察器，改动观察器相关代码时要额外小心。
- **Schema 变更**：新增 `scout/migrations/00N_xxx.sql`（必须幂等），服务启动和任务执行时会自动应用。
- **手动对生产跑任务**：用 `DATABASE_PUBLIC_URL` 在本地执行 `python -m scout.jobs.<name>`。本地经公网连生产库延迟较高，写入一定要批量。

---

## 6. 必须守住的约定

1. **隔离**：`scout/` 不得 import `services.execution*`、`services.qc_*`、`api.*`、`audit`、`dashboard`、`main`；观察器代码也不得 import `scout`。由 `tests/test_scout_core.py::ScoutFenceTests` 保证。
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
Dashboard（今日 / 机会看板 / 个股 K 线 / 记分板 / Watchlist / 问答）、周报、Telegram 问答。
第一次定时收盘扫描：2026-10-26（周一）。

| 问题 | 影响 | 建议 |
|---|---|---|
| QC 观察器自 2026-09-25 起没有收到任何事件 | 观察器状态显示 degraded | 确认 QC 实盘算法是否还在运行 |
| 历史基准有幸存者偏差 | 基准偏乐观 | 接入历史成分股数据 |
| yfinance 是免费、非官方的数据源 | 偶尔会失败或延迟；收盘后才有数据 | 盘中功能上线前换成付费数据源（`MarketDataProvider` 已预留） |
| `power_gap` 和 `sector_rotation` 的历史超额 ≤ 0 | 这些卡评分低，一般不会推送 | 积累约 8 周实盘数据后，按记分板决定降级或下线 |
| `db/models.py` 还保留着 50 个 ORM 类（含旧交易表） | 只是代码冗余，不影响运行 | 旧表连续 90 天无读写后导出并删除，同时清理模型 |
| `services/execution_policy.py` 只为新闻标的池留着 | 冗余 | 新闻标的池改成独立配置后删除 |
| 新闻 cron 覆盖约 110 只标的，按 Finnhub 免费层限速 | 一轮约 5–10 分钟 | 标的数量继续增加时考虑付费层 |

## 9. 路线图（下一步候选）

1. **P4 盘中与事件**：盘中监控活跃机会的触发和止损、盘前简报、财报前布局规则（D6）、SEC Form 4 内部人集中买入（D7）。需要盘中数据源。
2. **反馈闭环**：每周按记分板和 👍👎 自动降级或提示规则；在 Dashboard 上展示反馈统计。
3. **问答增强**：同行业对比工具、财报历史跳空统计、"为什么没推这只"的解释。
4. **数据质量**：历史成分股、付费行情、期权隐含波动。
5. **清理**：旧表与 `db/models.py`、`execution_policy.py`。
