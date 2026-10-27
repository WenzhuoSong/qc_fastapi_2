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
| `scout-eod` | `python -m scout.jobs.eod` | `0 22 * * 1-5`（北京时间 06:00，美东 18:00 夏令时 / 17:00 冬令时；休市日自动跳过） |
| `scout-intraday` | `python -m scout.jobs.intraday` | `*/15 13-21 * * 1-5`（只在开盘时段执行：持仓与机会的价位提醒、重要新闻） |
| `scout-premarket` | `python -m scout.jobs.premarket` | `0 13 * * 1-5`（北京时间 21:00，夏令时开盘前 30 分钟、冬令时 90 分钟；休市日跳过） |
| `scout-options` | `python -m scout.jobs.options` | `35,50 19,20 * * 1-5`（只在美东 15:30–16:00 开盘时执行一次：研究池期权快照） |
| `scout-research` | `python -m scout.jobs.research` | `30 23 * * 1-5`（收盘扫描之后：SEC 财报与文件、预期与评级、持仓与做空、FRED、价格交叉校验、存储检查） |
| `scout-weekly` | `python -m scout.jobs.weekly` | `0 14 * * 6`（周六） |
| `cron-pre-fetch-news` | `python -m cron.pre_fetch_news` | `*/30 * * * *`：工作日美东 04:00–20:00 每 30 分钟抓核心约 100 只；05:30、16:30 抓全量约 900 只；其余时间每 2 小时 |
| `Postgres` | — | — |

### 环境变量策略

**共享密钥只存放在环境级 Shared Variables 一处**（Railway 项目 → Settings → Shared Variables），各服务一律用 `${{shared.X}}` 引用。换 key 时只改 Shared Variables，然后重新部署引用它的服务（Railway 在部署时解析引用）。

| 变量 | 存放位置 | 使用者 |
|---|---|---|
| `DATABASE_URL` | 引用 `${{Postgres.DATABASE_URL}}` | 全部 |
| `OPENAI_API_KEY`、`FINNHUB_API_KEY`、`ALPHAVANTAGE_API_KEY` | Shared Variables | 新闻 cron、Scout |
| `TG_BOT_TOKEN`、`TG_CHAT_ID` | Shared Variables | Scout 推送与任务告警、新闻 cron（@wenzhuo_qc_bot） |
| `DASHBOARD_USER`、`DASHBOARD_PASSWORD` | Shared Variables | Scout Dashboard（`SCOUT_DASHBOARD_*` 引用它们） |
| `SCOUT_TG_WEBHOOK_SECRET` | scout-web | Telegram webhook 校验 |
| `WEBHOOK_SECRET`、`QC_OBSERVER_*`、`FASTAPI_EXECUTION_AUTHORITY` | 已从 Railway 删除 | 已归档的 QC 观察器；原值备份在 `~/Work/trading/qc_observer_archive/main-service-variables.env`（权限 600） |

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
| `trend_breakout` | 趋势突破 | **候选**（2026-10-26 降级） | 20 日 | 627 | +0.3% | **−0.6%** | 36% |
| `pullback_in_uptrend` | 强势回调 | 上线 | 10 日 | 1161 | +1.0% | **+0.4%** | 46% |
| `power_gap` | 放量跳空 | 上线（边缘） | 20 日 | 303 | +1.0% | **+0.3%**（trail_low） | 32% |
| `sector_rotation` | 板块轮动龙头 | **候选**（2026-10-26 降级） | 20 日 | 1708 | −0.6% | **−1.1%** | 37% |
| `volume_shock` | 放量异动 | 上线（watch） | 10 日 | 180 | +1.7% | **+1.8%** | 53% |
| `zscore_oversold` | z 分数超跌 | **候选（仅回放）** | 10 日 | 895 | — | **+0.4%** | 42% |

| `breakout_retest` | 突破回踩 | **候选（仅回放）** | 20 日 | 483 | — | **−0.6%** | — |

**上线标准（2026-10-26）**：全样本扣费后相对持有 > 0 才上线。据此趋势突破（−0.6%）和板块轮动龙头（−1.1%）降为候选：
继续回放、显示在记分板，已开出的机会跟踪到结束（首页折叠显示并标注"不建议据此开仓"），但不再扫描和推送。
放量跳空 +0.3% 勉强达标、在震荡和下跌市为负，属边缘规则。周报的「规则体检」每周列出跑输持有的上线规则和跑赢持有的候选规则，
是否调整由人决定（转正还需要前后两段样本都成立）。

所有规则的止损距入场中点至少 1 个 ATR。`live=False` 的候选规则会参与回放并显示在记分板上，但不扫描、不推送。
`zscore_oversold`（2026-10-26 评估）：和强势回调有 34% 重叠，不重叠的部分 +0.48%，但在上升市只有 +0.11%、止损率 50%，
所以暂不上线，等积累更多震荡市样本后再评估。

**追高与位置（2026-10-26）**：规则都写成声明式条件（`scout/detectors/__init__.py::Condition`），筛选和"为什么入选"用的是同一组条件对象，
真实数据上逐笔核对 5184 次命中，没有一例不一致。历史基准额外按"入选时距 52 周高点"分成三档（3% 以内 / 3–10% / 10% 以上）。
排序时优先用"同规则、同位置"的扣费后相对持有，这一档样本不足再退回按市场状态分组的基准；当这一档为负时，卡片会标注"⚠️ 追高风险"。
总体上贴近高点的信号跑输持有 0.95%，但各条规则差别很大，比如放量异动在贴近高点时仍是 +2.3%，所以按规则逐条度量，而不是一刀切。

**突破回踩（D）评估结论：不采用**。等回踩再入场（−0.59%）并不比直接追价（−0.56%）好。突破后从未回踩的 26% 才是好突破
（追价买入扣费后 +5.05%），会回踩的 74% 往往是正在失败的突破（−2.48%），等回踩恰好会买到这一批。趋势突破的问题在规则本身，不在入场方式。

**价位与出场（2026-10-26）**：
- **入场区间**：信号日收盘价附近，按 ATR 定宽；3 个交易日内价格进入区间才算成交（突破回踩类为 10 日）。
- **保护性止损**：放在各规则的结构位下方（突破位、5 日低点、50 日线、跳空日低点），并且距入场中点至少 1 个 ATR。
- **目标**：入场中点加 2R / 3R（R = 入场中点到止损的距离）；强势回调和 z 分数超跌用 20 日高点或 20 日均线。
- **出场方式**（`ExitPolicy`，属于规则定义的一部分）：
  - `fixed`：跌破止损离场，到目标 1 全部止盈，到期离场；
  - `trail_low`：先用保护性止损，持有满 N 日后止损上移到近 N 日最低价（只升不降），不设固定止盈，最长持有 60 日。
- 6 种出场方式的回放对比（扣费、对比一直持有、前后两半样本分别验证）：
  - 只有放量跳空改用 `trail_low(10)`：从 −0.38% 提高到 +0.32%，前后两半样本都有改善，也符合财报后漂移的理论；
  - 均值回归类（强势回调）保持固定目标，跟进止损会把利润回吐；
  - 其余规则改用跟进止损也没有改善；
  - "盈利 1R 后把止损移到成本价"在每条规则上都会拉低表现。
- 实盘追踪每天更新当前止损，存在 `opportunities.current_stop`。

**个股页布局**：K 线和成交量分区显示，价位只标在右侧价格轴上，入场区间用浅色阴影带表示，并标出信号、入场、离场的箭头；
图表纵轴会自动包含止损和目标价，鼠标悬停显示当日开高低收。右侧是「交易计划」面板（类似 TradingView 的多头仓位工具：
价格梯形图加上每个价位的距现价百分比和 R 倍数）；没有交易计划的股票显示「关键价位」面板。

### 3.4 评分（`scout/opportunities.py::score_candidate`）

```
score = 0.45·规则优势（扣费后相对持有；优先取同规则同位置，按样本量收缩） + 0.25·相对强度 + 0.15·多规则共振 + 0.15·盈亏比
        × 0.9（watch 类）  − 0.15（5 天内有财报且不是事件型规则）  + 0.05（在 watchlist 中）
        × 反馈权重（0.8–1.2，来自 👍👎）
```

### 3.4.1 卡片上的"大盘 vs 个股"

每只股票都计算 60 日 beta 和相关性（相对 SPY），当日涨跌拆成**大盘贡献**（前一日 beta × SPY 当日涨跌）和**个股自身**两部分，
同时给出 5 日和 20 日的个股自身累计涨跌。卡片、个股页、LLM 证据和问答（`/why`）都会用到。另外，风险画像特征也写入了每日快照：
20 日 z 分数、布林带位置、20 日年化波动率、60 日最大回撤。

### 3.4.2 AI 解读：多视角工作流（`scout/insight.py` + `scout/knowledge/`）

```
Python 确定性组装证据包 ──► 6 个视角，各带「读数 + 定义」与方法论检查清单 ──► LLM 一次结构化输出 ──► 程序校验 ──► 缓存（附证据包和校验记录）
```

- **数据字典** `scout/knowledge/features.yaml`：每个特征的中文名、单位、定义、解读区间、注意事项。
  它同时驱动 Dashboard 的字段名和说明、Scout 分析的阈值、交给 LLM 的"数值 + 读数"。新特征没有字典条目时测试会失败。
- **价格结构** `scout/structure.py`：道氏理论波段（高低点抬高或降低、跌破或突破最近波段）、主要支撑和阻力、上涨日与下跌日成交量比。
- **6 个视角** `scout/knowledge/lenses/*.md`：道氏趋势、支撑阻力与形态（以《股市趋势技术分析》为主）、动量与相对强度（Jegadeesh-Titman、O'Neil、Minervini）、
  均值回归与波动（Lehmann、Connors、Bollinger）、大盘与板块、催化与事件（PEAD）。每个视角写明看什么、检查清单、确认和矛盾信号、常见陷阱、出处。可以直接编辑。
- **校验**：数字必须出自证据包（52 周、50 日线、RSI(14) 这类指标名不算数字）；引用的证据 key 必须存在；6 个视角各出现一次；
  驱动力（个股自身 / 大盘与板块 / 混合）必须与规则判定一致；趋势结论不能与 Python 判定相反；描述的波段形态不能与 swing_trend 相反。不通过就重试一次，仍不通过则退回规则读数。
- 默认模型 `SCOUT_INSIGHT_MODEL=gpt-4o`；Dashboard 的"生成 AI 解读"和 Telegram 的 `/why` 共用这套流程，按数据日期缓存。
- **评测**：`python -m scout.jobs.eval_insight` 在 20 只固定标的上统计首次通过率、回退率和失败类型，结果写入 `scout.briefs`（kind = `insight_eval`）。
  2026-10-26 基线：首次通过率 95%，最终 100% 由 LLM 生成、没有回退，平均 12.6 秒。人工抽查发现并修复了 KO 的波段形态误读（已加入定性校验）。

### 3.4.3 市场全景（`scout/sectors.py`、`scout/sector_news.py`、`scout/market_focus.py`，页面 `/market`）

- **轮动**：JdK 相对轮动图（RRG）的开放近似。相对强度比 = （板块 ETF / SPY）÷ 其 50 日均值；动量 = 相对强度比 ÷ 10 日前的值。
  据此分为 领先 / 转弱 / 落后 / 改善 四个象限，并画出最近 8 周的轨迹。
- **宽度与参与度**（按板块内流动性足够的成分股计算）：站上 50/200 日线比例、20 日净新高、涨跌日成交量比、CMF、成交额热度和占比。
  **资金流向类指标都是日线代理**，不是真实的买卖单；只有 CMF 和涨跌量比方向一致时才写成"流入"或"流出"，否则写"信号分歧"。
- **热度**：六项指标在板块之间排名后取平均，≥ 0.67 为热，≤ 0.33 为冷。每个分项都会展示。
- **新闻**：新闻覆盖扩展到每个板块成交额最大的 5 只股票（约 100 只标的）。
  - 关注度 = 最近 7 天 ÷ 之前 7 天，只统计两段都有覆盖的标的；
  - 排除被标为不相关的新闻，以及挂在板块 ETF 上的 RSS 关键词匹配；
  - 同一篇文章出现在 3 个以上板块时，归为全市场新闻。
- **AI 市场关注点**：每天收盘后生成一次。程序会校验：数字能追溯到证据、引用存在、11 个板块全部覆盖、冷热标签和规则判定一致，
  并且资金流向和关注度的描述与规则标签一致（正文里点名的板块也要检查）。
- **用在哪里**：
  - 每张机会卡增加"板块环境"一行；
  - 个股页的 Scout 分析和 AI 解读都带上板块背景；
  - 收盘简报增加板块热度、轮动和关注点主题；
  - Telegram `/sectors` 命令；周报增加本周轮动。

### 3.4.5 交易者视角（2026-10-26 体验复盘后）

- **大盘状态**：`regime`（SPY 与 200/50 日线）只作为历史基准的分组；展示用的状态还看短期趋势（20/50 日线、20 日涨跌）、
  宽度（流动性股票站上 50 日线比例）和科技成长相对 SPY 的 20 日差，例如"长期上升 · 短期回调 · 宽度一般 · 科技成长明显落后"。
- **历史结局**：卡片和交易计划显示该规则回放的结局分布（达标 / 止损 / 跟踪止损 / 到期）、扣费收益中位数和平均持有天数。
  目标价是最好情况，不是预期结果。
- **事件驱动标记**：新闻标题命中并购/出售、药监/临床、增发/融资、监管/诉讼时，卡片标注"事件驱动"，提示技术规则统计参考价值有限。
- **仓位**：在「我的交易」设置账户规模、单笔风险（默认 1%）和单只上限（默认 20%），卡片按固定比例风险给出股数。
- **交易日志**（`scout/portfolio.py`，表 `scout.trades`、`scout.user_settings`）：从卡片"记录入场"预填，记录实际成交、改止损、平仓；
  显示浮动/已实现盈亏、R 倍数、全部止损的潜在亏损；Telegram `/trades`；盘前简报会检查持仓是否已跌破止损。
- **集中度**：活跃机会和持仓里同一板块 ≥ 3 个或同一细分行业 ≥ 2 个时提示"实际上接近一笔仓位"。
- **文字清洁**：LLM 正文里的英文字段名和括号引用（如"（swing_trend）""（news_3）"）在保存和展示时统一去掉或换成中文名。

### 3.4.7 异动归因与基本面（2026-10-27，起因：CORT 10-23 暴跌 20%，AI 解读却没有解释）

专业机构分析单只股票异动的顺序：先量化是否异常 → 找催化剂（公司消息 → 同业传导 → 评级 → 板块/大盘 → 无消息）→ 判断是否改变投资逻辑 → 看历史上同类事件之后怎么走 → 下一个催化剂。Scout 按这个顺序实现：

- **异动检测**（`scout/events.py`）：个股自身涨跌（扣除 β × SPY）≥ 事前 60 日个股自身波动的 3σ（且 ≥ 4%），或涨跌 ≥ 8%，或 3 倍放量且涨跌 ≥ 3%。
  记录标准差倍数、跳空与盘中的拆分（判断消息发布时段）、量比、板块和 SPY 当天表现、异动后至今涨跌。
- **原因检索**（`scout/event_cause.py`）：异动窗口（前一日收盘至当日 20:00 美东）的公司新闻、SEC 文件（8-K 等）、内部人交易、财报日期，
  加一次网络检索（OpenAI `web_search_preview`，约 5 秒、几美分）。网络检索是找"同业传导"的关键：CORT 的原因是 Genmab 的三期数据，
  这条新闻挂在 Genmab 名下，Finnhub 的同业列表里也没有 Genmab。AI 按固定分类（财报/指引、监管/临床、竞争对手/同业传导、分析师评级、
  并购/交易、融资/增发、法律/调查、管理层变动、大盘/板块、无明确消息）归因并引用来源；程序校验引用存在、数字来自证据、
  非"无明确消息"必须有来源、"大盘/板块"必须由板块或大盘解释大部分涨跌。结果按（代码, 日期）缓存在 `scout.stock_events`。
- **同类异动之后**（`scout.event_base_rates`，每周与历史基准一起重算）：全市场约 1.4 万次异动之后 5/20 日相对 SPY 的表现，
  按方向 × 时段 × 是否在 200 日线上方分组。整体结论：中位数接近 0、跑赢比例约 50%，平均值被少数大幅反弹拉高——
  异动之后没有稳定的延续或反转，要看具体原因。
- **基本面与预期**（`scout/fundamentals.py`，Finnhub 免费数据，按日缓存）：估值（市值、市盈率、市销率、市净率）、增长、利润率、
  资产负债、分析师买入占比及三个月变化、近 4 季 EPS 超预期次数、90 日内部人公开市场净买卖。
- **AI 解读**新增「异常事件」「基本面与预期」两个视角（共 8 个）。近 10 个交易日内有显著异动时，输出必须先在"近期异动"里解释它
  （写明日期、幅度、原因、影响），并在异常事件视角引用它，否则校验不通过；没有数据的视角由程序填"不适用"。
- **个股页**新增「异动」卡（原因、来源链接、同类异动统计，未检索的可点「检索异动原因」）和「基本面与预期」卡。
- **盘中提醒**：持仓或活跃机会盘中涨跌 ≥ 8% 时自动检索原因，随提醒推送。

### 3.4.8 价位指导（2026-10-28，价位图 2026-10-27 起每日保存）

个股页图表右侧是「价位参考」，参照专业平台（Trading Central 的枢轴与情景、O'Neil 的买点与 5% 买入区、TradingView 的仓位工具、
期权隐含波动区间）给出一张价位图（`scout/levels.py::price_map`）。**价位每天收盘后计算并按日期保存**（`scout.price_levels`，
研究任务为标的池预先计算，其他股票在当天第一次打开页面时计算），盘中不变；实时价格只更新状态（`levels.status`）。

1. **关键价位**：支撑 1–3、阻力 1–3（近一年主要波段高低点，1.5% 以内的合并），20 / 50 / 200 日线，52 周高点，
   按距现价排列。
2. **多空分界与情景**：分界取下方最近的结构位（波段低点或 50 / 200 日线；下降趋势中取上方最近的波段高点或 50 日线）。
   首选情景说明守住分界时上方先看哪里，备选情景说明收盘跌破（或站上）后看哪里；现价越过分界时提示"若收盘确认，按备选情景处理"。
3. **参考区间**：期权隐含到最近到期日的 ±1σ 区间（期权快照质量合格时），分析师目标价区间与中位数。
4. **买点**：回调买点（下方 4 个 ATR 内最近的支撑或均线，下降趋势不给）和突破买点（上方 15% 内的第一个波段高点；
   下降趋势中叫「反转确认」）。买入区间 = 买点到买点上方 min(0.5 ATR, 5%)，高于上沿就是"已脱离买入区间，不追"。
   止损结构优先（2026-10-27 起，`levels.stop_for`）：放在"判断失效"的结构位下方 0.25 ATR——回调买点就是买点本身那个支撑，
   突破买点是突破前整理区的波段低点或 50 / 200 日线，按成交价的计划是成交价下方最近的波段低点或 50 / 200 日线（20 日线太常被穿越，不算）；
   离入场不足 1 个 ATR 时加宽到 1 个 ATR（加宽区间里正好有另一个结构位就放在它下方）；超过 min(3.5 ATR, 10%) 视为太远，
   回调用 2 个 ATR，突破用"突破位下方 1 个 ATR（突破失败即离场）"，并写明原因；显示百分比和依据。价位图按规则版本保存（`version`），
   规则改动后重新计算、旧版本保留；已记录的持仓沿用买入时的计划；
   目标 1 = 2R 以内的第一个上方价位，否则 2R（到达减仓一部分）；目标 2 = 4R 以内的下一个价位，否则 3R（余下仓位用移动止损）；
   时间止损：回调 10 个、突破 20 个交易日不到目标 1 就离场。R:R < 1 标出"性价比低"。
   状态：等回调 / 未突破（距买点 x%）、在买入区间内、跌到回调买点下方（等收盘站回）、跌破止损（买点失效）、已脱离区间（不追）。
5. **持仓由系统管理止损止盈**（`portfolio.manage_trade`，2026-10-27 起）：「我的交易」只填代码、日期、成交价、股数。
   - 初始计划用买入前最后一个收盘的价位图（不看买入之后的数据）：成交价在某个买入区间内就用那个买点的止损和目标，
     否则按成交价用同一套方法算（止损取至少 1 个 ATR 外最紧的结构位，目标 1 / 2 同上），时间止损 15 个交易日。
   - 每天收盘后（`run_eod`）更新：止损按吊灯止损（买入以来最近 22 个收盘的最高价 − 3×ATR）只升不降，且须低于最新收盘；
     到目标 1 后止盈改看目标 2，过了目标 2 只用移动止损。买入当天只算收盘价是否到达目标（当天最高价可能在成交之前）。
   - 变动写入 `scout.trades.plan.history`，推送到 Telegram；个股页「我的持仓」显示当前止损的依据、止盈阶段、时间止损进度。
   - 盘中监控和盘前简报按系统止损和止盈提醒。用户不再手动改止损；Scout 不会下单。
   - **分批止盈**（`levels.exit_plan`）：目标 1 卖约 1/3、目标 2 再卖约 1/3，余下约 1/3 交给移动止损。每一档给出逻辑
     （目标来自哪个价位、目标 1 在 2R 以上时"余下仓位即使止损，整笔也大致不亏"）、价位、R、比例和建议股数。
     每档规定"之后应剩多少股"，所以建议会按实际卖出自我校正：到了某档没卖或少卖，提示还应卖多少；多卖了，下一档相应减少；
     一次越过两档时合并为一次卖出。盘中到达止盈位的提醒附带建议股数。
   - **记录卖出**（`scout.trade_sells`，`portfolio.add_sell`）：在「我的交易」、个股页「我的持仓」或 Telegram
     `/sell 代码 股数 价格 [日期]`（股数可写 all）记录，可部分卖出。记录后立即重算剩余股数、已实现 / 总盈亏、R（按初始止损）、
     未平仓风险和下一步建议；卖完自动平仓，出场价为按股数加权的平均卖出价。误录可在个股页撤销。
6. **规则信号**（有活跃机会时）：一行说明规则原计划，以及现价是否还在原入场区间内；高于区间时明确"此时买入已不属于规则入场"。
   规则原计划和假设成交价折叠在「系统跟踪」里：价位在信号当天固定、与历史回放一致，只用于给规则记分，不是用户的交易。

这些都是价位参考，不是经过回放检验的规则信号，页面上注明。持仓股另有「我的持仓」：浮动盈亏、R、系统止损和止盈（不提供"浮盈 1R 后移到成本价"，出场回放显示它降低了每条规则的表现）。

### 3.4.6 更新频率与盘中提醒（2026-10-27）

| 时段 | 新闻 | 价格 / 分析 |
|---|---|---|
| 美东 04:00–09:30 盘前 | 核心约 100 只每 30 分钟；05:30 全量约 900 只 | 09:00（冬令时 08:00）盘前简报，含期货与盘前价 |
| 09:30–16:00 盘中 | 核心每 30 分钟 | 每 15 分钟检查持仓和活跃机会的价位（进入入场区间、跌破止损、到达目标）和重要新闻，Telegram 提醒，每个事件只推一次 |
| 16:00–20:00 盘后 | 核心每 30 分钟；16:30 全量 | 18:00（冬令时 17:00）收盘扫描：日线、规则、机会结算、AI 卡片、市场关注点 |
| 其余时间 / 周末 | 每 2 小时核心 | 周六周报 |

- 新闻每只股票只调用一次 Finnhub（财报临近标记读 `scout.market_events`），按 1.1 秒节流，免费额度内全量一轮约 15–20 分钟。
- 盘中提醒不改变机会状态，成交和出场仍由收盘后用同一个模拟器结算；价格来自 Yahoo，可能有几分钟延迟。
- 自选股只在出现事件驱动新闻（并购、药监、增发、诉讼或被标为重大事件）时提醒，持仓和活跃机会的直接相关新闻都会提醒。
- Alpha Vantage 的新闻接口已改为付费，2026-10-27 移除；失效的 Reuters / Yahoo RSS 换成 CNBC Markets 和 Investing.com。

### 3.4.4 盘前简报（`scout/premarket.py`，cron `scout-premarket`，北京时间 21:00）

- **Python 整理的事实**：
  - 隔夜：标普/纳指/罗素期货、VIX、10 年美债、美元、原油、黄金、比特币相对上一交易日收盘（Yahoo 5 分钟数据，含盘前）；
  - 活跃机会的盘前价和"位置判断"（入场区间内 / 低于区间 / 高于上沿追高风险 / 跌破止损 / 到达目标 1）；
  - 自选股盘前涨跌超过 2% 的；
  - 财报：昨日盘后与今日盘前已公布的（EPS 实际 vs 预期、超预期幅度、盘前反应）、今日待公布、本周后续，
    范围是活跃机会、自选和成交额前 150 的股票；
  - 经济日历：Forex Factory 的免费周历（美国中高影响事件，美东/北京时间、预期、前值；没有实际值）。
    Finnhub 的经济日历不在免费套餐内（403）；
  - 上一交易日收盘以来的个股新闻和宏观新闻，以及昨晚的市场关注点。
- **AI 部分**只写标题、要点、个股关注和风险。校验数字、引用、个股必须在证据包内，机会的位置只能按 Python 的判断表述。
  未通过校验时退回规则整理。
- **推送与查看**：Telegram 推送；`/premarket` 命令；Dashboard 首页顶部卡片（当天的展开，往日的折叠）。
- 已生成过的交易日不重复推送，`--force` 可重跑；休市日跳过。

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
│   ├── agent.py           236     问答 agent：8 个只读工具，最多 6 轮工具调用（remember=False 用于一次性生成）
│   ├── analysis.py        200     个股页「Scout 分析」：趋势 / 价格结构 / 归因 / 相对强度 / 位置 / 风险 / 量能 / 事件 / 规则距离
│   ├── structure.py        90     道氏波段结构、支撑阻力、涨跌日量比
│   ├── insight.py         300     AI 解读多视角工作流：证据包 → 结构化输出 → 校验 → 缓存
│   ├── knowledge/                 数据字典 features.yaml + 6 个分析视角 lenses/*.md（可直接编辑）
│   ├── pipeline.py        236     run_eod / run_weekly / prepare / ensure_base_rates / 交易日判断 / 简报渲染
│   ├── market_calendar.py 162     美股交易日与节假日（从旧系统取回）
│   ├── sectors.py         240     板块轮动（RRG 近似）、宽度、资金流代理、热度
│   ├── sector_news.py     150     板块新闻：关注度、情绪、头条、全市场综述
│   ├── market_focus.py    260     AI 市场关注点（证据包 → 结构化输出 → 校验）
│   ├── premarket.py       380     盘前简报（隔夜行情、机会盘前位置、财报、经济日历、新闻 → AI + 校验）
│   ├── screens.py         170     策略筛选：规则筛选（全部命中 + 位置统计）与描述性信号筛选（新高、涨跌榜、放量、RSI、波段突破、支撑阻力、财报）
│   ├── web/app.py         480     FastAPI：/screens 策略筛选页，/health、/telegram、Dashboard（今日含板块与主题 ETF 表、个股页含 Scout 分析 / AI 解读 / 板块成分股）
│   ├── web/views.py       115     HTML / CSS（亮暗主题）/ K 线图（lightweight-charts）
│   └── jobs/              ...     _run.py（日志、job_runs、失败告警）、premarket、eod、weekly、backfill
│
├── cron/pre_fetch_news.py  683    ── 共用新闻管线（约 60 只标的，按 Finnhub 免费层限速）
├── services/                      新闻客户端：finnhub、rss（Alpha Vantage 新闻接口已收费，2026-10-27 移除）；news_summarizer；cron_audit；retry_protocol
├── tools/notify_tools.py    81    新闻任务的 Telegram 告警
├── db/                            新闻表的 ORM（4 个模型）、session、2 个 SQL migration
├── constants.py             61    新闻覆盖的标的：17 只 ETF + Scout watchlist / 活跃机会
├── config.py                23    新闻管线的 Settings
├── tests/                         unittest，81 个测试
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
| `scout` | `sector_daily` | pipeline | 每天每个板块 / 主题 ETF 的轮动、宽度、资金流和热度 |
| `scout` | `signal_hits` | pipeline | 每天所有规则（含候选规则）的全部命中，以及每个命中的入选理由，供策略筛选页使用 |
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

- **发布**：功能分支（当前是 `scout-rewrite`）→ 测试通过 → 推到 `main`，Railway 自动部署 4 个代码服务。
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

---

## 9. 路线图（下一步候选）

1. **P4 盘中与事件**：盘中监控活跃机会的触发和止损、财报前布局规则（D6）、SEC Form 4 内部人集中买入（D7）。需要盘中数据源。
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
  `main-service` 先断开 GitHub、只保留变量；同日把仍在使用的 7 个密钥迁到环境级 Shared Variables 后整体删除（含域名），
  删除前全部变量备份到 `qc_observer_archive/main-service-variables.env`（权限 600）。
- 数据：17 张 `qc_*` 表保留在库里，同时备份到 `qc_observer_archive/db/qc_observer_tables_20261026.dump`（4,393 行）。
- 恢复步骤见 `~/Work/trading/qc_observer_archive/README.md`。
