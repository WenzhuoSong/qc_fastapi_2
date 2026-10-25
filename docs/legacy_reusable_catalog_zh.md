# 旧交易系统可复用资产目录

旧的 agentic 交易系统已经从代码库移除，但它的最后一个提交被永久打上了 tag：**`legacy-trading-system`**（`5d9d458`）。
本目录列出其中对 Scout 后续功能**仍有复用价值**的模块。它们不放回生产代码，是为了避免没人调用的代码慢慢腐烂；
真正要用的时候，按下面的方式取回，适配后接入，并补上测试。

```bash
# 查看某个文件
git show legacy-trading-system:services/macro_watcher.py
# 取回到工作区（再移到合适的位置并适配）
git show legacy-trading-system:services/macro_watcher.py > scout/macro_events.py
# 列出某个目录
git ls-tree -r --name-only legacy-trading-system knowledge/
```

取回时的注意事项：旧模块大多依赖已删除的表（`MemoryDaily`、`MacroEventsCache` 等）或旧的 `config`/`db.session`。
接入 Scout 时要改成 `scout.db` 的 SQL 写法，表放进 `scout` schema（新建 migration），并遵守 CLAUDE.md 里的边界：
LLM 不算数字、规则不调参、不出现任何执行或下单路径。

已经取回并在用的：`services/market_calendar.py` → `scout/market_calendar.py`（收盘扫描的交易日判断）。

---

## 1. 事件与宏观（对应路线图 P4）

| 模块 | 行数 | 能做什么 | Scout 里的用途 |
|---|---|---|---|
| `services/macro_watcher.py` | 225 | 从 Finnhub 经济日历识别 FOMC（只认议息会议本身，排除纪要和讲话）、CPI、NFP、PMI，生成"未来 N 天关键日期" | 盘前简报的宏观日历；事件前预警；问答"这周有什么宏观事件" |
| `services/earnings_tracker.py` | 151 | 按标的拉取未来 N 周财报（含 EPS / 营收预期） | Scout 的 `context.refresh_earnings` 已经覆盖全市场，这个可作为单票详细版参考 |
| `services/transmission.py` | 202 | 6 种宏观事件 → 行业传导模式（`match_event_to_pattern`、`generate_transmission_vector`） | 问答"加息 / 油价暴涨对哪些板块有利"；事件发生时的板块提示 |
| `services/scenario_analyst.py` | 202 | 基于传导模式对一组权重做情景压测 | 将来做"我的持仓"情景分析 |
| `services/macro_regime_builder.py` | 225 | 用价格代理（TLT、GLD 等）判断利率 / 通胀 / 增长的 regime，并叠加新闻 | 让 Scout 的市场状态从"SPY 趋势"升级为多维宏观状态，历史基准也能按它分桶 |
| `services/sector_rotation.py` | 299 | 行业与因子（成长 / 小盘 / 等权）相对强弱改善或转弱检测 | 已部分被 D4 覆盖；因子维度（RSP vs SPY、IWM vs SPY）可以补进收盘简报 |

## 2. 新闻与证据

| 模块 | 行数 | 能做什么 | 用途 |
|---|---|---|---|
| `services/news_evidence.py` | 569 | 确定性新闻证据评分：来源可信度 × 时效 × 相关性 × 硬风险 | 卡片里的新闻排序和去噪；"有没有实质新闻"的判断（D5 无新闻放量） |
| `services/similar_case_retrieval.py` | 314 | 按 regime / VIX / 回撤 / 宽度检索历史相似情形 | 卡片和问答里的"历史相似情形"（需要改成基于 `features_daily`） |
| `services/evidence_bundle.py` | 773 | 把多来源事实汇成单一证据契约 | Scout 的 `opportunities.evidence` 已是简化版，字段设计可以参考 |

## 3. 新规则候选（`strategies/`）

旧策略都是 **ETF 层面的配置评分**，取回后要改写成 Scout 的 `Detector`（`mask(panel)` + `levels(row)`），而且不要按回放结果调参。

| 文件 | 思路 | 可转成的规则 |
|---|---|---|
| `sector_theme_relative_strength_lite.py` | 行业 / 主题 ETF 相对强度轮动 | 主题 ETF 转强提示（SMH、URA、ITA 等） |
| `sector_theme_relative_value_reversion_lite.py` | 同簇 ETF 之间的相对价值均值回归 | "同主题里掉队者补涨"规则 |
| `mean_reversion_lite.py` | 超卖、布林带下沿的均值回归 | 补充 D2（强势回调）的反向版本 |
| `seasonality_month_end_lite.py` | 月末 / 月初资金流季节性 | 日历型提示 |
| `momentum_lite.py` | 5 因子复合动量（20 / 60 / 252 日动量 z 分数等） | 可以替换或补充现在的 RS 分位算法 |
| `low_vol_factor.py`、`dual_momentum.py`、`macro_cyclical_inflation_rotation_lite.py`、`relative_value_reversion_lite.py` | 低波、双动量、宏观周期轮动、宽基相对价值 | 市场状态 / 板块层面的上下文 |

对应的策略说明在 `knowledge/strategies/*.yaml`（19 份）。

## 4. 规则评估与校准（记分板进阶）

| 模块 | 行数 | 能做什么 | 用途 |
|---|---|---|---|
| `services/outcome_label_contract.py` | 217 | 时点正确（point-in-time）的结果标签规范与校验 | 给 `opportunity_outcomes` 加防未来函数的约束 |
| `services/signal_outcome_labeler.py` | 413 | 给冻结的信号打远期收益标签（append-only） | 规则实盘表现的多周期标签（5 / 10 / 20 日） |
| `services/decay_detector.py` | 481 | 规则失效检测：滚动窗口准确率、动量有效性、regime 切换频率 | 路线图里的"规则自动降级" |
| `services/strategy_independence.py` | 818 | 规则之间的收益相关性与独立性 | 判断两条规则是不是在重复发同一个信号 |
| `services/walk_forward_validation.py` | 194 | 前向滚动验证 | 新规则上线前的样本外检验 |
| `services/strategy_conviction.py` | 1017 | 由冻结信号和结果推导置信度画像 | 卡片置信度从"样本数 + 超额"升级为校准后的概率 |

## 5. 风险与组合分析（将来若加"我的持仓"功能）

| 模块 | 行数 | 能做什么 |
|---|---|---|
| `services/portfolio_risk_diagnostic.py` | 892 | 组合 VaR / CVaR 与情景损失 |
| `services/performance_attribution.py` | 452 | 日度因子归因（SPY / QQQ beta、动量因子、残差 alpha） |
| `services/etf_decay_diagnostics.py` | 446 | 杠杆 ETF 和波动率 ETF 的日重置损耗量化 |
| `services/liquidity_proxy_diagnostics.py` | 486 | 用日线估算流动性和价差 |
| `services/transaction_cost_gate.py` | 418 | 交易成本对收益的拖累估算 |
| `services/group_contract.py` | 371 | 标的分组与因子暴露契约（防止同一主题重复暴露） |

## 6. LLM 相关

| 模块 | 用途 |
|---|---|
| `services/openai_chat_compat.py`（51 行） | **切换到 GPT-5 系列模型时需要**：处理 `max_completion_tokens` 等参数差异。Scout 目前用 gpt-4o-mini，暂时不需要 |
| `agents/bull_researcher.py`、`agents/bear_researcher.py`、`agents/cross_exam.py` | 多空辩论和交叉质询的 prompt 与结构化输出，可用于把 Top 卡片的"反方证据"升级成真正的反方审查 |
| `services/weekend_review_summarizer.py` | "LLM 只能解释确定性指标、不能计算"的契约写法，适合周报的 LLM 总结 |
| `agents/communicator.py` | Telegram 文案的 LLM + 强制 fallback 模式（Scout 的 thesis 已采用同样思路） |

## 7. 领域知识库（`knowledge/`）

| 内容 | 数量 | 用途 |
|---|---|---|
| `knowledge/assets/*.yaml` | 69 份 ETF 档案（类别、角色、风险驱动、适合 / 不适合的 regime、集中度提示） | 给问答 agent 加一个 `get_asset_profile` 工具；卡片里的主题 ETF 背景 |
| `knowledge/regimes/*.yaml` | 5 种市场状态（trending_bull、risk_on、mean_reverting、high_vol、defensive） | 收盘简报的状态描述 |
| `knowledge/risk_principles/*.yaml` | 8 条风险原则（高 ATR 不加仓、杠杆 ETF、板块集中等） | 卡片"风险提示"的规则化来源 |
| `services/knowledge_base.py`、`services/knowledge_resolver.py` | YAML 加载器，以及"静态知识 + 计算事实"的合并器 | 取回知识库时一起取回 |
| `services/execution_policy.py` 中的 `TICKER_ROLES` | 66 只 ETF 的角色分类（core / sector / thematic / hedge / satellite） | ETF 分类数据；只取数据表，不取执行逻辑 |

## 8. 运维

| 模块 | 用途 |
|---|---|
| `services/operational_health.py`、`services/operational_alerts.py` | 每日运行健康汇总和告警分级，可用于 Scout 的任务健康页面（配合 `scout.job_runs`） |
| `services/circuit_breaker.py` | 4 状态熔断状态机（VIX、回撤、连续失败触发）。可改成"市场风险提示"状态，例如 VIX > 30 时收紧推送 |

---

## 明确不再需要的部分

执行与下单（`execution_*`、`qc_command_sender`、`broker_order_filter`、`target_*`、`weight_*`）、审批与提案（`proposal*`、
`telegram_commands` 里的 /confirm）、组合构建与仓位治理（`portfolio_construction*`、`position_*`），以及旧的 10 阶段流水线
（`pipeline.py`、`researcher.py`、`synthesizer.py`、`risk_manager.py`）。这些和"交易执行"绑定，Scout 的定位是不交易，所以不会再用到。
它们同样保留在 tag 里，需要考古时可以查看。
