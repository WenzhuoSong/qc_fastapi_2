# Agentix Data Pipeline Upgrade Plan — QC Live vs yfinance

> 目标：重新划分 QC live 与 yfinance 的数据职责，减少重复和口径冲突，同时保留已有信息用于回溯、debug、fallback 和历史审计。
>
> 本文只制定开发计划，不包含代码实现。

---

## 1. 背景与发现

当前系统同时从两条路径获得 ETF 日线研究特征：

```text
QC live algo
  ├─ heartbeat，每 15 分钟
  │   └─ holdings: price, weights, mom/rsi/atr/hist_vol 等
  └─ daily_feature_snapshot，盘后
      └─ features: heartbeat 字段 + OHLCV

yfinance_backfill
  └─ market_daily_features
      └─ OHLCV + return/momentum/SMA/RSI/ATR/BB/hist_vol
```

线上数据库对比结果显示，同一 `ticker + trading_date` 下，QC 与 yfinance 的日线研究特征并非完全一致。

最近 45 天匹配样本：

| packet_type | rows | days | tickers |
|---|---:|---:|---:|
| daily_feature_snapshot | 241 | 8 | 52 |
| heartbeat | 666 | 33 | 52 |

关键发现：

1. QC 线上 snapshot 中的 `daily_return_pct`、`return_5d`、`mom_20d`、`mom_60d`、`mom_252d` 多数仍是百分数单位。
   例如 QC `35.748324` 对应 yfinance `0.322486`，即 35.7% vs 0.322。

2. 单位归一后，普通 ETF 的差异变小，但仍存在口径差异。
   主要原因包括盘中 vs 日终、QC MOMP 指标 vs yfinance `pct_change`、adjusted close vs raw close、杠杆/反向 ETF 重置机制。

3. `heartbeat` 是实时账户与盘中状态的唯一来源；yfinance 不能稳定替代。

4. `mom/rsi/atr/bb/hist_vol` 是日线研究特征，更适合由 yfinance / feature store 统一生产。

---

## 2. 新职责边界

### 2.1 QC Heartbeat：实时状态源

QC heartbeat 应成为 **live state source of truth**，只负责 yfinance 拿不到或不可靠的信息。

保留为主信号：

| 字段 | 原因 |
|---|---|
| `price` / `last_price` | 实时或准实时价格 |
| `weight_current` | 实盘当前持仓权重 |
| `weight_target` | QC 当前记忆中的目标权重 |
| `weight_drift` | 实盘漂移 |
| `unrealized_pnl_pct` | broker/QC 持仓 PnL |
| `holding_days` | 实盘持仓生命周期 |
| `portfolio.total_value` | 实盘组合净值 |
| `portfolio.cash` / `cash_pct` | 实盘现金 |
| `portfolio.daily_pnl_pct` | 实盘当日盈亏 |
| `portfolio.current_drawdown_pct` | 实盘回撤 |
| `trading_session` | 当前交易时段 |
| `target_weights` | QC 侧最后接受的目标权重 |

建议新增为主信号：

| 字段 | 用途 |
|---|---|
| `intraday_open_price` | 盘中判断和当日波动 |
| `intraday_high_price` | 盘中风险扩大检测 |
| `intraday_low_price` | 盘中 hard-risk / drawdown 辅助 |
| `intraday_volume` | 流动性与异常成交 |
| `intraday_return_pct` | 当日相对 open/prev close 的实时变化 |
| `minutes_since_open` | 判断开盘噪音、午盘、尾盘 |
| `is_market_open` | 下游直接判断市场状态 |
| `last_trade_time` | 数据新鲜度 |

暂不强制新增：

| 字段 | 原因 |
|---|---|
| `bid` / `ask` / `spread_pct` | 有价值，但 QC 获取稳定性需要单独验证 |
| `vix` | 更适合作为独立 market data source，不强塞 heartbeat |

### 2.2 yfinance / market_daily_features：日线研究特征源

yfinance 应成为 **daily research feature source of truth**。

主责字段：

| 语义 | yfinance 字段 |
|---|---|
| 日线 OHLCV | `open_price`, `high_price`, `low_price`, `close_price`, `volume` |
| 调整价格 | `adj_close_price` |
| 日线收益 | `return_1d`, `return_5d`, `return_20d`, `return_60d`, `return_252d` |
| 动量 | 使用 `return_20d/60d/252d`，替代 QC `mom_20d/60d/252d` |
| 均线 | `sma_20`, `sma_50`, `sma_200` |
| 波动 | `hist_vol_20d`, `atr_pct` |
| 技术状态 | `rsi_14`, `bb_position` |

### 2.3 QC Daily Snapshot：QC 口径审计源

QC `daily_feature_snapshot` 不再作为日线研究主源，而作为：

1. QC / broker 口径的 EOD 校验。
2. yfinance 缺失时的 fallback。
3. 数据口径审计样本。
4. 回溯 bug 时的 raw evidence。

---

## 3. 兼容原则

这次升级不能破坏和丢失已有信息。

### 3.1 不删除旧字段

短期不从 QC payload 删除这些字段：

```text
daily_return_pct
return_5d
mom_20d
mom_60d
mom_252d
sma_20
sma_50
sma_200
rsi_14
atr_pct
bb_position
hist_vol_20d
```

但它们在下游语义上降级为：

```text
source = qc_legacy_daily_indicator
authority = fallback/debug only
```

### 3.2 引入 feature authority，不靠字段名猜来源

下游不能再看到 `mom_60d` 就默认可用于主决策，而要看 source/authority。

建议新增内部概念：

```text
FeatureAuthority
  live_state: QC heartbeat authoritative
  daily_research: yfinance authoritative
  qc_eod_audit: QC daily snapshot audit/fallback
  legacy_debug: old QC indicator, not allocation-authoritative
```

### 3.3 保留 raw_payload

`qc_snapshots.raw_payload`、`market_daily_features.raw_payload` 都继续保留。

不要做 destructive migration，不回写覆盖历史 raw payload。

---

## 4. 目标数据流

升级后目标链路：

```text
QC heartbeat
  └─ live_state fields
       ↓
market_snapshot_merge
  ├─ live_state from heartbeat wins
  ├─ daily_research from yfinance wins
  ├─ qc_daily_snapshot only fills missing/audit fields
  └─ legacy QC indicators marked fallback/debug
       ↓
market_brief
  ├─ current_weights / PnL / holding_days from QC
  ├─ momentum / RSI / ATR / BB / hist_vol from yfinance
  └─ provenance summary exposes source counts and stale fields
       ↓
quant_baseline / playground / sector_rotation / risk / governance
  └─ consume normalized canonical fields, with provenance-aware confidence
```

---

## 5. 下游影响分析

### 5.1 `market_brief.py`

当前职责：

- 读取最新 heartbeat。
- 读取最新 daily_feature_snapshot。
- `merge_market_snapshots(heartbeat, feature_snapshot)`。
- 用 merged holdings 计算 key facts。

问题：

- 当前 merge 是 `{**feature_row, **live_row}`，heartbeat 会覆盖同名研究字段。
- 如果 heartbeat 的 `mom_60d` 是旧单位或盘中口径，会影响 breadth、regime、sector_rotation。

升级目标：

- live state 字段由 heartbeat 覆盖。
- daily research 字段由 yfinance feature map 覆盖。
- QC daily snapshot 只填 yfinance 缺失字段。
- legacy QC indicator 不再覆盖 canonical research field。

### 5.2 `market_snapshot_merge.py`

需要从简单 dict overlay 升级为 field-policy merge。

建议字段策略：

| 字段组 | 优先级 |
|---|---|
| current portfolio / weights / PnL | heartbeat > all |
| intraday OHLCV | heartbeat only，独立 namespace |
| daily OHLCV | yfinance > qc_daily_snapshot > heartbeat |
| daily research indicators | yfinance > qc_daily_snapshot > legacy heartbeat |
| debug legacy indicators | heartbeat retained under separate namespace |

关键约束：

1. `intraday_open_price/high_price/low_price/volume` 与 `open_price/high_price/low_price/volume` 不竞争。
   前者是盘中状态，后者是日线研究/EOD 数据。
2. merge 层禁止把 `intraday_open_price` 写回 `open_price`。
3. 如果旧 heartbeat 只有 `price/close_price`，只能填 live state，不应伪造成日线 OHLCV。

### 5.3 `quant_baseline.py`

依赖字段：

```text
mom_20d, mom_60d, rsi_14, bb_position, hist_vol_20d, atr_pct
```

升级目标：

- 短期可兼容旧名，但目标字段应切到 canonical return fields。
- `mom_20d/mom_60d/mom_252d` 只作为 legacy/debug input。
- canonical field 应由 yfinance 供给。
- 如果字段来源不是 `daily_research`，降低 confidence 或记录 warning。

### 5.4 `sector_rotation.py`

依赖字段：

```text
mom_20d, mom_60d, daily_return_pct, return_5d, hist_vol_20d
```

升级目标：

- `daily_return_pct` 应映射到 canonical `return_1d`。
- `mom_20d/mom_60d` 应映射到 yfinance `return_20d/return_60d`。
- 避免 QC heartbeat 旧百分数单位污染 rotation。
- 该模块需要和 Phase 3 同步提前改造，因为 rotation 对 momentum 排名最敏感。

### 5.5 `playground.py`

当前已经会在缺字段时从 `market_daily_features(source='yfinance')` enrichment。

升级目标：

- 不再只在缺字段时 enrichment。
- 对日线研究字段，应主动优先使用 yfinance feature store。
- 保留 QC live state 用于 current weights、turnover、drift、live replay。

### 5.6 `strategy_feature_contract.py`

当前能看 `feature_sources`。

升级目标：

- 增加 authority 检查。
- required field 不仅要 coverage，还要 authority 合格。
- 例如 `momentum_lite_v1.required_fields` 必须来自 `daily_research` 或合格 fallback，不能来自 `legacy_debug`。

### 5.7 `risk_manager.py`

风险经理依赖：

- current holdings / weights
- hist vol
- target weights
- scorecard / strategy support

升级目标：

- 当前权重、PnL、holding_days 必须继续信任 QC heartbeat。
- `hist_vol_20d`、`atr_pct` 应使用 canonical yfinance research field。
- 如果 yfinance stale，而 QC fallback 可用，则允许保守降级，不允许因此放大仓位。

### 5.8 `position_governance.py`

依赖：

```text
unrealized_pnl_pct, atr_pct, holding_days, position_role
```

升级目标：

- `unrealized_pnl_pct`、`holding_days` 必须继续来自 QC。
- `atr_pct` 应来自 yfinance canonical daily research。
- 如果 `atr_pct` 只有 QC legacy fallback，治理动作只能更保守，不能放宽。

### 5.9 `decision_ledger.py` / dashboard / communicator

升级目标：

- 展示每个 ticker 的关键字段来源。
- 明确区分：
  - `qc_live_state_available`
  - `yfinance_research_available`
  - `qc_legacy_indicator_used`
  - `fallback_used`
  - `stale_research_field`

---

## 6. 开发阶段计划

### Phase 1：数据口径审计工具

目标：把这次手工 SQL 变成可重复的只读审计。

新增建议：

```text
tools/audit_qc_yfinance_features.py
```

功能：

1. 对比同一 `ticker + trading_date` 的 QC vs yfinance。
2. 输出字段级 MAE / MaxE / coverage。
3. 支持按 `packet_type`、ticker role、levered ETF 拆分。
4. 自动检测疑似单位错误：
   - QC return 绝对值经常大于 2
   - QC / yfinance 约等于 100x
5. 输出 compact markdown report。
6. 支持 `--write-db` 将汇总指标写入审计表，供 dashboard 展示趋势。
7. 支持 `--fail-on-unit-risk` 用于 CI / cron 健康检查。

建议新增审计存储：

```text
data_quality_audit
  id
  created_at
  audit_name
  lookback_days
  summary JSONB
  status
```

该表只存统计摘要，不存敏感连接信息，也不复制大体量 raw payload。

验收：

```text
报告能复现：
- heartbeat joined rows
- daily_feature_snapshot joined rows
- mom_20d/mom_60d 单位风险
- levered ETF 差异更大
```

### Phase 2：定义 canonical field policy

目标：建立字段级 source-of-truth 表，不改业务逻辑。

新增建议：

```text
services/feature_authority.py
```

定义：

```text
LIVE_STATE_FIELDS
INTRADAY_FIELDS
DAILY_RESEARCH_FIELDS
QC_EOD_AUDIT_FIELDS
LEGACY_QC_INDICATOR_FIELDS
```

并提供：

```text
authority_for_field(field, source)
is_authoritative(field, source)
canonical_field_aliases()
```

字段别名建议：

| legacy/current | canonical |
|---|---|
| `daily_return_pct` | `return_1d` |
| `mom_20d` | `return_20d` |
| `mom_60d` | `return_60d` |
| `mom_252d` | `return_252d` |

字段名冻结规则：

1. merged holdings 的 canonical 日线收益字段只使用 `return_1d/5d/20d/60d/252d`。
2. `mom_20d/mom_60d/mom_252d` 不再出现在 canonical 顶层路径。
3. 旧 `mom_*` 字段进入 `legacy_qc_indicators` namespace。
4. Phase 5 下游统一迁移到 canonical return fields，避免长期双轨。

### Phase 3：升级 market snapshot merge

目标：让 merged holdings 中的 canonical research field 来自 yfinance。

改造点：

1. `market_brief._read_latest_market_snapshot()` 读取 heartbeat + QC daily snapshot 后，再读取 yfinance latest feature map。
2. `merge_market_snapshots()` 升级为三源 merge：

```text
merge_market_snapshots(
  heartbeat,
  qc_daily_snapshot,
  yfinance_feature_map,
)
```

3. 同名字段不再无脑 heartbeat 覆盖。
4. legacy QC indicator 移入 debug namespace：

```text
legacy_qc_indicators: {
  mom_20d,
  mom_60d,
  rsi_14,
  atr_pct,
  ...
}
```

5. 对 canonical field 填充 `feature_sources`。
6. 同步提前改造 `sector_rotation.py`，让 rotation 只读取 canonical return fields。
7. 输出 `schema_capabilities`：

```text
schema_capabilities: {
  heartbeat_schema_version: "1.4" | "1.5" | "legacy",
  intraday_live_state: "available" | "partial" | "unavailable",
  daily_research_authority: "yfinance" | "qc_daily_fallback" | "missing"
}
```

验收：

```text
merged["holdings"][ticker]["return_60d"] == yfinance return_60d
merged["holdings"][ticker]["mom_60d"] is absent from canonical top-level path
merged["holdings"][ticker]["weight_current"] == QC heartbeat weight_current
merged["holdings"][ticker]["intraday_open_price"] does not overwrite open_price
legacy_qc_indicators.mom_60d still available when heartbeat had it
sector_rotation reads return_20d/return_60d instead of mom_20d/mom_60d
```

### Phase 4：QC heartbeat schema v1.5

目标：增强 QC 实时独有信息，而不是继续强化日线研究字段。

QC payload 建议升级：

```text
schema_version = "1.5"

holdings row:
  ticker
  universe_role
  price
  last_price
  weight_current
  weight_target
  weight_drift
  unrealized_pnl_pct
  holding_days
  intraday_open_price
  intraday_high_price
  intraday_low_price
  intraday_volume
  intraday_return_pct
  last_trade_time

portfolio:
  total_value
  cash
  cash_pct
  daily_pnl_pct
  current_drawdown_pct
  is_market_open
  minutes_since_open
```

兼容：

- 旧日线指标短期保留。
- 标记为 `legacy_qc_indicators` 或继续原字段但 downstream 不再信任其 authority。

FastAPI 旧 schema 处理：

```text
schema_version missing or < 1.5:
  heartbeat_schema_version = "legacy" or actual version
  intraday_live_state = "partial" if price exists else "unavailable"
  intraday_* fields = missing
  do not fail pipeline
  do not infer intraday OHLCV from daily OHLCV
```

这样 QC 侧部署 v1.5 前，FastAPI 仍能安全处理旧 heartbeat。

### Phase 5：下游 provenance-aware migration

目标：下游继续读同名字段，但知道字段来源和质量。

改造顺序：

1. `strategy_feature_contract.py`
   - required fields 增加 authority 检查。
   - stale yfinance 降级为 blocked/stale。

2. `quant_baseline.py`
   - 若关键字段来自 fallback/debug，降低 regime confidence。
   - log 中显示 `feature_authority`.

3. `sector_rotation.py`（与 Phase 3 同步提前）
   - 使用 canonical return field。
   - 若只有 legacy QC indicator，结果标记 `data_quality=legacy_fallback`。

4. `playground.py`
   - 日线策略特征主动优先 yfinance。
   - QC live 只参与 live replay / turnover / current state。

5. `risk_manager.py` / `position_governance.py`
   - execution state 继续信任 QC。
   - research volatility 继续信任 yfinance。
   - fallback 只能收紧，不能放宽。

### Phase 6：观测面与文案

目标：让用户能看见数据来源，而不是只看到结果。

Dashboard 新增：

```text
Feature Source Summary
  QC live state fields
  yfinance research fields
  QC daily audit fields
  legacy fallback fields
  stale fields
```

Telegram / communicator 新增短线提示：

```text
Data: live_state=QC heartbeat, research=yfinance, fallback=none
```

当 fallback 被使用：

```text
⚠️ Research feature fallback used: mom_60d from qc_legacy_indicator for XSD
```

---

## 7. 测试矩阵

### Unit Tests

新增或更新：

```text
tests/test_feature_authority.py
tests/test_market_snapshot_merge.py
tests/test_market_brief_feature_sources.py
tests/test_strategy_feature_contract.py
tests/test_qc_yfinance_feature_audit.py
```

核心断言：

1. yfinance research field 覆盖 QC legacy indicator。
2. QC live state field 覆盖所有其它来源。
3. QC legacy indicator 不丢失，进入 debug/fallback namespace。
4. missing yfinance 时，可用 QC daily snapshot fallback，但标记 fallback。
5. stale yfinance 不可提升策略权限。
6. unit conversion 检测能识别百分数 vs decimal。
7. `intraday_*` 不会覆盖日线 `open_price/high_price/low_price/volume`。
8. canonical top-level 不再输出 `mom_20d/mom_60d/mom_252d`。
9. old schema heartbeat 会产生 `schema_capabilities.intraday_live_state=partial/unavailable`。

### Integration Tests

场景：

1. 只有 heartbeat，无 yfinance。
   - 系统可以运行，但 strategy evidence 降级。

2. heartbeat + yfinance。
   - current weights 来自 heartbeat。
   - `return_20d/return_60d/rsi/atr` 来自 yfinance。
   - `mom_*` 只出现在 `legacy_qc_indicators`。

3. heartbeat + stale yfinance + QC daily snapshot。
   - fallback 使用 QC daily snapshot。
   - risk / governance 只能收紧。

4. QC old schema heartbeat。
   - 不崩溃。
   - legacy fields 被保留但不作为 authoritative research。
   - intraday capability 被标记为 partial/unavailable。

5. levered ETF。
   - 不用 QC/yfinance 差异直接判定系统错误。
   - 单独在 audit report 标注 high-drift class。

---

## 8. Migration 与回滚

### 8.1 无 destructive migration

不删除：

- `qc_snapshots.raw_payload`
- `holdings_factors` 旧字段
- `market_daily_features`
- 旧 feature_sources

### 8.2 灰度开关

建议增加配置：

```text
feature_authority_mode:
  legacy_overlay       # 当前行为
  yfinance_research    # 新行为，推荐默认
  audit_only           # 只记录差异，不影响下游
```

开发初期默认：

```text
audit_only
```

通过测试和 dashboard 确认后切到：

```text
yfinance_research
```

### 8.3 回滚策略

如果新 merge 影响 pipeline：

1. 切回 `legacy_overlay`。
2. 保留 audit report。
3. 不回滚数据库。
4. 不删除新 provenance 字段。

运行时回滚命令：

```text
/config feature_authority_mode rollback
```

该命令只写入：

```text
system_config.feature_authority_mode.value = "legacy_overlay"
system_config.feature_authority_mode.rollback.preserve_audit_report = true
system_config.feature_authority_mode.rollback.no_database_rollback = true
system_config.feature_authority_mode.rollback.preserve_provenance_fields = true
```

不执行 destructive migration，不清空 `data_quality_audit`，不回写历史 `qc_snapshots.raw_payload` 或 `market_daily_features.raw_payload`。

---

## 9. DoD

完成标准：

1. QC 与 yfinance 字段职责在代码中有明确 source-of-truth policy。
2. 每次 pipeline 能输出 feature source summary。
3. `return_20d/return_60d/rsi/atr/hist_vol` 的主来源为 yfinance。
4. `weight_current/unrealized_pnl/holding_days/current_drawdown` 的主来源为 QC heartbeat。
5. QC legacy 日线指标仍保留，可审计，但不会覆盖 canonical research fields。
6. `mom_20d/mom_60d/mom_252d` 不再作为 canonical top-level research fields。
7. `intraday_*` 与日线 OHLCV 字段分 namespace，不互相覆盖。
8. old schema heartbeat 能安全降级，不阻塞 pipeline。
9. 数据缺失时不会静默使用错误来源，必须有 fallback/stale 标记。
10. Playground、quant_baseline、sector_rotation、risk_manager、position_governance 均完成 provenance-aware 适配。
11. dashboard / Telegram 能清晰说明本轮数据来源。
12. 全量测试通过。

---

## 10. 推荐开发顺序

```text
1. audit_qc_yfinance_features.py
2. feature_authority.py + field name freeze
3. market_snapshot_merge 三源 merge + sector_rotation canonical return fields
4. market_brief 接入 yfinance latest_feature_map + schema_capabilities
5. strategy_feature_contract authority check
6. quant_baseline provenance-aware
7. playground 主动使用 yfinance research fields
8. risk_manager / position_governance fallback conservative mode
9. QC heartbeat schema v1.5 增加 intraday live fields
10. audit cron/dashboard 趋势展示
11. dashboard / communicator 数据来源展示
```

这个顺序的好处是：先审计，再定义规则，再切数据流，最后改决策层和展示层。任何阶段都可以回滚到旧 overlay 行为，不会丢失历史信息。
