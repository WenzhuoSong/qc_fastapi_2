# Dual-Track Evidence Architecture 设计文档

**版本**: v1.0  
**日期**: 2026-05-21  
**背景**: 解决 Playground Evidence Bundle 中 QC live 与 yfinance 数据职责错位问题

---

## 1. 问题陈述

### 1.1 当前架构的根本缺陷

现有系统将 QC live snapshots 纳入 Evidence Bundle 的评分路径，用其 forward samples 计算 `live_fit_score`。由于 QC 只能返回部署上线后的数据，样本量天然不足，导致系统持续输出误导性的 `Live fit: conflicted`，并将 `execution_permission` 降级为 `human_required`。

这不是数据质量问题，是**职责定义错误**。

### 1.2 两类数据的本质差异

| | QC Live Snapshots | yfinance Historical |
|---|---|---|
| **能回答的问题** | 现在持仓是什么？执行代价多少？ | 这个策略历史上有效吗？ |
| **时间范围** | 部署后，天/周级别 | 任意回溯，年级别 |
| **统计用途** | 执行质量监控、持仓偏差 | Sharpe / 胜率 / 回撤 |
| **样本量限制** | 结构性不足（冷启动问题） | 充分 |
| **错误使用方式** | 用来验证策略有效性 ❌ | 用来描述当前持仓状态 ❌ |

---

## 2. 目标架构：双轨分离

### 2.1 架构总览

```
┌─────────────────────────────────────────────────────────┐
│                    Agentix Decision Layer                │
└────────────────────┬────────────────────────────────────┘
                     │
          ┌──────────┴──────────┐
          │                     │
          ▼                     ▼
┌─────────────────┐    ┌──────────────────────┐
│  策略分析层      │    │  执行情报层           │
│ Strategy Layer  │    │  Execution Intel Layer│
│                 │    │                      │
│  yfinance       │    │  QC Live             │
│  regime signal  │    │  snapshots           │
│  consensus vote │    │  positions           │
└────────┬────────┘    └──────────┬───────────┘
         │                        │
         │  "该跑什么策略？"       │  "当前状态如何？执行代价？"
         │                        │
         └──────────┬─────────────┘
                    │
                    ▼
         ┌──────────────────────┐
         │   执行决策合并点      │
         │  Execution Gateway   │
         │                      │
         │  策略分析 → APPROVED │
         │  +                   │
         │  执行代价 → ACCEPTABLE│
         │  ↓                   │
         │  → 下单              │
         └──────────────────────┘
```

### 2.2 策略分析层（Strategy Layer）

**数据来源**: yfinance historical + regime classifier + multi-strategy consensus

**职责**:
- 判断当前 regime 下哪些策略有历史支撑
- 计算 Sharpe / 胜率 / 最大回撤
- 多策略投票生成 consensus 权重
- 检测 regime ↔ consensus 冲突（如 trending_bull + 债券领头）

**输出结构**:
```json
{
  "strategy_analysis": {
    "regime": "trending_bull",
    "regime_confidence": "high",
    "regime_bond_adjusted": false,
    "strategies": [
      {
        "name": "momentum_lite_v1",
        "historical_sharpe": 1.56,
        "historical_samples": 289,
        "historical_verdict": "strong",
        "regime_alignment": "conflict",
        "regime_conflict_reason": "defensive_consensus_in_bull"
      }
    ],
    "consensus": {
      "top5": {"IEF": 0.103, "BND": 0.084, "TLT": 0.067},
      "defensive_weight_total": 0.42,
      "conflict_with_regime": true,
      "conflict_type": "bundle_level"
    },
    "strategy_layer_verdict": "watch_only",
    "strategy_layer_reason": "regime_consensus_mismatch"
  }
}
```

**不再包含**:
- QC forward sample count
- live_fit_score
- QC snapshot 数量对 verdict 的影响

---

### 2.3 执行情报层（Execution Intel Layer）

**数据来源**: QC Live API

**职责**:
- 实时持仓快照（当前权重 vs 目标权重）
- 换手量与预估成本
- 信号到达健康检查（signal latency / missing signals）
- 账户风险暴露（sector concentration, beta）
- 执行可行性评估（流动性、市场时段）

**输出结构**:
```json
{
  "execution_intel": {
    "snapshot_time": "2026-05-21T09:35:00Z",
    "qc_snapshot_count": 22,
    "data_status": "live_available",
    "current_positions": {
      "SPY": 0.35,
      "QQQ": 0.28,
      "IEF": 0.15
    },
    "target_positions": {
      "SPY": 0.40,
      "QQQ": 0.30,
      "IEF": 0.08
    },
    "turnover_estimate": {
      "gross_turnover_pct": 0.74,
      "estimated_cost_bps": 12,
      "cost_verdict": "high"
    },
    "execution_health": {
      "signal_latency_ok": true,
      "last_signal_age_minutes": 4,
      "missing_signals": []
    },
    "risk_exposure": {
      "defensive_weight": 0.23,
      "top_single_name_pct": 0.35,
      "estimated_beta": 0.82
    },
    "execution_intel_verdict": "proceed_with_caution",
    "execution_intel_reason": "high_turnover_cost"
  }
}
```

**不再参与**:
- Evidence Bundle 的 `live_fit_score` 计算
- forward sample 统计
- strategy validity 判断

---

### 2.4 执行决策合并点（Execution Gateway）

两轨独立评分，在 Gateway 汇合，**同时满足才执行**：

```python
def execution_gateway(strategy_analysis, execution_intel) -> ExecutionDecision:
    
    # 策略分析层独立否决
    if strategy_analysis.verdict in ["blocked", "watch_only"]:
        return ExecutionDecision(
            permission="denied",
            reason=strategy_analysis.reason,
            source="strategy_layer"
        )
    
    # 执行代价独立否决（高换手一票否决）
    if execution_intel.turnover_estimate.gross_turnover_pct > TURNOVER_THRESHOLD:
        return ExecutionDecision(
            permission="human_required",
            reason="high_turnover_cost",
            source="execution_intel_layer"
        )
    
    # 执行健康检查否决
    if not execution_intel.execution_health.signal_latency_ok:
        return ExecutionDecision(
            permission="denied",
            reason="signal_health_failure",
            source="execution_intel_layer"
        )
    
    # 两轨均通过
    return ExecutionDecision(
        permission="approved",
        strategy=strategy_analysis.top_strategy,
        target_positions=execution_intel.target_positions
    )
```

**阈值配置（可调）**:
```python
TURNOVER_THRESHOLD = 0.60        # 换手 > 60% 触发 human_required
DEFENSIVE_WEIGHT_THRESHOLD = 0.40 # 防御权重 > 40% 触发 bundle-level conflict
MIN_HISTORICAL_SAMPLES = 50       # yfinance 样本不足时标记 insufficient_history
```

---

## 3. 关键逻辑变更

### 3.1 QC Live 样本量不再影响 verdict

**现在（错误）**:
```
qc_forward_samples=6 → live_fit: conflicted → execution: human_required
```

**改后（正确）**:
```
qc_forward_samples=6 → execution_intel.qc_snapshot_count=6（仅展示，不评分）
                      → strategy_layer 独立基于 yfinance 评分
```

### 3.2 `insufficient_data` 与 `conflicted` 明确区分

| 状态 | 含义 | 处理方式 |
|---|---|---|
| `conflicted` | 数据充足，但两个信号互相矛盾 | 触发 human_required |
| `insufficient_data` | 数据不足，无法判断 | 标记后跳过，不参与评分 |
| `live_available` | QC 数据正常，执行层可用 | 正常进入 Gateway |

### 3.3 Regime Classifier 引入债券信号（中期）

当前 regime classifier 只看股票价格动量，遇到 risk-off 轮动失灵。

建议新增 feature：
```python
regime_features = {
    # 现有
    "spy_momentum_20d": ...,
    "qqq_momentum_20d": ...,
    
    # 新增
    "ief_vs_spy_relative_strength_20d": ...,  # 债券 vs 股票相对强度
    "spy_breadth_pct_above_200ma": ...,        # 市场广度
    "vix_level": ...,                          # 风险偏好
    "vix_term_structure_slope": ...,           # VIX 期限结构
}
```

引入 `bull_with_defensive_rotation` 子状态：
```
trending_bull
  └── bull_with_defensive_rotation  （IEF/BND 相对强度 > 阈值时）
  └── bull_broad_participation      （广度良好时）
```

---

## 4. Playground 展示结构调整

### 4.1 当前展示（混乱）

```
Regime: trending_bull (high)
QC snapshots=22
yfinance history=290
Consensus top5: IEF 10.3%, BND 8.4%, ...
Historical evidence: strong (289 samples)
Live fit: conflicted (QC snapshots=22, forward=6)   ← 误导
Execution permission: human_required
```

### 4.2 改后展示（双轨分离）

```
═══════════════════════════════════════════
  STRATEGY ANALYSIS  (yfinance)
═══════════════════════════════════════════
Regime:             trending_bull (high)
Consensus top5:     IEF 10.3%, BND 8.4%, TLT 6.7%  ⚠️ defensive-heavy
Regime conflict:    bundle-level (defensive in bull)
Best strategy:      momentum_lite_v1
  Historical:       Sharpe 1.56, 289 samples ✅
  Regime aligned:   NO ⚠️
Strategy verdict:   WATCH_ONLY

═══════════════════════════════════════════
  EXECUTION INTEL  (QC Live)
═══════════════════════════════════════════
Snapshot time:      2026-05-21 09:35 UTC
QC snapshots:       22  (live monitoring only)
Current positions:  SPY 35%, QQQ 28%, IEF 15%
Target deviation:   SPY +5%, QQQ +2%, IEF -7%
Turnover estimate:  74%  ⚠️ HIGH
Est. cost:          ~12 bps
Signal health:      OK ✅
Execution verdict:  PROCEED_WITH_CAUTION (high turnover)

═══════════════════════════════════════════
  EXECUTION GATEWAY
═══════════════════════════════════════════
Strategy layer:     WATCH_ONLY  → ❌ blocks execution
Execution layer:    CAUTION     → secondary check
Final decision:     HUMAN_REQUIRED
Primary reason:     regime_consensus_mismatch (strategy layer)
```

---

## 5. 开发任务拆解

### Phase 1：隔离（优先，最小改动）

- [ ] **P1-1** 从 Evidence Bundle 评分路径中移除 QC `forward_samples` 计数
- [ ] **P1-2** 新增 `execution_intel` 独立数据结构，QC live 数据只填入此结构
- [ ] **P1-3** `live_fit` 字段改为 `execution_intel_status`，区分 `live_available` / `insufficient_data` / `conflicted`
- [ ] **P1-4** Playground 展示分两个独立 section 输出

### Phase 2：Gateway 逻辑

- [ ] **P2-1** 实现 `execution_gateway()` 合并函数，替代现有的单一 verdict 逻辑
- [ ] **P2-2** 高换手独立触发 `human_required`（阈值 60%，可配置）
- [ ] **P2-3** `execution_intel_reason` 和 `strategy_layer_reason` 分别输出，不合并

### Phase 3：Regime 增强（中期）

- [ ] **P3-1** Regime classifier 引入 IEF vs SPY 相对强度 feature
- [ ] **P3-2** 新增 `bull_with_defensive_rotation` 子状态
- [ ] **P3-3** 在 `bull_with_defensive_rotation` 下，防御性 consensus 不触发冲突

---

## 6. 不变的内容

以下逻辑**保持不变**，避免过度重构：

- bundle-level vs strategy-level conflict 分离（已修复，保留）
- yfinance Sharpe / 胜率计算逻辑
- consensus top-5 权重计算
- QC live snapshot 的采集频率和存储方式
- `watch_only` / `human_required` / `approved` 三级权限体系

---

## 7. 验收标准

改完后，以下场景应产生正确输出：

| 场景 | 预期输出 |
|---|---|
| QC snapshots=22, yfinance=289, regime=bull, consensus=defensive | Strategy: watch_only; Execution: live_available; Gateway: human_required（因 strategy layer） |
| QC snapshots=0（系统刚部署）| execution_intel_status: insufficient_data（不触发 conflicted） |
| QC snapshots=50, 换手=74%, 策略通过 | Gateway: human_required（因高换手，来自 execution layer） |
| QC snapshots=50, 换手=30%, 策略通过, regime aligned | Gateway: approved |
| QC forward=6 | 不影响 strategy verdict，仅在 execution intel section 展示 |
