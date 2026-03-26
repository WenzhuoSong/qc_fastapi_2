# QC FastAPI 2: Anthropic → OpenAI 迁移说明

## 修改概述

QC FastAPI 2 项目已从 **Anthropic Claude API** 迁移到 **OpenAI GPT API**，以下是完整的修改清单。

## 🔄 修改文件清单

### 1. 配置文件

**`config.py`**
```diff
- anthropic_api_key: str
- claude_model: str = "claude-sonnet-4-5"
+ openai_api_key: str
+ openai_model: str = "gpt-4o"
+ openai_model_mini: str = "gpt-4o-mini"
```

**`.env.example`**
```diff
- ANTHROPIC_API_KEY=sk-ant-...
- CLAUDE_MODEL=claude-sonnet-4-5
+ OPENAI_API_KEY=sk-...
+ OPENAI_MODEL=gpt-4o
+ OPENAI_MODEL_MINI=gpt-4o-mini
```

**`requirements.txt`**
```diff
- anthropic==0.40.0
+ openai==1.55.0
```

### 2. 核心代码

**`agents/base_agent.py`** - 完全重写
- ✅ 从 `anthropic.Anthropic` 改为 `openai.AsyncOpenAI`
- ✅ 工具调用协议适配（Anthropic → OpenAI）
- ✅ 响应格式处理差异
- ✅ 支持异步运行（`run_async()` 方法）
- ✅ 自动转换 Anthropic 风格工具定义到 OpenAI 格式
- ✅ 新增 `use_mini_model` 参数支持 gpt-4o-mini

**主要 API 差异**:

| Anthropic | OpenAI |
|-----------|--------|
| `client.messages.create()` | `client.chat.completions.create()` |
| `tools` 直接传递 | `tools` 需要 `{"type": "function", "function": {...}}` 格式 |
| `stop_reason == "tool_use"` | `finish_reason == "tool_calls"` |
| `content` 数组中的 `tool_use` block | `message.tool_calls` 列表 |
| `tool_result` 格式 | `role: "tool"` 消息格式 |

### 3. 工具定义格式转换

BaseAgent 自动将 Anthropic 风格转换为 OpenAI 格式：

**输入（Anthropic 风格）**:
```python
{
    "name": "read_system_config",
    "description": "读取系统配置",
    "input_schema": {
        "type": "object",
        "properties": {}
    }
}
```

**转换后（OpenAI 格式）**:
```python
{
    "type": "function",
    "function": {
        "name": "read_system_config",
        "description": "读取系统配置",
        "parameters": {
            "type": "object",
            "properties": {}
        }
    }
}
```

### 4. 文档更新

已更新所有文档中的 API 引用：
- ✅ `README.md`
- ✅ `CLAUDE.md`
- ✅ `DEPLOYMENT.md`
- ✅ `setup.sh` 注释

## 🧪 测试验证

创建了测试脚本 `test_openai_integration.py`：

```bash
# 设置环境变量
export OPENAI_API_KEY=sk-...

# 运行测试
python test_openai_integration.py
```

测试内容：
- ✅ OpenAI API 连接
- ✅ 工具调用循环
- ✅ JSON 输出验证
- ✅ 错误重试机制

## 📋 迁移检查清单

在部署前，请确认：

- [ ] `.env` 文件中设置了 `OPENAI_API_KEY`
- [ ] 删除了旧的 `ANTHROPIC_API_KEY` 变量
- [ ] 安装了新的依赖：`pip install -r requirements.txt`
- [ ] Railway 环境变量已更新（如果使用 Railway）
- [ ] 测试脚本通过：`python test_openai_integration.py`

## 🚀 Railway 部署更新

如果已在 Railway 部署，需要更新环境变量：

1. **删除旧变量**:
   - `ANTHROPIC_API_KEY`
   - `CLAUDE_MODEL`

2. **添加新变量**:
   - `OPENAI_API_KEY` = `sk-...`
   - `OPENAI_MODEL` = `gpt-4o`
   - `OPENAI_MODEL_MINI` = `gpt-4o-mini`

3. **重新部署**:
   ```bash
   railway up
   ```

## 💰 成本对比

| 模型 | 每 1M tokens 输入 | 每 1M tokens 输出 |
|------|-------------------|-------------------|
| **Claude Sonnet 4.5** | $3.00 | $15.00 |
| **GPT-4o** | $2.50 | $10.00 |
| **GPT-4o-mini** | $0.15 | $0.60 |

建议：
- 🧠 复杂推理任务使用 `gpt-4o`（RESEARCHER, ALLOCATOR, RISK_MGR）
- ⚡ 简单任务使用 `gpt-4o-mini`（PLANNER, REPORTER）

## ⚠️ 已知差异

1. **输出质量**:
   - GPT-4o 在金融分析任务上与 Claude Sonnet 4 性能相近
   - 建议在生产环境进行 A/B 测试

2. **工具调用**:
   - OpenAI 支持并行工具调用（目前 BaseAgent 未启用）
   - 可以优化为同时调用多个独立工具

3. **Prompt 工程**:
   - Claude 和 GPT 的提示词风格略有不同
   - 如果输出质量下降，可能需要微调 system prompts

## 📚 参考资源

- [OpenAI API 文档](https://platform.openai.com/docs/api-reference)
- [OpenAI Function Calling](https://platform.openai.com/docs/guides/function-calling)
- [OpenAI Pricing](https://openai.com/api/pricing/)

## 🔄 回滚方案

如需回滚到 Anthropic：

```bash
# 恢复 requirements.txt
pip uninstall openai
pip install anthropic==0.40.0

# 恢复配置文件
git checkout HEAD -- config.py .env.example agents/base_agent.py

# 恢复环境变量
export ANTHROPIC_API_KEY=sk-ant-...
```

---

迁移完成日期：2026-03-26
