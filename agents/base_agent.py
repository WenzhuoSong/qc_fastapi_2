# agents/base_agent.py
import json
import logging
import time
from typing import Any

from openai import AsyncOpenAI
from config import get_settings

logger = logging.getLogger("qc_fastapi_2.agent")
settings = get_settings()
client = AsyncOpenAI(api_key=settings.openai_api_key)


class BaseAgent:
    """
    所有 Agent 的基类。
    负责：Prompt 加载 / OpenAI API 调用循环 / 工具执行 / 输出验证 / 重试。
    """

    def __init__(
        self,
        name: str,
        system_prompt: str,
        tools: list[dict],
        tool_executor: dict[str, callable],
        max_retries: int = 2,
        max_tokens: int = 4096,
        use_mini_model: bool = False,
    ):
        self.name          = name
        self.system_prompt = system_prompt
        self.tools         = self._convert_tools_to_openai_format(tools)  # 转换为OpenAI格式
        self.tool_executor = tool_executor    # {tool_name: callable}
        self.max_retries   = max_retries
        self.max_tokens    = max_tokens
        self.model         = settings.openai_model if use_mini_model else settings.openai_model_heavy

    def _convert_tools_to_openai_format(self, tools: list[dict]) -> list[dict]:
        """将 Anthropic 风格的工具定义转换为 OpenAI 格式。"""
        if not tools:
            return []

        openai_tools = []
        for tool in tools:
            # 如果已经是 OpenAI 格式（有 "type" 和 "function" 键），直接使用
            if "type" in tool and "function" in tool:
                openai_tools.append(tool)
            else:
                # 转换 Anthropic 格式到 OpenAI 格式
                openai_tools.append({
                    "type": "function",
                    "function": {
                        "name": tool.get("name", ""),
                        "description": tool.get("description", ""),
                        "parameters": tool.get("input_schema", {
                            "type": "object",
                            "properties": {},
                        })
                    }
                })
        return openai_tools

    def run(self, input_data: dict, output_schema: dict | None = None) -> dict:
        """
        同步运行 Agent，内包含工具调用循环 + 重试。
        返回解析后的 JSON 字典。
        """
        import asyncio
        return asyncio.run(self.run_async(input_data, output_schema))

    async def run_async(self, input_data: dict, output_schema: dict | None = None) -> dict:
        """异步运行 Agent"""
        attempt = 0
        last_error = None

        while attempt <= self.max_retries:
            try:
                result = await self._run_once(input_data, attempt, last_error)
                if output_schema:
                    self._validate(result, output_schema)
                return result
            except Exception as e:
                last_error = str(e)
                logger.warning(
                    f"[{self.name}] attempt {attempt} failed: {e}"
                )
                attempt += 1

        raise RuntimeError(
            f"[{self.name}] all {self.max_retries + 1} attempts failed. "
            f"Last error: {last_error}"
        )

    async def _run_once(
        self,
        input_data: dict,
        attempt: int,
        last_error: str | None,
    ) -> dict:
        """单次 OpenAI API 调用 + 工具循环。"""
        user_content = json.dumps(input_data, ensure_ascii=False)

        # 重试时附加提示
        if attempt > 0:
            user_content = (
                f"[RETRY {attempt}] 上次输出错误：{last_error}。"
                f"请严格遵循输出 Schema。\n\n" + user_content
            )

        messages = [
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": user_content}
        ]
        t0 = time.time()

        while True:
            # OpenAI API call
            response = await client.chat.completions.create(
                model=self.model,
                messages=messages,
                tools=self.tools if self.tools else None,
                temperature=0.0,
                max_tokens=self.max_tokens,
            )

            message = response.choices[0].message
            finish_reason = response.choices[0].finish_reason

            # 工具调用循环
            if finish_reason == "tool_calls" and message.tool_calls:
                # 执行工具
                tool_results = []
                for tool_call in message.tool_calls:
                    func_name = tool_call.function.name
                    func_args = json.loads(tool_call.function.arguments)

                    result = self._call_tool(func_name, func_args)

                    tool_results.append({
                        "role": "tool",
                        "tool_call_id": tool_call.id,
                        "name": func_name,
                        "content": json.dumps(result, ensure_ascii=False),
                    })

                # 添加 assistant 消息和工具结果
                messages.append({
                    "role": "assistant",
                    "content": message.content,
                    "tool_calls": [
                        {
                            "id": tc.id,
                            "type": "function",
                            "function": {
                                "name": tc.function.name,
                                "arguments": tc.function.arguments
                            }
                        }
                        for tc in message.tool_calls
                    ]
                })
                messages.extend(tool_results)
                continue

            # 输出完成
            text = message.content or ""
            elapsed = round(time.time() - t0, 2)
            logger.info(
                f"[{self.name}] done in {elapsed}s | "
                f"input_tokens={response.usage.prompt_tokens} "
                f"output_tokens={response.usage.completion_tokens}"
            )
            return self._parse_json(text)

    def _call_tool(self, tool_name: str, tool_input: dict) -> Any:
        """ToolRegistry 局部执行——拦截未授权工具。"""
        if tool_name not in self.tool_executor:
            raise ValueError(
                f"[{self.name}] 调用未授权工具: {tool_name}"
            )
        logger.debug(f"[{self.name}] tool_call: {tool_name}({tool_input})")
        return self.tool_executor[tool_name](tool_input)

    def _parse_json(self, text: str) -> dict:
        """JSON 提取——容错 markdown 代码块。"""
        text = text.strip()
        # 剔掉 ```json ... ```
        if text.startswith("```"):
            lines = text.split("\n")
            text = "\n".join(lines[1:-1] if lines[-1] == "```" else lines[1:])
        try:
            return json.loads(text)
        except json.JSONDecodeError as e:
            raise ValueError(f"JSON parse error: {e}\n\nRaw: {text[:500]}")

    def _validate(self, result: dict, schema: dict):
        """简化验证: 检查必要字段是否存在。"""
        required = schema.get("required", [])
        missing  = [f for f in required if f not in result]
        if missing:
            raise ValueError(f"缺少必要字段: {missing}")
