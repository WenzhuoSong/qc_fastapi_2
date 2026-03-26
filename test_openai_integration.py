#!/usr/bin/env python3
"""
Test script to verify OpenAI integration in BaseAgent
"""
import asyncio
from agents.base_agent import BaseAgent


def tool_echo(inp: dict) -> dict:
    """Simple echo tool for testing"""
    return {"echo": inp.get("message", "no message")}


TOOLS_DEF = [
    {
        "name": "echo",
        "description": "Echo back the input message",
        "input_schema": {
            "type": "object",
            "properties": {
                "message": {
                    "type": "string",
                    "description": "Message to echo"
                }
            },
            "required": ["message"]
        }
    }
]

SYSTEM_PROMPT = """You are a test agent.
When given a greeting, call the echo tool with a friendly response message.
Then output JSON with the structure: {"status": "success", "message": "..."}"""

OUTPUT_SCHEMA = {
    "required": ["status", "message"]
}


async def test_agent():
    """Test the BaseAgent with OpenAI"""
    print("🧪 Testing BaseAgent with OpenAI integration...\n")

    agent = BaseAgent(
        name="TEST_AGENT",
        system_prompt=SYSTEM_PROMPT,
        tools=TOOLS_DEF,
        tool_executor={"echo": tool_echo},
        max_retries=1,
        max_tokens=500,
        use_mini_model=True,  # Use gpt-4o-mini for testing
    )

    input_data = {"greeting": "Hello, Agentix!"}

    try:
        result = await agent.run_async(input_data, OUTPUT_SCHEMA)
        print("✅ Test passed!")
        print(f"Result: {result}\n")
        return True
    except Exception as e:
        print(f"❌ Test failed: {e}\n")
        return False


def main():
    """Run the test"""
    import os
    from config import get_settings

    settings = get_settings()

    if not settings.openai_api_key:
        print("❌ Error: OPENAI_API_KEY not set in .env")
        print("   Please copy .env.example to .env and add your API key")
        return False

    print(f"Using OpenAI model: {settings.openai_model_mini}\n")

    success = asyncio.run(test_agent())

    if success:
        print("🎉 OpenAI integration is working correctly!")
        return True
    else:
        print("⚠️  Please check your OpenAI API key and network connection")
        return False


if __name__ == "__main__":
    import sys
    sys.exit(0 if main() else 1)
