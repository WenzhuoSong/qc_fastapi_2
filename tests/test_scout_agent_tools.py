"""The chat must be able to answer what the dashboard shows: every dashboard page has at least one agent tool."""
import re
import unittest
from pathlib import Path

from scout import agent

APP = Path(__file__).resolve().parents[1] / "scout" / "web" / "app.py"
NOT_DATA = {"/chat"}            # the chat itself


def dashboard_pages() -> set[str]:
    routes = re.findall(r'@app\.get\("(/[^"]*)", response_class=HTMLResponse', APP.read_text())
    return {re.sub(r"/\{[^}]+\}$", "", r) or "/" for r in routes} - NOT_DATA


class ToolCoverageTests(unittest.TestCase):
    def test_every_dashboard_page_has_a_tool(self):
        missing = sorted(dashboard_pages() - set(agent.PAGES))
        self.assertEqual(missing, [], f"pages without an agent tool: {missing} (add one with @tool(..., pages=...))")

    def test_pages_named_by_tools_exist(self):
        self.assertEqual(sorted(set(agent.PAGES) - dashboard_pages()), [])

    def test_registry_is_consistent(self):
        names = [t["name"] for t in agent.TOOLS]
        self.assertEqual(len(names), len(set(names)))
        self.assertEqual(sorted(names), sorted(agent.HANDLERS))
        for t in agent.TOOLS:
            self.assertTrue(t["description"], t["name"])
            self.assertEqual(t["parameters"]["type"], "object", t["name"])

    def test_the_prompt_mentions_every_tool(self):
        quiet = {"get_price_history", "get_scoreboard"}           # self-explanatory, picked by description alone
        unmentioned = [t["name"] for t in agent.TOOLS if t["name"] not in agent.SYSTEM_PROMPT and t["name"] not in quiet]
        self.assertEqual(unmentioned, [])


if __name__ == "__main__":
    unittest.main()
