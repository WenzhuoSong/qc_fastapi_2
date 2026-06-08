import unittest
from dataclasses import dataclass
from unittest.mock import AsyncMock, patch

from services.trading_analysis_gate import evaluate_trading_analysis_gate


@dataclass
class _FakeMarketStatus:
    is_trading_day: bool
    is_open: bool
    reason: str
    phase: str = "open"

    def to_dict(self) -> dict:
        return {
            "is_trading_day": self.is_trading_day,
            "is_open": self.is_open,
            "reason": self.reason,
            "phase": self.phase,
        }


class TradingAnalysisGateTests(unittest.IsolatedAsyncioTestCase):
    async def test_blocks_trading_day_before_open_by_default(self):
        with patch(
            "services.trading_analysis_gate.us_equity_market_status",
            return_value=_FakeMarketStatus(True, False, "opens_soon", "opens_soon"),
        ), patch(
            "services.trading_analysis_gate.build_operational_health_snapshot",
            new=AsyncMock(),
        ) as health:
            gate = await evaluate_trading_analysis_gate()

        self.assertFalse(gate["allowed"])
        self.assertEqual(gate["reason"], "market_not_open:opens_soon")
        health.assert_not_awaited()

    async def test_analysis_only_can_allow_closed_market_but_still_requires_news(self):
        with patch(
            "services.trading_analysis_gate.us_equity_market_status",
            return_value=_FakeMarketStatus(True, False, "after_close", "closed"),
        ), patch(
            "services.trading_analysis_gate.build_operational_health_snapshot",
            new=AsyncMock(return_value={"overall": "ok", "checks": {"news_cache": {"state": "ok"}}}),
        ):
            gate = await evaluate_trading_analysis_gate(require_market_open=False)

        self.assertTrue(gate["allowed"])
        self.assertEqual(gate["reason"], "ok")

    async def test_open_market_blocks_stale_news(self):
        with patch(
            "services.trading_analysis_gate.us_equity_market_status",
            return_value=_FakeMarketStatus(True, True, "regular_hours", "open"),
        ), patch(
            "services.trading_analysis_gate.build_operational_health_snapshot",
            new=AsyncMock(return_value={"overall": "warning", "checks": {"news_cache": {"state": "stale", "reason": "missed_run"}}}),
        ):
            gate = await evaluate_trading_analysis_gate()

        self.assertFalse(gate["allowed"])
        self.assertEqual(gate["reason"], "news_cache_not_ready:missed_run")


if __name__ == "__main__":
    unittest.main()
