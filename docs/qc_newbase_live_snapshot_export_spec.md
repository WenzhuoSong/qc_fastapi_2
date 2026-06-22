# QC newBase Live Snapshot Export Spec

Purpose: export observer-only telemetry from QuantConnect/newBase to
FastAPI/Railway. This export must not read instructions back from FastAPI and
must not let FastAPI influence targets or orders.

Endpoint:

- `POST https://<fastapi-host>/api/webhook/qc`
- Body: gzip-compressed JSON
- Auth: existing `X-QC-Signature` HMAC if available, otherwise existing legacy
  webhook headers during migration.

Packet:

The test fixture at `examples/newbase_live_snapshot_example.json` is the
canonical minimal example used by the FastAPI normalizer tests.

```json
{
  "packet_type": "newbase_live_snapshot",
  "schema_version": "newbase_live_snapshot_v1",
  "snapshot_uid": "newbase:2026-06-22:close",
  "trading_date": "2026-06-22",
  "timestamp_utc": "2026-06-22T20:05:00Z",
  "source": "quantconnect",
  "strategy": {
    "strategy_id": "newbase",
    "mode": "live_paper",
    "algorithm_version": "newBase.py:<commit-or-version>"
  },
  "portfolio": {
    "total_value": 123456.78,
    "cash": 12345.67,
    "cash_pct": 0.10,
    "daily_return": 0.0042,
    "cumulative_return": 0.031,
    "current_drawdown": -0.025,
    "turnover": 0.012,
    "fees": 3.21
  },
  "benchmarks": {
    "QQQ": {
      "daily_return": 0.0031,
      "cumulative_return": 0.028,
      "price": 512.34
    },
    "SPY": {
      "daily_return": 0.0022,
      "cumulative_return": 0.018,
      "price": 630.12
    }
  },
  "holdings": [
    {
      "ticker": "NVDA",
      "quantity": 10,
      "weight": 0.051,
      "market_value": 6290.1,
      "unrealized_pnl": 123.45,
      "unrealized_pnl_pct": 0.02,
      "holding_days": 42
    }
  ],
  "orders": [
    {
      "order_id": 123,
      "ticker": "NVDA",
      "side": "buy",
      "quantity": 1,
      "status": "filled",
      "time_utc": "2026-06-22T15:31:00Z"
    }
  ],
  "fills": [
    {
      "order_id": 123,
      "ticker": "NVDA",
      "quantity": 1,
      "fill_price": 629.01,
      "fee": 0.01,
      "time_utc": "2026-06-22T15:31:02Z"
    }
  ],
  "metrics": {
    "rolling_beta_vs_qqq": 0.82,
    "rolling_excess_vs_qqq": 0.0018,
    "turnover": 0.012,
    "fees": 3.21
  }
}
```

Minimum viable fields:

- `packet_type`
- `schema_version`
- `snapshot_uid`
- `trading_date`
- `timestamp_utc`
- `strategy.strategy_id`
- `portfolio.total_value`
- `portfolio.daily_return`
- `benchmarks.QQQ.daily_return`
- `benchmarks.SPY.daily_return`
- `holdings`

Recommended QC helper sketch:

```python
import gzip
import hashlib
import hmac
import json

def send_newbase_live_snapshot(self):
    payload = {
        "packet_type": "newbase_live_snapshot",
        "schema_version": "newbase_live_snapshot_v1",
        "snapshot_uid": f"newbase:{self.Time.date().isoformat()}:{self.Time.strftime('%H%M%S')}",
        "trading_date": self.Time.date().isoformat(),
        "timestamp_utc": self.UtcTime.isoformat() + "Z",
        "source": "quantconnect",
        "strategy": {
            "strategy_id": "newbase",
            "mode": "live_paper" if self.LiveMode else "backtest",
            "algorithm_version": "newBase.py:<version>",
        },
        "portfolio": {
            "total_value": float(self.Portfolio.TotalPortfolioValue),
            "cash": float(self.Portfolio.Cash),
            "cash_pct": float(self.Portfolio.Cash / self.Portfolio.TotalPortfolioValue)
            if self.Portfolio.TotalPortfolioValue else None,
            "daily_return": self._newbase_daily_return(),
            "cumulative_return": self._newbase_cumulative_return(),
            "current_drawdown": self._newbase_current_drawdown(),
            "turnover": self._newbase_turnover_today(),
            "fees": self._newbase_fees_today(),
        },
        "benchmarks": {
            "QQQ": self._benchmark_payload("QQQ"),
            "SPY": self._benchmark_payload("SPY"),
        },
        "holdings": [
            {
                "ticker": holding.Symbol.Value,
                "quantity": float(holding.Quantity),
                "weight": float(holding.HoldingsValue / self.Portfolio.TotalPortfolioValue)
                if self.Portfolio.TotalPortfolioValue else 0.0,
                "market_value": float(holding.HoldingsValue),
                "unrealized_pnl": float(holding.UnrealizedProfit),
                "unrealized_pnl_pct": float(holding.UnrealizedProfitPercent),
                "holding_days": self._holding_days(holding.Symbol),
            }
            for holding in self.Portfolio.Values
            if holding.Invested
        ],
        "orders": self._newbase_recent_orders(),
        "fills": self._newbase_recent_fills(),
        "metrics": {
            "rolling_beta_vs_qqq": self._rolling_beta_vs_qqq(),
            "rolling_excess_vs_qqq": self._rolling_excess_vs_qqq(),
            "turnover": self._newbase_turnover_today(),
            "fees": self._newbase_fees_today(),
        },
    }
    body = json.dumps(payload, sort_keys=True).encode("utf-8")
    gz_body = gzip.compress(body)
    signature = hmac.new(self.webhook_secret.encode("utf-8"), gz_body, hashlib.sha256).hexdigest()
    self.Notify.Web(
        self.fastapi_webhook_url,
        gz_body,
        headers={"X-QC-Signature": signature, "Content-Encoding": "gzip"},
    )
```

Notes:

- The helper methods beginning with `_newbase_` can be simple internal counters
  at first. It is acceptable to send `null` for optional fields until the QC
  patch matures.
- Do not read any response from FastAPI as a trading instruction.
- Do not add any FastAPI-controlled target override to newBase.
