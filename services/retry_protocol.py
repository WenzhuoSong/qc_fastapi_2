# services/retry_protocol.py
"""
Structured retry with exponential backoff per service.

Wraps any async service call with configurable retry logic.
Safe no-op if no exceptions occur.

Usage:
    from services.retry_protocol import with_retry, RetryConfig, HTTP_RETRY_CONFIGS

    # Simple usage
    result = await with_retry(
        coro=lambda: finnhub_client.fetch_ticker_news(ticker),
        config=HTTP_RETRY_CONFIGS["finnhub"],
        service_name="finnhub",
    )

    # Custom config
    config = RetryConfig(max_attempts=3, base_delay=2.0, max_delay=30.0)
    result = await with_retry(my_async_fn, config=config, service_name="my_service")
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Callable, Awaitable, Any, Optional

import httpx

logger = logging.getLogger("qc_fastapi_2.retry")

# ─────────────────────────────── Config ──────────────────────────────────────


@dataclass
class RetryConfig:
    max_attempts: int = 3
    base_delay: float = 1.0          # seconds
    max_delay: float = 30.0          # cap at 30s
    exponential_base: float = 2.0     # delay multiplier per attempt
    jitter: bool = True              # add ±20% randomness to avoid thundering herd
    retryable_exceptions: tuple = (
        httpx.TimeoutException,
        httpx.ConnectError,
        httpx.ConnectTimeout,
        httpx.ReadTimeout,
        httpx.WriteError,
        httpx.PoolTimeout,
    )
    log_failures: bool = True

    def delay_for_attempt(self, attempt: int) -> float:
        """Compute delay in seconds for the given attempt number (0-indexed)."""
        delay = min(self.base_delay * (self.exponential_base ** attempt), self.max_delay)
        if self.jitter:
            import random
            jitter = delay * 0.2  # ±20%
            delay = delay + random.uniform(-jitter, jitter)
        return max(0, delay)


# ─────────────────────────────── Predefined Configs ─────────────────────────


HTTP_RETRY_CONFIGS: dict[str, RetryConfig] = {
    "finnhub": RetryConfig(
        max_attempts=3,
        base_delay=2.0,
        max_delay=30.0,
        exponential_base=2.0,
        retryable_exceptions=(
            httpx.TimeoutException,
            httpx.ConnectError,
            httpx.ConnectTimeout,
            httpx.ReadTimeout,
            httpx.WriteError,
            httpx.PoolTimeout,
        ),
    ),
    "alphavantage": RetryConfig(
        max_attempts=2,
        base_delay=5.0,
        max_delay=30.0,
        exponential_base=2.0,
        # Alpha Vantage has 5 req/min on free tier — longer base delay
        retryable_exceptions=(
            httpx.TimeoutException,
            httpx.ConnectError,
            httpx.ConnectTimeout,
            httpx.ReadTimeout,
            httpx.WriteError,
            httpx.PoolTimeout,
            httpx.HTTPStatusError,   # retry on 429 rate limit
        ),
    ),
    "rss": RetryConfig(
        max_attempts=2,
        base_delay=1.0,
        max_delay=10.0,
        exponential_base=2.0,
        retryable_exceptions=(
            httpx.TimeoutException,
            httpx.ConnectError,
            httpx.ConnectTimeout,
            httpx.ReadTimeout,
            httpx.WriteError,
        ),
    ),
    "qc_api": RetryConfig(
        max_attempts=3,
        base_delay=2.0,
        max_delay=15.0,
        exponential_base=2.0,
        retryable_exceptions=(
            httpx.TimeoutException,
            httpx.ConnectError,
            httpx.ConnectTimeout,
            httpx.ReadTimeout,
            httpx.WriteError,
            httpx.PoolTimeout,
        ),
    ),
    "telegram": RetryConfig(
        max_attempts=2,
        base_delay=2.0,
        max_delay=10.0,
        exponential_base=2.0,
        retryable_exceptions=(
            httpx.TimeoutException,
            httpx.ConnectError,
            httpx.ConnectTimeout,
            httpx.ReadTimeout,
        ),
    ),
    "llm": RetryConfig(
        max_attempts=3,
        base_delay=5.0,
        max_delay=30.0,
        exponential_base=2.0,
        retryable_exceptions=(
            httpx.TimeoutException,
            httpx.ConnectError,
            httpx.ConnectTimeout,
            httpx.ReadTimeout,
            httpx.WriteError,
            httpx.PoolTimeout,
        ),
    ),
}


# ─────────────────────────────── Core Retry Function ────────────────────────


async def with_retry(
    coro_fn: Callable[[], Awaitable[Any]],
    config: Optional[RetryConfig] = None,
    service_name: str = "unknown",
    on_retry: Optional[Callable[[int, Exception], Awaitable[None]]] = None,
) -> Any:
    """
    Execute coro_fn() with exponential backoff retry.

    Args:
        coro_fn: Async callable to execute. Must be a callable that returns
                 an awaitable (pass a lambda or partial if you need args).
        config: RetryConfig instance. Defaults to 3 attempts, 1s base.
        service_name: Label for logging.
        on_retry: Optional async callback called on each retry (attempt, error).
                  Can be used to record metrics or update circuit breaker.

    Returns:
        The result of the successful coro_fn() call.

    Raises:
        The last exception if all retries are exhausted.
        Re-raises non-retryable exceptions immediately (not counted as attempt).

    Example:
        result = await with_retry(
            lambda: client.get("/api/data"),
            config=HTTP_RETRY_CONFIGS["finnhub"],
            service_name="finnhub",
        )
    """
    config = config or RetryConfig()
    last_error: Optional[Exception] = None

    for attempt in range(config.max_attempts):
        try:
            return await coro_fn()
        except config.retryable_exceptions as e:
            last_error = e
            if config.log_failures:
                logger.warning(
                    f"[{service_name}] attempt {attempt + 1}/{config.max_attempts} "
                    f"failed: {type(e).__name__}: {e}"
                )
            if on_retry and asyncio.iscoroutinefunction(on_retry):
                try:
                    await on_retry(attempt + 1, e)
                except Exception as cb_err:
                    logger.warning(f"[{service_name}] on_retry callback failed: {cb_err}")

            if attempt < config.max_attempts - 1:
                delay = config.delay_for_attempt(attempt)
                logger.info(f"[{service_name}] retrying in {delay:.1f}s...")
                await asyncio.sleep(delay)
            else:
                logger.error(
                    f"[{service_name}] all {config.max_attempts} attempts exhausted. "
                    f"Last error: {last_error}"
                )
        except Exception as e:
            # Non-retryable exception — propagate immediately
            if config.log_failures:
                logger.error(
                    f"[{service_name}] non-retryable exception: {type(e).__name__}: {e}"
                )
            raise

    # All retries exhausted
    raise RuntimeError(
        f"[{service_name}] failed after {config.max_attempts} attempts. "
        f"Last error: {last_error}"
    )


# ─────────────────────────────── Sync Wrapper ────────────────────────────────


def retry_sync(
    fn: Callable[[], Any],
    config: Optional[RetryConfig] = None,
    service_name: str = "unknown",
) -> Any:
    """
    Synchronous version of with_retry for non-async callables.
    Uses time.sleep instead of asyncio.sleep.
    """
    config = config or RetryConfig()
    last_error: Optional[Exception] = None

    for attempt in range(config.max_attempts):
        try:
            return fn()
        except config.retryable_exceptions as e:  # type: ignore
            last_error = e
            if config.log_failures:
                logger.warning(
                    f"[{service_name}] attempt {attempt + 1}/{config.max_attempts} "
                    f"failed: {type(e).__name__}: {e}"
                )
            if attempt < config.max_attempts - 1:
                delay = config.delay_for_attempt(attempt)
                time.sleep(delay)
        except Exception as e:
            if config.log_failures:
                logger.error(
                    f"[{service_name}] non-retryable exception: {type(e).__name__}: {e}"
                )
            raise

    raise RuntimeError(
        f"[{service_name}] failed after {config.max_attempts} attempts. "
        f"Last error: {last_error}"
    )


# ─────────────────────────────── Convenience Decorators ──────────────────────


def with_http_retry(service: str):
    """
    Decorator for HTTP client methods using predefined configs.

    Usage:
        @with_http_retry("finnhub")
        async def fetch_news(self, ticker: str):
            return await self._get(f"/news?category=general")
    """
    def decorator(fn: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
        async def wrapper(*args, **kwargs) -> Any:
            config = HTTP_RETRY_CONFIGS.get(service, RetryConfig())
            return await with_retry(
                lambda: fn(*args, **kwargs),
                config=config,
                service_name=service,
            )
        return wrapper
    return decorator