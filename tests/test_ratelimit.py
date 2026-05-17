"""Tests for the async token-bucket rate limiter."""

from __future__ import annotations

import asyncio
import time

import pytest

from helios_connectors import RateLimitConfig, RateLimiter


def test_effective_burst_defaults() -> None:
    assert RateLimitConfig(rate_per_second=10.0).effective_burst() == 10
    assert RateLimitConfig(rate_per_second=0.5).effective_burst() == 1
    assert RateLimitConfig(rate_per_second=10.0, burst=20).effective_burst() == 20


@pytest.mark.asyncio
async def test_burst_consumed_immediately() -> None:
    limiter = RateLimiter(RateLimitConfig(rate_per_second=10.0, burst=5))
    start = time.monotonic()
    for _ in range(5):
        await limiter.acquire()
    elapsed = time.monotonic() - start
    # 5 in the bucket; should be near-instant
    assert elapsed < 0.1


@pytest.mark.asyncio
async def test_throttles_after_burst() -> None:
    # 5 RPS, burst 2 → after the burst, each subsequent call waits ~200ms
    limiter = RateLimiter(RateLimitConfig(rate_per_second=5.0, burst=2))
    # Drain the burst
    await limiter.acquire()
    await limiter.acquire()
    # Next two calls should incur waits
    start = time.monotonic()
    await limiter.acquire()
    await limiter.acquire()
    elapsed = time.monotonic() - start
    # Two refill periods at 5 RPS = ~400ms. Lower bound 300ms allows scheduler jitter.
    assert elapsed >= 0.3


@pytest.mark.asyncio
async def test_concurrent_acquire_serializes() -> None:
    """Concurrent acquire calls must respect the bucket capacity."""
    limiter = RateLimiter(RateLimitConfig(rate_per_second=10.0, burst=3))
    start = time.monotonic()
    await asyncio.gather(*(limiter.acquire() for _ in range(6)))
    elapsed = time.monotonic() - start
    # 3 burst tokens free, 3 more at 10 RPS → ~300ms refill
    assert elapsed >= 0.25


@pytest.mark.asyncio
async def test_request_too_many_tokens() -> None:
    limiter = RateLimiter(RateLimitConfig(rate_per_second=10.0, burst=5))
    with pytest.raises(ValueError):
        await limiter.acquire(tokens=10)


@pytest.mark.asyncio
async def test_zero_tokens_rejected() -> None:
    limiter = RateLimiter(RateLimitConfig(rate_per_second=10.0))
    with pytest.raises(ValueError):
        await limiter.acquire(tokens=0)


@pytest.mark.asyncio
async def test_multi_token_acquire() -> None:
    limiter = RateLimiter(RateLimitConfig(rate_per_second=10.0, burst=5))
    await limiter.acquire(tokens=3)
    # tokens left: ~2; next 2-token request still fits
    await limiter.acquire(tokens=2)
