"""Async token-bucket rate limiter.

The shape is dumb on purpose: each adapter owns one :class:`RateLimiter`
and ``await`` s :meth:`RateLimiter.acquire` before every outbound HTTP
request. There is no global registry; rate limits are *per adapter
instance* because we want test isolation and we never want a slow source
to back-pressure a fast one.

Sourcing policy:

- NASA CCMC services (DONKI, SEP Scoreboards) cap at 10 RPS by default.
  Per CCMC's published guidance and operator courtesy.
- NOAA SWPC caps at 5 RPS by default; their JSON endpoints have visibly
  tighter limits during space-weather events.
- DEMO_KEY (no-auth NASA API) is much tighter (~30 req/hr, 50 req/day).
  Adapters should bump down to ~1 RPS when DEMO_KEY is in use.

Override via :class:`RateLimitConfig`. The defaults are conservative; a
production deployment with a real NASA API key can safely set higher
caps.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass

logger = logging.getLogger(__name__)

__all__ = ["RateLimitConfig", "RateLimiter"]


@dataclass(frozen=True, slots=True)
class RateLimitConfig:
    """Configuration for a token-bucket rate limiter.

    Attributes:
        rate_per_second: target steady-state request rate (tokens
            replenished per second).
        burst: bucket capacity. A short burst of this many requests can
            fire instantly before throttling kicks in. Defaults to
            ``ceil(rate_per_second)`` if 0.
    """

    rate_per_second: float = 10.0
    burst: int = 0

    def effective_burst(self) -> int:
        """Return the burst capacity, defaulting from ``rate_per_second``."""
        if self.burst > 0:
            return self.burst
        return max(1, int(self.rate_per_second + 0.999))


class RateLimiter:
    """A thread-unsafe (but asyncio-safe) token bucket.

    Use one instance per adapter. Call :meth:`acquire` before every
    outbound HTTP request. The bucket refills continuously at
    ``config.rate_per_second`` tokens/sec and is capped at
    ``config.effective_burst()`` tokens.

    Implementation note: we use a monotonic clock so the limiter behaves
    consistently across system clock adjustments. The asyncio lock makes
    concurrent ``acquire`` calls in the same event loop serialize cleanly.
    """

    def __init__(self, config: RateLimitConfig | None = None) -> None:
        self._config = config or RateLimitConfig()
        self._capacity = self._config.effective_burst()
        self._tokens: float = float(self._capacity)
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    @property
    def config(self) -> RateLimitConfig:
        """The configuration this limiter was constructed with."""
        return self._config

    @property
    def tokens(self) -> float:
        """Current token count. Exposed for tests; do not consume directly."""
        return self._tokens

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(
            float(self._capacity),
            self._tokens + elapsed * self._config.rate_per_second,
        )
        self._last_refill = now

    async def acquire(self, tokens: int = 1) -> None:
        """Block until ``tokens`` tokens are available, then consume them.

        Args:
            tokens: number of tokens this request costs. The default of 1
                matches the "one token per request" convention. Increase
                only for bulk endpoints that should count as multiple
                requests against the source's quota.
        """

        if tokens <= 0:
            raise ValueError("tokens must be positive")
        if tokens > self._capacity:
            raise ValueError(f"requested {tokens} tokens but bucket capacity is {self._capacity}")

        async with self._lock:
            while True:
                self._refill()
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return
                deficit = tokens - self._tokens
                wait = deficit / self._config.rate_per_second
                logger.debug(
                    "rate-limit wait: deficit=%.3f tokens, sleeping %.3fs",
                    deficit,
                    wait,
                )
                await asyncio.sleep(wait)
