"""Shared httpx client factory with retry + sensible defaults.

Every adapter should obtain its httpx client from :func:`make_client`
rather than constructing one directly. Reasons:

1. **User-Agent is set in one place.** NASA APIs explicitly request a
   meaningful User-Agent; sending the default Python-httpx UA can get a
   client throttled or blacklisted at peak times.
2. **Retries are centralized.** We retry on transient 5xx, 429, and
   :class:`httpx.TransportError`. Per NASA API etiquette, exponential
   backoff capped at 30s.
3. **Timeouts are sensible.** Most space-weather JSON endpoints respond
   in well under a second; we use a 30s total timeout to survive cold
   AWS Lambda starts on the server side without hanging requests
   forever.
4. **Connection pooling.** A single client gets reused for the lifetime
   of an adapter instance, so HTTPS handshakes amortize.

API keys live in headers, never URLs. We never log query strings that
might include them.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable
from typing import Any

import httpx
from tenacity import (
    AsyncRetrying,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

__all__ = ["DEFAULT_USER_AGENT", "make_client", "request_with_retry"]

logger = logging.getLogger(__name__)

DEFAULT_USER_AGENT = (
    "helios-spaceweather-connectors/0.1.0 "
    "(https://github.com/577Industries/helios-spaceweather-connectors)"
)

# HTTP statuses we retry. 429 = rate-limited. 5xx = server-side glitch.
_RETRYABLE_STATUSES: frozenset[int] = frozenset({429, 500, 502, 503, 504})


def _is_retryable(exc: BaseException) -> bool:
    """True if an exception represents a transient failure worth retrying."""
    if isinstance(exc, httpx.TransportError):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in _RETRYABLE_STATUSES
    return False


def make_client(
    *,
    base_url: str = "",
    timeout: float = 30.0,
    user_agent: str = DEFAULT_USER_AGENT,
    extra_headers: dict[str, str] | None = None,
) -> httpx.AsyncClient:
    """Create a configured :class:`httpx.AsyncClient`.

    The caller is responsible for ``await client.aclose()`` (or
    ``async with``). We do not return a context manager here because
    adapters typically want to manage client lifetime themselves.

    Args:
        base_url: prefix for relative URLs. Empty by default.
        timeout: total request timeout in seconds. Includes connect,
            read, write, and pool acquisition.
        user_agent: User-Agent header. Override only if you have a good
            reason; NASA APIs prefer the default.
        extra_headers: additional headers (e.g. an Earthdata token).
    """

    headers: dict[str, str] = {
        "User-Agent": user_agent,
        "Accept": "application/json",
    }
    if extra_headers:
        headers.update(extra_headers)
    return httpx.AsyncClient(
        base_url=base_url,
        timeout=httpx.Timeout(timeout, connect=10.0),
        headers=headers,
        follow_redirects=True,
        # Modest connection pool: more than we need for any single adapter,
        # but small enough that an over-eager fanout can't open hundreds of
        # sockets at once.
        limits=httpx.Limits(max_connections=20, max_keepalive_connections=5),
    )


async def request_with_retry(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    max_attempts: int = 4,
    safe_log_params: Iterable[str] = (),
) -> httpx.Response:
    """Issue an HTTP request with exponential-backoff retry.

    Logs the URL and status at DEBUG level (never the API key). The
    ``safe_log_params`` allowlist controls which params are included in
    the debug log entry — by default, nothing is logged, which is safe
    but unhelpful; adapters should pass through user-visible filter
    params they want to see in debug output.

    Raises:
        httpx.HTTPStatusError: if the response is a final non-2xx after
            retries are exhausted.
        httpx.TransportError: on terminal network failure.
    """

    safe_log_set = set(safe_log_params)
    safe_params = {k: v for k, v in params.items() if k in safe_log_set} if params else None

    async for attempt in AsyncRetrying(
        stop=stop_after_attempt(max_attempts),
        wait=wait_exponential(multiplier=1.0, min=1.0, max=30.0),
        retry=retry_if_exception(_is_retryable),
        reraise=True,
    ):
        with attempt:
            logger.debug(
                "HTTP %s %s%s (attempt %d/%d)",
                method.upper(),
                url,
                f" params={safe_params}" if safe_params else "",
                attempt.retry_state.attempt_number,
                max_attempts,
            )
            response = await client.request(method, url, params=params)
            logger.debug(
                "HTTP %s %s -> %d",
                method.upper(),
                url,
                response.status_code,
            )
            response.raise_for_status()
            return response

    # tenacity's reraise=True means we never reach this line, but mypy needs it.
    raise RuntimeError("unreachable: tenacity should have raised")
