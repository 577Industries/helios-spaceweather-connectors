"""Tests for the shared httpx client factory + retry wrapper."""

from __future__ import annotations

import httpx
import pytest

from helios_connectors.http import (
    DEFAULT_USER_AGENT,
    make_client,
    request_with_retry,
)


def test_default_user_agent_is_repo_url() -> None:
    """The User-Agent must include the repo URL per NASA API etiquette."""
    assert "github.com/577-Industries" in DEFAULT_USER_AGENT
    assert "helios-spaceweather-connectors" in DEFAULT_USER_AGENT


def test_make_client_sets_headers() -> None:
    client = make_client(extra_headers={"X-Custom": "x"})
    try:
        assert client.headers["User-Agent"] == DEFAULT_USER_AGENT
        assert client.headers["Accept"] == "application/json"
        assert client.headers["X-Custom"] == "x"
    finally:
        # AsyncClient must be closed; using sync close via the underlying transport.
        # close() is safe to call directly on the sync wrapper.
        pass


@pytest.mark.asyncio
async def test_make_client_close() -> None:
    client = make_client()
    await client.aclose()


@pytest.mark.asyncio
async def test_request_with_retry_succeeds_first_try() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"ok": True})

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)
    response = await request_with_retry(client, "GET", "https://example.invalid/x")
    assert response.json() == {"ok": True}
    await client.aclose()


@pytest.mark.asyncio
async def test_request_with_retry_eventually_succeeds() -> None:
    """A transient 503 should be retried until success."""
    calls = {"n": 0}

    def handler(_request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] < 3:
            return httpx.Response(503, json={})
        return httpx.Response(200, json={"ok": True})

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)
    response = await request_with_retry(client, "GET", "https://example.invalid/x", max_attempts=4)
    assert response.status_code == 200
    assert calls["n"] == 3
    await client.aclose()


@pytest.mark.asyncio
async def test_request_with_retry_gives_up() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json={})

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)
    with pytest.raises(httpx.HTTPStatusError):
        await request_with_retry(client, "GET", "https://example.invalid/x", max_attempts=2)
    await client.aclose()


@pytest.mark.asyncio
async def test_request_with_retry_not_retried_on_4xx() -> None:
    """A 404 is a deterministic miss and must not be retried."""
    calls = {"n": 0}

    def handler(_request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(404, json={})

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)
    with pytest.raises(httpx.HTTPStatusError):
        await request_with_retry(client, "GET", "https://example.invalid/x", max_attempts=4)
    assert calls["n"] == 1
    await client.aclose()


@pytest.mark.asyncio
async def test_safe_log_params_filters(caplog: pytest.LogCaptureFixture) -> None:
    """Only params listed in safe_log_params should appear in DEBUG logs."""
    import logging

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={})

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)
    caplog.set_level(logging.DEBUG, logger="helios_connectors.http")
    await request_with_retry(
        client,
        "GET",
        "https://example.invalid/x",
        params={"startDate": "2024-05-10", "api_key": "SECRET"},
        safe_log_params=("startDate",),
    )
    for record in caplog.records:
        assert "SECRET" not in record.getMessage()
    await client.aclose()
