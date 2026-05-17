"""Contract tests for the abstract :class:`BaseAdapter`.

These tests are deliberately framework-y: they verify the *shape* of
the base class so that as we add SEP Scoreboards / SWPC / GIM / GOES /
DSCOVR adapters they all conform. If you change the base class API,
these tests should fail loudly and you should fix the downstream
adapters before merging.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any, ClassVar

import httpx
import pytest

from helios_connectors import (
    BaseAdapter,
    NormalizedRecord,
    SourceID,
)
from helios_connectors.ratelimit import RateLimitConfig


class _StubAdapter(BaseAdapter):
    """A minimal concrete adapter used to test the base class contract."""

    source_id: ClassVar[SourceID] = SourceID.DONKI

    async def fetch(  # type: ignore[override]
        self,
        *,
        start: datetime,
        end: datetime,
        **kwargs: Any,
    ) -> AsyncIterator[NormalizedRecord]:
        provenance = self._emit_provenance(
            model_id="stub/probe",
            dataset_refs=("stub-1",),
            timestamp=start,
            value="hello-world",
            value_units="none",
            extra={"lineage": ["upstream-1"]},
            record_id="stub-1",
        )
        yield NormalizedRecord(
            source=self.source_id,
            record_type="stub",
            event_time=start,
            value={"hello": "world"},
            value_units="none",
            provenance=provenance,
        )


@pytest.mark.asyncio
async def test_subclass_must_set_source_id() -> None:
    """A subclass that forgets to set source_id should fail loudly."""

    # Python lets you instantiate a class without a ClassVar at definition
    # time, but we want a sanity check that the field has been set on real
    # adapters.
    class _Bad(BaseAdapter):  # type: ignore[misc]
        async def fetch(  # type: ignore[override]
            self, *, start: datetime, end: datetime, **kwargs: Any
        ) -> AsyncIterator[NormalizedRecord]:
            if False:
                yield None  # pragma: no cover

    # Forgetting source_id surfaces as AttributeError on first access.
    bad = _Bad(cache=False)
    with pytest.raises(AttributeError):
        _ = bad.source_id
    await bad.aclose()


@pytest.mark.asyncio
async def test_fetch_streams_records() -> None:
    async with _StubAdapter(cache=False) as adapter:
        start = datetime(2024, 5, 10, tzinfo=UTC)
        recs = [r async for r in adapter.fetch(start=start, end=start)]
    assert len(recs) == 1
    assert recs[0].source == SourceID.DONKI
    assert recs[0].record_type == "stub"
    assert recs[0].provenance.id == "stub-1"
    assert recs[0].provenance.extra is not None
    assert recs[0].provenance.extra["lineage"] == ["upstream-1"]
    # Provenance UTC normalization
    assert recs[0].provenance.timestamp.tzinfo is not None
    assert recs[0].provenance.ingestion_timestamp.tzinfo is not None


def test_fetch_sync_drains_records() -> None:
    adapter = _StubAdapter(cache=False)
    start = datetime(2024, 5, 10, tzinfo=UTC)
    recs = adapter.fetch_sync(start=start, end=start)
    assert len(recs) == 1
    assert recs[0].value == {"hello": "world"}


@pytest.mark.asyncio
async def test_fetch_sync_refuses_when_loop_running() -> None:
    """Sync wrapper must reject calls from inside an event loop."""
    adapter = _StubAdapter(cache=False)
    start = datetime(2024, 5, 10, tzinfo=UTC)
    with pytest.raises(RuntimeError, match="running event loop"):
        adapter.fetch_sync(start=start, end=start)
    await adapter.aclose()


@pytest.mark.asyncio
async def test_default_rate_limit_applied() -> None:
    """If no rate limit is passed, the subclass default is used."""

    class _SlowAdapter(_StubAdapter):
        def _default_rate_limit(self) -> RateLimitConfig:
            return RateLimitConfig(rate_per_second=2.0, burst=1)

    async with _SlowAdapter(cache=False) as adapter:
        assert adapter._ratelimiter.config.rate_per_second == 2.0


@pytest.mark.asyncio
async def test_provenance_helper_normalizes_timestamps() -> None:
    """_emit_provenance must produce UTC-aware timestamps."""
    async with _StubAdapter(cache=False) as adapter:
        # Naive datetime in — should come out aware UTC
        prov = adapter._emit_provenance(
            model_id="m",
            dataset_refs=("x",),
            timestamp=datetime(2024, 5, 10, 12, 0, 0),
            value=1,
            value_units="none",
        )
    assert prov.timestamp.tzinfo is not None
    offset = prov.timestamp.utcoffset()
    assert offset is not None
    assert offset.total_seconds() == 0  # UTC


@pytest.mark.asyncio
async def test_external_client_not_closed() -> None:
    """If the caller supplies a client, the adapter must not close it."""

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[])

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)
    adapter = _StubAdapter(client=client, cache=False)
    await adapter.aclose()
    # client must still be usable
    response = await client.get("https://example.invalid/")
    assert response.status_code == 200
    await client.aclose()
