"""Abstract base class shared by all helios-connector adapters.

A *connector adapter* is a single class that knows how to talk to one
upstream space-weather data source and emit
:class:`~helios_connectors.schema.NormalizedRecord` objects against a
unified shape. Every adapter:

1. Owns its own httpx client, rate limiter, and (optional) cache.
2. Exposes an async, streaming :meth:`BaseAdapter.fetch` for production
   pipelines that want to apply backpressure.
3. Exposes a sync :meth:`BaseAdapter.fetch_sync` for notebooks and
   batch scripts that don't want to think about event loops.
4. Emits provenance for every record via
   :meth:`BaseAdapter._emit_provenance`.

Subclasses must:

- Set :attr:`BaseAdapter.source_id` as a class attribute (e.g.
  ``source_id = SourceID.DONKI``).
- Implement :meth:`BaseAdapter.fetch`.

The base class deliberately makes the cache and rate limiter optional
(both default to live but configurable, and tests pass disabled ones).
"""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Iterable
from datetime import UTC, datetime
from typing import Any, ClassVar
from uuid import uuid4

import httpx
from helios_provenance.models import Agent, HeliosModelOutputRecord

from ..cache import FileCache
from ..http import make_client
from ..ratelimit import RateLimitConfig, RateLimiter
from ..schema import NormalizedRecord, SourceID

__all__ = ["BaseAdapter"]

logger = logging.getLogger(__name__)


def _ensure_utc(ts: datetime) -> datetime:
    """Return a UTC-aware datetime; naive values are assumed UTC."""

    if ts.tzinfo is None:
        return ts.replace(tzinfo=UTC)
    return ts.astimezone(UTC)


class BaseAdapter(ABC):
    """Abstract base for every helios connector adapter.

    Lifecycle:

    .. code-block:: python

        async with MyAdapter() as adapter:
            async for record in adapter.fetch(start=..., end=...):
                ...

    The context-manager protocol guarantees the underlying httpx client
    is closed even if the consumer iterates partway and bails out.

    Subclasses **must** override:
        - :attr:`source_id`: which :class:`SourceID` this adapter speaks for.
        - :meth:`fetch`: the streaming production interface.
    """

    #: Which upstream source this adapter handles. Must be set by subclasses.
    source_id: ClassVar[SourceID]

    def __init__(
        self,
        *,
        client: httpx.AsyncClient | None = None,
        rate_limit: RateLimitConfig | None = None,
        cache: FileCache | None | bool = True,
        base_url: str = "",
    ) -> None:
        """Construct an adapter.

        Args:
            client: optional pre-built httpx client. If omitted, the
                adapter builds one with :func:`~helios_connectors.http.make_client`.
                Pass your own when you want to share a pool across
                adapters, or pass a transport with mock responses for
                tests.
            rate_limit: optional rate-limit configuration. Defaults are
                source-specific and chosen conservatively.
            cache: ``True`` to enable a default file cache, ``False`` to
                disable, or a :class:`FileCache` instance to use a
                custom one. Defaults to ``True``.
            base_url: optional base URL for the httpx client. Most
                adapters set this from a class constant.
        """

        self._owns_client = client is None
        self._client = client or make_client(base_url=base_url)
        self._ratelimiter = RateLimiter(rate_limit or self._default_rate_limit())
        self._cache: FileCache | None
        if cache is False:
            self._cache = None
        elif cache is True:
            self._cache = FileCache()
        else:
            self._cache = cache

    # ------------------------------------------------------------------ #
    # context manager
    # ------------------------------------------------------------------ #

    async def __aenter__(self) -> BaseAdapter:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        """Close the underlying httpx client if this adapter owns it."""
        if self._owns_client:
            await self._client.aclose()

    # ------------------------------------------------------------------ #
    # configuration hooks
    # ------------------------------------------------------------------ #

    def _default_rate_limit(self) -> RateLimitConfig:
        """Override per subclass to set a sensible per-source default.

        Base default is 10 RPS (matches CCMC-class APIs). NOAA SWPC
        subclasses should drop this to ~5; adapters using ``DEMO_KEY``
        should drop further still.
        """

        return RateLimitConfig(rate_per_second=10.0)

    # ------------------------------------------------------------------ #
    # abstract
    # ------------------------------------------------------------------ #

    @abstractmethod
    def fetch(
        self,
        *,
        start: datetime,
        end: datetime,
        **kwargs: Any,
    ) -> AsyncIterator[NormalizedRecord]:
        """Stream :class:`NormalizedRecord` for ``[start, end]``.

        Implementations should be async generators. Yielding records as
        they're parsed allows downstream consumers to begin processing
        before the upstream call has finished — important for sources
        that paginate or return large windows.

        ``kwargs`` is reserved for source-specific filters (e.g. event
        types). Each subclass documents its own keyword arguments.
        """

    # ------------------------------------------------------------------ #
    # sync convenience
    # ------------------------------------------------------------------ #

    def fetch_sync(
        self,
        *,
        start: datetime,
        end: datetime,
        **kwargs: Any,
    ) -> list[NormalizedRecord]:
        """Synchronous wrapper around :meth:`fetch`.

        Spins up an asyncio event loop, drains :meth:`fetch` into a
        list, closes the adapter, and returns. Good for notebooks and
        one-shot scripts; do not use inside an already-running event
        loop (use ``await self.fetch(...)`` directly there).
        """

        async def runner() -> list[NormalizedRecord]:
            async with self:
                return [record async for record in self.fetch(start=start, end=end, **kwargs)]

        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(runner())
        raise RuntimeError(
            "fetch_sync() called from inside a running event loop; "
            "await the async fetch() method instead"
        )

    # ------------------------------------------------------------------ #
    # provenance helper
    # ------------------------------------------------------------------ #

    #: Override per-subclass to pin the upstream model/API version that
    #: produced a record (e.g. ``"v1"`` for DONKI, ``"realtime"`` for SWPC
    #: real-time JSON, ``"kyoto_wdc_final"`` when archive-routed). When an
    #: adapter routes between products at different versions, pass the
    #: ``model_version`` kwarg to :meth:`_emit_provenance` instead.
    model_version: ClassVar[str] = "v1"

    def _emit_provenance(
        self,
        *,
        model_id: str,
        dataset_refs: Iterable[str],
        timestamp: datetime,
        value: float | int | str | bool,
        value_units: str,
        model_version: str | None = None,
        extra: dict[str, Any] | None = None,
        record_id: str | None = None,
    ) -> HeliosModelOutputRecord:
        """Build a :class:`HeliosModelOutputRecord` for a normalized value.

        Centralized so subclasses can't drift on UTC normalization, UUID
        generation, agent attribution, or schema-version tagging. Keep
        the interface narrow: anything an adapter doesn't pass through
        here doesn't end up in the provenance, full stop.

        Args:
            model_id: scoped identifier for the upstream model/product
                (e.g. ``"donki/CME"``, ``"swpc/kp"``, ``"goes/xray"``).
            dataset_refs: dataset URLs / identifiers contributing to this
                record. Must be non-empty per the provenance spec; the
                helper synthesizes a fallback (the adapter source name)
                when the iterable is empty so adapters that don't have a
                canonical URL still produce a spec-valid record.
            timestamp: event time of the science observation in UTC.
            value: the scalar value being recorded (float / int / str /
                bool). Compound payloads must be flattened to a scalar by
                the caller, with the full dict carried in ``extra``.
            value_units: human-readable units for ``value``.
            model_version: optional per-call override of :attr:`model_version`
                (e.g. switch between ``"realtime"`` and ``"kyoto_wdc_final"``).
            extra: optional dict of additional context. Lineage segments,
                the full upstream payload, frame/band/threshold metadata,
                etc. all live here. ``None`` and ``{}`` are equivalent.
            record_id: optional stable record identifier; defaults to a
                fresh UUID4.
        """

        now = datetime.now(UTC)
        refs = list(dataset_refs)
        if not refs:
            # Spec requires dataset_refs with min_length=1. When an adapter
            # cannot supply a canonical URL (DONKI events with no published
            # detail page yet, synthesised forecast envelopes, etc.) fall
            # back to the source enum string so the record is still valid.
            refs = [f"helios-connectors://{self.source_id.value}"]
        return HeliosModelOutputRecord(
            id=record_id or str(uuid4()),
            created_at=now,
            agent=self._helios_agent(),
            model_id=model_id,
            model_version=model_version or self.model_version,
            dataset_refs=refs,
            timestamp=_ensure_utc(timestamp),
            value=value,
            value_units=value_units,
            ingestion_timestamp=now,
            extra=extra if extra else None,
        )

    def _helios_agent(self) -> Agent:
        """Build the :class:`Agent` attribution for this adapter.

        Cached per-instance under ``_agent`` so the pydantic object is
        only constructed once even on hot fetch loops.
        """

        cached: Agent | None = getattr(self, "_agent", None)
        if cached is not None:
            return cached
        from .. import __version__

        agent = Agent(
            id=f"helios-spaceweather-connectors/{type(self).__name__}",
            name=type(self).__name__,
            type="software",
            version=__version__,
        )
        self._agent = agent
        return agent
