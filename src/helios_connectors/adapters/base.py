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
from datetime import datetime
from typing import Any, ClassVar

import httpx

from ..cache import FileCache
from ..http import make_client
from ..ratelimit import RateLimitConfig, RateLimiter
from ..schema import NormalizedRecord, ProvenanceRecord, SourceID

__all__ = ["BaseAdapter"]

logger = logging.getLogger(__name__)


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

    def _emit_provenance(
        self,
        *,
        model_id: str,
        dataset_refs: Iterable[str],
        timestamp: datetime,
        value: Any,
        value_units: str,
        lineage: Iterable[str] = (),
        record_id: str | None = None,
    ) -> ProvenanceRecord:
        """Build a :class:`ProvenanceRecord` for a normalized value.

        Centralized so subclasses can't drift on UTC normalization,
        UUID generation, or schema-version tagging. Keep the interface
        narrow: anything an adapter doesn't pass through here doesn't
        end up in the provenance, full stop.
        """

        return ProvenanceRecord.new(
            model_id=model_id,
            dataset_refs=tuple(dataset_refs),
            timestamp=timestamp,
            value=value,
            value_units=value_units,
            lineage=tuple(lineage),
            record_id=record_id,
        )
