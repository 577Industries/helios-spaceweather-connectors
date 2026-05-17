"""helios-spaceweather-connectors: production-grade adapters for space-weather data sources.

This package exposes a uniform :class:`~helios_connectors.adapters.base.BaseAdapter`
interface plus one concrete adapter per upstream source (DONKI first; SWPC,
CDDIS GIMs, SEP Scoreboards, GOES, DSCOVR to follow). Every adapter emits
:class:`~helios_connectors.schema.NormalizedRecord` objects with full
provenance metadata.

Quick start:

.. code-block:: python

    from datetime import datetime
    from helios_connectors import DonkiAdapter

    async with DonkiAdapter() as donki:
        async for record in donki.fetch_flr(
            start=datetime(2024, 5, 8),
            end=datetime(2024, 5, 15),
        ):
            print(record.event_time, record.value.get("classType"))

See ``docs/`` for the adapter pattern and per-source reference docs.
"""

from __future__ import annotations

from .adapters.base import BaseAdapter
from .adapters.donki import DonkiAdapter
from .adapters.swpc import SwpcAdapter
from .cache import CacheKey, FileCache, default_cache_root
from .http import DEFAULT_USER_AGENT, make_client
from .ratelimit import RateLimitConfig, RateLimiter
from .schema import NormalizedRecord, ProvenanceRecord, SourceID

__version__ = "0.1.0"

__all__ = [
    "DEFAULT_USER_AGENT",
    "BaseAdapter",
    "CacheKey",
    "DonkiAdapter",
    "FileCache",
    "NormalizedRecord",
    "ProvenanceRecord",
    "RateLimitConfig",
    "RateLimiter",
    "SourceID",
    "SwpcAdapter",
    "__version__",
    "default_cache_root",
    "make_client",
]
