"""helios-spaceweather-connectors: production-grade adapters for space-weather data sources.

This package exposes a uniform :class:`~helios_connectors.adapters.base.BaseAdapter`
interface plus one concrete adapter per upstream source. Every adapter emits
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
from .adapters.cddis_gim import CddisGimAdapter
from .adapters.donki import DonkiAdapter
from .adapters.dscovr import DscovrAdapter
from .adapters.goes import GoesAdapter
from .adapters.sep_scoreboards import SepScoreboardsAdapter
from .adapters.swpc import SwpcAdapter
from .cache import CacheKey, FileCache, default_cache_root
from .http import DEFAULT_USER_AGENT, make_client
from .ratelimit import RateLimitConfig, RateLimiter
from .schema import HeliosModelOutputRecord, NormalizedRecord, SourceID

__version__ = "0.2.1"

__all__ = [
    "DEFAULT_USER_AGENT",
    "BaseAdapter",
    "CacheKey",
    "CddisGimAdapter",
    "DonkiAdapter",
    "DscovrAdapter",
    "FileCache",
    "GoesAdapter",
    "HeliosModelOutputRecord",
    "NormalizedRecord",
    "RateLimitConfig",
    "RateLimiter",
    "SepScoreboardsAdapter",
    "SourceID",
    "SwpcAdapter",
    "__version__",
    "default_cache_root",
    "make_client",
]
