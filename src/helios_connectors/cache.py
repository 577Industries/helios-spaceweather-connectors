"""File-based parquet cache for adapter outputs.

The cache is intentionally minimal: a content-addressed key derived from
``(source_id, sorted(query_params))`` maps to a single parquet file under
``HELIOS_CACHE_ROOT/<source>/<fingerprint>.parquet``.

Cache contents are pandas DataFrames serialized to parquet via pyarrow.
Parquet is preferred over Python-native serialization formats because:

1. The cache is inspectable from any DuckDB / Polars / pandas user.
2. It survives Python version upgrades.
3. Schema mismatches surface at read time rather than as silent payload
   corruption.

The cache is **not** a database. We do not handle concurrent writes,
TTL eviction beyond a simple mtime check, or schema migrations. It is
intended for development iteration speed and CI test repeatability.
Production deployments should swap this out for a real cache.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

from .schema import SourceID

__all__ = ["CacheKey", "FileCache", "default_cache_root"]

logger = logging.getLogger(__name__)


def default_cache_root() -> Path:
    """Resolve the cache root directory.

    Respects ``HELIOS_CACHE_ROOT`` if set, otherwise
    ``~/.cache/helios-connectors``. The directory is created on first
    write (this function does not create it; callers do).
    """

    override = os.environ.get("HELIOS_CACHE_ROOT")
    if override:
        return Path(override).expanduser().resolve()
    return Path.home() / ".cache" / "helios-connectors"


@dataclass(frozen=True, slots=True)
class CacheKey:
    """Stable identifier for a cached query.

    The ``params`` mapping is normalized to a sorted JSON string before
    hashing so equivalent queries with different dict ordering collide
    deterministically. Non-JSON-serializable values are coerced via
    ``repr`` so the key never raises; this is OK because the key is
    advisory, not authoritative.
    """

    source_id: SourceID
    params: Mapping[str, Any]

    def fingerprint(self) -> str:
        """Stable hex fingerprint for the (source, params) pair."""
        canonical = json.dumps(
            {"source": self.source_id.value, "params": dict(sorted(self.params.items()))},
            sort_keys=True,
            default=repr,
        )
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


class FileCache:
    """Disk-backed parquet cache for normalized records.

    Layout:

        <root>/<source>/<fingerprint>.parquet

    The fingerprint stays short (16 hex chars from sha256) but is large
    enough that collisions in our request space are astronomical. We
    deliberately do *not* use the date-as-filename scheme described in
    the master plan because it forces date-bucketing on every caller;
    fingerprinting the full param set is more general and equally fast.

    Set ``ttl_seconds`` to a positive value to expire stale entries by
    file mtime; the default of 0 means "never expire" (test fixtures
    and exploratory work want stable hits).
    """

    def __init__(
        self,
        root: Path | None = None,
        *,
        ttl_seconds: float = 0.0,
    ) -> None:
        self._root = (root or default_cache_root()).expanduser().resolve()
        self._ttl = ttl_seconds

    @property
    def root(self) -> Path:
        """The directory this cache writes to. Lazily ensured on write."""
        return self._root

    def _path(self, key: CacheKey) -> Path:
        return self._root / key.source_id.value / f"{key.fingerprint()}.parquet"

    def _expired(self, path: Path) -> bool:
        if self._ttl <= 0:
            return False
        age = time.time() - path.stat().st_mtime
        return age > self._ttl

    def exists(self, key: CacheKey) -> bool:
        """True if the cache has a fresh entry for this key."""
        path = self._path(key)
        if not path.exists():
            return False
        if self._expired(path):
            logger.debug("cache entry expired: %s", path)
            return False
        return True

    def read(self, key: CacheKey) -> pd.DataFrame:
        """Read the DataFrame at ``key``.

        Raises:
            FileNotFoundError: if no entry exists. Use :meth:`exists`
                first if you need a hit/miss decision.
        """

        path = self._path(key)
        if not path.exists():
            raise FileNotFoundError(f"no cache entry at {path}")
        logger.debug("cache read: %s", path)
        return pd.read_parquet(path)

    def write(self, key: CacheKey, df: pd.DataFrame) -> Path:
        """Persist ``df`` to the cache and return the file path."""
        path = self._path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(path, engine="pyarrow", index=False)
        logger.debug("cache write: %s (rows=%d)", path, len(df))
        return path

    def invalidate(self, key: CacheKey) -> bool:
        """Remove the entry at ``key`` if it exists. Returns whether one was found."""
        path = self._path(key)
        if path.exists():
            path.unlink()
            logger.debug("cache invalidate: %s", path)
            return True
        return False

    def clear(self, source_id: SourceID | None = None) -> int:
        """Remove all entries (optionally only for one source). Returns count."""
        target = self._root if source_id is None else self._root / source_id.value
        if not target.exists():
            return 0
        count = 0
        for path in target.rglob("*.parquet"):
            path.unlink()
            count += 1
        logger.debug("cache clear: removed %d files from %s", count, target)
        return count

    # Inspection helpers
    @staticmethod
    def read_metadata(path: Path) -> dict[str, Any]:
        """Read parquet metadata without loading the full table.

        Mostly useful for debugging; returns row count + schema string.
        """
        pf = pq.ParquetFile(path)  # type: ignore[no-untyped-call]
        return {"rows": pf.metadata.num_rows, "schema": str(pf.schema)}
