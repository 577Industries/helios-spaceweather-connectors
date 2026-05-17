"""Tests for the parquet file cache."""

from __future__ import annotations

import time
from pathlib import Path

import pandas as pd
import pytest

from helios_connectors import CacheKey, FileCache, SourceID, default_cache_root


@pytest.fixture
def cache(tmp_path: Path) -> FileCache:
    return FileCache(root=tmp_path)


def test_round_trip(cache: FileCache) -> None:
    df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
    key = CacheKey(SourceID.DONKI, {"start": "2024-05-10", "end": "2024-05-15"})
    assert not cache.exists(key)
    path = cache.write(key, df)
    assert path.exists()
    assert cache.exists(key)
    got = cache.read(key)
    pd.testing.assert_frame_equal(df, got)


def test_param_order_independent(cache: FileCache) -> None:
    """Two CacheKeys with identical params in different order must collide."""
    k1 = CacheKey(SourceID.DONKI, {"a": 1, "b": 2})
    k2 = CacheKey(SourceID.DONKI, {"b": 2, "a": 1})
    assert k1.fingerprint() == k2.fingerprint()


def test_distinct_params_distinct_keys(cache: FileCache) -> None:
    """Different params must produce different fingerprints."""
    k1 = CacheKey(SourceID.DONKI, {"start": "2024-05-10"})
    k2 = CacheKey(SourceID.DONKI, {"start": "2024-05-11"})
    assert k1.fingerprint() != k2.fingerprint()


def test_distinct_sources_distinct_keys() -> None:
    """Same params under different sources must not collide."""
    p = {"start": "2024-05-10"}
    k1 = CacheKey(SourceID.DONKI, p)
    k2 = CacheKey(SourceID.GOES_XRAY, p)
    assert k1.fingerprint() != k2.fingerprint()


def test_missing_read_raises(cache: FileCache) -> None:
    key = CacheKey(SourceID.DONKI, {"x": 1})
    with pytest.raises(FileNotFoundError):
        cache.read(key)


def test_ttl_expiry(tmp_path: Path) -> None:
    cache = FileCache(root=tmp_path, ttl_seconds=0.1)
    df = pd.DataFrame({"a": [1]})
    key = CacheKey(SourceID.DONKI, {"x": 1})
    cache.write(key, df)
    assert cache.exists(key)
    time.sleep(0.2)
    assert not cache.exists(key)


def test_invalidate_and_clear(cache: FileCache) -> None:
    df = pd.DataFrame({"a": [1]})
    k1 = CacheKey(SourceID.DONKI, {"x": 1})
    k2 = CacheKey(SourceID.DONKI, {"x": 2})
    k3 = CacheKey(SourceID.GOES_XRAY, {"x": 1})
    cache.write(k1, df)
    cache.write(k2, df)
    cache.write(k3, df)
    assert cache.invalidate(k1) is True
    assert cache.invalidate(k1) is False
    assert cache.exists(k2)
    n = cache.clear(SourceID.DONKI)
    assert n == 1  # k1 already gone, only k2 remains under DONKI
    assert cache.exists(k3)


def test_default_root_env_override(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HELIOS_CACHE_ROOT", str(tmp_path / "custom"))
    root = default_cache_root()
    assert root == (tmp_path / "custom").resolve()


def test_default_root_no_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("HELIOS_CACHE_ROOT", raising=False)
    root = default_cache_root()
    assert root.name == "helios-connectors"
    assert root.parent.name == ".cache"


def test_read_metadata(cache: FileCache, tmp_path: Path) -> None:
    df = pd.DataFrame({"a": [1, 2, 3]})
    key = CacheKey(SourceID.DONKI, {"x": 1})
    path = cache.write(key, df)
    meta = FileCache.read_metadata(path)
    assert meta["rows"] == 3
    assert "schema" in meta
