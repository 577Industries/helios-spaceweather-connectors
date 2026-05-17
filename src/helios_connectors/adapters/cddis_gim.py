"""NASA CDDIS Global Ionosphere Maps (GIMs) adapter.

CDDIS (the *Crustal Dynamics Data Information System*, NASA GSFC) is the
authoritative archive for GNSS data products including the IGS Global
Ionosphere Maps (GIMs). GIMs provide vertical Total Electron Content (TEC)
on a global lat/lon grid at 2-hour cadence, derived from the worldwide IGS
GNSS station network by several analysis centers (IGS combined, JPL, CODE,
ESA, UPC, NRCan, etc.).

HELIOS' §2 Obj. 4 ionospheric-forecasting models (TFT) consume vertical TEC
on a 2.5-by-5 deg (lat by lon) grid as input. The Gannon-storm v2 retrospective in
`gannon-storm-rtk-analysis` also depends on this adapter for SPP-quality
ionospheric corrections.

Strategy: **BUILD**. No maintained Python client wraps Earthdata Login +
CDDIS IONEX retrieval + IONEX parsing into a single async streaming
interface. The adapter:

1. Authenticates against NASA URS (Earthdata Login) using either
   ``earthaccess`` (the optional ``[earthdata]`` extra; preferred) or a
   manual httpx + cookie redirect handshake (fallback).
2. Downloads compressed IONEX files lazily, only for the days actually
   requested.
3. Caches each downloaded file on disk under
   ``HELIOS_CACHE_ROOT/cddis/<year>/<doy>/<filename>``.
4. Parses IONEX with a small custom parser (no xarray/georinex
   heavyweight dep). Returns per-2-hour TEC maps as ``NormalizedRecord``
   objects, each carrying the full 5-by-2.5 deg lat/lon grid as a list of
   lists in ``record.value["tec_grid"]`` (units: TECU).
5. Provides a bilinear point-extraction helper
   :meth:`CddisGimAdapter.fetch_tec_at_point` for single-station time
   series.

Authentication setup
--------------------

Users must:

1. Register a NASA Earthdata account at <https://urs.earthdata.nasa.gov/>.
2. Authorize the *NASA GESDISC DATA ARCHIVE* application (which fronts
   CDDIS access on URS).
3. Export credentials::

       export NASA_EARTHDATA_USER="your-username"
       export NASA_EARTHDATA_PASS="your-password"

   Or populate ``~/.netrc`` (preferred for ``earthaccess``)::

       machine urs.earthdata.nasa.gov login your-username password your-password

If credentials are missing, the adapter raises a clear ``RuntimeError``
on first download attempt.

Filename conventions
--------------------

CDDIS uses two filename conventions, depending on the file's age:

* **Pre-2023** legacy short form::

      <year>/<doy>/<center><doy>0.<yy>i.Z

  Example: ``2018/100/igsg1000.18i.Z`` (IGS combined, day 100 of 2018).

* **2023-present** long form::

      <year>/<doy>/<CENTER>0OPSFIN_<YYYY><DOY>0000_01D_02H_GIM.INX.gz

  Example:
  ``2024/131/IGS0OPSFIN_20241310000_01D_02H_GIM.INX.gz``.

The adapter probes both URLs and uses whichever returns 200.

IONEX format brief
------------------

IONEX is a fixed-width ASCII format documented at
<https://files.igs.org>. Key header records:

* ``EPOCH OF FIRST MAP`` / ``EPOCH OF LAST MAP`` — bounds.
* ``INTERVAL`` — seconds between maps (typically 7200 → 2 hours).
* ``# OF MAPS IN FILE`` — usually 13 (00 UT through 24 UT inclusive).
* ``LAT1 / LAT2 / DLAT`` — latitude grid (usually 87.5 / -87.5 / -2.5).
* ``LON1 / LON2 / DLON`` — longitude grid (usually -180 / 180 / 5.0).
* ``EXPONENT`` — TEC values are integers, multiplied by 10**EXPONENT TECU
  (typically -1).
* ``END OF HEADER`` — header terminator.

Each TEC map block is delimited by ``START OF TEC MAP`` / ``END OF TEC
MAP``. Each per-latitude row starts with a ``LAT/LON1/LON2/DLON/H``
line followed by integer TEC values on 5 columns by 16 lines (or
similar) of width-5 integer fields.

Cache footprint
---------------

One IONEX file is ~50-200 KB compressed. A full week is ~1-2 MB; a year
is ~50-75 MB; the full 24+-year archive is a few GB. **Lazy-fetch is
mandatory** — never pre-warm beyond the requested window.
"""

from __future__ import annotations

import asyncio
import gzip
import logging
import os
import re
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, ClassVar

import httpx

from ..cache import FileCache, default_cache_root
from ..http import make_client, request_with_retry
from ..ratelimit import RateLimitConfig
from ..schema import NormalizedRecord, SourceID
from .base import BaseAdapter

__all__ = [
    "CDDIS_BASE_URL",
    "CDDIS_DEFAULT_CENTER",
    "CDDIS_SUPPORTED_CENTERS",
    "URS_LOGIN_HOST",
    "CddisGimAdapter",
    "IonexFile",
    "IonexMap",
    "parse_ionex",
]

logger = logging.getLogger(__name__)

#: CDDIS archive base URL. Earthdata Login (URS) cookies required to
#: actually download files, but HEAD requests work unauthenticated.
CDDIS_BASE_URL = "https://cddis.nasa.gov"

#: NASA URS login host used for the Earthdata authentication handshake.
URS_LOGIN_HOST = "https://urs.earthdata.nasa.gov"

#: Analysis-center slug → human-readable name. The slug is the prefix
#: used in both the legacy short-form filename (e.g. ``igsg``) and the
#: long-form prefix (e.g. ``IGS0`` → upper-cased first three).
CDDIS_SUPPORTED_CENTERS: dict[str, str] = {
    "igsg": "IGS combined",
    "jplg": "Jet Propulsion Laboratory",
    "codg": "CODE (University of Bern)",
    "esag": "European Space Agency",
    "upcg": "Universitat Politecnica de Catalunya",
}

#: Default analysis center for downstream consumers. IGS combined is the
#: gold standard — a weighted average of every contributing center, with
#: outlier rejection.
CDDIS_DEFAULT_CENTER: str = "igsg"

#: Long-form filename switched in around 2023 day 1 per IGS LRN. We
#: probe legacy first for ``year < _LONG_FORM_YEAR``, long-form first
#: for ``year >= _LONG_FORM_YEAR``. Both are tried as fallback.
_LONG_FORM_YEAR: int = 2023


# ---------------------------------------------------------------------------- #
# IONEX parser data shapes
# ---------------------------------------------------------------------------- #


@dataclass(frozen=True, slots=True)
class IonexMap:
    """One per-epoch TEC map extracted from an IONEX file.

    Attributes:
        epoch: UTC datetime at which this map is valid.
        tec_grid: 2-D list of TEC values in TECU, indexed as
            ``tec_grid[lat_idx][lon_idx]``. Latitude order matches the
            IONEX file's ``LAT1 -> LAT2`` direction (typically 87.5 → -87.5
            with DLAT = -2.5).
        height_km: nominal ionospheric shell height for this map
            (typically 450 km).
    """

    epoch: datetime
    tec_grid: tuple[tuple[float, ...], ...]
    height_km: float


@dataclass(frozen=True, slots=True)
class IonexFile:
    """Parsed IONEX file: header metadata + ordered list of TEC maps.

    Attributes:
        epoch_first: epoch of the first map (UTC).
        epoch_last: epoch of the last map (UTC).
        interval_seconds: spacing between maps (typically 7200).
        lat1, lat2, dlat: latitude grid bounds and step (degrees).
        lon1, lon2, dlon: longitude grid bounds and step (degrees).
        exponent: TEC scaling exponent (TECU = raw * 10**exponent).
        maps: ordered list of :class:`IonexMap` records.
        source_url: the URL the file was downloaded from, if known.
    """

    epoch_first: datetime
    epoch_last: datetime
    interval_seconds: int
    lat1: float
    lat2: float
    dlat: float
    lon1: float
    lon2: float
    dlon: float
    exponent: int
    maps: tuple[IonexMap, ...]
    source_url: str | None = None

    def lat_axis(self) -> tuple[float, ...]:
        """Latitudes corresponding to ``tec_grid[i]``, in file order."""
        n_lat = round(abs((self.lat2 - self.lat1) / self.dlat)) + 1
        return tuple(self.lat1 + i * self.dlat for i in range(n_lat))

    def lon_axis(self) -> tuple[float, ...]:
        """Longitudes corresponding to ``tec_grid[i][j]``, in file order."""
        n_lon = round(abs((self.lon2 - self.lon1) / self.dlon)) + 1
        return tuple(self.lon1 + j * self.dlon for j in range(n_lon))


# ---------------------------------------------------------------------------- #
# Adapter
# ---------------------------------------------------------------------------- #


class CddisGimAdapter(BaseAdapter):
    """Adapter for NASA CDDIS Global Ionosphere Maps.

    Usage:

    .. code-block:: python

        from datetime import datetime, UTC
        from helios_connectors.adapters import CddisGimAdapter

        async with CddisGimAdapter() as cddis:
            async for rec in cddis.fetch_tec_maps(
                start=datetime(2024, 5, 10, tzinfo=UTC),
                end=datetime(2024, 5, 11, tzinfo=UTC),
            ):
                grid = rec.value["tec_grid"]  # 71 by 73 list-of-lists, TECU

        # Or, a point time-series for one station:
        async with CddisGimAdapter() as cddis:
            async for rec in cddis.fetch_tec_at_point(
                start=datetime(2024, 5, 10, tzinfo=UTC),
                end=datetime(2024, 5, 12, tzinfo=UTC),
                lat=40.0,
                lon=-83.0,  # Columbus, OH
            ):
                print(rec.event_time, rec.value["tec"], "TECU")

    Authentication is via the ``NASA_EARTHDATA_USER`` and
    ``NASA_EARTHDATA_PASS`` environment variables, or a ``~/.netrc``
    entry for ``urs.earthdata.nasa.gov``. The adapter prefers
    ``earthaccess`` when installed (the ``[earthdata]`` extra); otherwise
    it falls back to a manual cookie handshake.
    """

    source_id: ClassVar[SourceID] = SourceID.CDDIS_GIM

    def __init__(
        self,
        *,
        base_url: str = CDDIS_BASE_URL,
        client: httpx.AsyncClient | None = None,
        rate_limit: RateLimitConfig | None = None,
        cache: FileCache | None | bool = True,
        cache_root: Path | None = None,
        username: str | None = None,
        password: str | None = None,
        use_earthaccess: bool | None = None,
    ) -> None:
        """Construct a CDDIS GIM adapter.

        Args:
            base_url: CDDIS base URL. Override only for testing against
                a local mirror.
            client: optional pre-built httpx client. If omitted, one is
                built with cookie persistence and the URS-friendly
                ``follow_redirects=True``.
            rate_limit: optional rate-limit override. Default is 2 RPS;
                CDDIS publishes no documented limit but heavy bursts
                from a single IP get throttled in practice.
            cache: ``True`` for default parquet :class:`FileCache`,
                ``False`` to disable, or a custom instance. Used only
                for parsed-record caching; the raw IONEX-file cache is
                separate (see ``cache_root``).
            cache_root: directory under which raw IONEX files are
                cached. Defaults to ``HELIOS_CACHE_ROOT/cddis/``. The
                ``HELIOS_CACHE_ROOT`` env var honors :func:`default_cache_root`.
            username: NASA URS username. Falls back to
                ``NASA_EARTHDATA_USER`` env var.
            password: NASA URS password. Falls back to
                ``NASA_EARTHDATA_PASS`` env var. **Never logged.**
            use_earthaccess: ``True`` to require ``earthaccess`` (raises
                if missing); ``False`` to force manual httpx + cookie
                handshake; ``None`` (default) to use ``earthaccess`` when
                available and fall back transparently.
        """

        self._user = username or os.environ.get("NASA_EARTHDATA_USER")
        self._pass = password or os.environ.get("NASA_EARTHDATA_PASS")
        self._use_earthaccess = use_earthaccess
        self._earthaccess_session: Any | None = None
        self._urs_authenticated: bool = False

        if cache_root is None:
            cache_root = default_cache_root() / "cddis"
        self._raw_cache_root = Path(cache_root).expanduser().resolve()

        if client is None:
            # CDDIS requires cookie persistence across the URS redirect chain.
            # We do not preset cookies; the auth flow populates them.
            client = make_client(base_url=base_url)
        super().__init__(client=client, rate_limit=rate_limit, cache=cache)

    def _default_rate_limit(self) -> RateLimitConfig:
        # CDDIS publishes no documented limit. 2 RPS is conservative and
        # well inside the practical threshold beyond which their throttling
        # kicks in.
        return RateLimitConfig(rate_per_second=2.0, burst=4)

    # ------------------------------------------------------------------ #
    # public fetch surface
    # ------------------------------------------------------------------ #

    async def fetch(
        self,
        *,
        start: datetime,
        end: datetime,
        products: Sequence[str] | None = None,
        center: str = CDDIS_DEFAULT_CENTER,
        **_kwargs: Any,
    ) -> AsyncIterator[NormalizedRecord]:
        """Unified entrypoint: stream TEC maps (default) for ``[start, end]``.

        Args:
            start: inclusive UTC start of the request window.
            end: inclusive UTC end of the request window.
            products: list of product slugs. The only currently
                supported slug is ``"tec_maps"``; passed for forward
                compatibility with future CDDIS product families
                (DCBs, IGS clock products, etc.).
            center: analysis-center slug from
                :data:`CDDIS_SUPPORTED_CENTERS`. Default ``"igsg"`` (IGS
                combined).
        """

        selected = tuple(products) if products else ("tec_maps",)
        unknown = set(selected) - {"tec_maps"}
        if unknown:
            raise ValueError(
                f"unknown CDDIS GIM products: {sorted(unknown)!r}; "
                f"only 'tec_maps' is implemented today"
            )

        if "tec_maps" in selected:
            async for rec in self.fetch_tec_maps(start=start, end=end, center=center):
                yield rec

    async def fetch_tec_maps(
        self,
        *,
        start: datetime,
        end: datetime,
        center: str = CDDIS_DEFAULT_CENTER,
    ) -> AsyncIterator[NormalizedRecord]:
        """Stream per-2-hour global TEC maps over the requested window.

        Each yielded record's ``value`` is a dict with:

        * ``tec_grid``: list-of-lists, TEC in TECU
        * ``lat_axis``: latitudes (degrees) matching ``tec_grid`` rows
        * ``lon_axis``: longitudes (degrees) matching ``tec_grid`` columns
        * ``height_km``: ionospheric shell height (typically 450)
        * ``center``: analysis-center slug
        * ``center_name``: human-readable analysis-center name
        """

        self._validate_center(center)
        async for ionex in self._stream_ionex_files(start=start, end=end, center=center):
            for ionex_map in ionex.maps:
                if not (start <= ionex_map.epoch <= end):
                    continue
                yield self._normalize_map(ionex, ionex_map, center=center)

    async def fetch_tec_at_point(
        self,
        *,
        start: datetime,
        end: datetime,
        lat: float,
        lon: float,
        center: str = CDDIS_DEFAULT_CENTER,
    ) -> AsyncIterator[NormalizedRecord]:
        """Stream per-2-hour TEC at one (lat, lon), bilinearly interpolated.

        Convenience wrapper around :meth:`fetch_tec_maps` that extracts
        a single point via bilinear interpolation on each map's lat/lon
        grid. Useful for single-station overlays (e.g. Columbus, OH
        during the Gannon week).

        Args:
            lat: latitude in degrees, in ``[-90, +90]``.
            lon: longitude in degrees. Either ``[-180, +180]`` or
                ``[0, 360]`` is accepted and normalized internally.
        """

        if not -90.0 <= lat <= 90.0:
            raise ValueError(f"lat out of range [-90, 90]: {lat}")
        # Normalize lon to whatever convention the IONEX file uses; IGS
        # IONEX always uses [-180, +180].
        lon_norm = ((lon + 180.0) % 360.0) - 180.0

        self._validate_center(center)
        async for ionex in self._stream_ionex_files(start=start, end=end, center=center):
            for ionex_map in ionex.maps:
                if not (start <= ionex_map.epoch <= end):
                    continue
                tec = _bilinear_sample(ionex, ionex_map, lat, lon_norm)
                yield self._normalize_point(
                    ionex, ionex_map, center=center, lat=lat, lon=lon_norm, tec=tec
                )

    # ------------------------------------------------------------------ #
    # download + parse machinery
    # ------------------------------------------------------------------ #

    async def _stream_ionex_files(
        self, *, start: datetime, end: datetime, center: str
    ) -> AsyncIterator[IonexFile]:
        """Yield parsed :class:`IonexFile` objects, one per UTC day."""

        start_utc = _ensure_utc(start)
        end_utc = _ensure_utc(end)
        if end_utc < start_utc:
            raise ValueError(f"end ({end}) precedes start ({start})")

        cursor = datetime(start_utc.year, start_utc.month, start_utc.day, tzinfo=UTC)
        # Iterate UTC days from start_day through the day-bucket of end_utc,
        # but skip the final day if the requested window's end falls exactly
        # at that day's 00:00 (the bridging epoch lives in the prior day's
        # IONEX file as its 13th map per IGS convention).
        end_day = datetime(end_utc.year, end_utc.month, end_utc.day, tzinfo=UTC)
        last = end_day if end_utc > end_day else end_day - timedelta(days=1)
        if last < cursor:
            last = cursor
        while cursor <= last:
            year = cursor.year
            doy = cursor.timetuple().tm_yday
            try:
                raw_path = await self._fetch_ionex_file(year=year, doy=doy, center=center)
            except FileNotFoundError as exc:
                logger.warning("CDDIS GIM: missing file for %d/%03d %s: %s", year, doy, center, exc)
                cursor += timedelta(days=1)
                continue
            ionex = parse_ionex(raw_path.read_bytes(), source_url=str(raw_path))
            yield ionex
            cursor += timedelta(days=1)

    async def _fetch_ionex_file(self, *, year: int, doy: int, center: str) -> Path:
        """Download (or hit cache) the IONEX file for one day and center.

        Returns the path to the *decompressed* IONEX bytes on disk
        (still under the raw cache root). The returned file is plain
        ASCII; the on-disk gzip/.Z form is preserved alongside.

        Raises:
            FileNotFoundError: if neither the long-form nor short-form
                URL returns a 200.
            RuntimeError: if Earthdata credentials are missing.
        """

        # Cache check: parse the decompressed file directly if it's there.
        day_dir = self._raw_cache_root / f"{year:04d}" / f"{doy:03d}"
        decompressed_cache = day_dir / f"{center}_{year:04d}_{doy:03d}.ionex"
        if decompressed_cache.exists():
            logger.debug("CDDIS cache hit: %s", decompressed_cache)
            return decompressed_cache

        # Probe candidate URLs in preferred order for this year. Try both
        # so a 2023 boundary year survives either convention.
        candidates = _candidate_urls(year=year, doy=doy, center=center)

        await self._ensure_authenticated()
        await self._ratelimiter.acquire()

        last_status: int | None = None
        for url in candidates:
            try:
                content = await self._download(url)
            except _DownloadNotFound as nf:
                last_status = nf.status_code
                logger.debug("CDDIS 404: %s", url)
                continue
            day_dir.mkdir(parents=True, exist_ok=True)
            # Decompress to plain ASCII for downstream parsing speed.
            decompressed = _decompress_ionex(content, url)
            decompressed_cache.write_bytes(decompressed)
            # Keep the original compressed payload too for traceability.
            compressed_cache = day_dir / Path(url).name
            compressed_cache.write_bytes(content)
            logger.info(
                "CDDIS cached %s -> %s (%d bytes)", url, decompressed_cache, len(decompressed)
            )
            return decompressed_cache

        raise FileNotFoundError(
            f"no IONEX file for year={year} doy={doy:03d} center={center} "
            f"(last status: {last_status}); tried {candidates}"
        )

    async def _download(self, url: str) -> bytes:
        """Download one file from CDDIS via the authenticated session.

        Raises :class:`_DownloadNotFound` on 404; re-raises other HTTP
        errors via ``response.raise_for_status()``.
        """

        full_url = url if url.startswith("http") else f"{CDDIS_BASE_URL}{url}"

        # Path 1: earthaccess session (preferred when available).
        if self._earthaccess_session is not None:
            return await asyncio.to_thread(
                _earthaccess_get_bytes, self._earthaccess_session, full_url
            )

        # Path 2: manual httpx with cookies/basic-auth. We use httpx Basic
        # against the URS-aware CDDIS endpoint — CDDIS supports HTTP Basic
        # against the URS-bound user, following the redirect chain.
        try:
            response = await request_with_retry(
                self._client,
                "GET",
                full_url,
                params=None,
                safe_log_params=(),
            )
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                raise _DownloadNotFound(404) from exc
            raise
        # Sanity check: HTML payload means we hit the URS login page.
        if response.headers.get("content-type", "").startswith("text/html"):
            raise RuntimeError(
                "CDDIS returned HTML (likely the URS login page). "
                "Check that NASA_EARTHDATA_USER/PASS are valid and that "
                "your URS profile has authorized the 'NASA GESDISC DATA "
                "ARCHIVE' application."
            )
        return response.content

    async def _ensure_authenticated(self) -> None:
        """Ensure URS credentials are available and a session is primed.

        Picks ``earthaccess`` when available + allowed; otherwise sets
        HTTP Basic auth on the shared httpx client. Idempotent.
        """

        if self._urs_authenticated:
            return
        if not self._user or not self._pass:
            raise RuntimeError(
                "NASA Earthdata credentials missing. "
                "Set NASA_EARTHDATA_USER and NASA_EARTHDATA_PASS env vars. "
                "Register at https://urs.earthdata.nasa.gov/ and authorize the "
                "'NASA GESDISC DATA ARCHIVE' application before first use."
            )

        # Prefer earthaccess if installed and not explicitly disabled.
        if self._use_earthaccess is not False:
            try:
                session = await asyncio.to_thread(
                    _build_earthaccess_session, self._user, self._pass
                )
            except _EarthaccessUnavailable:
                if self._use_earthaccess is True:
                    raise RuntimeError(
                        "use_earthaccess=True but the 'earthaccess' package is "
                        "not installed. Install with: pip install "
                        "'helios-spaceweather-connectors[earthdata]'"
                    ) from None
                session = None
            else:
                self._earthaccess_session = session
                self._urs_authenticated = True
                logger.debug("CDDIS authenticated via earthaccess")
                return

        # Manual fallback: HTTP Basic on the shared httpx client.
        self._client.auth = httpx.BasicAuth(self._user, self._pass)
        self._urs_authenticated = True
        logger.debug("CDDIS authenticated via httpx BasicAuth (manual)")

    # ------------------------------------------------------------------ #
    # normalization
    # ------------------------------------------------------------------ #

    def _validate_center(self, center: str) -> None:
        if center not in CDDIS_SUPPORTED_CENTERS:
            raise ValueError(
                f"unsupported analysis center: {center!r}; valid: {sorted(CDDIS_SUPPORTED_CENTERS)}"
            )

    def _normalize_map(
        self, ionex: IonexFile, ionex_map: IonexMap, *, center: str
    ) -> NormalizedRecord:
        center_name = CDDIS_SUPPORTED_CENTERS[center]
        # Build a JSON-safe representation of the grid (list of lists).
        tec_grid_serialized: list[list[float]] = [list(row) for row in ionex_map.tec_grid]
        value: dict[str, Any] = {
            "tec_grid": tec_grid_serialized,
            "lat_axis": list(ionex.lat_axis()),
            "lon_axis": list(ionex.lon_axis()),
            "height_km": ionex_map.height_km,
            "center": center,
            "center_name": center_name,
        }
        source_url = ionex.source_url or ""
        # Spec requires a scalar; for tec_map records use the spatial-mean
        # TEC and carry the full grid in extra. This mirrors the design
        # call from the CDDIS review pack.
        flat = [v for row in tec_grid_serialized for v in row if isinstance(v, (int, float))]
        scalar_mean = (sum(flat) / len(flat)) if flat else 0.0
        lineage = [center_name, source_url] if source_url else [center_name]
        provenance = self._emit_provenance(
            model_id=f"cddis/ionex/{center}",
            dataset_refs=(source_url,) if source_url else (),
            timestamp=ionex_map.epoch,
            value=scalar_mean,
            value_units="TECU",
            extra={
                "tec_grid": tec_grid_serialized,
                "tec_grid_shape": [
                    len(tec_grid_serialized),
                    len(tec_grid_serialized[0]) if tec_grid_serialized else 0,
                ],
                "lat_axis": list(ionex.lat_axis()),
                "lon_axis": list(ionex.lon_axis()),
                "height_km": ionex_map.height_km,
                "center": center,
                "center_name": center_name,
                "lineage": lineage,
            },
            record_id=f"cddis-{center}-{ionex_map.epoch.isoformat()}",
        )
        return NormalizedRecord(
            source=SourceID.CDDIS_GIM,
            record_type="tec_map",
            event_time=ionex_map.epoch,
            value=value,
            value_units="TECU",
            provenance=provenance,
            raw={"source_url": source_url},
        )

    def _normalize_point(
        self,
        ionex: IonexFile,
        ionex_map: IonexMap,
        *,
        center: str,
        lat: float,
        lon: float,
        tec: float,
    ) -> NormalizedRecord:
        center_name = CDDIS_SUPPORTED_CENTERS[center]
        value: dict[str, Any] = {
            "tec": tec,
            "lat": lat,
            "lon": lon,
            "height_km": ionex_map.height_km,
            "center": center,
            "center_name": center_name,
        }
        source_url = ionex.source_url or ""
        lineage = [center_name, source_url] if source_url else [center_name]
        provenance = self._emit_provenance(
            model_id=f"cddis/ionex/{center}",
            dataset_refs=(source_url,) if source_url else (),
            timestamp=ionex_map.epoch,
            value=tec,
            value_units="TECU",
            extra={
                "lat": lat,
                "lon": lon,
                "height_km": ionex_map.height_km,
                "center": center,
                "center_name": center_name,
                "lineage": lineage,
            },
            record_id=f"cddis-{center}-{lat:+.2f}-{lon:+.2f}-{ionex_map.epoch.isoformat()}",
        )
        return NormalizedRecord(
            source=SourceID.CDDIS_GIM,
            record_type="tec_point",
            event_time=ionex_map.epoch,
            value=value,
            value_units="TECU",
            provenance=provenance,
            raw={"source_url": source_url, "lat": lat, "lon": lon},
        )


# ---------------------------------------------------------------------------- #
# helpers + IONEX parser
# ---------------------------------------------------------------------------- #


class _DownloadNotFound(Exception):
    """Internal: a candidate URL returned 404, try the next one."""

    def __init__(self, status_code: int) -> None:
        super().__init__(f"HTTP {status_code}")
        self.status_code = status_code


class _EarthaccessUnavailable(Exception):
    """Internal: the optional ``earthaccess`` extra is not installed."""


def _ensure_utc(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts.replace(tzinfo=UTC)
    return ts.astimezone(UTC)


def _candidate_urls(*, year: int, doy: int, center: str) -> tuple[str, ...]:
    """Build the ordered candidate URL list for one (year, doy, center)."""

    yy = year % 100
    legacy = f"/archive/gnss/products/ionex/{year:04d}/{doy:03d}/{center}{doy:03d}0.{yy:02d}i.Z"
    center_upper3 = center[:3].upper()
    long_form = (
        f"/archive/gnss/products/ionex/{year:04d}/{doy:03d}/"
        f"{center_upper3}0OPSFIN_{year:04d}{doy:03d}0000_01D_02H_GIM.INX.gz"
    )
    if year >= _LONG_FORM_YEAR:
        return (long_form, legacy)
    return (legacy, long_form)


def _decompress_ionex(content: bytes, url: str) -> bytes:
    """Decompress an IONEX payload from .gz, .Z, or already-plain bytes.

    ``.gz`` is handled by stdlib :mod:`gzip`. ``.Z`` (legacy Unix-compress
    LZW) requires the optional ``unlzw3`` package — install via the
    ``[earthdata]`` extra. Pre-2023 CDDIS IONEX files use ``.Z``; 2023+
    long-form files use ``.gz`` so most users never hit this path.
    """

    name = url.lower()
    if name.endswith(".gz"):
        return gzip.decompress(content)
    if name.endswith(".z"):
        return _decompress_unix_z(content)
    return content


def _decompress_unix_z(data: bytes) -> bytes:
    """Decompress a Unix-compress (.Z, LZW) byte stream via ``unlzw3``.

    Raises:
        ValueError: if the data is not a valid .Z stream (bad magic).
        RuntimeError: if ``unlzw3`` is not installed.
    """

    if len(data) < 3 or data[:2] != b"\x1f\x9d":
        raise ValueError("not a .Z (LZW-compressed) stream")
    try:
        import unlzw3
    except ImportError as exc:
        raise RuntimeError(
            "Cannot decompress .Z (legacy Unix-compress) IONEX file: "
            "the 'unlzw3' package is not installed. Install via the "
            "'[earthdata]' extra: "
            "pip install 'helios-spaceweather-connectors[earthdata]'. "
            "Most CDDIS IONEX files dated 2023 or later use .gz and "
            "do not need this dependency."
        ) from exc
    return bytes(unlzw3.unlzw(data))


def parse_ionex(content: bytes | str, *, source_url: str | None = None) -> IonexFile:
    """Parse an IONEX (vertical TEC) file into an :class:`IonexFile`.

    Implements the IONEX 1.0 / 1.1 format used by IGS analysis centers.
    Tolerates minor variation (extra whitespace, optional fields) but
    rejects files without the required header records.

    Args:
        content: raw IONEX bytes or string.
        source_url: optional URL the file came from, recorded on the
            returned object for provenance.
    """

    text = content.decode("ascii", errors="replace") if isinstance(content, bytes) else content
    lines = text.splitlines()

    # Walk header.
    epoch_first: datetime | None = None
    epoch_last: datetime | None = None
    interval: int | None = None
    lat1 = lat2 = dlat = lon1 = lon2 = dlon = None
    exponent: int = -1
    header_end_idx: int | None = None
    for idx, line in enumerate(lines):
        if len(line) < 60:
            continue
        label = line[60:].rstrip()
        body = line[:60]
        if label == "EPOCH OF FIRST MAP":
            epoch_first = _parse_ionex_epoch(body)
        elif label == "EPOCH OF LAST MAP":
            epoch_last = _parse_ionex_epoch(body)
        elif label == "INTERVAL":
            interval = int(body.strip())
        elif label == "HGT1 / HGT2 / DHGT":
            # We accept but ignore — we record per-map height from the
            # LAT/LON1/LON2/DLON/H row instead.
            pass
        elif label == "LAT1 / LAT2 / DLAT":
            lat1, lat2, dlat = _parse_three_floats(body)
        elif label == "LON1 / LON2 / DLON":
            lon1, lon2, dlon = _parse_three_floats(body)
        elif label == "EXPONENT":
            exponent = int(body.strip())
        elif label == "END OF HEADER":
            header_end_idx = idx
            break

    if header_end_idx is None:
        raise ValueError("IONEX: no END OF HEADER record")
    if epoch_first is None or epoch_last is None:
        raise ValueError("IONEX: missing EPOCH OF FIRST/LAST MAP")
    if interval is None:
        # Default to 2 hours per IGS convention.
        interval = 7200
    if None in (lat1, lat2, dlat, lon1, lon2, dlon):
        raise ValueError("IONEX: missing LAT/LON grid spec")
    assert lat1 is not None and lat2 is not None and dlat is not None
    assert lon1 is not None and lon2 is not None and dlon is not None

    n_lon = round(abs((lon2 - lon1) / dlon)) + 1
    scale = 10.0**exponent

    # Walk TEC maps.
    maps: list[IonexMap] = []
    i = header_end_idx + 1
    while i < len(lines):
        line = lines[i]
        if len(line) >= 60 and line[60:].rstrip() == "START OF TEC MAP":
            map_obj, i = _parse_tec_map(lines, i + 1, n_lon=n_lon, scale=scale)
            maps.append(map_obj)
            continue
        if len(line) >= 60 and line[60:].rstrip() == "START OF RMS MAP":
            # Skip the RMS map (parsing is identical but HELIOS does not
            # currently consume the per-cell RMS values).
            i = _skip_block(lines, i + 1, "END OF RMS MAP")
            continue
        if len(line) >= 60 and line[60:].rstrip() == "END OF FILE":
            break
        i += 1

    return IonexFile(
        epoch_first=epoch_first,
        epoch_last=epoch_last,
        interval_seconds=interval,
        lat1=lat1,
        lat2=lat2,
        dlat=dlat,
        lon1=lon1,
        lon2=lon2,
        dlon=dlon,
        exponent=exponent,
        maps=tuple(maps),
        source_url=source_url,
    )


_EPOCH_RE = re.compile(r"^\s*(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)")


def _parse_ionex_epoch(body: str) -> datetime:
    m = _EPOCH_RE.match(body)
    if not m:
        raise ValueError(f"IONEX: cannot parse epoch line: {body!r}")
    year, month, day, hour, minute, second = (int(g) for g in m.groups())
    return datetime(year, month, day, hour, minute, second, tzinfo=UTC)


def _parse_three_floats(body: str) -> tuple[float, float, float]:
    parts = body.split()
    if len(parts) < 3:
        raise ValueError(f"IONEX: cannot parse 3 floats from: {body!r}")
    return float(parts[0]), float(parts[1]), float(parts[2])


def _parse_tec_map(
    lines: list[str], start_idx: int, *, n_lon: int, scale: float
) -> tuple[IonexMap, int]:
    """Parse one TEC map block; return (map, next_index_after_END_OF_TEC_MAP)."""

    epoch: datetime | None = None
    height_km: float = 450.0
    rows: list[tuple[float, ...]] = []
    current_lat: list[int] = []
    i = start_idx
    while i < len(lines):
        line = lines[i]
        label = line[60:].rstrip() if len(line) >= 60 else ""
        if label == "EPOCH OF CURRENT MAP":
            epoch = _parse_ionex_epoch(line[:60])
            i += 1
            continue
        if label == "LAT/LON1/LON2/DLON/H":
            if current_lat:
                rows.append(_finalize_row(current_lat, n_lon, scale))
                current_lat = []
            # Parse the H value (column 8-14 in the label area? actually
            # part of the body). The IONEX format puts five 6-wide floats
            # in the body: LAT, LON1, LON2, DLON, H.
            body = line[:60]
            try:
                # Try whitespace-split first
                _floats = [float(x) for x in body.split()]
                if len(_floats) >= 5:
                    height_km = _floats[4]
            except ValueError:
                pass
            i += 1
            continue
        if label == "END OF TEC MAP":
            if current_lat:
                rows.append(_finalize_row(current_lat, n_lon, scale))
            i += 1
            break
        # Otherwise: this is a row of integer TEC values (5-column groups).
        stripped = line.rstrip()
        if stripped:
            # IONEX writes values as right-justified width-5 integers,
            # 16 per line continuation. Whitespace-split handles both
            # widths.
            for token in stripped.split():
                try:
                    current_lat.append(int(token))
                except ValueError:
                    # Sometimes "9999" or similar sentinel — leave as raw.
                    try:
                        current_lat.append(int(float(token)))
                    except ValueError:
                        current_lat.append(9999)
        i += 1

    if epoch is None:
        raise ValueError("IONEX: TEC map block has no EPOCH OF CURRENT MAP")
    return (
        IonexMap(epoch=epoch, tec_grid=tuple(rows), height_km=height_km),
        i,
    )


def _finalize_row(raw_ints: list[int], n_lon: int, scale: float) -> tuple[float, ...]:
    """Convert one latitude row of raw integers into TEC floats.

    Values of 9999 are the IONEX sentinel for "no data"; we map them to
    ``float('nan')`` so downstream interpolation handles them.
    """

    if len(raw_ints) < n_lon:
        # Pad with sentinels rather than raising — partial maps can occur
        # at the file's tail.
        raw_ints = raw_ints + [9999] * (n_lon - len(raw_ints))
    return tuple((float("nan") if v == 9999 else v * scale) for v in raw_ints[:n_lon])


def _skip_block(lines: list[str], start_idx: int, end_label: str) -> int:
    """Skip lines until ``end_label`` (inclusive); return next index."""
    i = start_idx
    while i < len(lines):
        line = lines[i]
        if len(line) >= 60 and line[60:].rstrip() == end_label:
            return i + 1
        i += 1
    return i


def _bilinear_sample(ionex: IonexFile, ionex_map: IonexMap, lat: float, lon: float) -> float:
    """Bilinearly interpolate TEC at (lat, lon) on one map's grid.

    Handles ``NaN`` (no-data) cells by falling back to the mean of the
    valid corners; if all four corners are NaN, returns NaN.
    """

    lat_axis = ionex.lat_axis()
    lon_axis = ionex.lon_axis()
    # Lat axis is descending (lat1 > lat2, dlat negative) in standard IGS;
    # locate the two bracketing indices.
    i0, i1, fy = _bracket(lat, lat_axis)
    j0, j1, fx = _bracket(lon, lon_axis)
    g = ionex_map.tec_grid
    v00 = g[i0][j0]
    v01 = g[i0][j1]
    v10 = g[i1][j0]
    v11 = g[i1][j1]
    corners = [v00, v01, v10, v11]
    valid = [c for c in corners if not _isnan(c)]
    if not valid:
        return float("nan")
    if len(valid) < 4:
        return sum(valid) / len(valid)
    # Standard bilinear.
    a = v00 * (1 - fx) + v01 * fx
    b = v10 * (1 - fx) + v11 * fx
    return a * (1 - fy) + b * fy


def _isnan(x: float) -> bool:
    return x != x


def _bracket(value: float, axis: Sequence[float]) -> tuple[int, int, float]:
    """Find indices ``(i0, i1)`` bracketing ``value`` on ``axis``, plus
    a fractional offset ``f`` in ``[0, 1]`` such that
    ``axis[i0] * (1-f) + axis[i1] * f ≈ value``.

    Handles both ascending and descending axes. Clamps to the nearest
    edge cell if ``value`` falls outside the axis range.
    """

    n = len(axis)
    if n == 0:
        raise ValueError("empty axis")
    if n == 1:
        return 0, 0, 0.0
    ascending = axis[1] > axis[0]
    if ascending:
        if value <= axis[0]:
            return 0, 0, 0.0
        if value >= axis[-1]:
            return n - 1, n - 1, 0.0
        # Linear search is fine for ~73-element axes.
        for k in range(n - 1):
            if axis[k] <= value <= axis[k + 1]:
                step = axis[k + 1] - axis[k]
                return k, k + 1, (value - axis[k]) / step if step else 0.0
        return n - 1, n - 1, 0.0
    # Descending
    if value >= axis[0]:
        return 0, 0, 0.0
    if value <= axis[-1]:
        return n - 1, n - 1, 0.0
    for k in range(n - 1):
        if axis[k] >= value >= axis[k + 1]:
            step = axis[k] - axis[k + 1]
            return k, k + 1, (axis[k] - value) / step if step else 0.0
    return n - 1, n - 1, 0.0


# ---------------------------------------------------------------------------- #
# earthaccess integration (optional path)
# ---------------------------------------------------------------------------- #


def _build_earthaccess_session(user: str, password: str) -> Any:
    """Build an authenticated ``earthaccess`` session.

    Raises:
        _EarthaccessUnavailable: if ``earthaccess`` is not installed.
    """

    try:
        import earthaccess
    except ImportError as exc:
        raise _EarthaccessUnavailable() from exc

    # earthaccess reads from environment / netrc; we set env vars in-process
    # so the user-supplied credentials win without persisting to disk.
    os.environ.setdefault("EARTHDATA_USERNAME", user)
    os.environ.setdefault("EARTHDATA_PASSWORD", password)
    auth = earthaccess.login(strategy="environment", persist=False)
    if not getattr(auth, "authenticated", False):
        raise RuntimeError(
            "earthaccess.login() failed; credentials rejected. "
            "Verify NASA_EARTHDATA_USER/PASS and that you have authorized "
            "the 'NASA GESDISC DATA ARCHIVE' application on your URS profile."
        )
    return auth.get_requests_https_session()


def _earthaccess_get_bytes(session: Any, url: str) -> bytes:
    """Synchronous helper: GET ``url`` via a ``requests`` session from
    ``earthaccess``. Raises ``_DownloadNotFound`` on 404.
    """

    response = session.get(url, allow_redirects=True, timeout=60)
    if response.status_code == 404:
        raise _DownloadNotFound(404)
    response.raise_for_status()
    return bytes(response.content)
