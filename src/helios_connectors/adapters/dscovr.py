"""DSCOVR L1 upstream solar-wind adapter.

The Deep Space Climate Observatory (DSCOVR) is NOAA's operational space-weather
monitor at the Sun-Earth L1 Lagrange point — about 1.5 million km sunward of
Earth. It carries the *only* sustained near-real-time view of the interplanetary
magnetic field (IMF) and solar-wind plasma upstream of Earth's magnetosphere,
arriving roughly 30-60 minutes before the wind hits the bow shock. That makes
DSCOVR HELIOS's primary exogenous input for the §2 Obj. 4 ionospheric forecasting
models (the TFT exogenous variables include DSCOVR-derived solar wind).

DSCOVR provides two relevant product families:

- **Magnetic field** (PlasMag fluxgate magnetometer, ``h0/mag`` CDFs):
  Bx/By/Bz in GSE (and RTN) coordinates plus magnitude ``Bt``.
  *Coordinate-frame note*: DSCOVR's L1 magnetometer Level-2 product is published
  natively in **GSE** (and RTN), not GSM. NOAA SWPC re-publishes the same data
  in **GSM** in its near-real-time JSON feed. Downstream consumers should be
  explicit about which frame they want; this adapter records the frame in
  ``record.value["frame"]`` and propagates the source CDF/JSON's native frame
  verbatim.
- **Plasma** (Faraday Cup, ``h1/faraday_cup`` CDFs): proton number density
  ``Np`` (cm^-3), bulk speed magnitude / V_GSE (km/s), thermal temperature
  ``THERMAL_TEMP`` (K). Cadence ~1 minute on the SWPC realtime feed; ~3 seconds
  in the L2 archive CDFs.

**Routing rule** — the adapter picks a backend per request based on the age of
the requested time window:

- *Historical path* (start older than ~48 hours): use PySPEDAS's
  ``pyspedas.projects.dscovr.mag`` / ``.fc`` loaders, which fetch the
  authoritative Level-2 CDFs from the NOAA NCEI archive. Authoritative,
  re-calibrated, and includes ground-station quality flags.
- *Near-real-time path* (last ~24-48 hours): NOAA SWPC's products JSON
  endpoints. Lower latency, lower fidelity (no post-processing).

The crossover threshold is configurable via ``recent_threshold_hours`` (default
48 h, matching the typical NCEI publishing lag).

**Intentional overlap with ``SwpcAdapter``** — the SWPC adapter ALSO consumes
the same NOAA SWPC near-real-time plasma/mag JSON. This is deliberate, paralleling
the GOES/SWPC overlap:

- ``SwpcAdapter`` records → ``source_id = SourceID.SWPC_*`` (operator-tagged).
- ``DscovrAdapter`` records → ``source_id = SourceID.DSCOVR_*`` (instrument-tagged).

Downstream consumers that want "instrument-tagged" data prefer ``DscovrAdapter``;
those that want "what the operator just published" prefer ``SwpcAdapter``.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Iterable, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any, ClassVar

import httpx
from dateutil.parser import isoparse

from ..cache import FileCache
from ..http import make_client, request_with_retry
from ..ratelimit import RateLimitConfig
from ..schema import NormalizedRecord, SourceID
from .base import BaseAdapter

__all__ = [
    "DSCOVR_PRODUCTS",
    "SWPC_BASE_URL",
    "SWPC_MAG_URL_PATH",
    "SWPC_PLASMA_URL_PATH",
    "DscovrAdapter",
]

logger = logging.getLogger(__name__)

#: Base URL for NOAA SWPC near-real-time product feeds.
SWPC_BASE_URL = "https://services.swpc.noaa.gov"

#: SWPC products JSON path for the DSCOVR-derived 7-day plasma feed.
SWPC_PLASMA_URL_PATH = "/products/solar-wind/plasma-7-day.json"

#: SWPC products JSON path for the DSCOVR-derived 7-day magnetometer feed.
SWPC_MAG_URL_PATH = "/products/solar-wind/mag-7-day.json"

#: Canonical product slugs this adapter knows about.
DSCOVR_PRODUCTS: tuple[str, ...] = ("mag", "plasma")

#: Number of hours back from "now" we treat as the SWPC near-real-time window.
#: Older than this routes to the PySPEDAS / NCEI archive path.
_DEFAULT_RECENT_THRESHOLD_HOURS = 48

# PySPEDAS tplot-variable names produced by the dscovr.mag and .fc loaders.
# Verified against pyspedas 2.1 and the upstream test suite.
_MAG_GSE_VAR = "dsc_h0_mag_B1GSE"
_MAG_RTN_VAR = "dsc_h0_mag_B1RTN"
_FC_NP_VAR = "dsc_h1_fc_Np"
_FC_VGSE_VAR = "dsc_h1_fc_V_GSE"
_FC_SPEED_VAR = "dsc_h1_fc_THERMAL_SPD"
_FC_TEMP_VAR = "dsc_h1_fc_THERMAL_TEMP"


class DscovrAdapter(BaseAdapter):
    """Adapter for DSCOVR L1 magnetometer + Faraday-cup plasma.

    Wraps PySPEDAS for the historical NCEI archive path and NOAA SWPC's
    near-real-time JSON for the last ~24-48 hours. The dispatch is automatic
    based on the requested time range; callers can also force either path via
    ``backend="pyspedas"`` / ``backend="swpc"``.

    Example:

    .. code-block:: python

        from datetime import datetime, UTC, timedelta
        from helios_connectors.adapters import DscovrAdapter

        async with DscovrAdapter() as dscovr:
            # Historical: Gannon week (May 2024), routed to PySPEDAS/NCEI.
            async for rec in dscovr.fetch_mag(
                start=datetime(2024, 5, 8, tzinfo=UTC),
                end=datetime(2024, 5, 14, tzinfo=UTC),
            ):
                print(rec.event_time, rec.value["bz"])

            # Near-real-time: last 6 h, routed to SWPC products JSON.
            now = datetime.now(UTC)
            async for rec in dscovr.fetch_plasma(start=now - timedelta(hours=6), end=now):
                print(rec.event_time, rec.value["speed"])

    Provenance records emit ``model_id = "dscovr/mag"`` or ``"dscovr/plasma"``
    and a ``dataset_refs`` tuple naming the upstream archive or service. The
    coordinate frame (GSE/GSM/RTN) is captured in ``value["frame"]``.
    """

    source_id: ClassVar[SourceID] = SourceID.DSCOVR

    def __init__(
        self,
        *,
        client: httpx.AsyncClient | None = None,
        rate_limit: RateLimitConfig | None = None,
        cache: FileCache | None | bool = True,
        swpc_base_url: str = SWPC_BASE_URL,
        recent_threshold_hours: int = _DEFAULT_RECENT_THRESHOLD_HOURS,
    ) -> None:
        """Construct a DSCOVR adapter.

        Args:
            client: optional pre-built httpx client used for the SWPC path.
                If omitted, a client bound to ``swpc_base_url`` is created.
            rate_limit: optional rate-limit override. SWPC defaults are
                conservative (5 RPS). PySPEDAS-archive calls are gated by
                the same limiter (PySPEDAS itself rate-limits to NCEI at
                1-2 RPS internally, so this is belt-and-braces).
            cache: ``True`` for default file cache, ``False`` to disable,
                or a :class:`FileCache` instance.
            swpc_base_url: base URL for SWPC product calls. Override only
                for a CDN mirror or test transport.
            recent_threshold_hours: how many hours back from "now" we treat
                as the SWPC near-real-time window. Older = NCEI archive.
        """

        self._swpc_base_url = swpc_base_url
        self._recent_threshold = timedelta(hours=recent_threshold_hours)
        if client is None:
            client = make_client(base_url=swpc_base_url)
        super().__init__(client=client, rate_limit=rate_limit, cache=cache)

    def _default_rate_limit(self) -> RateLimitConfig:
        # NOAA SWPC etiquette: ~5 RPS for products JSON. PySPEDAS NCEI
        # downloads are even slower; we don't try to model them here and
        # rely on PySPEDAS's internal pacing.
        return RateLimitConfig(rate_per_second=5.0)

    # ------------------------------------------------------------------ #
    # unified entry point
    # ------------------------------------------------------------------ #

    async def fetch(
        self,
        *,
        start: datetime,
        end: datetime,
        products: Iterable[str] | None = None,
        backend: str | None = None,
        **_kwargs: Any,
    ) -> AsyncIterator[NormalizedRecord]:
        """Stream DSCOVR records for one or more products in [start, end].

        Args:
            start: inclusive start of the requested window (UTC).
            end: inclusive end of the requested window (UTC).
            products: iterable of product slugs from :data:`DSCOVR_PRODUCTS`
                (``"mag"`` and/or ``"plasma"``). Default: both.
            backend: force a specific backend (``"pyspedas"`` or ``"swpc"``).
                Default: auto-select based on age vs ``recent_threshold_hours``.

        Yields:
            NormalizedRecord values, one per upstream sample, in upstream
            time order within each product but unspecified order across
            products.
        """

        selected = tuple(products) if products is not None else DSCOVR_PRODUCTS
        unknown = set(selected) - set(DSCOVR_PRODUCTS)
        if unknown:
            raise ValueError(
                f"unknown DSCOVR products: {sorted(unknown)!r}; valid: {DSCOVR_PRODUCTS}"
            )
        if backend not in (None, "pyspedas", "swpc"):
            raise ValueError(f"unknown backend {backend!r}; expected 'pyspedas', 'swpc', or None")

        for product in selected:
            if product == "mag":
                async for rec in self.fetch_mag(start=start, end=end, backend=backend):
                    yield rec
            elif product == "plasma":
                async for rec in self.fetch_plasma(start=start, end=end, backend=backend):
                    yield rec

    # ------------------------------------------------------------------ #
    # per-product conveniences
    # ------------------------------------------------------------------ #

    async def fetch_mag(
        self,
        *,
        start: datetime,
        end: datetime,
        backend: str | None = None,
    ) -> AsyncIterator[NormalizedRecord]:
        """Stream DSCOVR magnetometer records (Bx/By/Bz, magnitude).

        Routing depends on ``backend`` (or the auto-rule when omitted):

        - ``pyspedas`` → :func:`pyspedas.projects.dscovr.mag` over NCEI archive;
          emits GSE-frame samples at native ~1s cadence.
        - ``swpc`` → SWPC ``mag-7-day.json``; emits GSM-frame samples at
          1-minute cadence (the NOAA real-time aggregate).
        """

        chosen = backend or self._choose_backend(start, end)
        if chosen == "pyspedas":
            records = await asyncio.to_thread(_load_mag_archive, start, end)
            for raw in records:
                yield self._normalize_mag(raw, frame=raw.get("frame", "GSE"), backend="pyspedas")
        else:
            samples = await self._fetch_swpc_mag(start=start, end=end)
            for sample in samples:
                yield self._normalize_mag(sample, frame="GSM", backend="swpc")

    async def fetch_plasma(
        self,
        *,
        start: datetime,
        end: datetime,
        backend: str | None = None,
    ) -> AsyncIterator[NormalizedRecord]:
        """Stream DSCOVR Faraday-cup plasma records (density, speed, temperature)."""

        chosen = backend or self._choose_backend(start, end)
        if chosen == "pyspedas":
            records = await asyncio.to_thread(_load_plasma_archive, start, end)
            for raw in records:
                yield self._normalize_plasma(raw, backend="pyspedas")
        else:
            samples = await self._fetch_swpc_plasma(start=start, end=end)
            for sample in samples:
                yield self._normalize_plasma(sample, backend="swpc")

    # ------------------------------------------------------------------ #
    # backend selection
    # ------------------------------------------------------------------ #

    def _choose_backend(self, start: datetime, end: datetime) -> str:  # noqa: ARG002
        """Pick a backend based on the requested window's age.

        If ``start`` is within the recent-threshold window we route to SWPC;
        otherwise we route to the PySPEDAS NCEI-archive path. The decision
        is made on ``start`` (not ``end``) because PySPEDAS can serve up to
        "yesterday" reliably and SWPC only goes back ~7 days — using start
        keeps both backends within their published windows.
        """

        now = datetime.now(UTC)
        start_aware = _ensure_utc(start)
        age = now - start_aware
        if age <= self._recent_threshold:
            return "swpc"
        return "pyspedas"

    # ------------------------------------------------------------------ #
    # SWPC near-real-time path
    # ------------------------------------------------------------------ #

    async def _fetch_swpc_mag(self, *, start: datetime, end: datetime) -> list[dict[str, Any]]:
        await self._ratelimiter.acquire()
        response = await request_with_retry(
            self._client,
            "GET",
            SWPC_MAG_URL_PATH,
            params=None,
            safe_log_params=(),
        )
        payload = response.json()
        rows = _parse_swpc_csv_json(payload)
        return _filter_rows(rows, start=start, end=end, time_key="time_tag")

    async def _fetch_swpc_plasma(self, *, start: datetime, end: datetime) -> list[dict[str, Any]]:
        await self._ratelimiter.acquire()
        response = await request_with_retry(
            self._client,
            "GET",
            SWPC_PLASMA_URL_PATH,
            params=None,
            safe_log_params=(),
        )
        payload = response.json()
        rows = _parse_swpc_csv_json(payload)
        return _filter_rows(rows, start=start, end=end, time_key="time_tag")

    # ------------------------------------------------------------------ #
    # normalization
    # ------------------------------------------------------------------ #

    def _normalize_mag(self, raw: dict[str, Any], *, frame: str, backend: str) -> NormalizedRecord:
        event_time = _coerce_timestamp(raw)
        # Pull Bx/By/Bz/Bt regardless of source layout, supporting both the
        # SWPC JSON keys (bx_gsm/by_gsm/bz_gsm/bt) and the PySPEDAS-archive
        # canonical keys (bx/by/bz/bt).
        bx = _coerce_float(raw.get("bx") if "bx" in raw else raw.get("bx_gsm"))
        by = _coerce_float(raw.get("by") if "by" in raw else raw.get("by_gsm"))
        bz = _coerce_float(raw.get("bz") if "bz" in raw else raw.get("bz_gsm"))
        bt = _coerce_float(raw.get("bt"))
        value: dict[str, Any] = {
            "bx": bx,
            "by": by,
            "bz": bz,
            "bt": bt,
            "frame": frame,
        }
        # Carry through any extra coordinate readouts (lon/lat in GSM, etc.)
        for k in ("lon_gsm", "lat_gsm", "bx_gse", "by_gse", "bz_gse"):
            if k in raw:
                value[k] = _coerce_float(raw[k])
        dataset_ref = _dataset_ref_for(backend, "mag")
        record_id = f"dscovr-mag-{event_time.isoformat()}"
        # Spec value is a scalar; Bz drives geomagnetic activity, so use
        # it as the headline. Fall back to Bt magnitude if Bz is missing.
        scalar: float | str
        if bz is not None:
            scalar = bz
        elif bt is not None:
            scalar = bt
        else:
            scalar = "compound"
        provenance = self._emit_provenance(
            model_id="dscovr/mag",
            dataset_refs=(dataset_ref,),
            timestamp=event_time,
            value=scalar,
            value_units="nT",
            model_version=backend,
            extra={
                "frame": frame,
                "payload": dict(value),
                "lineage": [dataset_ref],
            },
            record_id=record_id,
        )
        return NormalizedRecord(
            source=SourceID.DSCOVR,
            record_type="mag",
            event_time=event_time,
            value=value,
            value_units="nT",
            provenance=provenance,
            raw=dict(raw),
        )

    def _normalize_plasma(self, raw: dict[str, Any], *, backend: str) -> NormalizedRecord:
        event_time = _coerce_timestamp(raw)
        density = _coerce_float(raw.get("density") or raw.get("Np"))
        speed = _coerce_float(raw.get("speed") or raw.get("V") or raw.get("v"))
        temperature = _coerce_float(raw.get("temperature") or raw.get("THERMAL_TEMP"))
        value: dict[str, Any] = {
            "density": density,
            "speed": speed,
            "temperature": temperature,
            "density_units": "cm^-3",
            "speed_units": "km/s",
            "temperature_units": "K",
        }
        # Pass-through any GSE plasma-velocity components if PySPEDAS loaded
        # them so consumers can compute Vx/Vy/Vz alignment.
        for k in ("vx_gse", "vy_gse", "vz_gse"):
            if k in raw:
                value[k] = _coerce_float(raw[k])
        dataset_ref = _dataset_ref_for(backend, "plasma")
        record_id = f"dscovr-plasma-{event_time.isoformat()}"
        # Speed is the canonical bulk-plasma indicator and the value
        # fusion consumers key on; use it as the scalar, fall back to
        # density, then to ``"compound"``.
        scalar: float | str
        if speed is not None:
            scalar = speed
        elif density is not None:
            scalar = density
        else:
            scalar = "compound"
        provenance = self._emit_provenance(
            model_id="dscovr/plasma",
            dataset_refs=(dataset_ref,),
            timestamp=event_time,
            value=scalar,
            value_units="km/s" if speed is not None else "compound",
            model_version=backend,
            extra={"payload": dict(value), "lineage": [dataset_ref]},
            record_id=record_id,
        )
        return NormalizedRecord(
            source=SourceID.DSCOVR,
            record_type="plasma",
            event_time=event_time,
            value=value,
            value_units="compound",
            provenance=provenance,
            raw=dict(raw),
        )


# ---------------------------------------------------------------------------- #
# Module-level helpers (testable in isolation)
# ---------------------------------------------------------------------------- #


def _ensure_utc(ts: datetime) -> datetime:
    """Return a UTC-aware datetime; treat naive as UTC."""
    if ts.tzinfo is None:
        return ts.replace(tzinfo=UTC)
    return ts.astimezone(UTC)


def _coerce_float(value: Any) -> float | None:
    """Parse a SWPC/PySPEDAS numeric value to a float, gracefully.

    SWPC sometimes returns strings like ``"-99999.9"`` to mean "missing";
    we map those to ``None`` rather than letting downstream consumers
    accidentally use the sentinel. The threshold is intentionally
    generous because the magnetic field at L1 rarely exceeds 100 nT and
    plasma temperatures don't reach 1e8 K.
    """

    if value is None:
        return None
    if isinstance(value, bool):
        return float(value)
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    # SWPC fill values are usually negative large numbers
    if f <= -9000.0 or f >= 1.0e30:
        return None
    return f


def _coerce_timestamp(raw: dict[str, Any]) -> datetime:
    """Pull the most appropriate timestamp out of a raw record.

    Supports SWPC's ``time_tag`` (``"YYYY-MM-DD HH:MM:SS.fff"``), PySPEDAS's
    Unix-epoch ``time`` floats, and ISO-8601 strings under ``timestamp``.
    Falls back to ``datetime.now(UTC)`` and logs a warning if nothing
    parses — better than crashing on an otherwise-usable record.
    """

    candidates: tuple[Any, ...] = (
        raw.get("time_tag"),
        raw.get("timestamp"),
        raw.get("time"),
    )
    for value in candidates:
        if value is None:
            continue
        if isinstance(value, datetime):
            return _ensure_utc(value)
        if isinstance(value, int | float):
            try:
                return datetime.fromtimestamp(float(value), tz=UTC)
            except (OverflowError, OSError, ValueError):
                continue
        if isinstance(value, str) and value:
            # SWPC uses "YYYY-MM-DD HH:MM:SS.fff" (space, no Z). isoparse
            # handles both that and full ISO-8601.
            try:
                ts = isoparse(value)
            except (ValueError, TypeError):
                continue
            return _ensure_utc(ts)
    logger.warning("DSCOVR: record has no parseable timestamp; using now()")
    return datetime.now(UTC)


def _parse_swpc_csv_json(payload: Any) -> list[dict[str, Any]]:
    """Convert SWPC's header-first array-of-arrays JSON into list-of-dicts.

    SWPC publishes its near-real-time feeds as JSON with the first row
    being column headers and subsequent rows being values, e.g.::

        [["time_tag","density","speed","temperature"],
         ["2025-05-10 16:36:00.000","2.25","393.5","84236"],
         ...]

    This helper normalizes that shape; if SWPC ever switches to a more
    conventional list-of-dicts (they occasionally do for new feeds),
    we accept that too without forcing the caller to know which.
    """

    if isinstance(payload, list) and payload and isinstance(payload[0], dict):
        # Already list-of-dicts form
        return [dict(row) for row in payload if isinstance(row, dict)]
    if not isinstance(payload, list) or not payload:
        return []
    header = payload[0]
    if not isinstance(header, list) or not all(isinstance(c, str) for c in header):
        raise httpx.DecodingError("SWPC payload missing header row; cannot parse")
    rows: list[dict[str, Any]] = []
    for row in payload[1:]:
        if not isinstance(row, list) or len(row) != len(header):
            continue
        rows.append(dict(zip(header, row, strict=True)))
    return rows


def _filter_rows(
    rows: Sequence[dict[str, Any]],
    *,
    start: datetime,
    end: datetime,
    time_key: str,
) -> list[dict[str, Any]]:
    """Filter a list-of-dicts by an inclusive [start, end] window on ``time_key``.

    Rows whose timestamp cannot be parsed are dropped silently (the
    ``_coerce_timestamp`` fallback handles them downstream; here we
    don't want to ingest unparseable timestamps as "now").
    """

    start_aware = _ensure_utc(start)
    end_aware = _ensure_utc(end)
    out: list[dict[str, Any]] = []
    for row in rows:
        value = row.get(time_key)
        if not isinstance(value, str):
            continue
        try:
            ts = isoparse(value)
        except (ValueError, TypeError):
            continue
        ts = _ensure_utc(ts)
        if start_aware <= ts <= end_aware:
            out.append(row)
    return out


def _dataset_ref_for(backend: str, product: str) -> str:
    """Return the canonical lineage URL for a given backend + product."""
    if backend == "pyspedas":
        if product == "mag":
            return "https://www.ngdc.noaa.gov/dscovr/portal/index.html#/data/mag"
        return "https://www.ngdc.noaa.gov/dscovr/portal/index.html#/data/faraday_cup"
    # SWPC near-real-time
    if product == "mag":
        return SWPC_BASE_URL + SWPC_MAG_URL_PATH
    return SWPC_BASE_URL + SWPC_PLASMA_URL_PATH


# ---------------------------------------------------------------------------- #
# PySPEDAS loaders (thin, blocking, run via asyncio.to_thread)
# ---------------------------------------------------------------------------- #


def _load_mag_archive(start: datetime, end: datetime) -> list[dict[str, Any]]:
    """Load DSCOVR L1 magnetometer samples from the NCEI archive via PySPEDAS.

    Imported lazily so the helios-spaceweather-connectors core package does
    not have to pull pyspedas + cdflib + astropy when the historical path
    is unused. Returns a list of dicts with keys ``time``, ``bx``, ``by``,
    ``bz``, ``bt``, ``frame`` (always ``"GSE"`` for the L2 archive product).
    """

    trange = [_pyspedas_trange(start), _pyspedas_trange(end)]
    try:
        from pyspedas.projects.dscovr import mag as _pyspedas_mag
        from pyspedas.tplot_tools import get_data
    except ImportError as exc:  # pragma: no cover - exercised via mocked tests
        raise RuntimeError(
            "DscovrAdapter PySPEDAS path requires the 'pyspedas' extra: "
            "pip install 'helios-spaceweather-connectors[pyspedas]'"
        ) from exc

    var_names = _pyspedas_mag(trange=trange, time_clip=True, no_update=False)
    if not var_names or _MAG_GSE_VAR not in var_names:
        logger.warning(
            "DSCOVR: pyspedas.dscovr.mag returned no GSE variable for trange %s; got %r",
            trange,
            var_names,
        )
        return []
    data = get_data(_MAG_GSE_VAR)
    if data is None:
        return []
    return _tplot_to_mag_rows(data, frame="GSE")


def _load_plasma_archive(start: datetime, end: datetime) -> list[dict[str, Any]]:
    """Load DSCOVR Faraday-cup plasma samples from the NCEI archive."""

    trange = [_pyspedas_trange(start), _pyspedas_trange(end)]
    try:
        from pyspedas.projects.dscovr import fc as _pyspedas_fc
        from pyspedas.tplot_tools import get_data
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError(
            "DscovrAdapter PySPEDAS path requires the 'pyspedas' extra: "
            "pip install 'helios-spaceweather-connectors[pyspedas]'"
        ) from exc

    var_names = _pyspedas_fc(trange=trange, time_clip=True, no_update=False)
    if not var_names:
        logger.warning("DSCOVR: pyspedas.dscovr.fc returned no variables for trange %s", trange)
        return []
    np_data = get_data(_FC_NP_VAR) if _FC_NP_VAR in var_names else None
    v_data = get_data(_FC_VGSE_VAR) if _FC_VGSE_VAR in var_names else None
    temp_data = get_data(_FC_TEMP_VAR) if _FC_TEMP_VAR in var_names else None
    return _tplot_to_plasma_rows(np_data=np_data, v_data=v_data, temp_data=temp_data)


def _pyspedas_trange(ts: datetime) -> str:
    """Format a datetime as PySPEDAS expects: ``YYYY-MM-DD/HH:MM:SS``."""
    aware = _ensure_utc(ts)
    return aware.strftime("%Y-%m-%d/%H:%M:%S")


def _tplot_to_mag_rows(data: Any, *, frame: str) -> list[dict[str, Any]]:
    """Flatten a PySPEDAS tplot dataclass into list-of-dicts mag rows.

    The ``data`` object follows PySPEDAS's ``time, y`` namedtuple shape with
    ``y`` of shape ``(n_samples, 3)`` for vector quantities. We attach a
    derived ``bt`` magnitude so downstream consumers don't have to.
    """

    rows: list[dict[str, Any]] = []
    times = getattr(data, "times", None)
    y = getattr(data, "y", None)
    if times is None or y is None:
        return rows
    for t, vec in zip(times, y, strict=False):
        try:
            bx = float(vec[0])
            by = float(vec[1])
            bz = float(vec[2])
        except (IndexError, TypeError, ValueError):
            continue
        bt = (bx * bx + by * by + bz * bz) ** 0.5
        try:
            ts = datetime.fromtimestamp(float(t), tz=UTC)
        except (OverflowError, OSError, ValueError):
            continue
        rows.append(
            {
                "time": ts.isoformat(),
                "bx": bx,
                "by": by,
                "bz": bz,
                "bt": bt,
                "frame": frame,
            }
        )
    return rows


def _tplot_to_plasma_rows(
    *,
    np_data: Any,
    v_data: Any,
    temp_data: Any,
) -> list[dict[str, Any]]:
    """Align PySPEDAS plasma tplot variables onto a single per-timestamp dict.

    ``np_data``, ``v_data``, and ``temp_data`` are the three independent
    tplot variables returned by ``pyspedas.dscovr.fc``. We use the density
    variable's time grid as the reference and pull V_GSE / T at the same
    indices when shapes match; mismatched lengths just emit what we have.
    """

    if np_data is None:
        return []
    np_times = getattr(np_data, "times", None)
    np_y = getattr(np_data, "y", None)
    if np_times is None or np_y is None:
        return []
    v_y = getattr(v_data, "y", None) if v_data is not None else None
    temp_y = getattr(temp_data, "y", None) if temp_data is not None else None
    rows: list[dict[str, Any]] = []
    for i, t in enumerate(np_times):
        try:
            ts = datetime.fromtimestamp(float(t), tz=UTC)
        except (OverflowError, OSError, ValueError):
            continue
        try:
            density = float(np_y[i])
        except (IndexError, TypeError, ValueError):
            density = float("nan")
        speed: float | None = None
        vx_gse: float | None = None
        vy_gse: float | None = None
        vz_gse: float | None = None
        if v_y is not None:
            try:
                v_row = v_y[i]
                vx_gse = float(v_row[0])
                vy_gse = float(v_row[1])
                vz_gse = float(v_row[2])
                speed = (vx_gse * vx_gse + vy_gse * vy_gse + vz_gse * vz_gse) ** 0.5
            except (IndexError, TypeError, ValueError):
                speed = None
        temperature: float | None = None
        if temp_y is not None:
            try:
                temperature = float(temp_y[i])
            except (IndexError, TypeError, ValueError):
                temperature = None
        row: dict[str, Any] = {
            "time": ts.isoformat(),
            "Np": density,
            "THERMAL_TEMP": temperature,
        }
        if speed is not None:
            row["v"] = speed
        if vx_gse is not None:
            row["vx_gse"] = vx_gse
            row["vy_gse"] = vy_gse
            row["vz_gse"] = vz_gse
        rows.append(row)
    return rows
