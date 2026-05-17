"""NOAA GOES adapter — X-ray and integral proton flux.

GOES (the *Geostationary Operational Environmental Satellites*) carry the
flagship in-situ instruments NASA SRAG and NOAA SWPC use to declare flare
classifications and SEP all-clear status. HELIOS needs two product families:

* **X-ray flux** in the standard 1-8 Å (long) and 0.5-4 Å (short) bands.
  These drive flare classification (C / M / X) and feed the §2 Obj.1 fused
  flare-onset estimator.
* **Integral proton flux** at >10 MeV, >50 MeV, and >100 MeV. These are the
  exact thresholds the §2 Obj.3 SEP all-clear logic acts on.

Strategy: **WRAP**. NOAA NCEI publishes the authoritative GOES record and
``pyspedas.projects.goes`` is the well-maintained community wrapper for the
NCEI archive. HELIOS adds a thin shim around it that:

1. Normalises the output into :class:`NormalizedRecord` with full provenance.
2. Adds a near-real-time path through NOAA SWPC's JSON services for the most
   recent ~30 days, where the NCEI archive has not yet been published.

Two paths, one adapter:

* **Historical** (older than :data:`SWPC_NRT_WINDOW_DAYS` ≈ 30 days):
  :func:`pyspedas.projects.goes.xrs` or :func:`...goes.sgps`.  Data flows from
  the NCEI archive at https://www.ncei.noaa.gov/data/.
* **Near-real-time** (last ~30 days): NOAA SWPC JSON endpoints at
  ``services.swpc.noaa.gov/json/goes/primary/``.

The boundary is computed *per call*: a window that straddles the 30-day
boundary will fan-out to both paths and merge results, with the lineage
distinguishing which sample came from which.

Coordination with :class:`SwpcAdapter`: the SWPC adapter also exposes GOES
proton flux through its own SWPC-branded entrypoint. That's intentional
duplication — SWPC's view of GOES is the "real-time consumer" view
(``source_id = SourceID.SWPC``), while this adapter's view is the
"instrument archive" view (``source_id = SourceID.GOES``). Both yield
records with full provenance; choose based on whether you want to record
*who consumed the value* (SWPC) or *which instrument produced it* (GOES).

Satellites: GOES-East/West are the operational pair. As of 2026-05, GOES-16
is GOES-East and GOES-18 is GOES-West, with GOES-17 in standby. The default
is GOES-16 because all 2024 Gannon-storm reference data is keyed to it; pass
``satellite='GOES-18'`` for west-pacific coverage.

Rate limits: NOAA NCEI publishes no documented limit; we cap at 2 RPS as a
courtesy. SWPC's published soft cap is 5 RPS.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
from collections.abc import AsyncIterator, Iterator, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any, ClassVar, cast

import httpx
from dateutil.parser import isoparse

from ..cache import FileCache
from ..http import make_client, request_with_retry
from ..ratelimit import RateLimitConfig
from ..schema import NormalizedRecord, SourceID
from .base import BaseAdapter

__all__ = [
    "GOES_PROTON_PRODUCTS",
    "GOES_XRAY_PRODUCTS",
    "PROTON_THRESHOLDS_MEV",
    "SUPPORTED_SATELLITES",
    "SWPC_BASE_URL",
    "SWPC_NRT_WINDOW_DAYS",
    "XRAY_BANDS",
    "GoesAdapter",
]

logger = logging.getLogger(__name__)

#: Base URL for the NOAA SWPC near-real-time JSON services.
SWPC_BASE_URL = "https://services.swpc.noaa.gov"

#: Days from "now" within which the NCEI archive is generally not yet
#: published and we should route to SWPC near-real-time JSON instead.
#: NCEI publishes with a multi-week latency; 30 days is the operationally
#: safe threshold.
SWPC_NRT_WINDOW_DAYS: int = 30

#: GOES X-ray product slugs (the SWPC JSON endpoint slug *and* the
#: HELIOS-internal record-type discriminator).
GOES_XRAY_PRODUCTS: tuple[str, ...] = ("xray",)

#: GOES proton product slug.
GOES_PROTON_PRODUCTS: tuple[str, ...] = ("protons",)

#: The two GOES X-ray wavelength bands HELIOS consumes.
XRAY_BANDS: tuple[str, ...] = ("0.05-0.4nm", "0.1-0.8nm")

#: The integral proton energy thresholds (MeV) HELIOS's §2 Obj.3 all-clear
#: logic acts on. SWPC publishes a wider set; we filter to these three.
PROTON_THRESHOLDS_MEV: tuple[int, ...] = (10, 50, 100)

#: GOES satellites we accept.
SUPPORTED_SATELLITES: tuple[str, ...] = ("GOES-16", "GOES-17", "GOES-18")

# Map our public ``satellite`` parameter to the integer probe number pyspedas
# wants. pyspedas accepts the bare integer as a string.
_SATELLITE_TO_PROBE: dict[str, str] = {
    "GOES-16": "16",
    "GOES-17": "17",
    "GOES-18": "18",
}


class GoesAdapter(BaseAdapter):
    """Adapter for GOES X-ray and integral proton flux.

    Usage:

    .. code-block:: python

        from datetime import datetime, UTC
        from helios_connectors.adapters import GoesAdapter

        async with GoesAdapter() as goes:
            async for rec in goes.fetch_protons(
                start=datetime(2024, 5, 8, tzinfo=UTC),
                end=datetime(2024, 5, 14, tzinfo=UTC),
            ):
                print(rec.event_time, rec.value)

    The unified :meth:`fetch` routes by date:

    * window entirely older than ~30 days → PySPEDAS / NCEI archive
    * window entirely within ~30 days → SWPC near-real-time JSON
    * straddling window → both, merged

    Provenance lineage always cites the upstream URL the data was sourced
    from, so downstream consumers can distinguish the two routes via
    :attr:`NormalizedRecord.provenance.lineage`.
    """

    source_id: ClassVar[SourceID] = SourceID.GOES

    def __init__(
        self,
        *,
        base_url: str = SWPC_BASE_URL,
        client: httpx.AsyncClient | None = None,
        rate_limit: RateLimitConfig | None = None,
        cache: FileCache | None | bool = True,
        nrt_window_days: int = SWPC_NRT_WINDOW_DAYS,
        pyspedas_loader: Any | None = None,
    ) -> None:
        """Construct a GOES adapter.

        Args:
            base_url: SWPC base URL (only used by the near-real-time path).
            client: optional pre-built httpx client. If omitted, one is
                built with the SWPC base URL pre-bound.
            rate_limit: optional rate-limit override. The default 2 RPS
                covers both paths conservatively; SWPC's documented cap
                is 5 and NCEI has none published, so 2 is well inside both.
            cache: ``True`` for default file cache, ``False`` to disable,
                or a :class:`FileCache` instance.
            nrt_window_days: how many days back from "now" count as
                near-real-time and route to SWPC. The NCEI archive lags
                ~3-4 weeks; ``30`` is conservative.
            pyspedas_loader: optional override of the pyspedas loader
                callable. Useful for tests (inject a mock). The expected
                signature is ``loader(product, probe, trange) -> list[dict]``
                where each dict has ``timestamp``, ``value``, ``units``,
                and ``band``/``threshold_mev``/``record_type`` keys.
                When ``None``, the real ``pyspedas.projects.goes`` is used.
        """

        self._nrt_window_days = nrt_window_days
        self._pyspedas_loader_override = pyspedas_loader
        if client is None:
            client = make_client(base_url=base_url)
        super().__init__(client=client, rate_limit=rate_limit, cache=cache)

    def _default_rate_limit(self) -> RateLimitConfig:
        # NCEI archive has no published limit; SWPC is 5 RPS. 2 RPS is
        # conservative on both sides and matches the master-plan etiquette.
        return RateLimitConfig(rate_per_second=2.0, burst=4)

    # ------------------------------------------------------------------ #
    # public per-product conveniences
    # ------------------------------------------------------------------ #

    async def fetch_xray(
        self,
        *,
        start: datetime,
        end: datetime,
        satellite: str = "GOES-16",
    ) -> AsyncIterator[NormalizedRecord]:
        """Stream GOES X-ray flux records (both 0.05-0.4 nm and 0.1-0.8 nm bands).

        Routing follows :data:`SWPC_NRT_WINDOW_DAYS`: windows entirely
        within the last ~30 days come from SWPC JSON; older windows come
        from the NCEI archive via PySPEDAS. Straddling windows fan-out
        to both paths.
        """
        async for rec in self._route_and_stream(
            product="xray", start=start, end=end, satellite=satellite
        ):
            yield rec

    async def fetch_protons(
        self,
        *,
        start: datetime,
        end: datetime,
        satellite: str = "GOES-16",
    ) -> AsyncIterator[NormalizedRecord]:
        """Stream GOES integral proton flux at >10, >50, and >100 MeV.

        Routing matches :meth:`fetch_xray`. These are the three thresholds
        §2 Obj.3 SEP all-clear logic acts on.
        """
        async for rec in self._route_and_stream(
            product="protons", start=start, end=end, satellite=satellite
        ):
            yield rec

    async def fetch(
        self,
        *,
        start: datetime,
        end: datetime,
        products: Sequence[str] | None = None,
        satellite: str = "GOES-16",
        **_kwargs: Any,
    ) -> AsyncIterator[NormalizedRecord]:
        """Unified streaming entry point.

        Args:
            start: inclusive start of the query window (UTC; naive
                datetimes are treated as UTC).
            end: inclusive end of the query window.
            products: which product families to fetch — any combination of
                ``"xray"`` and ``"protons"``. Defaults to both.
            satellite: which GOES satellite. Defaults to ``GOES-16``
                (East). Must be one of :data:`SUPPORTED_SATELLITES`.

        Yields:
            :class:`NormalizedRecord` values, one per sample. Records
            carry an ``event_time`` (UTC), the science ``value`` (a dict
            with band/threshold metadata), and a provenance record whose
            ``lineage`` distinguishes NCEI-archive vs. SWPC origin.
        """

        if satellite not in SUPPORTED_SATELLITES:
            raise ValueError(f"unsupported satellite: {satellite!r}; valid: {SUPPORTED_SATELLITES}")
        selected = tuple(products) if products is not None else ("xray", "protons")
        unknown = set(selected) - {"xray", "protons"}
        if unknown:
            raise ValueError(
                f"unknown GOES products: {sorted(unknown)!r}; valid: ('xray', 'protons')"
            )

        for product in selected:
            async for rec in self._route_and_stream(
                product=product, start=start, end=end, satellite=satellite
            ):
                yield rec

    # ------------------------------------------------------------------ #
    # routing
    # ------------------------------------------------------------------ #

    def _route_split(
        self, start: datetime, end: datetime, *, now: datetime | None = None
    ) -> tuple[tuple[datetime, datetime] | None, tuple[datetime, datetime] | None]:
        """Split a [start, end] window into (archive_range, nrt_range).

        ``archive_range`` is the older portion (route to PySPEDAS / NCEI);
        ``nrt_range`` is the recent portion (route to SWPC). Either may
        be ``None`` if the whole window falls on one side.

        The boundary is ``now - nrt_window_days``. Times in either input
        that are naive are treated as UTC.
        """

        if now is None:
            now = datetime.now(UTC)
        start_u = _ensure_utc(start)
        end_u = _ensure_utc(end)
        if end_u < start_u:
            raise ValueError(f"end {end} is before start {start}")
        boundary = now - timedelta(days=self._nrt_window_days)
        archive: tuple[datetime, datetime] | None
        nrt: tuple[datetime, datetime] | None
        if end_u <= boundary:
            archive = (start_u, end_u)
            nrt = None
        elif start_u >= boundary:
            archive = None
            nrt = (start_u, end_u)
        else:
            archive = (start_u, boundary)
            nrt = (boundary, end_u)
        return archive, nrt

    async def _route_and_stream(
        self, *, product: str, start: datetime, end: datetime, satellite: str
    ) -> AsyncIterator[NormalizedRecord]:
        if satellite not in SUPPORTED_SATELLITES:
            raise ValueError(f"unsupported satellite: {satellite!r}; valid: {SUPPORTED_SATELLITES}")
        archive_range, nrt_range = self._route_split(start, end)
        if archive_range is not None:
            a_start, a_end = archive_range
            logger.info(
                "GOES %s: archive path (PySPEDAS/NCEI) %s..%s sat=%s",
                product,
                a_start.isoformat(),
                a_end.isoformat(),
                satellite,
            )
            for rec in await self._fetch_pyspedas(
                product=product, start=a_start, end=a_end, satellite=satellite
            ):
                yield rec
        if nrt_range is not None:
            n_start, n_end = nrt_range
            logger.info(
                "GOES %s: near-real-time path (SWPC) %s..%s sat=%s",
                product,
                n_start.isoformat(),
                n_end.isoformat(),
                satellite,
            )
            for rec in await self._fetch_swpc_nrt(
                product=product, start=n_start, end=n_end, satellite=satellite
            ):
                yield rec

    # ------------------------------------------------------------------ #
    # PySPEDAS / NCEI archive path
    # ------------------------------------------------------------------ #

    async def _fetch_pyspedas(
        self, *, product: str, start: datetime, end: datetime, satellite: str
    ) -> list[NormalizedRecord]:
        await self._ratelimiter.acquire()
        probe = _SATELLITE_TO_PROBE[satellite]
        trange = [start.strftime("%Y-%m-%d/%H:%M:%S"), end.strftime("%Y-%m-%d/%H:%M:%S")]
        loader = self._pyspedas_loader_override or _default_pyspedas_loader
        # pyspedas is blocking I/O; run it in a thread to avoid jamming the
        # event loop. The loader returns a list of {timestamp, value, units,
        # band/threshold_mev, record_type} dicts — the normaliser handles
        # provenance.
        samples = await asyncio.to_thread(loader, product, probe, trange)
        return [self._normalize_archive_sample(product, satellite, sample) for sample in samples]

    def _normalize_archive_sample(
        self,
        product: str,
        satellite: str,
        sample: dict[str, Any],
    ) -> NormalizedRecord:
        event_time = _ensure_utc(_coerce_timestamp(sample.get("timestamp")))
        record_type = str(sample.get("record_type") or product)
        band_or_threshold = sample.get("band") or sample.get("threshold_mev")
        units = str(sample.get("units") or _default_units(product))
        value = {
            "satellite": satellite,
            "flux": sample.get("value"),
            "units": units,
        }
        if "band" in sample:
            value["band"] = sample["band"]
        if "threshold_mev" in sample:
            value["threshold_mev"] = sample["threshold_mev"]
        dataset_url = _ncei_archive_url(satellite, product)
        provenance = self._emit_provenance(
            model_id=f"goes/{product}/ncei-archive",
            dataset_refs=(dataset_url,),
            timestamp=event_time,
            value=value,
            value_units=units,
            lineage=(dataset_url,),
            record_id=_synthesise_record_id(satellite, product, band_or_threshold, event_time),
        )
        return NormalizedRecord(
            source=SourceID.GOES,
            record_type=record_type,
            event_time=event_time,
            value=value,
            value_units=units,
            provenance=provenance,
            raw=sample,
        )

    # ------------------------------------------------------------------ #
    # SWPC near-real-time JSON path
    # ------------------------------------------------------------------ #

    async def _fetch_swpc_nrt(
        self, *, product: str, start: datetime, end: datetime, satellite: str
    ) -> list[NormalizedRecord]:
        await self._ratelimiter.acquire()
        endpoint = _swpc_endpoint_for(product, span_days=_swpc_span_days(start, end))
        url = f"/json/goes/primary{endpoint}"
        response = await request_with_retry(
            self._client,
            "GET",
            url,
            safe_log_params=(),
        )
        raw: Any = response.json()
        if not isinstance(raw, list):
            raise httpx.DecodingError(
                f"GOES SWPC {product}: expected JSON array, got {type(raw).__name__}"
            )
        records: list[NormalizedRecord] = []
        for item in raw:
            if not isinstance(item, dict):
                logger.warning("GOES SWPC %s: skipping non-dict entry %r", product, item)
                continue
            normalised = self._normalize_swpc_sample(product, satellite, item, url)
            if normalised is None:
                continue
            # Window-filter on event_time because SWPC's "6-hour" / "7-day"
            # buckets are coarse and almost always overshoot the requested
            # window edges.
            if normalised.event_time < start or normalised.event_time > end:
                continue
            records.append(normalised)
        return records

    def _normalize_swpc_sample(
        self,
        product: str,
        satellite: str,
        item: dict[str, Any],
        source_url: str,
    ) -> NormalizedRecord | None:
        time_tag = item.get("time_tag")
        if not isinstance(time_tag, str):
            logger.warning("GOES SWPC %s: missing/invalid time_tag in %r", product, item)
            return None
        try:
            event_time = _ensure_utc(isoparse(time_tag))
        except (ValueError, TypeError):
            logger.warning("GOES SWPC %s: unparseable time_tag %r", product, time_tag)
            return None

        flux = item.get("flux")
        if product == "xray":
            band = str(item.get("energy") or "")
            units = "W/m^2"
            record_type = "xray"
            band_or_threshold: Any = band
            value: dict[str, Any] = {
                "satellite": satellite,
                "flux": flux,
                "units": units,
                "band": band,
            }
        elif product == "protons":
            # SWPC's integral-protons JSON reports the energy threshold in
            # MeV under "energy" as a string like ">=10 MeV".
            threshold = _parse_proton_threshold(item.get("energy"))
            if threshold is None or threshold not in PROTON_THRESHOLDS_MEV:
                return None
            units = "pfu"
            record_type = "protons"
            band_or_threshold = threshold
            value = {
                "satellite": satellite,
                "flux": flux,
                "units": units,
                "threshold_mev": threshold,
            }
        else:  # pragma: no cover - guarded by callers
            raise ValueError(f"unknown product: {product!r}")

        full_url = f"{SWPC_BASE_URL}{source_url}"
        provenance = self._emit_provenance(
            model_id=f"goes/{product}/swpc-nrt",
            dataset_refs=(full_url,),
            timestamp=event_time,
            value=value,
            value_units=units,
            lineage=(full_url,),
            record_id=_synthesise_record_id(satellite, product, band_or_threshold, event_time),
        )
        return NormalizedRecord(
            source=SourceID.GOES,
            record_type=record_type,
            event_time=event_time,
            value=value,
            value_units=units,
            provenance=provenance,
            raw=item,
        )

    # ------------------------------------------------------------------ #
    # provenance-spec bridge
    # ------------------------------------------------------------------ #

    @staticmethod
    def to_helios_model_output(record: NormalizedRecord) -> dict[str, Any]:
        """Convert a :class:`NormalizedRecord` to a HELIOS provenance-spec
        :class:`helios_provenance.models.HeliosModelOutputRecord` payload.

        The spec requires the ``value`` field to be a primitive
        (float/int/str/bool); the GOES adapter's ``value`` is a dict with
        satellite/band metadata so the conversion flattens to:

        * ``value`` ← ``record.value["flux"]`` (the scalar flux)
        * ``extra`` ← the rest of the dict (satellite + band/threshold)

        The returned object is a real ``HeliosModelOutputRecord`` validated
        against the helios-provenance-spec v0.1 pydantic models.
        """

        # Import lazily so the helios_connectors top-level doesn't take a
        # hard runtime dependency on the spec for downstream users who only
        # consume the placeholder NormalizedRecord shape.
        from helios_provenance.models import (
            Agent,
            HeliosModelOutputRecord,
        )

        flux = record.value.get("flux") if isinstance(record.value, dict) else None
        extra: dict[str, Any] = {}
        if isinstance(record.value, dict):
            extra = {k: v for k, v in record.value.items() if k not in ("flux", "units")}

        if not isinstance(flux, (int, float, str, bool)) or isinstance(flux, bool):
            # Coerce; the spec needs a scalar. Use float when possible.
            try:
                flux = float(flux)  # type: ignore[arg-type]
            except (TypeError, ValueError):
                flux = str(flux)

        agent = Agent(
            id="helios-spaceweather-connectors/GoesAdapter",
            name="GoesAdapter",
            type="software",
            version="0.2.0",
        )
        return HeliosModelOutputRecord(
            id=record.provenance.id,
            created_at=record.provenance.ingestion_timestamp,
            agent=agent,
            model_id=record.provenance.model_id,
            model_version="0.2.0",
            dataset_refs=list(record.provenance.dataset_refs)
            or [_ncei_archive_url("GOES-16", "xray")],
            timestamp=record.provenance.timestamp,
            value=flux,
            value_units=record.value_units,
            ingestion_timestamp=record.provenance.ingestion_timestamp,
            extra=extra or None,
        ).model_dump(mode="json")


# ---------------------------------------------------------------------------- #
# module-level helpers
# ---------------------------------------------------------------------------- #


def _ensure_utc(ts: datetime) -> datetime:
    """Make a datetime UTC-aware; treat naive as UTC."""
    if ts.tzinfo is None:
        return ts.replace(tzinfo=UTC)
    return ts.astimezone(UTC)


def _coerce_timestamp(value: Any) -> datetime:
    """Parse a timestamp from a pyspedas sample dict.

    Accepts a datetime, an ISO string, or a unix-epoch float (seconds).
    """

    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return isoparse(value)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=UTC)
    raise ValueError(f"cannot coerce timestamp from {value!r}")


def _default_units(product: str) -> str:
    if product == "xray":
        return "W/m^2"
    if product == "protons":
        return "pfu"
    return "none"


def _ncei_archive_url(satellite: str, product: str) -> str:
    """The NCEI archive URL prefix that backs ``pyspedas.projects.goes``."""
    probe = _SATELLITE_TO_PROBE.get(satellite, "16")
    if product == "xray":
        return (
            "https://www.ncei.noaa.gov/data/goes-r-series-satellites/"
            f"goes-{probe}/l2/data/xrsf-l2-avg1m_science/"
        )
    if product == "protons":
        return (
            "https://www.ncei.noaa.gov/data/goes-r-series-satellites/"
            f"goes-{probe}/l2/data/sgps-l2-avg1m_science/"
        )
    return f"https://www.ncei.noaa.gov/data/goes-r-series-satellites/goes-{probe}/"


def _swpc_span_days(start: datetime, end: datetime) -> int:
    """Pick the smallest SWPC endpoint variant that covers ``[start, end]``."""
    delta = end - start
    if delta <= timedelta(hours=6):
        return 0  # the "6-hour" endpoint
    if delta <= timedelta(days=1):
        return 1
    if delta <= timedelta(days=3):
        return 3
    return 7


def _swpc_endpoint_for(product: str, *, span_days: int) -> str:
    """Endpoint suffix under ``/json/goes/primary/`` for a product + span."""
    if product == "xray":
        prefix = "xrays"
    elif product == "protons":
        prefix = "integral-protons"
    else:  # pragma: no cover - guarded by callers
        raise ValueError(f"unknown product: {product!r}")
    if span_days <= 0:
        return f"/{prefix}-6-hour.json"
    return f"/{prefix}-{span_days}-day.json"


def _parse_proton_threshold(energy: Any) -> int | None:
    """Parse a SWPC integral-protons energy string into a MeV integer.

    SWPC's JSON reports e.g. ``"energy": ">=10 MeV"``. We strip the
    comparator and the units and return the integer.
    """

    if not isinstance(energy, str):
        return None
    cleaned = energy.replace(">=", "").replace(">", "").replace("MeV", "").strip()
    try:
        return int(cleaned)
    except ValueError:
        return None


def _synthesise_record_id(
    satellite: str,
    product: str,
    band_or_threshold: Any,
    event_time: datetime,
) -> str:
    """Build a stable, human-readable record identifier."""
    bot = band_or_threshold if band_or_threshold not in (None, "") else "unk"
    return f"{satellite}/{product}/{bot}/{event_time.isoformat()}"


# ---------------------------------------------------------------------------- #
# Real PySPEDAS loader (live path)
# ---------------------------------------------------------------------------- #


def _default_pyspedas_loader(product: str, probe: str, trange: list[str]) -> list[dict[str, Any]]:
    """Default loader used when no override is passed to :class:`GoesAdapter`.

    Imports ``pyspedas`` lazily so the package itself can be installed
    without pyspedas (which has a heavy dependency tree). Returns a list
    of normalised sample dicts; the adapter handles provenance.

    Per-product behaviour:

    * ``"xray"``: invokes :func:`pyspedas.projects.goes.xrs` and yields
      one sample per ``(timestamp, band)`` pair.
    * ``"protons"``: invokes :func:`pyspedas.projects.goes.sgps` and
      yields one sample per ``(timestamp, energy_threshold)`` pair,
      filtered to :data:`PROTON_THRESHOLDS_MEV`.
    """

    try:
        import pyspedas
        from pyspedas.projects.goes import sgps, xrs
    except ImportError as exc:  # pragma: no cover - exercised only in environments without pyspedas
        raise RuntimeError(
            "pyspedas is required for the GOES NCEI-archive path; "
            "install `pip install pyspedas` or pass pyspedas_loader= to the adapter."
        ) from exc

    # pyspedas is chatty on stdout; quiet it locally where possible.
    samples: list[dict[str, Any]] = []
    with _quiet_pyspedas():
        if product == "xray":
            var_names = xrs(trange=trange, probe=probe, datatype="1min")
            samples.extend(_extract_xray_samples(pyspedas, var_names))
        elif product == "protons":
            var_names = sgps(trange=trange, probe=probe, datatype="1min")
            samples.extend(_extract_proton_samples(pyspedas, var_names))
        else:  # pragma: no cover
            raise ValueError(f"unknown product: {product!r}")
    return samples


@contextlib.contextmanager
def _quiet_pyspedas() -> Iterator[None]:
    """Suppress pyspedas's print() chatter during the call.

    pyspedas writes progress to stdout; for a library we want that on the
    logger, not stdout. We redirect stdout to ``os.devnull`` for the
    duration of the loader call. Network errors still raise.
    """

    import sys
    from pathlib import Path

    saved = sys.stdout
    with Path(os.devnull).open("w") as devnull:
        sys.stdout = devnull
        try:
            yield
        finally:
            sys.stdout = saved


def _extract_xray_samples(pyspedas: Any, var_names: Any) -> list[dict[str, Any]]:
    """Pull (time, flux, band) tuples out of pyspedas tplot variables."""
    out: list[dict[str, Any]] = []
    if not var_names:
        return out
    # Two GOES-R xray bands: xrsa (short, 0.05-0.4 nm), xrsb (long, 0.1-0.8 nm).
    # Variable naming differs across pyspedas releases; match on a substring.
    band_map = {"xrsa": "0.05-0.4nm", "xrsb": "0.1-0.8nm"}
    for var in var_names:
        var_lc = str(var).lower()
        matched_band: str | None = None
        for needle, band in band_map.items():
            if needle in var_lc:
                matched_band = band
                break
        if matched_band is None:
            continue
        try:
            data = pyspedas.get_data(var)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("get_data(%r) failed: %s", var, exc)
            continue
        if data is None:
            continue
        times = getattr(data, "times", None)
        flux_series = getattr(data, "y", None)
        if times is None or flux_series is None:
            continue
        for t_unix, flux in zip(times, flux_series, strict=False):
            out.append(
                {
                    "timestamp": float(t_unix),
                    "value": float(flux) if flux is not None else None,
                    "units": "W/m^2",
                    "band": matched_band,
                    "record_type": "xray",
                }
            )
    return out


#: Differential SGPS channel indices that bracket the canonical HELIOS
#: integral thresholds (10 / 50 / 100 MeV). GOES-R SGPS-L2-avg1m exposes 13
#: differential channels with effective energies (keV) approximately:
#: 1377, 2090, 2778, 4694, 8015, 16458, 30801, 54388, 90799, 108573,
#: 128238, 196774, 333922. Channels 5 (16 MeV), 7 (54 MeV), 8 (91 MeV)
#: are the operationally appropriate proxies for >=10, >=50, >=100 MeV.
#: The exact integral product is not in the 1-min L2 file — production
#: consumers can integrate the full differential series; the SWPC NRT
#: path already provides true integral values.
_SGPS_DIFF_CHANNEL_FOR_THRESHOLD: dict[int, int] = {
    10: 5,
    50: 7,
    100: 8,
}

# Match the SGPS variable that carries the differential proton flux.
_SGPS_DIFF_PROTON_VAR_SUFFIXES: tuple[str, ...] = ("avgdiffprotonflux",)

# Match the SGPS variable that carries the (single) integral proton channel.
_SGPS_INT_PROTON_VAR_SUFFIXES: tuple[str, ...] = ("avgintprotonflux",)


def _extract_proton_samples(pyspedas: Any, var_names: Any) -> list[dict[str, Any]]:
    """Pull (time, flux, threshold_mev) tuples out of pyspedas SGPS variables.

    The GOES-R SGPS 1-minute L2 file exposes:

    * ``g16_sgps_AvgDiffProtonFlux`` — shape ``(N, 2, 13)`` (time x
      east/west sensor x 13 differential channels). We average the
      east/west sensors and select the three channels closest to the
      HELIOS thresholds (10 / 50 / 100 MeV) — see
      :data:`_SGPS_DIFF_CHANNEL_FOR_THRESHOLD`.
    * ``g16_sgps_AvgIntProtonFlux`` — shape ``(N, 2)``; the >=500 MeV
      integral channel. We emit it as a ``threshold_mev=500`` record so
      downstream code can use it for high-energy SEP detection even
      though it is not one of the §2 Obj.3 standard thresholds.

    For the §2 Obj.3 standard integrals (true >=10 / >=50 / >=100 MeV),
    the SWPC near-real-time path is the canonical source; the
    archive-path values reported here are *differential-channel
    proxies* tagged with the same ``threshold_mev`` key so the schema
    is uniform — downstream code that needs strict integrals must
    re-integrate from the full differential channel set or fall back
    to SWPC.
    """

    out: list[dict[str, Any]] = []
    if not var_names:
        return out
    for var in var_names:
        var_lc = str(var).lower()
        if any(sfx in var_lc for sfx in _SGPS_DIFF_PROTON_VAR_SUFFIXES):
            out.extend(_extract_diff_proton_samples(pyspedas, var))
        elif any(sfx in var_lc for sfx in _SGPS_INT_PROTON_VAR_SUFFIXES):
            out.extend(_extract_int_proton_samples(pyspedas, var))
    return out


def _extract_diff_proton_samples(pyspedas: Any, var: str) -> list[dict[str, Any]]:
    """Extract differential-flux proxies for the HELIOS integral thresholds."""
    try:
        data = pyspedas.get_data(var)
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("get_data(%r) failed: %s", var, exc)
        return []
    if data is None:
        return []
    times = getattr(data, "times", None)
    flux_series = getattr(data, "y", None)
    if times is None or flux_series is None:
        return []
    out: list[dict[str, Any]] = []
    for t_unix, sample in zip(times, flux_series, strict=False):
        # sample shape is (sensor_units=2, diff_channels=13). Average the
        # two sensor units, then index the canonical channels.
        for threshold, channel_idx in _SGPS_DIFF_CHANNEL_FOR_THRESHOLD.items():
            try:
                east = float(sample[0][channel_idx])
                west = float(sample[1][channel_idx])
            except (IndexError, TypeError, ValueError):
                continue
            # GOES uses fill values like -1e+31; reject anything clearly invalid.
            if east < -1e29 or west < -1e29:
                continue
            avg = (east + west) / 2.0
            out.append(
                {
                    "timestamp": float(t_unix),
                    "value": avg,
                    "units": "pfu",
                    "threshold_mev": threshold,
                    "record_type": "protons",
                }
            )
    return out


def _extract_int_proton_samples(pyspedas: Any, var: str) -> list[dict[str, Any]]:
    """Extract the >=500 MeV integral channel as a 500-MeV record."""
    try:
        data = pyspedas.get_data(var)
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("get_data(%r) failed: %s", var, exc)
        return []
    if data is None:
        return []
    times = getattr(data, "times", None)
    flux_series = getattr(data, "y", None)
    if times is None or flux_series is None:
        return []
    out: list[dict[str, Any]] = []
    for t_unix, sample in zip(times, flux_series, strict=False):
        try:
            east = float(sample[0])
            west = float(sample[1])
        except (IndexError, TypeError, ValueError):
            continue
        if east < -1e29 or west < -1e29:
            continue
        avg = (east + west) / 2.0
        out.append(
            {
                "timestamp": float(t_unix),
                "value": avg,
                "units": "pfu",
                "threshold_mev": 500,
                "record_type": "protons",
            }
        )
    return out


def _coerce_scalar_flux(value: Any) -> float | None:
    """Coerce an upstream flux value (possibly a numpy scalar or array) to float."""
    if value is None:
        return None
    try:
        # numpy arrays / scalars; take the float() of the first element if needed
        if hasattr(value, "__len__") and not isinstance(value, (str, bytes)):
            seq = cast(Sequence[Any], value)
            if len(seq) == 0:
                return None
            return float(seq[0])
        return float(value)
    except (TypeError, ValueError):
        return None
