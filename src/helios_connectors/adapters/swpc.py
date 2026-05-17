"""NOAA SWPC adapter — plasma, IMF, Kp, Dst, SEP forecast.

NOAA SWPC (Space Weather Prediction Center) is the operational US source
for geomagnetic indices and solar-wind situational awareness. Their
real-time JSON endpoints feed every NOAA-derived operational product
(G-scale alerts, RTK accuracy outlooks, ARRT dose-rate inputs) and they
are the primary source for the May 10-12, 2024 Gannon G5 retrospective
work that anchors HELIOS' propositions.

This adapter follows the **EXTEND** strategy: SunPy already exposes some
SWPC indices (Kp, Dst) but its coverage is gap-shaped — plasma, IMF, and
the 3-day probabilistic SEP/G-storm/R-blackout forecast are not in
SunPy's catalogue. The fusion engine needs all of these. So we wrap
SWPC's services.swpc.noaa.gov endpoints directly, while documenting
which products overlap with SunPy / GOES adapter coverage for downstream
deduplication.

Endpoints
---------

* **Real-time Kp** (3-hourly):
  ``https://services.swpc.noaa.gov/products/noaa-planetary-k-index.json``

* **Solar wind plasma** (1-min cadence, DSCOVR-derived):
  ``https://services.swpc.noaa.gov/products/solar-wind/plasma-7-day.json``
  (NOTE: the DSCOVR adapter in this package also covers historical
  plasma; SWPC is the real-time fast path.)

* **Interplanetary magnetic field** (1-min cadence, DSCOVR-derived):
  ``https://services.swpc.noaa.gov/products/solar-wind/mag-7-day.json``
  (Same DSCOVR-overlap note as plasma.)

* **3-day probabilistic forecast** (text product, daily issue):
  ``https://services.swpc.noaa.gov/text/3-day-forecast.txt``
  Provides daily geomagnetic-storm, solar-radiation, and radio-blackout
  probabilities. The "S1+" row is the SEP / radiation-storm probability
  HELIOS uses for its all-clear-revocation baseline.

* **GOES integral proton flux** (1-min):
  ``https://services.swpc.noaa.gov/json/goes/primary/integral-protons-7-day.json``
  (NOTE: the **GoesAdapter** in this package also wraps GOES proton
  flux directly from the GOES JSON service. Use that adapter when you
  want GOES-native field names; this SWPC endpoint is included here so
  the SWPC adapter is self-contained for the operational "everything
  SWPC publishes" use case. Coordinate with the GOES adapter on
  cross-source dedup at the fusion layer.)

The 30-day archive gotcha
-------------------------

NOAA SWPC's public JSON products only carry the last **~30 days** of
data. For any retrospective window with ``start < (now - 30 days)`` —
e.g. the Gannon G5 storm of May 10-12, 2024 — :meth:`fetch_kp` and
:meth:`fetch_dst` transparently fall back to authoritative academic
archives:

* **Kp** → GFZ Potsdam at
  ``https://kp.gfz.de/app/files/Kp_ap_Ap_SN_F107_since_1932.txt``
  (CC-BY-4.0; the IAGA-authoritative Kp series back to 1932).
* **Dst** → Kyoto WDC at
  ``http://wdc.kugi.kyoto-u.ac.jp/dst_provisional/<YYYYMM>/dst<YYMM>.for.request``
  (provisional; the final-quality file at ``dst_final/.../`` is
  preferred when published — typically 6-12 months lag).

This pattern was first applied in ``gannon-storm-rtk-analysis``; this
adapter generalizes it so any HELIOS-internal consumer gets the same
fallback behavior with proper provenance lineage.

Rate limits
-----------

* services.swpc.noaa.gov: 5 RPS default.
* kp.gfz.de and wdc.kugi.kyoto-u.ac.jp: 1 RPS — academic infrastructure.

Provenance
----------

Records carry the SWPC product URL as their ``dataset_refs`` and a
two-element ``lineage`` for archive fallbacks::

    ("swpc/<product>", "<archive-provider>/<archive-file>")

where ``<archive-provider>`` is ``"GFZ Potsdam"`` or ``"Kyoto WDC"``.
"""

from __future__ import annotations

import logging
import re
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, ClassVar

import httpx
from dateutil.parser import isoparse

from ..cache import FileCache
from ..http import make_client, request_with_retry
from ..ratelimit import RateLimitConfig, RateLimiter
from ..schema import NormalizedRecord, SourceID
from .base import BaseAdapter

__all__ = [
    "GFZ_KP_ARCHIVE_URL",
    "KYOTO_DST_FINAL_URL_TEMPLATE",
    "KYOTO_DST_PROVISIONAL_URL_TEMPLATE",
    "SWPC_BASE_URL",
    "SWPC_PRODUCTS",
    "SWPC_REALTIME_DAYS",
    "SwpcAdapter",
]

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------- #
# Endpoint constants
# ---------------------------------------------------------------------------- #

SWPC_BASE_URL = "https://services.swpc.noaa.gov"
"""Base URL for NOAA SWPC's services CDN."""

SWPC_REALTIME_DAYS = 30
"""How far back the SWPC real-time JSON products serve data.

Any query with ``start < now - SWPC_REALTIME_DAYS`` triggers the archive
fallback to GFZ Potsdam (Kp) or Kyoto WDC (Dst).
"""

SWPC_PRODUCTS: dict[str, str] = {
    "kp": "/products/noaa-planetary-k-index.json",
    "plasma": "/products/solar-wind/plasma-7-day.json",
    "mag": "/products/solar-wind/mag-7-day.json",
    "goes_protons": "/json/goes/primary/integral-protons-7-day.json",
    "sep_forecast": "/text/3-day-forecast.txt",
}
"""Product slug -> relative URL path."""

GFZ_KP_ARCHIVE_URL = "https://kp.gfz.de/app/files/Kp_ap_Ap_SN_F107_since_1932.txt"
"""GFZ Potsdam Kp archive URL (CC-BY-4.0).

Daily-row format with eight 3-hour Kp values + eight ap values per line;
coverage back to 1932. The brief specifies ``kp.gfz-potsdam.de`` which
301-redirects to ``kp.gfz.de`` — we use the canonical short URL to skip
a redirect hop.
"""

KYOTO_DST_PROVISIONAL_URL_TEMPLATE = (
    "http://wdc.kugi.kyoto-u.ac.jp/dst_provisional/{yyyymm}/dst{yymm}.for.request"
)
"""Kyoto WDC provisional Dst URL template.

``{yyyymm}`` is six-digit year+month, ``{yymm}`` is two-digit form. The
``.for.request`` extension is the machine-readable WDC fixed-width format.
"""

KYOTO_DST_FINAL_URL_TEMPLATE = (
    "http://wdc.kugi.kyoto-u.ac.jp/dst_final/{yyyymm}/dst{yymm}.for.request"
)
"""Kyoto WDC final-quality Dst URL template.

Currently lags 6-12 months behind real time; for recent windows the
provisional template is the only one that resolves.
"""


# ---------------------------------------------------------------------------- #
# Helper dataclasses
# ---------------------------------------------------------------------------- #


@dataclass(frozen=True, slots=True)
class _Endpoint:
    """Internal SWPC product descriptor."""

    slug: str
    path: str
    model_id: str
    source_id: SourceID


_ENDPOINTS: dict[str, _Endpoint] = {
    "kp": _Endpoint("kp", SWPC_PRODUCTS["kp"], "swpc/kp-3-hour", SourceID.SWPC_KP),
    "plasma": _Endpoint(
        "plasma", SWPC_PRODUCTS["plasma"], "swpc/plasma-1min", SourceID.SWPC_PLASMA
    ),
    "mag": _Endpoint("mag", SWPC_PRODUCTS["mag"], "swpc/mag-1min", SourceID.SWPC_MAG),
    "goes_protons": _Endpoint(
        "goes_protons",
        SWPC_PRODUCTS["goes_protons"],
        "swpc/goes-protons-1min",
        SourceID.GOES_PROTON,
    ),
    "sep_forecast": _Endpoint(
        "sep_forecast",
        SWPC_PRODUCTS["sep_forecast"],
        "swpc/sep-3-day-forecast",
        SourceID.SWPC_SEP_FORECAST,
    ),
}


# ---------------------------------------------------------------------------- #
# Adapter
# ---------------------------------------------------------------------------- #


class SwpcAdapter(BaseAdapter):
    """Adapter for NOAA SWPC's real-time JSON + text products.

    Per the EXTEND strategy, this adapter covers the
    services.swpc.noaa.gov surface that SunPy does not: plasma, IMF, and
    the 3-day probabilistic SEP forecast. For Kp and Dst it falls back
    automatically to GFZ Potsdam and Kyoto WDC for windows older than
    the SWPC real-time archive (~30 days).

    Usage:

    .. code-block:: python

        from datetime import datetime, timedelta, UTC
        from helios_connectors import SwpcAdapter

        async with SwpcAdapter() as swpc:
            # Real-time Kp from the SWPC product
            async for rec in swpc.fetch_kp(
                start=datetime.now(UTC) - timedelta(days=1),
                end=datetime.now(UTC),
            ):
                print(rec.event_time, rec.value["kp"])

            # Gannon retrospective — automatically routes to GFZ archive
            async for rec in swpc.fetch_kp(
                start=datetime(2024, 5, 8, tzinfo=UTC),
                end=datetime(2024, 5, 14, tzinfo=UTC),
            ):
                print(rec.event_time, rec.value["kp"], rec.provenance.lineage)
    """

    source_id: ClassVar[SourceID] = SourceID.SWPC_KP

    def __init__(
        self,
        *,
        base_url: str = SWPC_BASE_URL,
        archive_client: httpx.AsyncClient | None = None,
        client: httpx.AsyncClient | None = None,
        rate_limit: RateLimitConfig | None = None,
        archive_rate_limit: RateLimitConfig | None = None,
        cache: FileCache | None | bool = True,
    ) -> None:
        """Construct a SWPC adapter.

        Args:
            base_url: services.swpc.noaa.gov by default.
            archive_client: optional httpx client for GFZ + Kyoto archives.
                Defaults to a separately-pooled client because those are
                different hosts with their own rate budget.
            client: optional pre-built httpx client for the SWPC host.
            rate_limit: optional SWPC-host rate limit (default 5 RPS).
            archive_rate_limit: optional archive-host rate limit
                (default 1 RPS — academic infrastructure).
            cache: ``True`` for default file cache, ``False`` to
                disable, or a :class:`FileCache` instance.
        """
        self._base_url = base_url
        if client is None:
            client = make_client(base_url=base_url)
        super().__init__(client=client, rate_limit=rate_limit, cache=cache)
        # Independent client + limiter for archive hosts so SWPC and archive
        # fetches never starve each other.
        self._archive_owns_client = archive_client is None
        self._archive_client = archive_client or make_client(base_url="")
        self._archive_ratelimiter = RateLimiter(
            archive_rate_limit or RateLimitConfig(rate_per_second=1.0, burst=2)
        )

    def _default_rate_limit(self) -> RateLimitConfig:
        # Per docs/design.md: NOAA SWPC at 5 RPS.
        return RateLimitConfig(rate_per_second=5.0)

    async def aclose(self) -> None:
        """Close both the SWPC and archive httpx clients."""
        await super().aclose()
        if self._archive_owns_client:
            await self._archive_client.aclose()

    # ------------------------------------------------------------------ #
    # public per-product conveniences
    # ------------------------------------------------------------------ #

    async def fetch_kp(self, *, start: datetime, end: datetime) -> AsyncIterator[NormalizedRecord]:
        """Stream 3-hourly planetary Kp records over ``[start, end]``.

        Routes to the GFZ Potsdam archive when ``start`` is older than
        the SWPC real-time window. Records carry the originating archive
        provider in their ``provenance.lineage``.
        """
        if _needs_archive(start):
            async for rec in self._fetch_kp_archive(start=start, end=end):
                yield rec
        else:
            async for rec in self._fetch_kp_realtime(start=start, end=end):
                yield rec

    async def fetch_dst(self, *, start: datetime, end: datetime) -> AsyncIterator[NormalizedRecord]:
        """Stream hourly Dst records over ``[start, end]``.

        SWPC does not publish a real-time Dst JSON product, so all Dst
        fetches resolve via Kyoto WDC. For older windows we prefer the
        final-quality tier and fall back to provisional; for recent
        windows we go straight to provisional.
        """
        async for rec in self._fetch_dst_kyoto(start=start, end=end):
            yield rec

    async def fetch_plasma(
        self, *, start: datetime, end: datetime
    ) -> AsyncIterator[NormalizedRecord]:
        """Stream 1-min DSCOVR-derived solar wind plasma records.

        Real-time only. For historical plasma data, use the DSCOVR
        adapter's CDAWeb fallback.
        """
        if _needs_archive(start):
            logger.warning(
                "SwpcAdapter.fetch_plasma: start=%s is older than the SWPC "
                "real-time window (%d days); plasma historical data is not "
                "served by SWPC — use DscovrAdapter for archive plasma.",
                _ensure_utc(start).isoformat(),
                SWPC_REALTIME_DAYS,
            )
        async for rec in self._fetch_columnar(
            endpoint=_ENDPOINTS["plasma"],
            start=start,
            end=end,
            value_units="mixed",  # density:1/cm^3 speed:km/s temperature:K
            time_field="time_tag",
        ):
            yield rec

    async def fetch_mag(self, *, start: datetime, end: datetime) -> AsyncIterator[NormalizedRecord]:
        """Stream 1-min DSCOVR-derived IMF records.

        Real-time only. Bx/By/Bz in GSM, Bt magnitude in nT.
        """
        if _needs_archive(start):
            logger.warning(
                "SwpcAdapter.fetch_mag: start=%s is older than the SWPC "
                "real-time window (%d days); IMF historical data is not "
                "served by SWPC — use DscovrAdapter for archive IMF.",
                _ensure_utc(start).isoformat(),
                SWPC_REALTIME_DAYS,
            )
        async for rec in self._fetch_columnar(
            endpoint=_ENDPOINTS["mag"],
            start=start,
            end=end,
            value_units="nT",
            time_field="time_tag",
        ):
            yield rec

    async def fetch_goes_protons(
        self, *, start: datetime, end: datetime
    ) -> AsyncIterator[NormalizedRecord]:
        """Stream GOES integral proton flux via SWPC's JSON service.

        NOTE: overlaps with :class:`GoesAdapter.fetch_proton_flux`.
        Prefer GoesAdapter for GOES-native field names; this method is
        included so SwpcAdapter is self-contained for the operational
        "everything from services.swpc.noaa.gov" workflow.
        """
        async for rec in self._fetch_listdict(
            endpoint=_ENDPOINTS["goes_protons"],
            start=start,
            end=end,
            value_units="pfu",
            time_field="time_tag",
        ):
            yield rec

    async def fetch_sep_forecast(
        self, *, issue_time: datetime | None = None
    ) -> AsyncIterator[NormalizedRecord]:
        """Yield the current 3-day SEP / G-storm / R-blackout forecast.

        SWPC issues this text product once daily. There is no historical
        replay endpoint, so ``issue_time`` is informational only — the
        returned record always corresponds to the currently-served
        snapshot. We surface ``issue_time`` for callers who want to
        record their query parameters.
        """
        await self._ratelimiter.acquire()
        response = await request_with_retry(
            self._client,
            "GET",
            _ENDPOINTS["sep_forecast"].path,
            safe_log_params=(),
        )
        text = response.text
        forecast = _parse_3_day_forecast(text)
        provenance = self._emit_provenance(
            model_id=_ENDPOINTS["sep_forecast"].model_id,
            dataset_refs=(self._base_url + _ENDPOINTS["sep_forecast"].path,),
            timestamp=forecast["issued"],
            value=forecast,
            value_units="percent",
            lineage=(f"swpc/{_ENDPOINTS['sep_forecast'].slug}",),
        )
        yield NormalizedRecord(
            source=SourceID.SWPC_SEP_FORECAST,
            record_type="sep_forecast",
            event_time=forecast["issued"],
            value=forecast,
            value_units="percent",
            provenance=provenance,
            raw={
                "text": text,
                "issue_time_requested": issue_time.isoformat() if issue_time else None,
            },
        )

    # ------------------------------------------------------------------ #
    # unified fetch
    # ------------------------------------------------------------------ #

    async def fetch(
        self,
        *,
        start: datetime,
        end: datetime,
        products: list[str] | None = None,
        **_kwargs: Any,
    ) -> AsyncIterator[NormalizedRecord]:
        """Stream records across one or more SWPC products.

        Args:
            start: inclusive query window start (UTC).
            end: inclusive query window end (UTC).
            products: optional list of product slugs from
                :data:`SWPC_PRODUCTS`. If omitted, all products are
                queried. Unknown slugs raise ``ValueError``.

        Yields:
            NormalizedRecord values. Order across products is not
            guaranteed; within a single product, order matches the
            upstream sequence.
        """
        selected = list(products) if products is not None else list(_ENDPOINTS)
        unknown = set(selected) - set(_ENDPOINTS)
        if unknown:
            raise ValueError(
                f"unknown SWPC products: {sorted(unknown)!r}; valid products: {sorted(_ENDPOINTS)}"
            )

        for slug in selected:
            if slug == "kp":
                async for rec in self.fetch_kp(start=start, end=end):
                    yield rec
            elif slug == "plasma":
                async for rec in self.fetch_plasma(start=start, end=end):
                    yield rec
            elif slug == "mag":
                async for rec in self.fetch_mag(start=start, end=end):
                    yield rec
            elif slug == "goes_protons":
                async for rec in self.fetch_goes_protons(start=start, end=end):
                    yield rec
            elif slug == "sep_forecast":
                async for rec in self.fetch_sep_forecast():
                    yield rec

    # ------------------------------------------------------------------ #
    # Kp: real-time + GFZ archive
    # ------------------------------------------------------------------ #

    async def _fetch_kp_realtime(
        self, *, start: datetime, end: datetime
    ) -> AsyncIterator[NormalizedRecord]:
        ep = _ENDPOINTS["kp"]
        await self._ratelimiter.acquire()
        response = await request_with_retry(self._client, "GET", ep.path, safe_log_params=())
        raw = response.json()
        if not isinstance(raw, list):
            raise httpx.DecodingError(f"SWPC {ep.slug}: expected list, got {type(raw).__name__}")
        start_utc = _ensure_utc(start)
        end_utc = _ensure_utc(end)
        for item in raw:
            if not isinstance(item, dict):
                continue
            ts_raw = item.get("time_tag")
            if not isinstance(ts_raw, str):
                continue
            try:
                ts = isoparse(ts_raw)
            except (ValueError, TypeError):
                continue
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=UTC)
            if ts < start_utc or ts > end_utc:
                continue
            kp_val = _coerce_float(item.get("Kp"))
            if kp_val is None:
                continue
            value: dict[str, Any] = {
                "kp": kp_val,
                "a_running": item.get("a_running"),
                "station_count": item.get("station_count"),
                "g_scale": _g_scale_from_kp(kp_val),
            }
            provenance = self._emit_provenance(
                model_id=ep.model_id,
                dataset_refs=(self._base_url + ep.path,),
                timestamp=ts,
                value=kp_val,
                value_units="none",
                lineage=(f"swpc/{ep.slug}",),
            )
            yield NormalizedRecord(
                source=SourceID.SWPC_KP,
                record_type="kp",
                event_time=ts,
                value=value,
                value_units="none",
                provenance=provenance,
                raw=item,
            )

    async def _fetch_kp_archive(
        self, *, start: datetime, end: datetime
    ) -> AsyncIterator[NormalizedRecord]:
        ep = _ENDPOINTS["kp"]
        await self._archive_ratelimiter.acquire()
        response = await request_with_retry(
            self._archive_client, "GET", GFZ_KP_ARCHIVE_URL, safe_log_params=()
        )
        text = response.text
        start_utc = _ensure_utc(start)
        end_utc = _ensure_utc(end)
        for ts, kp_val, ap_val in _parse_gfz_kp(text):
            if ts < start_utc or ts > end_utc:
                continue
            value = {
                "kp": kp_val,
                "ap": ap_val,
                "g_scale": _g_scale_from_kp(kp_val),
            }
            provenance = self._emit_provenance(
                model_id=ep.model_id,
                dataset_refs=(GFZ_KP_ARCHIVE_URL,),
                timestamp=ts,
                value=kp_val,
                value_units="none",
                lineage=(
                    f"swpc/{ep.slug}",
                    "GFZ Potsdam/Kp_ap_Ap_SN_F107_since_1932.txt",
                ),
            )
            yield NormalizedRecord(
                source=SourceID.SWPC_KP,
                record_type="kp",
                event_time=ts,
                value=value,
                value_units="none",
                provenance=provenance,
                raw={
                    "source": "GFZ Potsdam",
                    "ts": ts.isoformat(),
                    "kp": kp_val,
                    "ap": ap_val,
                },
            )

    # ------------------------------------------------------------------ #
    # Dst via Kyoto WDC
    # ------------------------------------------------------------------ #

    async def _fetch_dst_kyoto(
        self, *, start: datetime, end: datetime
    ) -> AsyncIterator[NormalizedRecord]:
        start_utc = _ensure_utc(start)
        end_utc = _ensure_utc(end)
        # Iterate by calendar month, fetching one file per month.
        months: list[tuple[int, int]] = []
        cur = datetime(start_utc.year, start_utc.month, 1, tzinfo=UTC)
        last = datetime(end_utc.year, end_utc.month, 1, tzinfo=UTC)
        while cur <= last:
            months.append((cur.year, cur.month))
            year = cur.year + (1 if cur.month == 12 else 0)
            month = 1 if cur.month == 12 else cur.month + 1
            cur = datetime(year, month, 1, tzinfo=UTC)

        prefer_final = _needs_archive(start)
        for year, month in months:
            yyyymm = f"{year:04d}{month:02d}"
            yymm = f"{year % 100:02d}{month:02d}"
            templates = (
                [KYOTO_DST_FINAL_URL_TEMPLATE, KYOTO_DST_PROVISIONAL_URL_TEMPLATE]
                if prefer_final
                else [KYOTO_DST_PROVISIONAL_URL_TEMPLATE]
            )
            text = ""
            chosen_url = ""
            tier = "provisional"
            for tmpl in templates:
                url = tmpl.format(yyyymm=yyyymm, yymm=yymm)
                await self._archive_ratelimiter.acquire()
                try:
                    response = await request_with_retry(
                        self._archive_client, "GET", url, safe_log_params=()
                    )
                except httpx.HTTPStatusError as exc:
                    if exc.response.status_code == 404:
                        logger.debug("Kyoto Dst not available at %s; trying next tier", url)
                        continue
                    raise
                text = response.text
                chosen_url = url
                tier = "final" if "dst_final" in tmpl else "provisional"
                break
            if not text:
                logger.warning("Kyoto Dst: no data available for %s; skipping month", yyyymm)
                continue
            for ts, dst_val in _parse_kyoto_dst(text, year=year, month=month):
                if ts < start_utc or ts > end_utc:
                    continue
                value = {"dst": dst_val, "quality_tier": tier}
                provenance = self._emit_provenance(
                    model_id="swpc/dst-1-hour",
                    dataset_refs=(chosen_url,),
                    timestamp=ts,
                    value=dst_val,
                    value_units="nT",
                    lineage=(
                        "swpc/dst",
                        f"Kyoto WDC/{tier}/dst{yymm}",
                    ),
                )
                yield NormalizedRecord(
                    # HELIOS treats Dst as part of the geomag-index suite; we
                    # tag it SWPC_KP because there is no dedicated SWPC_DST
                    # SourceID and Dst flows alongside Kp in the fusion layer.
                    source=SourceID.SWPC_KP,
                    record_type="dst",
                    event_time=ts,
                    value=value,
                    value_units="nT",
                    provenance=provenance,
                    raw={
                        "source": "Kyoto WDC",
                        "tier": tier,
                        "ts": ts.isoformat(),
                        "dst": dst_val,
                    },
                )

    # ------------------------------------------------------------------ #
    # Columnar (plasma/mag) and list-of-dict (proton) helpers
    # ------------------------------------------------------------------ #

    async def _fetch_columnar(
        self,
        *,
        endpoint: _Endpoint,
        start: datetime,
        end: datetime,
        value_units: str,
        time_field: str,
    ) -> AsyncIterator[NormalizedRecord]:
        """Fetch SWPC's header-as-first-row CSV-style JSON arrays.

        plasma-7-day.json and mag-7-day.json use this format::

            [[col_a, col_b, ...], [val_a, val_b, ...], ...]
        """
        await self._ratelimiter.acquire()
        response = await request_with_retry(self._client, "GET", endpoint.path, safe_log_params=())
        raw = response.json()
        if not isinstance(raw, list) or not raw:
            raise httpx.DecodingError(f"SWPC {endpoint.slug}: empty or non-list response")
        header = raw[0]
        if not isinstance(header, list) or time_field not in header:
            raise httpx.DecodingError(
                f"SWPC {endpoint.slug}: header missing {time_field!r}: {header}"
            )
        time_idx = header.index(time_field)
        start_utc = _ensure_utc(start)
        end_utc = _ensure_utc(end)

        for row in raw[1:]:
            if not isinstance(row, list) or len(row) != len(header):
                continue
            ts_raw = row[time_idx]
            if not isinstance(ts_raw, str):
                continue
            try:
                ts = isoparse(ts_raw)
            except (ValueError, TypeError):
                continue
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=UTC)
            if ts < start_utc or ts > end_utc:
                continue
            value: dict[str, Any] = {}
            for idx, col in enumerate(header):
                if idx == time_idx:
                    continue
                raw_val = row[idx]
                if isinstance(raw_val, str):
                    coerced = _coerce_float(raw_val)
                    value[col] = coerced if coerced is not None else raw_val
                else:
                    value[col] = raw_val
            provenance = self._emit_provenance(
                model_id=endpoint.model_id,
                dataset_refs=(self._base_url + endpoint.path,),
                timestamp=ts,
                value=value,
                value_units=value_units,
                lineage=(f"swpc/{endpoint.slug}",),
            )
            yield NormalizedRecord(
                source=endpoint.source_id,
                record_type=endpoint.slug,
                event_time=ts,
                value=value,
                value_units=value_units,
                provenance=provenance,
                raw={"header": header, "row": row},
            )

    async def _fetch_listdict(
        self,
        *,
        endpoint: _Endpoint,
        start: datetime,
        end: datetime,
        value_units: str,
        time_field: str,
    ) -> AsyncIterator[NormalizedRecord]:
        """Fetch SWPC's list-of-dict JSON arrays (goes/protons)."""
        await self._ratelimiter.acquire()
        response = await request_with_retry(self._client, "GET", endpoint.path, safe_log_params=())
        raw = response.json()
        if not isinstance(raw, list):
            raise httpx.DecodingError(
                f"SWPC {endpoint.slug}: expected list, got {type(raw).__name__}"
            )
        start_utc = _ensure_utc(start)
        end_utc = _ensure_utc(end)
        for item in raw:
            if not isinstance(item, dict):
                continue
            ts_raw = item.get(time_field)
            if not isinstance(ts_raw, str):
                continue
            try:
                ts = isoparse(ts_raw)
            except (ValueError, TypeError):
                continue
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=UTC)
            if ts < start_utc or ts > end_utc:
                continue
            provenance = self._emit_provenance(
                model_id=endpoint.model_id,
                dataset_refs=(self._base_url + endpoint.path,),
                timestamp=ts,
                value=item,
                value_units=value_units,
                lineage=(f"swpc/{endpoint.slug}",),
            )
            yield NormalizedRecord(
                source=endpoint.source_id,
                record_type=endpoint.slug,
                event_time=ts,
                value=dict(item),
                value_units=value_units,
                provenance=provenance,
                raw=dict(item),
            )


# ---------------------------------------------------------------------------- #
# Parsers
# ---------------------------------------------------------------------------- #


def _parse_gfz_kp(text: str) -> list[tuple[datetime, float, int]]:
    """Parse the GFZ daily-row Kp archive into 3-hourly samples.

    Each non-comment line is one day with eight Kp values and eight ap
    values. Layout (0-indexed token positions)::

        [0] year  [1] month  [2] day  [3] days  [4] days_m  [5] Bsr  [6] dB
        [7..14] Kp1..Kp8     [15..22] ap1..ap8  [23] Ap     [24] SN  ...

    We emit one ``(timestamp, kp, ap)`` per 3-hour bin.
    """
    out: list[tuple[datetime, float, int]] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if len(parts) < 23:
            continue
        try:
            year = int(parts[0])
            month = int(parts[1])
            day = int(parts[2])
        except (ValueError, IndexError):
            continue
        try:
            kp_vals = [float(parts[7 + i]) for i in range(8)]
            ap_vals = [int(float(parts[15 + i])) for i in range(8)]
        except (ValueError, IndexError):
            continue
        for bin_idx, (kp, ap) in enumerate(zip(kp_vals, ap_vals, strict=True)):
            if kp < 0:  # missing-data sentinel
                continue
            try:
                ts = datetime(year, month, day, bin_idx * 3, 0, 0, tzinfo=UTC)
            except ValueError:
                continue
            out.append((ts, kp, ap))
    return out


def _parse_kyoto_dst(text: str, *, year: int, month: int) -> list[tuple[datetime, int]]:
    """Parse a Kyoto WDC monthly Dst file into ``(timestamp, dst_nT)``.

    Each line has the WDC fixed-width format::

        DST<YYMM>*<DD>RRR<HHH>HHHH HHHH HHHH ... daily-mean

    24 hourly values, 4 columns each, starting at column 20. Sentinel
    values ``9999`` or ``-999`` mark missing data.
    """
    out: list[tuple[datetime, int]] = []
    for raw_line in text.splitlines():
        line = raw_line.rstrip()
        if not line.startswith("DST"):
            continue
        try:
            day = int(line[8:10])
        except ValueError:
            continue
        block = line[20 : 20 + 24 * 4]
        for hour in range(24):
            chunk = block[hour * 4 : (hour + 1) * 4].strip()
            if not chunk or chunk in {"9999", "-999"}:
                continue
            try:
                dst_val = int(chunk)
            except ValueError:
                continue
            try:
                ts = datetime(year, month, day, hour, 0, 0, tzinfo=UTC)
            except ValueError:
                continue
            out.append((ts, dst_val))
    return out


def _parse_3_day_forecast(text: str) -> dict[str, Any]:
    """Parse SWPC's text 3-day forecast into a structured snapshot.

    Extracted fields:

    - ``issued``: the issue timestamp (UTC) from the ``:Issued:`` header.
    - ``kp_breakdown``: list of dicts, one per 3-hour UT bin per day.
    - ``radiation_storm_probability``: list of ``{date, percent}`` for
      the S1-or-greater probability (the SEP signal HELIOS consumes).
    - ``radio_blackout_probability``: list of
      ``{date, r1_r2_percent, r3_or_greater_percent}``.
    """
    return {
        "issued": _parse_issued(text),
        "kp_breakdown": _parse_kp_breakdown(text),
        "radiation_storm_probability": _parse_radiation_probability(text),
        "radio_blackout_probability": _parse_radio_blackout(text),
    }


_ISSUED_RE = re.compile(r":Issued:\s*(\d{4})\s+(\w{3})\s+(\d{1,2})\s+(\d{4})\s*UTC")
_MONTHS = {
    "Jan": 1,
    "Feb": 2,
    "Mar": 3,
    "Apr": 4,
    "May": 5,
    "Jun": 6,
    "Jul": 7,
    "Aug": 8,
    "Sep": 9,
    "Oct": 10,
    "Nov": 11,
    "Dec": 12,
}


def _parse_issued(text: str) -> datetime:
    match = _ISSUED_RE.search(text)
    if not match:
        logger.warning("SWPC 3-day forecast: no :Issued: header found; using now()")
        return datetime.now(UTC)
    year = int(match.group(1))
    month = _MONTHS.get(match.group(2), 1)
    day = int(match.group(3))
    time4 = match.group(4)
    hour = int(time4[:2])
    minute = int(time4[2:])
    return datetime(year, month, day, hour, minute, 0, tzinfo=UTC)


def _parse_kp_breakdown(text: str) -> list[dict[str, Any]]:
    """Extract the NOAA Kp index breakdown table from the 3-day forecast."""
    rows: list[dict[str, Any]] = []
    lines = text.splitlines()
    in_table = False
    header_dates: list[str] = []
    for line in lines:
        if "NOAA Kp index breakdown" in line:
            in_table = True
            continue
        if in_table and not header_dates:
            stripped = line.strip()
            tokens = stripped.split()
            if len(tokens) >= 4 and tokens[0] in _MONTHS:
                header_dates = [
                    f"{tokens[i]} {tokens[i + 1]}" for i in range(0, len(tokens) - 1, 2)
                ]
                continue
        if in_table and header_dates:
            stripped = line.strip()
            if not stripped:
                if rows:
                    break
                continue
            m = re.match(r"^(\d{2}-\d{2}UT)\s+(.*)$", stripped)
            if not m:
                if rows:
                    break
                continue
            bin_label = m.group(1)
            values = re.findall(r"(\d+\.\d+)", m.group(2))
            row: dict[str, Any] = {"ut_bin": bin_label}
            for date_label, val in zip(header_dates, values, strict=False):
                row[date_label] = float(val)
            rows.append(row)
    return rows


def _parse_radiation_probability(text: str) -> list[dict[str, Any]]:
    """Extract the S1+ solar-radiation-storm probability row."""
    out: list[dict[str, Any]] = []
    lines = text.splitlines()
    in_section = False
    header_dates: list[str] = []
    for line in lines:
        if "Solar Radiation Storm Forecast" in line:
            in_section = True
            continue
        if in_section and not header_dates:
            stripped = line.strip()
            tokens = stripped.split()
            if len(tokens) >= 4 and tokens[0] in _MONTHS:
                header_dates = [
                    f"{tokens[i]} {tokens[i + 1]}" for i in range(0, len(tokens) - 1, 2)
                ]
                continue
        if in_section and header_dates:
            stripped = line.strip()
            if stripped.startswith("S1 or greater"):
                pcts = [int(p) for p in re.findall(r"(\d+)%", stripped)]
                for date_label, pct in zip(header_dates, pcts, strict=False):
                    out.append({"date": date_label, "percent": pct})
                break
    return out


def _parse_radio_blackout(text: str) -> list[dict[str, Any]]:
    """Extract the R1-R2 and R3+ radio-blackout probability rows."""
    out: list[dict[str, Any]] = []
    lines = text.splitlines()
    in_section = False
    header_dates: list[str] = []
    r12_pcts: list[int] = []
    r3_pcts: list[int] = []
    for line in lines:
        if "Radio Blackout Forecast" in line:
            in_section = True
            continue
        if in_section and not header_dates:
            stripped = line.strip()
            tokens = stripped.split()
            if len(tokens) >= 4 and tokens[0] in _MONTHS:
                header_dates = [
                    f"{tokens[i]} {tokens[i + 1]}" for i in range(0, len(tokens) - 1, 2)
                ]
                continue
        if in_section and header_dates:
            stripped = line.strip()
            if stripped.startswith("R1-R2"):
                r12_pcts = [int(p) for p in re.findall(r"(\d+)%", stripped)]
            elif stripped.startswith("R3 or greater"):
                r3_pcts = [int(p) for p in re.findall(r"(\d+)%", stripped)]
            if r12_pcts and r3_pcts:
                break
    for idx, date_label in enumerate(header_dates):
        out.append(
            {
                "date": date_label,
                "r1_r2_percent": r12_pcts[idx] if idx < len(r12_pcts) else None,
                "r3_or_greater_percent": r3_pcts[idx] if idx < len(r3_pcts) else None,
            }
        )
    return out


# ---------------------------------------------------------------------------- #
# Small utilities
# ---------------------------------------------------------------------------- #


def _needs_archive(start: datetime) -> bool:
    """True iff ``start`` is older than the SWPC real-time window."""
    now = datetime.now(UTC)
    return _ensure_utc(start) < now - timedelta(days=SWPC_REALTIME_DAYS)


def _ensure_utc(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts.replace(tzinfo=UTC)
    return ts.astimezone(UTC)


def _coerce_float(val: Any) -> float | None:
    """Parse SWPC's string-encoded floats; tolerate already-numeric values."""
    if val is None:
        return None
    if isinstance(val, float | int):
        return float(val)
    if isinstance(val, str):
        s = val.strip()
        if not s or s.lower() == "nan":
            return None
        try:
            return float(s)
        except ValueError:
            return None
    return None


def _g_scale_from_kp(kp: float) -> str:
    """NOAA G-scale letter (G0-G5) for a Kp value.

    Reference: https://www.swpc.noaa.gov/noaa-scales-explanation.
    """
    if kp < 5.0:
        return "G0"
    if kp < 6.0:
        return "G1"
    if kp < 7.0:
        return "G2"
    if kp < 8.0:
        return "G3"
    if kp < 9.0:
        return "G4"
    return "G5"
