"""NASA DONKI (CCMC) adapter.

DONKI — the *Space Weather Database Of Notifications, Knowledge, Information* —
is the operational chronicle of space-weather events maintained by CCMC and
the M2M SWAO at NASA GSFC. It exposes JSON endpoints for every major event
class (CME, flare, SEP, GST, IPS, MPC, RBE, HSS, notifications) and crucially
maintains *intelligent linkages* between events: a CME analysis points back at
the originating flare(s); a SEP event points at the upstream CMEs and flares
that produced it; a geomagnetic storm points at the IPS that triggered it.

These linkages are what makes DONKI worth the engineering: every other source
gives you isolated observations, but DONKI gives you the connectivity graph
that ML fusion needs to weight contemporaneous evidence correctly. The
:attr:`NormalizedRecord.provenance.lineage` field captures exactly these
linkages so downstream fusion code can use them as features.

API base: ``https://api.nasa.gov/DONKI``. Authentication is via the
``api_key`` query parameter. The shared NASA ``DEMO_KEY`` works but has
visible per-hour and per-day caps — every adapter logs a warning when no
``NASA_API_KEY`` env var is set.

Endpoint contract (per CCMC documentation, observed empirically):

- ``GET /DONKI/{kind}?startDate=YYYY-MM-DD&endDate=YYYY-MM-DD&api_key=...``
- Returns a JSON array; each element is an event object with ``activityID``
  (the canonical stable identifier), a kind-specific time field, and
  ``linkedEvents`` (a list of ``{activityID: ...}`` dicts giving cross-links).
- Date math is inclusive on both ends; the maximum window is roughly 30 days
  per call before the API quietly truncates. Callers should not request
  windows wider than ~30 days at a time.
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import AsyncIterator, Iterable
from datetime import UTC, datetime
from typing import Any, ClassVar

import httpx
from dateutil.parser import isoparse

from ..cache import FileCache
from ..http import make_client, request_with_retry
from ..ratelimit import RateLimitConfig
from ..schema import NormalizedRecord, SourceID
from .base import BaseAdapter

__all__ = ["DONKI_BASE_URL", "DONKI_EVENT_TYPES", "DonkiAdapter"]

logger = logging.getLogger(__name__)

#: Default base URL for DONKI: NASA's api.nasa.gov gateway. Requires an
#: ``api_key`` query parameter on every call.
DONKI_BASE_URL = "https://api.nasa.gov"

#: Alternative base URL: CCMC's kauai server. Does NOT require an api_key
#: but uses a slightly different path prefix (``/DONKI/WS/get/``). Useful
#: when api.nasa.gov is throttling or for unauthenticated demos.
DONKI_KAUAI_BASE_URL = "https://kauai.ccmc.gsfc.nasa.gov"

#: All event-type endpoint slugs the unified ``fetch()`` knows how to dispatch.
#: Order matters only for the union-mode call sequence (we use ``asyncio.gather``
#: but for deterministic iteration during tests we sort by this list).
DONKI_EVENT_TYPES: tuple[str, ...] = (
    "CME",
    "CMEAnalysis",
    "FLR",
    "SEP",
    "GST",
    "IPS",
    "MPC",
    "RBE",
    "HSS",
    "notifications",
)

# Each endpoint reports its event time under a different key. Mapping documented
# from CCMC DONKI spec and empirical responses.
_EVENT_TIME_FIELDS: dict[str, tuple[str, ...]] = {
    "CME": ("startTime", "activityID"),
    "CMEAnalysis": ("time21_5", "submissionTime"),
    "FLR": ("beginTime", "peakTime"),
    "SEP": ("eventTime",),
    "GST": ("startTime",),
    "IPS": ("eventTime",),
    "MPC": ("eventTime",),
    "RBE": ("eventTime",),
    "HSS": ("eventTime",),
    "notifications": ("messageIssueTime",),
}

# Each endpoint reports its stable identifier under a different key.
_ACTIVITY_ID_FIELDS: dict[str, str] = {
    "CME": "activityID",
    "CMEAnalysis": "associatedCMEID",
    "FLR": "flrID",
    "SEP": "sepID",
    "GST": "gstID",
    "IPS": "activityID",
    "MPC": "mpcID",
    "RBE": "rbeID",
    "HSS": "hssID",
    "notifications": "messageID",
}


class DonkiAdapter(BaseAdapter):
    """Adapter for NASA DONKI's JSON event endpoints.

    Usage:

    .. code-block:: python

        from datetime import datetime
        from helios_connectors.adapters import DonkiAdapter

        async with DonkiAdapter() as donki:
            async for rec in donki.fetch_cme(
                start=datetime(2024, 5, 8),
                end=datetime(2024, 5, 15),
            ):
                print(rec.event_time, rec.value["speed"])

    Or, for the unified call across all event types:

    .. code-block:: python

        async for rec in donki.fetch(start=..., end=..., types=["CME", "FLR"]):
            ...

    The adapter authenticates via the ``NASA_API_KEY`` environment
    variable. If unset, it falls back to NASA's shared ``DEMO_KEY`` and
    logs a warning at module import time the first time a request goes
    out — the demo key's per-hour cap (~30 req/hr) is too tight for any
    production workload but fine for notebooks.
    """

    source_id: ClassVar[SourceID] = SourceID.DONKI

    def __init__(
        self,
        *,
        api_key: str | None = None,
        base_url: str = DONKI_BASE_URL,
        client: httpx.AsyncClient | None = None,
        rate_limit: RateLimitConfig | None = None,
        cache: FileCache | bool | None = True,
    ) -> None:
        """Construct a DONKI adapter.

        Args:
            api_key: NASA API key. Falls back to ``NASA_API_KEY`` env
                var, then to ``DEMO_KEY``. The key is sent as a query
                param ``api_key`` per NASA API convention and is *never*
                logged. Ignored when ``base_url`` is the kauai endpoint.
            base_url: defaults to NASA's api.nasa.gov gateway. Set to
                :data:`DONKI_KAUAI_BASE_URL` to use CCMC's kauai server
                directly (no api_key needed; different path prefix).
            client: optional pre-built httpx client (overrides
                ``base_url``).
            rate_limit: optional rate-limit override. The default is
                10 RPS for a real key, dropped to 1 RPS when DEMO_KEY
                is in use.
            cache: ``True`` for default file cache, ``False`` to
                disable, or a :class:`FileCache` instance.
        """

        resolved_key = api_key or os.environ.get("NASA_API_KEY", "DEMO_KEY")
        self._api_key = resolved_key
        self._using_demo_key = resolved_key == "DEMO_KEY"
        self._base_url = base_url
        self._uses_kauai = base_url == DONKI_KAUAI_BASE_URL
        if self._using_demo_key and not self._uses_kauai:
            logger.warning(
                "DonkiAdapter: NASA_API_KEY env var not set; falling back to "
                "DEMO_KEY which has tight rate limits (~30 req/hr, 50 req/day). "
                "Register at https://api.nasa.gov/ for a free unlimited key."
            )
        # If the caller passed their own client we honor that; otherwise we
        # make one bound to the chosen base URL.
        if client is None:
            client = make_client(base_url=base_url)
        super().__init__(client=client, rate_limit=rate_limit, cache=cache)

    def _default_rate_limit(self) -> RateLimitConfig:
        # Demo key is ~30 req/hr → ~0.008 RPS; rounded to 1 RPS gives us a
        # safe burst of a few. Real key gets 10 RPS per CCMC operational
        # guidance.
        if self._using_demo_key:
            return RateLimitConfig(rate_per_second=1.0, burst=3)
        return RateLimitConfig(rate_per_second=10.0)

    # ------------------------------------------------------------------ #
    # core unified fetch
    # ------------------------------------------------------------------ #

    async def fetch(
        self,
        *,
        start: datetime,
        end: datetime,
        types: Iterable[str] | None = None,
        **_kwargs: Any,
    ) -> AsyncIterator[NormalizedRecord]:
        """Stream events from one or more DONKI endpoints.

        Args:
            start: inclusive start of the query window (UTC; naive
                datetimes are treated as UTC).
            end: inclusive end of the query window.
            types: optional iterable of endpoint slugs from
                :data:`DONKI_EVENT_TYPES`. If omitted, all endpoints are
                queried concurrently.

        Yields:
            NormalizedRecord values, one per upstream event, in
            unspecified order across endpoints but in upstream order
            within each endpoint.
        """

        selected = tuple(types) if types is not None else DONKI_EVENT_TYPES
        unknown = set(selected) - set(DONKI_EVENT_TYPES)
        if unknown:
            raise ValueError(
                f"unknown DONKI event types: {sorted(unknown)!r}; valid types: {DONKI_EVENT_TYPES}"
            )

        # Fire all endpoint calls concurrently. We collect per-endpoint lists
        # and then yield merged so consumers get a unified stream while we
        # still get pipeline parallelism for the underlying HTTP calls.
        per_endpoint = await asyncio.gather(
            *(self._fetch_endpoint(kind, start=start, end=end) for kind in selected)
        )
        for kind, records in zip(selected, per_endpoint, strict=True):
            logger.info("DONKI %s: %d records [%s..%s]", kind, len(records), start, end)
            for record in records:
                yield record

    # ------------------------------------------------------------------ #
    # per-endpoint conveniences
    # ------------------------------------------------------------------ #

    async def fetch_cme(self, *, start: datetime, end: datetime) -> AsyncIterator[NormalizedRecord]:
        """Stream coronal-mass-ejection events."""
        async for rec in self._stream("CME", start=start, end=end):
            yield rec

    async def fetch_cme_analysis(
        self, *, start: datetime, end: datetime
    ) -> AsyncIterator[NormalizedRecord]:
        """Stream CCMC CME analysis records (CMEAnalysis endpoint)."""
        async for rec in self._stream("CMEAnalysis", start=start, end=end):
            yield rec

    async def fetch_flr(self, *, start: datetime, end: datetime) -> AsyncIterator[NormalizedRecord]:
        """Stream solar flare events."""
        async for rec in self._stream("FLR", start=start, end=end):
            yield rec

    async def fetch_sep(self, *, start: datetime, end: datetime) -> AsyncIterator[NormalizedRecord]:
        """Stream solar energetic particle events."""
        async for rec in self._stream("SEP", start=start, end=end):
            yield rec

    async def fetch_gst(self, *, start: datetime, end: datetime) -> AsyncIterator[NormalizedRecord]:
        """Stream geomagnetic storm events."""
        async for rec in self._stream("GST", start=start, end=end):
            yield rec

    async def fetch_ips(self, *, start: datetime, end: datetime) -> AsyncIterator[NormalizedRecord]:
        """Stream interplanetary shock events."""
        async for rec in self._stream("IPS", start=start, end=end):
            yield rec

    async def fetch_mpc(self, *, start: datetime, end: datetime) -> AsyncIterator[NormalizedRecord]:
        """Stream magnetopause-crossing events."""
        async for rec in self._stream("MPC", start=start, end=end):
            yield rec

    async def fetch_rbe(self, *, start: datetime, end: datetime) -> AsyncIterator[NormalizedRecord]:
        """Stream radiation-belt enhancement events."""
        async for rec in self._stream("RBE", start=start, end=end):
            yield rec

    async def fetch_hss(self, *, start: datetime, end: datetime) -> AsyncIterator[NormalizedRecord]:
        """Stream high-speed solar-wind stream events."""
        async for rec in self._stream("HSS", start=start, end=end):
            yield rec

    async def fetch_notifications(
        self, *, start: datetime, end: datetime
    ) -> AsyncIterator[NormalizedRecord]:
        """Stream DONKI's own notification messages."""
        async for rec in self._stream("notifications", start=start, end=end):
            yield rec

    # ------------------------------------------------------------------ #
    # internals
    # ------------------------------------------------------------------ #

    async def _stream(
        self, kind: str, *, start: datetime, end: datetime
    ) -> AsyncIterator[NormalizedRecord]:
        for record in await self._fetch_endpoint(kind, start=start, end=end):
            yield record

    async def _fetch_endpoint(
        self, kind: str, *, start: datetime, end: datetime
    ) -> list[NormalizedRecord]:
        await self._ratelimiter.acquire()
        params: dict[str, Any] = {
            "startDate": _fmt_date(start),
            "endDate": _fmt_date(end),
        }
        if self._uses_kauai:
            url = f"/DONKI/WS/get/{kind}"
        else:
            url = f"/DONKI/{kind}"
            params["api_key"] = self._api_key
        response = await request_with_retry(
            self._client,
            "GET",
            url,
            params=params,
            safe_log_params=("startDate", "endDate"),
        )
        raw: Any = response.json()
        # All DONKI endpoints return a JSON array even when empty. Defensively
        # accept a singleton dict (the notifications endpoint historically did
        # this in some error paths).
        if isinstance(raw, dict):
            raw_list: list[dict[str, Any]] = [raw]
        elif isinstance(raw, list):
            raw_list = raw
        else:
            raise httpx.DecodingError(
                f"DONKI/{kind}: unexpected response type {type(raw).__name__}"
            )
        return [self._normalize(kind, item) for item in raw_list]

    def _normalize(self, kind: str, raw: dict[str, Any]) -> NormalizedRecord:
        activity_id = _coerce_activity_id(kind, raw)
        event_time = _coerce_event_time(kind, raw)
        lineage = _coerce_lineage(kind, raw)
        scalar = _coerce_scalar(kind, raw, activity_id)
        value: dict[str, Any] = {"record_type": kind, **raw}
        extra: dict[str, Any] = {"payload": dict(raw)}
        if lineage:
            extra["lineage"] = list(lineage)
        provenance = self._emit_provenance(
            model_id=f"donki/{kind}",
            dataset_refs=(activity_id,) if activity_id else (),
            timestamp=event_time,
            value=scalar,
            value_units=_value_units_for(kind),
            extra=extra,
            record_id=activity_id or None,
        )
        return NormalizedRecord(
            source=SourceID.DONKI,
            record_type=kind,
            event_time=event_time,
            value=value,
            value_units="none",
            provenance=provenance,
            raw=raw,
        )


# ---------------------------------------------------------------------------- #
# Module-level helpers
# ---------------------------------------------------------------------------- #


def _fmt_date(ts: datetime) -> str:
    """Format a datetime as YYYY-MM-DD for DONKI's date params."""
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=UTC)
    return ts.astimezone(UTC).strftime("%Y-%m-%d")


def _coerce_activity_id(kind: str, raw: dict[str, Any]) -> str:
    """Pick the canonical identifier from a DONKI event response.

    Different endpoints use different ID field names; we look up the
    expected field from :data:`_ACTIVITY_ID_FIELDS` and fall back to
    ``activityID`` then a synthetic ``{kind}-{eventTime}`` string so the
    return value is never empty.
    """

    field = _ACTIVITY_ID_FIELDS.get(kind, "activityID")
    value = raw.get(field) or raw.get("activityID")
    if isinstance(value, str) and value:
        return value
    # Synthesize a fallback so downstream code never sees an empty ID.
    event_time = _coerce_event_time(kind, raw)
    return f"{kind}-{event_time.isoformat()}"


def _coerce_event_time(kind: str, raw: dict[str, Any]) -> datetime:
    """Pick the most appropriate timestamp from a DONKI event response.

    Each endpoint exposes its own timestamp field; we walk the
    configured priority list, returning the first parseable value. If
    nothing parses we fall back to UTC now — better than crashing on
    a record we can otherwise use, and the caller can detect this via
    the ``raw`` field if needed.
    """

    candidates = _EVENT_TIME_FIELDS.get(kind, ("eventTime", "startTime"))
    for field in candidates:
        value = raw.get(field)
        if isinstance(value, str) and value:
            try:
                ts = isoparse(value)
            except (ValueError, TypeError):
                continue
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=UTC)
            return ts.astimezone(UTC)
    logger.warning("DONKI/%s: no parseable timestamp in record; using now()", kind)
    return datetime.now(UTC)


# Per-kind scalar pick: what's the headline value for this event class?
_SCALAR_FIELDS: dict[str, tuple[str, ...]] = {
    "CME": ("sourceLocation",),  # heliographic coords as string like "N12W34"
    "CMEAnalysis": ("speed",),  # km/s
    "FLR": ("classType",),  # "X1.2", "M5.6"
    "SEP": ("instruments",),  # contributes via string fallback
    "GST": ("kpIndex",),  # numeric Kp if reported
    "IPS": ("location",),
    "MPC": ("eventTime",),
    "RBE": ("eventTime",),
    "HSS": ("eventTime",),
    "notifications": ("messageType",),
}

# Per-kind units for the scalar value.
_SCALAR_UNITS: dict[str, str] = {
    "CMEAnalysis": "km/s",
    "GST": "none",  # Kp is dimensionless
    "FLR": "GOES_class",
}


def _value_units_for(kind: str) -> str:
    """Return the units string for a DONKI scalar value."""
    return _SCALAR_UNITS.get(kind, "none")


def _coerce_scalar(kind: str, raw: dict[str, Any], activity_id: str) -> float | int | str | bool:
    """Pick a single scalar value for the provenance record.

    DONKI events have rich JSON payloads — the placeholder schema kept
    the whole dict. The HELIOS provenance spec requires a scalar; we
    pick the most informative single field per event class (e.g. FLR
    ``classType`` like ``"X1.2"``, CMEAnalysis ``speed`` km/s, GST
    ``kpIndex``) and fall back to the activity identifier so the value
    is always present and non-empty. The full payload lives in
    ``extra["payload"]``.
    """

    fields = _SCALAR_FIELDS.get(kind, ())
    for f in fields:
        val = raw.get(f)
        if isinstance(val, (int, float, bool)) and not isinstance(val, bool):
            return val
        if isinstance(val, bool):
            return val
        if isinstance(val, str) and val:
            return val
    return activity_id


def _coerce_lineage(kind: str, raw: dict[str, Any]) -> tuple[str, ...]:
    """Extract DONKI's intelligent linkages into a lineage tuple.

    The primary source is the ``linkedEvents`` field, when present, as
    a list of ``{"activityID": "..."}`` dicts. For event classes where
    DONKI doesn't populate ``linkedEvents`` but does record a direct
    parent relationship in another field, we substitute the parent
    identifier: notably, ``CMEAnalysis`` records carry only
    ``associatedCMEID``, and a CMEAnalysis is meaningless without the
    CME that anchors it.

    Linkages preserve DONKI's order, which empirically goes
    upstream-cause first (flare → CME → SEP/IPS → GST → MPC → RBE).
    """

    out: list[str] = []
    linked = raw.get("linkedEvents")
    if isinstance(linked, list):
        for entry in linked:
            if isinstance(entry, dict):
                activity = entry.get("activityID")
                if isinstance(activity, str) and activity:
                    out.append(activity)
            elif isinstance(entry, str) and entry:
                out.append(entry)

    # CMEAnalysis records do not include linkedEvents but always have a
    # parent-CME pointer; we use it so the analysis isn't an orphan in
    # the provenance graph.
    if kind == "CMEAnalysis":
        parent = raw.get("associatedCMEID")
        if isinstance(parent, str) and parent and parent not in out:
            out.insert(0, parent)

    return tuple(out)
