"""CCMC SEP Scoreboards (A onset probability / B peak flux / C event time profiles).

CCMC's *ISEP* project hosts three SEP scoreboards as part of NASA's R2O2R
framework. The scoreboards aggregate per-model JSON forecast submissions
into consensus views over time, energy channel, and threshold:

* **Scoreboard A — onset probability**: probability of exceeding a given
  flux threshold within a forecast window (24h typical). Sourced from
  per-forecast ``probabilities`` arrays.
* **Scoreboard B — peak flux prediction**: most-likely peak flux value
  plus uncertainty for each energy channel. Sourced from per-forecast
  ``peak_intensity`` objects.
* **Scoreboard C — event time profiles**: time series of expected flux
  through an event, with onset/end timestamps and threshold crossings.
  Sourced from per-forecast ``event_lengths``, ``threshold_crossings``,
  and optional ``sep_profile`` (an external text file).

The three scoreboards are **views into the same per-model JSON envelope**.
Each contributing model emits a `sep_forecast_submission` file (per
``sep_json_writer.py`` schema) carrying probabilities, peak_intensity, and
event-profile data for one or more energy channels. The HELIOS adapter
walks the ISWA data tree, fetches per-model JSON, and projects each
``forecast`` into A-, B-, and C-shaped :class:`NormalizedRecord` rows
according to which fields are populated.

Data layout
-----------

Canonical machine-accessible mirror is the ISWA data tree (NOT the
interactive web apps at ``sep.ccmc.gsfc.nasa.gov`` which are SPAs):

.. code-block:: text

    https://iswa.ccmc.gsfc.nasa.gov/iswa_data_tree/model/heliosphere/sep_scoreboard/
        <MODEL>/[<variant>/]<energy>/<YYYY>/<MM>/<filename>.json

Examples discovered live during adapter development (2026-05):

* ``UMASEP/v3_X/10MeV/2024/05/UMASEP10_prediction_2024_05_01_000516__2024_05_01_000920.json``
* ``SEPSTER/Parker/2024/05/sepster_20240501_0636_0794_Parker_Spiral_iss_20240501_1547.json``
* ``SEPSTER/Parker/2017/09/sepster_20170906_1000_0260.json`` (September 2017 event)

Per-model "variants" subdirectory layer (``v3_X``, ``Parker``, ``WSA-ENLIL``,
etc.) varies by model. The adapter exposes a configurable model registry
so callers can extend coverage without touching internals.

HESPERIA REleASE exclusion
--------------------------

Per the HELIOS proposal §3 T1 (ref [30]), the **HESPERIA REleASE** model
requires a separate licensing agreement for commercial use. The HELIOS
adapter must consume the *aggregated scoreboard consensus* without ever
issuing requests against REleASE-specific paths. To enforce this:

1. The default :data:`SCOREBOARD_MODELS` registry deliberately excludes
   ``RELEASE`` and ``RELEASE_PLUS``, ``STEREO_RELEASE``,
   ``STEREO_RELEASE_PLUS`` from the model list.
2. Every constructed request URL is guarded by
   :func:`_assert_no_hesperia_release` which raises ``ValueError`` if a
   path component contains ``release`` or ``hesperia`` (case-insensitive).
3. A regression test (``tests/test_sep_scoreboards.py``) asserts that no
   URL the adapter would request for a standard fetch contains
   ``release`` or ``hesperia``.

The HELIOS adapter still consumes the *consensus* aggregated scoreboard
output even when other models in the consensus happen to be informed by
REleASE-class processing — we never call a REleASE-specific endpoint.

Rate limiting
-------------

CCMC publishes no formal rate-limit policy for ISWA. The adapter defaults
to a conservative **3 RPS** with exponential backoff on 429, matching the
shared ``helios_connectors.http`` retry policy.
"""

from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import AsyncIterator, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, ClassVar, Literal

import httpx
from dateutil.parser import isoparse

from ..cache import FileCache
from ..http import make_client, request_with_retry
from ..ratelimit import RateLimitConfig
from ..schema import NormalizedRecord, SourceID
from .base import BaseAdapter

__all__ = [
    "FORBIDDEN_PATH_TOKENS",
    "ISWA_BASE_URL",
    "SCOREBOARD_MODELS",
    "Scoreboard",
    "ScoreboardModelSpec",
    "SepScoreboardsAdapter",
]

logger = logging.getLogger(__name__)

#: Canonical base URL for the ISWA data tree. CCMC publishes the
#: scoreboard JSON files here.
ISWA_BASE_URL = "https://iswa.ccmc.gsfc.nasa.gov"

#: ISWA data-tree path prefix for SEP scoreboard model submissions.
ISWA_SCOREBOARD_PREFIX = "/iswa_data_tree/model/heliosphere/sep_scoreboard"

#: Path-segment tokens that MUST NEVER appear in any URL the adapter
#: requests. Per the HELIOS proposal §3 T1, HESPERIA REleASE requires a
#: separate commercial-use licence; the consensus scoreboard is fine,
#: but a REleASE-specific endpoint is not.
FORBIDDEN_PATH_TOKENS: frozenset[str] = frozenset({"release", "hesperia"})

#: Type alias for the three scoreboards. Strings rather than an enum so
#: the unified ``fetch`` kwarg accepts the natural ``"A"``/``"B"``/``"C"``
#: form callers will reach for first.
Scoreboard = Literal["A", "B", "C"]

ALL_SCOREBOARDS: tuple[Scoreboard, ...] = ("A", "B", "C")


@dataclass(frozen=True, slots=True)
class ScoreboardModelSpec:
    """Configuration for one contributing scoreboard model.

    Each model lives under ``ISWA_SCOREBOARD_PREFIX/<name>/<variants>...``
    on the ISWA data tree. The variants chain captures the
    model-specific subdirectory hierarchy (version, instrument,
    coordinate system) between the model name and the energy directory.

    Attributes:
        name: top-level model directory under the scoreboard prefix
            (e.g. ``"UMASEP"``, ``"SEPSTER"``).
        variants: subdirectory chain between the model directory and
            the energy directory. ``UMASEP`` uses one level
            (``("v3_X",)``); ``SEPSTER`` uses one (``("Parker",)``);
            ``MagPy`` uses two (``("3.X", "LOS")``).
        energies: tuple of energy-channel directory names to walk.
            ``"10MeV"``, ``"100MeV"``, etc.
        model_id: HELIOS-side identifier for provenance lineage.
            Defaults to ``f"ccmc/sep_scoreboard/{name}"`` if empty.
    """

    name: str
    variants: tuple[str, ...] = ()
    energies: tuple[str, ...] = ("10MeV",)
    model_id: str = ""

    def resolved_model_id(self) -> str:
        return self.model_id or f"ccmc/sep_scoreboard/{self.name}"

    def energy_dirs(self, energy_filter: Sequence[str] | None = None) -> tuple[str, ...]:
        if energy_filter is None:
            return self.energies
        wanted = set(energy_filter)
        return tuple(e for e in self.energies if e in wanted)

    def base_path(self) -> str:
        parts = (self.name, *self.variants)
        return "/".join(parts)


# Default registry of contributing models. Discovered live from the ISWA
# data tree (2026-05). Deliberately excludes RELEASE and all REleASE-
# variant directories per the licensing constraint described in the
# module docstring.
SCOREBOARD_MODELS: tuple[ScoreboardModelSpec, ...] = (
    ScoreboardModelSpec(
        name="UMASEP",
        variants=("v3_X",),
        energies=("10MeV", "30MeV", "50MeV", "100MeV", "500MeV"),
    ),
    ScoreboardModelSpec(
        name="SEPSTER",
        variants=("Parker",),
        energies=("10MeV",),
    ),
    ScoreboardModelSpec(
        name="SEPSTER2D",
        variants=("1.X",),
        energies=("10MeV",),
    ),
    ScoreboardModelSpec(
        name="SAWS_ASPECS",
        variants=("1.X",),
        energies=("10MeV",),
    ),
    ScoreboardModelSpec(
        name="SEPMOD",
        variants=(),
        energies=("10MeV",),
    ),
    ScoreboardModelSpec(
        name="MagPy",
        variants=("3.X", "VEC"),
        energies=("10MeV",),
    ),
    ScoreboardModelSpec(
        name="SPRINTS-SEP",
        variants=(),
        energies=("10MeV",),
    ),
    ScoreboardModelSpec(
        name="iPATH",
        variants=(),
        energies=("10MeV",),
    ),
)


# ---------------------------------------------------------------------------- #
# Apache directory-listing parser
# ---------------------------------------------------------------------------- #


# ISWA serves Apache mod_autoindex directory listings as HTML. Lines we care
# about look like:
#   <a href="UMASEP10_prediction_2024_05_01_000516__2024_05_01_000920.json">
# We use a tight regex rather than pulling in BeautifulSoup; the listings
# are mechanical and we only want href values that end in .json or '/'.
_HREF_RE = re.compile(r'href="([^"?/][^"]*?)"', re.IGNORECASE)


def _parse_listing(html: str) -> tuple[list[str], list[str]]:
    """Extract subdirectory and file hrefs from an Apache index page.

    Returns a tuple ``(subdirs, files)`` where ``subdirs`` are hrefs
    ending in ``/`` and ``files`` are everything else. Both lists are
    de-duplicated and drop the ``Parent Directory`` link plus the
    sort-control links (``?C=...``).
    """

    subdirs: list[str] = []
    files: list[str] = []
    seen: set[str] = set()
    for match in _HREF_RE.finditer(html):
        href = match.group(1)
        if href in seen or href.startswith("?") or href.startswith("/"):
            continue
        seen.add(href)
        if href.endswith("/"):
            subdirs.append(href.rstrip("/"))
        else:
            files.append(href)
    return subdirs, files


# ---------------------------------------------------------------------------- #
# Time helpers
# ---------------------------------------------------------------------------- #


def _ensure_utc(ts: datetime) -> datetime:
    if ts.tzinfo is None:
        return ts.replace(tzinfo=UTC)
    return ts.astimezone(UTC)


def _months_in_window(start: datetime, end: datetime) -> list[tuple[int, str]]:
    """Yield ``(year, month_zero_padded)`` for every month touched by [start, end]."""

    s = _ensure_utc(start).replace(day=1)
    e = _ensure_utc(end)
    cursor = s
    out: list[tuple[int, str]] = []
    while cursor <= e:
        out.append((cursor.year, f"{cursor.month:02d}"))
        # advance one month
        if cursor.month == 12:
            cursor = cursor.replace(year=cursor.year + 1, month=1)
        else:
            cursor = cursor.replace(month=cursor.month + 1)
    return out


def _coerce_issue_time(envelope: dict[str, Any]) -> datetime:
    """Pick the best timestamp from a ``sep_forecast_submission``.

    Falls back through ``issue_time``, the first forecast's
    ``prediction_window.start_time``, and finally now() so the
    record always carries a tz-aware UTC datetime.
    """

    raw = envelope.get("issue_time")
    if isinstance(raw, str) and raw:
        try:
            return _ensure_utc(isoparse(raw))
        except (ValueError, TypeError):
            pass
    forecasts = envelope.get("forecasts")
    if isinstance(forecasts, list) and forecasts:
        first = forecasts[0]
        if isinstance(first, dict):
            window = first.get("prediction_window")
            if isinstance(window, dict):
                raw = window.get("start_time")
                if isinstance(raw, str) and raw:
                    try:
                        return _ensure_utc(isoparse(raw))
                    except (ValueError, TypeError):
                        pass
    logger.warning("sep_scoreboards: no parseable issue_time; falling back to now()")
    return datetime.now(UTC)


def _assert_no_hesperia_release(url: str) -> None:
    """Guarantee a URL contains no REleASE-related path tokens.

    Raises:
        ValueError: if any path segment (case-insensitive) contains
            a token from :data:`FORBIDDEN_PATH_TOKENS`.
    """

    lowered = url.lower()
    for token in FORBIDDEN_PATH_TOKENS:
        if token in lowered:
            raise ValueError(
                f"refusing to request {url!r}: contains forbidden path token "
                f"{token!r} (HESPERIA REleASE licensing constraint)."
            )


# ---------------------------------------------------------------------------- #
# adapter
# ---------------------------------------------------------------------------- #


@dataclass(slots=True)
class _Forecast:
    """Parsed single forecast row out of a ``sep_forecast_submission``."""

    envelope: dict[str, Any]
    forecast: dict[str, Any]
    energy_min: float | None
    energy_max: float | None
    energy_units: str
    species: str
    location: str
    prediction_start: datetime | None
    prediction_end: datetime | None
    extra: dict[str, Any] = field(default_factory=dict)


class SepScoreboardsAdapter(BaseAdapter):
    """Adapter for CCMC's three SEP Scoreboards via the ISWA data tree.

    BUILD-strategy adapter: no Python client exists upstream, so the
    adapter walks Apache directory listings under
    :data:`ISWA_BASE_URL` and fetches per-model JSON files directly.

    Usage:

    .. code-block:: python

        from datetime import datetime
        from helios_connectors import SepScoreboardsAdapter

        async with SepScoreboardsAdapter() as sb:
            async for rec in sb.fetch_scoreboard_a(
                start=datetime(2024, 5, 8),
                end=datetime(2024, 5, 12),
            ):
                print(rec.event_time, rec.value)

    The ``source_id`` class attribute is :attr:`SourceID.SEP_SCOREBOARD_A`
    by default, but each yielded record carries its own per-scoreboard
    ``source`` (one of ``SEP_SCOREBOARD_A``/``B``/``C``) so downstream
    fusion code can dispatch correctly when multiple scoreboards are
    fetched in one call.
    """

    source_id: ClassVar[SourceID] = SourceID.SEP_SCOREBOARD_A

    def __init__(
        self,
        *,
        base_url: str = ISWA_BASE_URL,
        client: httpx.AsyncClient | None = None,
        rate_limit: RateLimitConfig | None = None,
        cache: FileCache | None | bool = True,
        models: Iterable[ScoreboardModelSpec] | None = None,
    ) -> None:
        """Construct a SEP Scoreboards adapter.

        Args:
            base_url: ISWA host. Override only for a mirror.
            client: optional pre-built httpx client (overrides ``base_url``).
            rate_limit: optional rate-limit config. Default 3 RPS.
            cache: ``True`` for default file cache, ``False`` to disable,
                or a :class:`FileCache` instance.
            models: optional model registry override. Defaults to
                :data:`SCOREBOARD_MODELS`. Models in the override are
                also subject to the HESPERIA REleASE guard at request
                time; specs that would build a forbidden URL will raise.
        """

        self._base_url = base_url
        self._models: tuple[ScoreboardModelSpec, ...] = (
            tuple(models) if models is not None else SCOREBOARD_MODELS
        )
        # Eager validation: any spec referencing a forbidden path token
        # rejects construction immediately so the operator finds out at
        # adapter init, not on a request mid-fetch.
        for spec in self._models:
            _assert_no_hesperia_release(spec.base_path())
        if client is None:
            client = make_client(
                base_url=base_url,
                extra_headers={"Accept": "*/*"},
            )
        super().__init__(client=client, rate_limit=rate_limit, cache=cache)

    def _default_rate_limit(self) -> RateLimitConfig:
        # CCMC publishes no formal rate limit; 3 RPS is the conservative
        # default for adapters that hit their hosts heavily.
        return RateLimitConfig(rate_per_second=3.0, burst=3)

    # ------------------------------------------------------------------ #
    # public API
    # ------------------------------------------------------------------ #

    async def fetch(
        self,
        *,
        start: datetime,
        end: datetime,
        scoreboards: Iterable[Scoreboard] = ALL_SCOREBOARDS,
        **_kwargs: Any,
    ) -> AsyncIterator[NormalizedRecord]:
        """Stream scoreboard records across one or more scoreboards.

        Args:
            start: inclusive query window start (UTC; naive treated as UTC).
            end: inclusive query window end.
            scoreboards: iterable of scoreboard letters ``"A"``, ``"B"``,
                ``"C"``. Defaults to all three.

        Yields:
            NormalizedRecord values with ``source`` set to the per-board
            ``SourceID.SEP_SCOREBOARD_A``/``B``/``C``.
        """

        selected = tuple(scoreboards)
        unknown = set(selected) - set(ALL_SCOREBOARDS)
        if unknown:
            raise ValueError(f"unknown scoreboards: {sorted(unknown)!r}; valid: {ALL_SCOREBOARDS}")

        envelopes = await self._collect_envelopes(start=start, end=end)
        for source_url, env in envelopes:
            issue_time = _coerce_issue_time(env)
            if issue_time < _ensure_utc(start) or issue_time > _ensure_utc(end):
                continue
            forecasts = _expand_forecasts(env)
            for fc in forecasts:
                for board in selected:
                    record = self._maybe_normalize(board, fc, source_url, issue_time)
                    if record is not None:
                        yield record

    async def fetch_scoreboard_a(
        self, *, start: datetime, end: datetime
    ) -> AsyncIterator[NormalizedRecord]:
        """Stream Scoreboard A (onset probability) records."""
        async for rec in self.fetch(start=start, end=end, scoreboards=("A",)):
            yield rec

    async def fetch_scoreboard_b(
        self, *, start: datetime, end: datetime
    ) -> AsyncIterator[NormalizedRecord]:
        """Stream Scoreboard B (peak-flux prediction) records."""
        async for rec in self.fetch(start=start, end=end, scoreboards=("B",)):
            yield rec

    async def fetch_scoreboard_c(
        self, *, start: datetime, end: datetime
    ) -> AsyncIterator[NormalizedRecord]:
        """Stream Scoreboard C (event time profile) records."""
        async for rec in self.fetch(start=start, end=end, scoreboards=("C",)):
            yield rec

    # ------------------------------------------------------------------ #
    # discovery + retrieval
    # ------------------------------------------------------------------ #

    async def _collect_envelopes(
        self, *, start: datetime, end: datetime
    ) -> list[tuple[str, dict[str, Any]]]:
        """Walk the ISWA tree, gather per-file envelopes for [start, end]."""

        months = _months_in_window(start, end)
        # Build the cross-product of (model, energy, year, month) listing URLs.
        listing_paths: list[tuple[ScoreboardModelSpec, str, str]] = []
        for spec in self._models:
            for energy in spec.energies:
                for year, month in months:
                    path = f"{ISWA_SCOREBOARD_PREFIX}/{spec.base_path()}/{energy}/{year}/{month}/"
                    listing_paths.append((spec, energy, path))

        # Fetch all listings concurrently; tolerate 404s for model/energy/month
        # combinations that don't exist (very common for older years).
        listing_results = await asyncio.gather(
            *(self._fetch_listing(path) for _, _, path in listing_paths),
            return_exceptions=True,
        )

        file_urls: list[tuple[ScoreboardModelSpec, str]] = []
        for (spec, _energy, path), listing_result in zip(
            listing_paths, listing_results, strict=True
        ):
            if isinstance(listing_result, BaseException):
                logger.debug("sep_scoreboards: listing %s skipped (%s)", path, listing_result)
                continue
            _subdirs, files = listing_result
            for fname in files:
                if not fname.lower().endswith(".json"):
                    continue
                file_urls.append((spec, f"{path}{fname}"))

        # Fetch all the JSON files concurrently.
        envelope_tasks = [self._fetch_envelope(url) for _spec, url in file_urls]
        envelope_results = await asyncio.gather(*envelope_tasks, return_exceptions=True)

        out: list[tuple[str, dict[str, Any]]] = []
        for (_spec, url), env_result in zip(file_urls, envelope_results, strict=True):
            if isinstance(env_result, BaseException):
                logger.debug("sep_scoreboards: envelope %s skipped (%s)", url, env_result)
                continue
            if env_result is None:
                continue
            out.append((url, env_result))
        return out

    async def _fetch_listing(self, path: str) -> tuple[list[str], list[str]]:
        """Fetch one Apache directory listing as ``(subdirs, files)``."""
        _assert_no_hesperia_release(path)
        await self._ratelimiter.acquire()
        try:
            response = await request_with_retry(self._client, "GET", path, safe_log_params=())
        except httpx.HTTPStatusError as exc:
            # 404 is normal for non-existent (model, year, month) combos.
            if exc.response.status_code == 404:
                return ([], [])
            raise
        return _parse_listing(response.text)

    async def _fetch_envelope(self, url: str) -> dict[str, Any] | None:
        """Fetch and parse one ``sep_forecast_submission`` JSON file."""
        _assert_no_hesperia_release(url)
        await self._ratelimiter.acquire()
        response = await request_with_retry(self._client, "GET", url, safe_log_params=())
        try:
            raw = response.json()
        except ValueError:
            logger.warning("sep_scoreboards: non-JSON body at %s", url)
            return None
        if not isinstance(raw, dict):
            logger.warning("sep_scoreboards: non-dict envelope at %s", url)
            return None
        envelope = raw.get("sep_forecast_submission")
        if not isinstance(envelope, dict):
            logger.warning("sep_scoreboards: missing sep_forecast_submission key at %s", url)
            return None
        return envelope

    # ------------------------------------------------------------------ #
    # normalization
    # ------------------------------------------------------------------ #

    def _maybe_normalize(
        self,
        board: Scoreboard,
        fc: _Forecast,
        source_url: str,
        issue_time: datetime,
    ) -> NormalizedRecord | None:
        """Project one forecast into a NormalizedRecord for the given board.

        Returns ``None`` if this forecast row has no data for the
        requested board (e.g. a peak-intensity-only forecast queried
        for Scoreboard A returns ``None``).
        """

        if board == "A":
            return _scoreboard_a_record(self, fc, source_url, issue_time)
        if board == "B":
            return _scoreboard_b_record(self, fc, source_url, issue_time)
        return _scoreboard_c_record(self, fc, source_url, issue_time)

    # ------------------------------------------------------------------ #
    # provenance-spec bridge
    # ------------------------------------------------------------------ #

    @staticmethod
    def to_helios_model_output(record: NormalizedRecord) -> dict[str, Any]:
        """Convert a :class:`NormalizedRecord` to a HELIOS provenance-spec
        :class:`helios_provenance.models.HeliosModelOutputRecord` payload.

        The spec requires ``value`` to be a primitive (float/int/str/bool).
        Scoreboard records carry a structured ``value`` dict; this helper
        flattens to:

        * For A: ``value`` ← ``record.value["probability"]``
        * For B: ``value`` ← ``record.value["intensity"]``
        * For C: ``value`` ← ``record.value["onset_time"]`` (ISO string)
                  when present, else the threshold-crossing time.

        Returns the validated pydantic model serialized to a JSON-mode
        dict; callers can ``json.dumps`` directly.
        """

        from helios_provenance.models import (
            Agent,
            HeliosModelOutputRecord,
        )

        value: Any = None
        extra: dict[str, Any] = {}
        if isinstance(record.value, dict):
            board = record.value.get("scoreboard")
            if board == "A":
                value = record.value.get("probability")
            elif board == "B":
                value = record.value.get("intensity")
            elif board == "C":
                value = record.value.get("onset_time") or record.value.get("crossing_time") or ""
            extra = {k: v for k, v in record.value.items() if k not in {"probability", "intensity"}}

        if not isinstance(value, (int, float, str, bool)) or isinstance(value, bool):
            try:
                value = float(value)
            except (TypeError, ValueError):
                value = str(value)

        agent = Agent(
            id="helios-spaceweather-connectors/SepScoreboardsAdapter",
            name="SepScoreboardsAdapter",
            type="software",
            version="0.2.0",
        )
        dataset_refs = list(record.provenance.dataset_refs) or [
            f"{ISWA_BASE_URL}{ISWA_SCOREBOARD_PREFIX}/"
        ]
        return HeliosModelOutputRecord(
            id=record.provenance.id,
            created_at=record.provenance.ingestion_timestamp,
            agent=agent,
            model_id=record.provenance.model_id,
            model_version="0.2.0",
            dataset_refs=dataset_refs,
            timestamp=record.provenance.timestamp,
            value=value,
            value_units=record.value_units,
            ingestion_timestamp=record.provenance.ingestion_timestamp,
            extra=extra or None,
        ).model_dump(mode="json")


# ---------------------------------------------------------------------------- #
# module helpers — forecast expansion + per-board normalization
# ---------------------------------------------------------------------------- #


def _expand_forecasts(envelope: dict[str, Any]) -> list[_Forecast]:
    """Parse the ``forecasts`` array into typed-ish ``_Forecast`` rows."""

    raw_list = envelope.get("forecasts")
    if not isinstance(raw_list, list):
        return []
    out: list[_Forecast] = []
    for entry in raw_list:
        if not isinstance(entry, dict):
            continue
        energy_min, energy_max, energy_units = _coerce_energy_channel(entry.get("energy_channel"))
        prediction = entry.get("prediction_window")
        start_ts = _maybe_isoparse((prediction or {}).get("start_time"))
        end_ts = _maybe_isoparse((prediction or {}).get("end_time"))
        out.append(
            _Forecast(
                envelope=envelope,
                forecast=entry,
                energy_min=energy_min,
                energy_max=energy_max,
                energy_units=energy_units,
                species=str(entry.get("species") or "proton"),
                location=str(entry.get("location") or "earth"),
                prediction_start=start_ts,
                prediction_end=end_ts,
            )
        )
    return out


def _coerce_energy_channel(raw: Any) -> tuple[float | None, float | None, str]:
    if not isinstance(raw, dict):
        return (None, None, "MeV")
    return (
        _maybe_float(raw.get("min")),
        _maybe_float(raw.get("max")),
        str(raw.get("units") or "MeV"),
    )


def _maybe_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _maybe_isoparse(value: Any) -> datetime | None:
    if isinstance(value, str) and value:
        try:
            return _ensure_utc(isoparse(value))
        except (ValueError, TypeError):
            return None
    return None


def _model_short_name(envelope: dict[str, Any]) -> str:
    model = envelope.get("model")
    if isinstance(model, dict):
        name = model.get("short_name")
        if isinstance(name, str):
            return name
    return "unknown"


def _spase_id(envelope: dict[str, Any]) -> str | None:
    model = envelope.get("model")
    if isinstance(model, dict):
        spase = model.get("spase_id")
        if isinstance(spase, str):
            return spase
    return None


def _lineage_for(envelope: dict[str, Any], source_url: str) -> tuple[str, ...]:
    """Build the lineage tuple for a scoreboard record.

    Order: SPASE id (when available) → model short_name → source URL →
    any trigger identifiers (CME catalog IDs, flare IDs, etc.). This
    captures the chain from the upstream-cause event through the
    contributing model to the file on disk.
    """

    out: list[str] = []
    spase = _spase_id(envelope)
    if spase:
        out.append(spase)
    out.append(f"model/{_model_short_name(envelope)}")
    out.append(source_url)
    triggers = envelope.get("triggers")
    if isinstance(triggers, list):
        for trig in triggers:
            if not isinstance(trig, dict):
                continue
            for kind, payload in trig.items():
                if not isinstance(payload, dict):
                    continue
                catalog_id = payload.get("catalog_id")
                if isinstance(catalog_id, str) and catalog_id:
                    out.append(f"trigger/{kind}/{catalog_id}")
                else:
                    # Fallback: trigger time
                    for tkey in ("start_time", "peak_time", "last_data_time"):
                        tval = payload.get(tkey)
                        if isinstance(tval, str) and tval:
                            out.append(f"trigger/{kind}/{tval}")
                            break
    return tuple(out)


def _record_id(envelope: dict[str, Any], scoreboard: Scoreboard, energy: float | None) -> str:
    short = _model_short_name(envelope)
    issue = envelope.get("issue_time", "")
    energy_part = "noE" if energy is None else f"{int(energy)}MeV"
    return f"sep_scoreboard_{scoreboard.lower()}/{short}/{energy_part}/{issue}"


def _scoreboard_a_record(
    adapter: SepScoreboardsAdapter,
    fc: _Forecast,
    source_url: str,
    issue_time: datetime,
) -> NormalizedRecord | None:
    """Project a forecast into a Scoreboard A (onset probability) record.

    Returns ``None`` if the forecast has no ``probabilities`` array and
    no ``all_clear`` boolean (Scoreboard A surfaces both; an all-clear
    record is a probability of 0 at the configured threshold).
    """

    probs = fc.forecast.get("probabilities")
    all_clear = fc.forecast.get("all_clear")
    rows: list[dict[str, Any]] = []
    if isinstance(probs, list):
        for entry in probs:
            if not isinstance(entry, dict):
                continue
            prob = _maybe_float(entry.get("probability_value"))
            if prob is None:
                continue
            rows.append(
                {
                    "probability": prob,
                    "uncertainty": _maybe_float(entry.get("uncertainty")),
                    "threshold": _maybe_float(entry.get("threshold")),
                    "threshold_units": str(entry.get("threshold_units") or "pfu"),
                }
            )
    if not rows and isinstance(all_clear, dict):
        boolean = all_clear.get("all_clear_boolean")
        if isinstance(boolean, bool):
            rows.append(
                {
                    "probability": 0.0 if boolean else 1.0,
                    "uncertainty": None,
                    "threshold": _maybe_float(all_clear.get("threshold")),
                    "threshold_units": str(all_clear.get("threshold_units") or "pfu"),
                    "from_all_clear": True,
                }
            )
    if not rows:
        return None
    # Emit a single record per forecast row; the value carries the full
    # probabilities list as ``thresholds`` and the first row as the
    # primary scalar.
    primary = rows[0]
    value: dict[str, Any] = {
        "scoreboard": "A",
        "model": _model_short_name(fc.envelope),
        "energy_min": fc.energy_min,
        "energy_max": fc.energy_max,
        "energy_units": fc.energy_units,
        "species": fc.species,
        "location": fc.location,
        "prediction_window_start": fc.prediction_start.isoformat() if fc.prediction_start else None,
        "prediction_window_end": fc.prediction_end.isoformat() if fc.prediction_end else None,
        "probability": primary["probability"],
        "uncertainty": primary["uncertainty"],
        "threshold": primary["threshold"],
        "threshold_units": primary["threshold_units"],
        "from_all_clear": bool(primary.get("from_all_clear", False)),
        "all_thresholds": rows,
    }
    lineage = _lineage_for(fc.envelope, source_url)
    scalar_prob = (
        float(primary["probability"]) if isinstance(primary["probability"], (int, float)) else 0.0
    )
    provenance = adapter._emit_provenance(
        model_id=f"sep_scoreboard_a/{_model_short_name(fc.envelope)}",
        dataset_refs=(source_url,),
        timestamp=issue_time,
        value=scalar_prob,
        value_units="probability",
        extra={"payload": dict(value), "lineage": list(lineage)},
        record_id=_record_id(fc.envelope, "A", fc.energy_min),
    )
    return NormalizedRecord(
        source=SourceID.SEP_SCOREBOARD_A,
        record_type="onset_probability",
        event_time=issue_time,
        value=value,
        value_units="probability",
        provenance=provenance,
        raw=fc.envelope,
    )


def _scoreboard_b_record(
    adapter: SepScoreboardsAdapter,
    fc: _Forecast,
    source_url: str,
    issue_time: datetime,
) -> NormalizedRecord | None:
    """Project a forecast into a Scoreboard B (peak flux) record."""

    peak = fc.forecast.get("peak_intensity")
    if not isinstance(peak, dict):
        return None
    intensity = _maybe_float(peak.get("intensity"))
    if intensity is None:
        return None
    units = str(peak.get("units") or "pfu")
    peak_time = _maybe_isoparse(peak.get("time"))
    uncertainty = _maybe_float(peak.get("uncertainty"))
    value: dict[str, Any] = {
        "scoreboard": "B",
        "model": _model_short_name(fc.envelope),
        "energy_min": fc.energy_min,
        "energy_max": fc.energy_max,
        "energy_units": fc.energy_units,
        "species": fc.species,
        "location": fc.location,
        "intensity": intensity,
        "uncertainty": uncertainty,
        "peak_time": peak_time.isoformat() if peak_time else None,
        "prediction_window_start": fc.prediction_start.isoformat() if fc.prediction_start else None,
        "prediction_window_end": fc.prediction_end.isoformat() if fc.prediction_end else None,
    }
    lineage = _lineage_for(fc.envelope, source_url)
    provenance = adapter._emit_provenance(
        model_id=f"sep_scoreboard_b/{_model_short_name(fc.envelope)}",
        dataset_refs=(source_url,),
        timestamp=peak_time or issue_time,
        value=intensity,
        value_units=units,
        extra={"payload": dict(value), "lineage": list(lineage)},
        record_id=_record_id(fc.envelope, "B", fc.energy_min),
    )
    return NormalizedRecord(
        source=SourceID.SEP_SCOREBOARD_B,
        record_type="peak_flux",
        event_time=peak_time or issue_time,
        value=value,
        value_units=units,
        provenance=provenance,
        raw=fc.envelope,
    )


def _scoreboard_c_record(
    adapter: SepScoreboardsAdapter,
    fc: _Forecast,
    source_url: str,
    issue_time: datetime,
) -> NormalizedRecord | None:
    """Project a forecast into a Scoreboard C (event time profile) record.

    Scoreboard C lives in three places per the schema:

    * ``event_lengths`` — onset/end with thresholds (preferred shape)
    * ``threshold_crossings`` — threshold cross times
    * ``sep_profile`` — an external text file with the full time series

    Records that carry none of these are dropped (``None``).
    """

    event_lengths = fc.forecast.get("event_lengths")
    crossings = fc.forecast.get("threshold_crossings")
    sep_profile = fc.forecast.get("sep_profile")

    onset_time: datetime | None = None
    end_time: datetime | None = None
    threshold: float | None = None
    threshold_units = "pfu"
    crossing_time: datetime | None = None

    if isinstance(event_lengths, list) and event_lengths:
        first = event_lengths[0]
        if isinstance(first, dict):
            onset_time = _maybe_isoparse(first.get("start_time"))
            end_time = _maybe_isoparse(first.get("end_time"))
            threshold = _maybe_float(first.get("threshold_start"))
            threshold_units = str(first.get("threshold_units") or threshold_units)

    if isinstance(crossings, list) and crossings:
        first = crossings[0]
        if isinstance(first, dict):
            crossing_time = _maybe_isoparse(first.get("crossing_time"))
            if threshold is None:
                threshold = _maybe_float(first.get("threshold"))
            threshold_units = str(first.get("threshold_units") or threshold_units)

    if onset_time is None and crossing_time is None and not isinstance(sep_profile, str):
        return None

    event_time = onset_time or crossing_time or issue_time
    value: dict[str, Any] = {
        "scoreboard": "C",
        "model": _model_short_name(fc.envelope),
        "energy_min": fc.energy_min,
        "energy_max": fc.energy_max,
        "energy_units": fc.energy_units,
        "species": fc.species,
        "location": fc.location,
        "onset_time": onset_time.isoformat() if onset_time else None,
        "end_time": end_time.isoformat() if end_time else None,
        "crossing_time": crossing_time.isoformat() if crossing_time else None,
        "threshold": threshold,
        "threshold_units": threshold_units,
        "sep_profile": sep_profile if isinstance(sep_profile, str) else None,
    }
    lineage = _lineage_for(fc.envelope, source_url)
    # Scoreboard C records a single threshold-crossing event per record;
    # the scalar value is a binary 1 (a crossing/onset was reported) or
    # 0 (only an external SEP profile reference was found). Downstream
    # consumers needing onset/crossing times read them from ``value`` on
    # the NormalizedRecord (preserved) or from ``extra["payload"]`` on
    # the provenance record.
    crossing_count = 1 if (onset_time is not None or crossing_time is not None) else 0
    provenance = adapter._emit_provenance(
        model_id=f"sep_scoreboard_c/{_model_short_name(fc.envelope)}",
        dataset_refs=(source_url,),
        timestamp=event_time,
        value=crossing_count,
        value_units=threshold_units,
        extra={"payload": dict(value), "lineage": list(lineage)},
        record_id=_record_id(fc.envelope, "C", fc.energy_min),
    )
    return NormalizedRecord(
        source=SourceID.SEP_SCOREBOARD_C,
        record_type="event_time_profile",
        event_time=event_time,
        value=value,
        value_units=threshold_units,
        provenance=provenance,
        raw=fc.envelope,
    )


# Re-export for tests that want to construct synthetic windows
__all_helpers__ = (
    "_assert_no_hesperia_release",
    "_coerce_energy_channel",
    "_coerce_issue_time",
    "_ensure_utc",
    "_expand_forecasts",
    "_lineage_for",
    "_maybe_float",
    "_maybe_isoparse",
    "_model_short_name",
    "_months_in_window",
    "_parse_listing",
    "_record_id",
    "_scoreboard_a_record",
    "_scoreboard_b_record",
    "_scoreboard_c_record",
    "_spase_id",
)
