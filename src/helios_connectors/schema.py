"""Common data contracts for helios-spaceweather-connectors.

This module defines the shapes every adapter produces and the placeholder
``ProvenanceRecord`` that will be replaced when the companion package
``helios-provenance`` ships its v0.1 schema.

The split is intentional: adapters return :class:`NormalizedRecord` objects
that carry a science value plus a :class:`ProvenanceRecord` describing where
the value came from and how it was derived. Downstream fusion code consumes
the science value; auditors consume the provenance record. The two travel
together because the value without provenance is unaudited (and the
provenance without a value is meaningless).

When the upstream schema is pinned, the import will simply move:

.. code-block:: python

    from helios_provenance import ProvenanceRecord  # post-v0.1 swap

…and the placeholder in this module will be deleted.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import uuid4

__all__ = [
    "NormalizedRecord",
    "ProvenanceRecord",
    "SourceID",
]


class SourceID(StrEnum):
    """Stable identifiers for every upstream data source HELIOS speaks to.

    These strings are deliberately suitable as filesystem path components
    and parquet partition keys. They are *not* free-form labels — every new
    source must be registered here so downstream code can rely on equality
    rather than string matching.
    """

    DONKI = "donki"
    SEP_SCOREBOARD_A = "sep_scoreboard_a"
    SEP_SCOREBOARD_B = "sep_scoreboard_b"
    SEP_SCOREBOARD_C = "sep_scoreboard_c"
    SWPC_KP = "swpc_kp"
    SWPC_PLASMA = "swpc_plasma"
    SWPC_MAG = "swpc_mag"
    SWPC_SEP_FORECAST = "swpc_sep_forecast"
    CDDIS_GIM = "cddis_gim"
    GOES_XRAY = "goes_xray"
    GOES_PROTON = "goes_proton"
    DSCOVR = "dscovr"
    DSCOVR_MAG = "dscovr_mag"
    DSCOVR_PLASMA = "dscovr_plasma"


@dataclass(frozen=True, slots=True)
class ProvenanceRecord:
    """PLACEHOLDER — will be replaced with an import from ``helios-provenance``
    once the v0.1 schema ships.

    Field shape matches the master plan's specification so the swap is a
    pure import rename. Fields:

    - ``id``: globally unique record identifier (UUID4 by default).
    - ``schema_version``: the provenance-record schema version this record
      conforms to. Pinned to ``"0.0.0-placeholder"`` for now.
    - ``model_id``: an identifier for the upstream model / data product
      (e.g. ``"donki/CME"`` or ``"swpc/kp-3-hour"``).
    - ``dataset_refs``: a tuple of stable identifiers for the raw datasets
      contributing to this record. For DONKI events these are typically
      DONKI ``activityID`` strings.
    - ``timestamp``: the event-time of the science observation in UTC.
    - ``value``: the normalized science value (typed downstream).
    - ``value_units``: human-readable units for ``value`` (e.g. ``"pfu"``,
      ``"degrees"``, ``"none"``).
    - ``ingestion_timestamp``: when *this adapter* observed the upstream
      record. Always UTC.
    - ``lineage``: ordered tuple of identifiers describing the chain of
      events / models that produced this value. For DONKI, this is where
      the "intelligent linkages" (CME → flare → SEP) live.
    """

    id: str
    schema_version: str
    model_id: str
    dataset_refs: tuple[str, ...]
    timestamp: datetime
    value: Any
    value_units: str
    ingestion_timestamp: datetime
    lineage: tuple[str, ...]

    @classmethod
    def new(
        cls,
        *,
        model_id: str,
        dataset_refs: tuple[str, ...],
        timestamp: datetime,
        value: Any,
        value_units: str,
        lineage: tuple[str, ...] = (),
        ingestion_timestamp: datetime | None = None,
        record_id: str | None = None,
    ) -> ProvenanceRecord:
        """Construct with sensible defaults (UTC timestamps, UUID4 id).

        The placeholder ``schema_version`` is fixed; callers do not get to
        override it because the whole point of a placeholder is that it
        should be replaced wholesale, not modified in-place.
        """

        return cls(
            id=record_id or str(uuid4()),
            schema_version="0.0.0-placeholder",
            model_id=model_id,
            dataset_refs=tuple(dataset_refs),
            timestamp=_ensure_utc(timestamp),
            value=value,
            value_units=value_units,
            ingestion_timestamp=_ensure_utc(ingestion_timestamp or datetime.now(UTC)),
            lineage=tuple(lineage),
        )


@dataclass(slots=True)
class NormalizedRecord:
    """The universal output shape every adapter produces.

    An adapter's job is to turn one or more upstream representations into a
    sequence of these. The science payload lives in ``value`` (and is typed
    as ``dict[str, Any]`` because each event class has its own field set:
    a CME has speed/angle/half-angle, a flare has class/peak time, etc.).
    The *interpretation* of that payload is a downstream concern.

    Attributes:
        source: which upstream data source this record came from.
        record_type: a source-local discriminator (e.g. ``"CME"``, ``"FLR"``,
            ``"GST"`` for DONKI). Allows multiple event types to share a
            single source.
        event_time: the time the science event happened (UTC).
        value: the normalized payload as a JSON-serializable dict.
        value_units: a units string. Use ``"none"`` for compound payloads.
        provenance: full provenance chain for this record.
        raw: the unaltered upstream response object, kept for debugging.
            Adapters should *not* mutate this after construction.
    """

    source: SourceID
    record_type: str
    event_time: datetime
    value: dict[str, Any]
    value_units: str
    provenance: ProvenanceRecord
    raw: dict[str, Any] = field(default_factory=dict)


def _ensure_utc(ts: datetime) -> datetime:
    """Ensure a datetime is timezone-aware in UTC.

    Naive datetimes are assumed UTC (we never accept naive local-time);
    aware datetimes in other zones are converted.
    """

    if ts.tzinfo is None:
        return ts.replace(tzinfo=UTC)
    return ts.astimezone(UTC)
