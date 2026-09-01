"""Common data contracts for helios-spaceweather-connectors.

This module defines the shapes every adapter produces. The provenance
record type is :class:`helios_provenance.models.HeliosModelOutputRecord`
imported from the companion ``helios-provenance-spec`` package — every
:class:`NormalizedRecord` carries one.

Adapters return :class:`NormalizedRecord` objects that carry a science
value plus a :class:`HeliosModelOutputRecord` describing where the value
came from and how it was derived. Downstream fusion code consumes the
science value; auditors consume the provenance record. The two travel
together because the value without provenance is unaudited (and the
provenance without a value is meaningless).

Source identifiers (:class:`SourceID`) are deliberately coarse-grained:
one enum member per upstream **service** (DONKI, SWPC, GOES, DSCOVR,
CDDIS, the three SEP Scoreboards). Per-product distinctions (Kp vs.
plasma vs. mag for SWPC; X-ray vs. proton for GOES) are recorded by
``NormalizedRecord.record_type`` and by the scoped ``model_id`` on the
provenance record. This lets downstream fusion key partitions by source
without exploding the enum.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any

from helios_provenance.models import HeliosModelOutputRecord

__all__ = [
    "HeliosModelOutputRecord",
    "NormalizedRecord",
    "SourceID",
]


class SourceID(StrEnum):
    """Stable identifiers for every upstream data source HELIOS speaks to.

    These strings are deliberately suitable as filesystem path components
    and parquet partition keys. One member per upstream **service**;
    per-product detail lives on :attr:`NormalizedRecord.record_type` and
    on the provenance record's ``model_id``.
    """

    DONKI = "donki"
    SWPC = "swpc"
    GOES = "goes"
    DSCOVR = "dscovr"
    # NOAA SWPC's real-time-solar-wind feed as a *service*: multi-observatory
    # (SOLAR1 / IMAP / ACE, prime-selected upstream). Not DSCOVR — since the
    # 2026-08 /products/solar-wind retirement, DSCOVR only describes the NCEI
    # archive leg of DscovrAdapter.
    RTSW = "rtsw"
    CDDIS_GIM = "cddis_gim"
    SEP_SCOREBOARD_A = "sep_scoreboard_a"
    SEP_SCOREBOARD_B = "sep_scoreboard_b"
    SEP_SCOREBOARD_C = "sep_scoreboard_c"


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
            ``"kp"``, ``"dst"``, ``"plasma"``, ``"mag"``, ``"xray"``,
            ``"proton"``, ``"tec_map"``, ``"onset_probability"``). Allows
            multiple event types to share a single source.
        event_time: the time the science event happened (UTC).
        value: the normalized payload as a JSON-serializable dict.
        value_units: a units string. Use ``"none"`` for compound payloads.
        provenance: full HELIOS provenance-spec record for this value.
        raw: the unaltered upstream response object, kept for debugging.
            Adapters should *not* mutate this after construction.
    """

    source: SourceID
    record_type: str
    event_time: datetime
    value: dict[str, Any]
    value_units: str
    provenance: HeliosModelOutputRecord
    raw: dict[str, Any] = field(default_factory=dict)
