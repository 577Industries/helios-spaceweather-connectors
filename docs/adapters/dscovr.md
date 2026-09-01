# DSCOVR adapter

The `DscovrAdapter` exposes the **Deep Space Climate Observatory** L1 upstream
solar-wind monitor: the only sustained near-real-time view of the interplanetary
magnetic field (IMF) and solar-wind plasma arriving 30-60 minutes before it hits
Earth's magnetosphere.

DSCOVR is HELIOS's **primary exogenous input** for the §2 Obj. 4 ionospheric
forecasting models (the TFT exogenous variables include DSCOVR-derived solar
wind).

## Product table

| Product | Variables | Coordinate frame | Cadence | Backend |
|---|---|---|---|---|
| `mag` | Bx, By, Bz, Bt (magnitude) | **GSE** (archive) / **GSM** (SWPC) | ~1 s (archive) / 1 min (SWPC) | PlasMag fluxgate magnetometer |
| `plasma` | Np (density), V or V_GSE, Thermal Temp | GSE (V components) | ~3 s (archive) / 1 min (SWPC) | Faraday Cup |

All records carry the coordinate frame in `record.value["frame"]` to remove
ambiguity. See **Coordinate-frame notes** below.

## Routing rule

The adapter picks a backend automatically based on the age of the requested
window. The decision is made on `start` (not `end`) so both backends stay within
their published time windows.

| Start age vs now | Backend | SourceID | Lineage URL |
|---|---|---|---|
| ≤ 48 h | NOAA SWPC RTSW JSON (`services.swpc.noaa.gov/json/rtsw/`) | `RTSW` | `https://services.swpc.noaa.gov/json/rtsw/rtsw_{mag,wind}_1m.json` |
| > 48 h | PySPEDAS `pyspedas.projects.dscovr.{mag,fc}` → NCEI archive | `DSCOVR` | `https://www.ngdc.noaa.gov/dscovr/portal/` |

Override with `fetch_mag(..., backend="pyspedas")` or `backend="swpc"`.

The 48-hour threshold matches NOAA's typical NCEI publishing lag. Tune via
`DscovrAdapter(recent_threshold_hours=...)`.

**The 2026-08 RTSW migration**: NOAA retired the DSCOVR-derived
`/products/solar-wind/*-7-day.json` feeds. The RTSW successor is
multi-observatory (`SOLAR1`/`IMAP`/`ACE`, prime rows flagged `active`) —
so near-real-time records are now tagged `SourceID.RTSW` with the
observing spacecraft in `value["observatory"]`; the DSCOVR instrument tag
survives only on the archive leg, which really is DSCOVR L2 data. RTSW
depth is ~24 h (the old feeds held 7 days), leaving a 24-48 h coverage
gap on the near-real-time leg — force `backend="pyspedas"` when partial
coverage there is unacceptable.

## L1 propagation lag

DSCOVR sits at the Sun-Earth L1 Lagrange point (~1.5 million km sunward). Solar
wind arriving at DSCOVR reaches Earth's bow shock **30-60 minutes later**,
depending on wind speed. HELIOS downstream consumers that compare DSCOVR
upstream samples to ground-based geomagnetic indices must apply this propagation
delay explicitly. The adapter does not propagate timestamps — it returns the
DSCOVR observation time as published.

## Intentional overlap with `SwpcAdapter`

The SWPC adapter (`SwpcAdapter`) ALSO consumes DSCOVR-derived plasma/mag from
the same SWPC near-real-time JSON. This overlap is **intentional** and parallels
the GOES/SWPC overlap:

| Adapter | Tagging | When to use |
|---|---|---|
| `SwpcAdapter` (records → `source_id = SourceID.SWPC_*`) | Operator-tagged | Real-time pipelines that want "what NOAA operations just published" with a coherent SWPC lineage tree |
| `DscovrAdapter` (records → `source_id = SourceID.DSCOVR_*`) | Instrument-tagged | Pipelines that want **DSCOVR-the-spacecraft** as the canonical attribution, regardless of whether the data path used SWPC's JSON or NCEI's CDFs |

Downstream fusion code can distinguish via `record.source` without parsing
provenance strings. Both adapters produce records whose `provenance.lineage`
cites the actual upstream URL (SWPC's `services.swpc.noaa.gov` or NCEI's portal),
preserving full traceability either way.

## Coordinate-frame notes

**This is the most common source of bugs when consuming DSCOVR magnetometer
data.** Read carefully.

- **Archive Level-2 product (PySPEDAS path)** publishes the magnetic field
  in two coordinate frames natively: **GSE** (Geocentric Solar Ecliptic — X
  toward the Sun, Z perpendicular to the ecliptic plane) and **RTN**
  (Radial-Tangential-Normal — heliocentric, useful for plasma-physics
  studies). It does **NOT** publish GSM (Geocentric Solar Magnetospheric)
  natively. The `DscovrAdapter` archive path emits `frame = "GSE"`.
- **NOAA SWPC near-real-time JSON** publishes the same magnetometer data
  pre-transformed to **GSM** coordinates (`bx_gsm`, `by_gsm`, `bz_gsm`).
  This is the frame magnetospheric physics uses (the Z axis is aligned with
  Earth's magnetic dipole projection), and it's what HELIOS's ionospheric
  models want. The `DscovrAdapter` SWPC path emits `frame = "GSM"`.
- **Bz sign convention** is identical in both frames: negative Bz = southward
  IMF, the canonical driver of geomagnetic storms. The Gannon G5 event of
  May 10-12, 2024 produced peak Bz on the order of **-50 nT** (southward) —
  the dramatic IMF reversal that opened the magnetosphere to reconnection.

Plasma velocity vectors (`vx_gse`, `vy_gse`, `vz_gse`) are always GSE on the
archive path. The SWPC near-real-time plasma feed publishes only a scalar
`speed`, not the vector components.

If your downstream code needs everything in one frame, transform GSE → GSM
externally; PySPEDAS provides `pyspedas.cotrans_tools.cotrans` for this. The
adapter deliberately does not transform — frame transformation belongs in the
science pipeline, not the data-access layer.

## Rate-limit / etiquette

- **NOAA SWPC products JSON**: 5 RPS default (matches the SWPC adapter).
- **NOAA NCEI archive (via PySPEDAS)**: 1-2 RPS effective; PySPEDAS itself
  paces archive downloads internally. The adapter's rate limiter gates the
  call entry so two concurrent windows don't accidentally hammer NCEI.

Per NCEI's posted etiquette, do not pre-warm the cache for windows you don't
intend to consume — DSCOVR L2 CDFs are 30-200 MB per day per instrument and
NCEI bandwidth is shared with other space-weather research users.

## Installation

The PySPEDAS backend is an optional extra to avoid pulling `cdflib`, `astropy`,
and a chain of plotting libs into JSON-only consumers:

```bash
pip install 'helios-spaceweather-connectors[pyspedas]'
```

The SWPC path needs no extras.

## Example

```python
from datetime import UTC, datetime, timedelta
from helios_connectors.adapters import DscovrAdapter

# Historical: Gannon week
async with DscovrAdapter() as dscovr:
    async for rec in dscovr.fetch_mag(
        start=datetime(2024, 5, 8, tzinfo=UTC),
        end=datetime(2024, 5, 14, tzinfo=UTC),
    ):
        # Routes to PySPEDAS / NCEI archive automatically
        print(rec.event_time, rec.value["bz"], rec.value["frame"])

# Near-real-time
now = datetime.now(UTC)
async with DscovrAdapter() as dscovr:
    async for rec in dscovr.fetch_plasma(start=now - timedelta(hours=6), end=now):
        # Routes to SWPC products JSON automatically
        print(rec.event_time, rec.value["speed"], rec.value["density"])
```
