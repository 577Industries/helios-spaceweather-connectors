# SWPC adapter

`SwpcAdapter` wraps NOAA Space Weather Prediction Center's
`services.swpc.noaa.gov` JSON + text products, with transparent fallback
to authoritative academic archives (GFZ Potsdam for Kp, Kyoto WDC for
Dst) when the requested window predates SWPC's ~30-day public archive.

**Strategy**: EXTEND. SunPy already exposes some SWPC indices (Kp, Dst),
but plasma, IMF, and the 3-day probabilistic SEP forecast are not in
SunPy's catalogue. This adapter covers the full operational surface
HELIOS' §2 Obj.2 fusion layer needs.

## Products

| Slug             | Endpoint                                                          | Cadence | Source                       | SourceID                |
| ---------------- | ----------------------------------------------------------------- | ------- | ---------------------------- | ----------------------- |
| `kp`             | `/products/noaa-planetary-k-index.json`                           | 3-hour  | SWPC (real-time)             | `SWPC_KP`               |
| `kp` (archive)   | `kp.gfz.de/app/files/Kp_ap_Ap_SN_F107_since_1932.txt`             | 3-hour  | GFZ Potsdam (since 1932)     | `SWPC_KP`               |
| `dst`            | `wdc.kugi.kyoto-u.ac.jp/dst_{provisional,final}/<yyyymm>/...`     | 1-hour  | Kyoto WDC                    | `SWPC_KP` *(see below)* |
| `plasma`         | `/products/solar-wind/plasma-7-day.json`                          | 1-min   | SWPC (DSCOVR-derived)        | `SWPC_PLASMA`           |
| `mag`            | `/products/solar-wind/mag-7-day.json`                             | 1-min   | SWPC (DSCOVR-derived)        | `SWPC_MAG`              |
| `goes_protons`   | `/json/goes/primary/integral-protons-7-day.json`                  | 1-min   | SWPC (GOES-derived)          | `GOES_PROTON`           |
| `sep_forecast`   | `/text/3-day-forecast.txt`                                        | daily   | SWPC (forecast text product) | `SWPC_SEP_FORECAST`     |

Dst is tagged with `SWPC_KP` (the closest existing SourceID; HELIOS
treats Kp/Dst as a single geomag-index suite at the fusion layer).
A dedicated `SWPC_DST` SourceID can be added in a follow-up PR if
downstream consumers need to discriminate.

## The 30-day archive limit (the gotcha)

**NOAA SWPC's public JSON products only carry the last ~30 days.** A
naive `fetch_kp(start=date(2024, 5, 8), ...)` against the real-time
endpoint would silently return the last 30 days of data — months
later than the window the caller asked for. This is a credibility risk
for any retrospective study (Gannon, Halloween 2003, etc.).

The `SwpcAdapter` solves this by inspecting `start` and routing
transparently:

- `start >= now - 30 days` → SWPC real-time JSON product.
- `start <  now - 30 days` → archive provider, with provenance noting
  the source.

Archive providers:

- **Kp**: [GFZ Potsdam Kp index](https://kp.gfz.de/) at
  `https://kp.gfz.de/app/files/Kp_ap_Ap_SN_F107_since_1932.txt`
  (CC-BY-4.0). IAGA-authoritative; 8 3-hour Kp values per daily row,
  going back to 1932.

- **Dst**: [World Data Center for Geomagnetism, Kyoto](https://wdc.kugi.kyoto-u.ac.jp/)
  via the per-month file
  `http://wdc.kugi.kyoto-u.ac.jp/dst_provisional/<yyyymm>/dst<yymm>.for.request`.
  We prefer the `dst_final` tier for older windows (≥30 days old) and
  fall back to `dst_provisional` if final is not yet published — final
  Dst typically lags 6-12 months.

This pattern was first applied in
[`gannon-storm-rtk-analysis`](https://github.com/577Industries/gannon-storm-rtk-analysis)
for the Gannon retrospective; this adapter generalizes it.

## Rate limits

- `services.swpc.noaa.gov`: 5 RPS (per
  [adapter pattern docs](../design.md)).
- `kp.gfz.de` and `wdc.kugi.kyoto-u.ac.jp`: 1 RPS — these are academic
  servers. Independent token bucket so SWPC fetches and archive fetches
  never starve each other.

## Provenance lineage

Each emitted `NormalizedRecord` carries a lineage tuple describing the
data's path. Examples:

- Real-time Kp: `("swpc/kp",)`
- Archive Kp (GFZ): `("swpc/kp", "GFZ Potsdam/Kp_ap_Ap_SN_F107_since_1932.txt")`
- Dst (Kyoto final): `("swpc/dst", "Kyoto WDC/final/dst2405")`
- Dst (Kyoto provisional): `("swpc/dst", "Kyoto WDC/provisional/dst2405")`
- Plasma / Mag / GOES protons / SEP forecast: `("swpc/<slug>",)`

The `dataset_refs` field on the provenance record holds the
fully-qualified upstream URL of the data file, so audit consumers can
reconstruct the exact byte-for-byte source.

## Worked example: Gannon-week Kp retrospective

```python
from datetime import datetime, UTC
from helios_connectors import SwpcAdapter

async with SwpcAdapter() as swpc:
    records = [
        r
        async for r in swpc.fetch_kp(
            start=datetime(2024, 5, 8, tzinfo=UTC),
            end=datetime(2024, 5, 14, tzinfo=UTC),
        )
    ]

for r in records[:3]:
    print(
        r.event_time.isoformat(),
        f"Kp={r.value['kp']}",
        f"G-scale={r.value['g_scale']}",
        f"lineage={r.provenance.lineage}",
    )
```

Expected output (first three 3-hour bins on May 8, 2024):

```
2024-05-08T00:00:00+00:00 Kp=2.667 G-scale=G0 lineage=('swpc/kp', 'GFZ Potsdam/Kp_ap_Ap_SN_F107_since_1932.txt')
2024-05-08T03:00:00+00:00 Kp=2.667 G-scale=G0 lineage=('swpc/kp', 'GFZ Potsdam/Kp_ap_Ap_SN_F107_since_1932.txt')
2024-05-08T06:00:00+00:00 Kp=2.333 G-scale=G0 lineage=('swpc/kp', 'GFZ Potsdam/Kp_ap_Ap_SN_F107_since_1932.txt')
```

And on May 11, 00-03 UT (the Gannon G5 peak):

```
2024-05-11T00:00:00+00:00 Kp=9.0 G-scale=G5 lineage=('swpc/kp', 'GFZ Potsdam/Kp_ap_Ap_SN_F107_since_1932.txt')
```

Note that **no request was made to `services.swpc.noaa.gov`** during
this call — the adapter routed entirely to the GFZ archive because
`start < now - 30 days`. The provenance lineage records this fact;
downstream auditors can verify the data path without contacting SWPC.

## Coordination with sibling adapters

- **GoesAdapter** (Wave 2a sibling): also exposes GOES integral proton
  flux. `SwpcAdapter.fetch_goes_protons` is provided so SwpcAdapter is
  self-contained for the operational "everything SWPC publishes"
  workflow, but the GOES adapter is the preferred source when you want
  GOES-native field names. Fusion-layer dedup is the consumer's
  responsibility.
- **DscovrAdapter** (Wave 2a sibling): authoritative historical source
  for solar-wind plasma and IMF. SWPC's plasma and mag products are
  derived from DSCOVR; they are the real-time fast path but not the
  archive source. `SwpcAdapter.fetch_plasma` and `fetch_mag` log a
  warning when invoked with `start` older than the SWPC real-time
  window and recommend DscovrAdapter for historical data.

## Rate-limit + caching notes

The adapter accepts independent `rate_limit` and `archive_rate_limit`
arguments, and uses two pooled httpx clients (one per host class). The
file cache (default at `~/.cache/helios-connectors/swpc/`) is keyed by
(source_id, sorted query params) and contents persist as parquet.
Archive files (GFZ, Kyoto) are deliberately small so re-fetching them
on cache miss is cheap.
