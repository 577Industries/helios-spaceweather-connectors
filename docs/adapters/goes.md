# GOES adapter

`GoesAdapter` exposes NOAA GOES X-ray flux and integral proton flux to
HELIOS through a single uniform interface. It is the canonical "WRAP"
adapter in the package: a thin shim over `pyspedas.projects.goes` for
the historical NCEI archive plus a direct httpx client for the NOAA
SWPC near-real-time JSON services.

## Products

| Product | Bands / thresholds | Units | Cadence (typical) | Satellites |
|---|---|---|---|---|
| **X-ray flux** | `0.05-0.4 nm` (short), `0.1-0.8 nm` (long) | W/m² | 1 min (NCEI archive); 1 min (SWPC) | GOES-16 / 17 / 18 |
| **Integral proton flux** | `>=10 MeV`, `>=50 MeV`, `>=100 MeV` | pfu (proton flux units) | 1 min (NCEI archive); 5 min (SWPC) | GOES-16 / 17 / 18 |

The three integral proton thresholds are the exact ones HELIOS §2 Obj.3
SEP all-clear logic acts on. SWPC publishes a wider channel set
(`>=500 MeV` etc.); the adapter filters down to the three HELIOS targets
because they are what the operational decision uses.

## Routing rule

| When the requested window… | Path | Backed by |
|---|---|---|
| ends ≥ 30 days ago | **PySPEDAS / NCEI archive** | `pyspedas.projects.goes.xrs` and `.sgps`, sourced from `https://www.ncei.noaa.gov/data/goes-r-series-satellites/` |
| starts < 30 days ago | **NOAA SWPC near-real-time JSON** | `https://services.swpc.noaa.gov/json/goes/primary/` |
| straddles the 30-day boundary | **Both**, merged in-stream | as above |

The 30-day boundary is configurable via the `nrt_window_days`
constructor argument. The default reflects NCEI's typical multi-week
publish latency; production deployments that ingest from a private
near-real-time archive can drop it to 0 to route everything through
PySPEDAS.

## Provenance lineage

Every record carries a `provenance.lineage` tuple whose single entry is
the upstream URL prefix the data was sourced from. This is the
primary downstream signal for "which route did this sample take":

| Route | `model_id` | `lineage[0]` starts with |
|---|---|---|
| PySPEDAS / NCEI | `goes/<product>/ncei-archive` | `https://www.ncei.noaa.gov/data/goes-r-series-satellites/` |
| SWPC NRT | `goes/<product>/swpc-nrt` | `https://services.swpc.noaa.gov/json/goes/primary/` |

`source_id` is always `SourceID.GOES` regardless of route.

## Coordination with `SwpcAdapter`

The [SWPC adapter](swpc.md) also exposes GOES integral proton flux,
through `SwpcAdapter.fetch_sep_forecast()` and friends. **This overlap
is intentional**: two adapters provide two different framings of the
same physical observation.

| Adapter | `source_id` | Lineage cites | Use when |
|---|---|---|---|
| `GoesAdapter` | `goes` | NCEI archive OR `services.swpc.noaa.gov` (depending on route) | You want to attribute the value to the GOES instrument suite — "instrument archive" framing. |
| `SwpcAdapter` | `swpc` | `services.swpc.noaa.gov` | You want to attribute the value to the SWPC operational pipeline — "real-time consumer" framing. The SWPC view applies operational quality flags before publishing. |

Downstream fusion code that wants the operational SWPC view should use
`SwpcAdapter`; downstream code that wants the raw archived
instrument record should use `GoesAdapter`. Both are valid for §2
Obj.3 SEP all-clear: SWPC is what NOAA's published forecasts are
keyed to, the GOES archive is what NASA SRAG uses for post-event
review.

## Satellites

`satellite` accepts:

- `'GOES-16'` (default; GOES-East as of 2026-05)
- `'GOES-17'` (standby)
- `'GOES-18'` (GOES-West as of 2026-05)

The default is `GOES-16` because all 2024 Gannon-storm reference data
in the HELIOS test suite is keyed to GOES-16. For west-Pacific
coverage during a real-time event, pass `satellite='GOES-18'`.

## Rate-limit / etiquette

- NCEI publishes no documented rate limit. We cap at **2 RPS** as a
  courtesy; bulk archival downloads should use PySPEDAS's own caching
  and run overnight.
- NOAA SWPC publishes a soft cap of **5 RPS**; 2 RPS is well inside it.
- The adapter sets a meaningful User-Agent
  (`helios-spaceweather-connectors/...`) per NOAA's published
  expectation.

## Example

```python
import asyncio
from datetime import UTC, datetime
from helios_connectors import GoesAdapter


async def main() -> None:
    async with GoesAdapter() as goes:
        async for rec in goes.fetch_protons(
            start=datetime(2024, 5, 8, tzinfo=UTC),
            end=datetime(2024, 5, 14, tzinfo=UTC),
        ):
            print(
                rec.event_time,
                rec.value["threshold_mev"],
                rec.value["flux"],
                rec.provenance.lineage[0],
            )


asyncio.run(main())
```

For the Gannon window above (>30 days old as of 2026-05), the adapter
routes to the PySPEDAS / NCEI archive path. Records carry
`provenance.model_id = "goes/protons/ncei-archive"` and lineage citing
`https://www.ncei.noaa.gov/data/goes-r-series-satellites/goes-16/`.

## Provenance-spec bridge

The adapter exposes a helper to convert a `NormalizedRecord` into a
`helios_provenance.models.HeliosModelOutputRecord` payload (validated
against helios-provenance-spec v0.1):

```python
payload = GoesAdapter.to_helios_model_output(record)
# payload["value"]       -> the scalar flux (float)
# payload["value_units"] -> "pfu" or "W/m^2"
# payload["extra"]       -> { satellite, band|threshold_mev }
```

The conversion flattens the compound `value` dict into the
spec-mandated scalar value plus an `extra` map. This keeps every GOES
record cleanly serialisable into a HELIOS provenance graph.

## Implementation notes

- The PySPEDAS call is wrapped in `asyncio.to_thread` because pyspedas
  is blocking I/O.
- `pyspedas` is imported lazily inside the loader function so the
  base package can be installed without it. Pass
  `pyspedas_loader=<callable>` to inject a mock in tests.
- The SWPC near-real-time endpoint set publishes "6-hour", "1-day",
  "3-day", and "7-day" snapshots. The adapter picks the smallest one
  that covers the requested window and post-filters the result to
  exactly `[start, end]`.
- `_extract_proton_samples` selects the SGPS variable that best matches
  each canonical >=10 / >=50 / >=100 MeV threshold by string heuristic
  on the pyspedas-generated variable name. PySPEDAS variable naming
  has shifted across versions; document any regression as a PR.
