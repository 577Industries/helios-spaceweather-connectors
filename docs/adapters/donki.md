# DONKI adapter

`helios_connectors.adapters.donki.DonkiAdapter` is the v0.1
proof-of-pattern adapter against NASA CCMC's **DONKI** — the *Space
Weather Database Of Notifications, Knowledge, Information*.

DONKI is the operational chronicle of space-weather events maintained
by CCMC and the M2M SWAO at NASA GSFC. Its key innovation versus other
sources is the **intelligent linkages** field: a CME analysis points
back at its originating flares; a SEP event points at the CMEs and
flares that produced it; a geomagnetic storm points at the upstream
IPS / CMEs that triggered it. These linkages populate
`NormalizedRecord.provenance.lineage` so downstream fusion code can
walk the event graph.

## Endpoints supported

All major DONKI endpoints are covered, each exposed as both a
convenience method and via the unified `fetch(types=[...])`:

| Method | Endpoint | What it returns |
|---|---|---|
| `fetch_cme` | `/DONKI/CME` | Coronal mass ejections |
| `fetch_cme_analysis` | `/DONKI/CMEAnalysis` | CCMC CME analysis records |
| `fetch_flr` | `/DONKI/FLR` | Solar flares |
| `fetch_sep` | `/DONKI/SEP` | Solar energetic particle events |
| `fetch_gst` | `/DONKI/GST` | Geomagnetic storms |
| `fetch_ips` | `/DONKI/IPS` | Interplanetary shocks |
| `fetch_mpc` | `/DONKI/MPC` | Magnetopause crossings |
| `fetch_rbe` | `/DONKI/RBE` | Radiation belt enhancements |
| `fetch_hss` | `/DONKI/HSS` | High-speed solar-wind streams |
| `fetch_notifications` | `/DONKI/notifications` | DONKI's own notifications |

## Authentication

DONKI on `api.nasa.gov` requires an `api_key`. The adapter looks up
`NASA_API_KEY` from the environment and falls back to NASA's shared
`DEMO_KEY` (which has tight rate limits: ~30 requests / hour, 50 /
day). Get a free unlimited key at <https://api.nasa.gov/>.

Alternatively, point the adapter at CCMC's `kauai` server directly —
no API key required:

```python
from helios_connectors import DonkiAdapter
from helios_connectors.adapters.donki import DONKI_KAUAI_BASE_URL

async with DonkiAdapter(base_url=DONKI_KAUAI_BASE_URL) as donki:
    ...
```

The kauai endpoint uses a different path prefix (`/DONKI/WS/get/...`)
and is what we use in the live integration test by default.

## Rate limiting

| Configuration | Default RPS | Burst |
|---|---|---|
| Real `NASA_API_KEY` | 10 | 10 |
| `DEMO_KEY` fallback | 1 | 3 |

Override with the `rate_limit=` parameter if your deployment has
negotiated higher caps with CCMC.

## Usage examples

### Fetch a single endpoint

```python
from datetime import datetime, timezone

from helios_connectors import DonkiAdapter

async with DonkiAdapter() as donki:
    async for rec in donki.fetch_cme(
        start=datetime(2024, 5, 8, tzinfo=timezone.utc),
        end=datetime(2024, 5, 15, tzinfo=timezone.utc),
    ):
        print(rec.event_time, rec.value.get("activityID"))
```

### Fetch multiple endpoints concurrently

```python
async with DonkiAdapter() as donki:
    async for rec in donki.fetch(
        start=datetime(2024, 5, 8, tzinfo=timezone.utc),
        end=datetime(2024, 5, 15, tzinfo=timezone.utc),
        types=["CME", "FLR", "GST"],
    ):
        print(rec.source, rec.record_type, rec.event_time)
```

### Walk the provenance lineage

The Gannon G5 GST (2024-05-10) carries an 8-deep lineage back through
the contributing CMEs and an IPS shock:

```python
gannon = next(r for r in gsts if r.provenance.id.startswith("2024-05-10"))
print(f"GST {gannon.provenance.id} traces to:")
for upstream in gannon.provenance.lineage:
    print(f"  - {upstream}")
```

Output (real data, captured 2026-05-17):

```
GST 2024-05-10T15:00:00-GST-001 traces to:
  - 2024-05-08T05:36:00-CME-001
  - 2024-05-08T12:24:00-CME-001
  - 2024-05-08T19:12:00-CME-001
  - 2024-05-08T22:24:00-CME-001
  - 2024-05-09T09:24:00-CME-001
  - 2024-05-10T16:36:00-IPS-001
```

## Known DONKI API quirks

These were captured during initial adapter development; they're worth
keeping in mind when relying on DONKI in production:

- **`api.nasa.gov` is flakier than `kauai.ccmc.gsfc.nasa.gov`.** During
  development we saw a string of 503s and read timeouts from the
  api.nasa.gov gateway, while the underlying kauai endpoint was happy.
  The adapter's retry logic copes with this transparently, but if you
  need consistent latency, prefer `DONKI_KAUAI_BASE_URL`.
- **`linkedEvents` can be `null`.** Not all event records have linkages
  (e.g. early flares before their CME is identified). The adapter
  produces an empty `lineage` tuple in that case.
- **CMEAnalysis records have no `linkedEvents` field at all** — only an
  `associatedCMEID` pointing at the parent CME. The adapter
  substitutes the parent CME as the single-element lineage so analyses
  aren't orphaned in the provenance graph.
- **Date math is inclusive on both ends.** Requesting
  `startDate=2024-05-08&endDate=2024-05-08` returns events on May 8.
- **Maximum window is roughly 30 days per call.** DONKI silently
  truncates wider windows. Pagination by chunked date windows is the
  caller's responsibility for now; a future adapter helper will
  auto-chunk.
- **DONKI returns a JSON array even when empty** (`[]`), with one
  empirically-observed exception: the `notifications` endpoint
  sometimes returns a single dict instead of a one-element list. The
  adapter handles both.

## Example notebook

A runnable quickstart against the Gannon storm window lives at
[`examples/donki_quickstart.ipynb`](https://github.com/577Industries/helios-spaceweather-connectors/blob/main/examples/donki_quickstart.ipynb).
Regenerate it from source via:

```bash
python examples/build_donki_quickstart.py
```
