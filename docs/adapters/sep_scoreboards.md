# CCMC SEP Scoreboards adapter

`helios_connectors.adapters.sep_scoreboards.SepScoreboardsAdapter` —
BUILD-strategy adapter for the CCMC ISEP project's three SEP
scoreboards. No Python client existed upstream; this adapter walks the
ISWA data tree directly.

## The three scoreboards

CCMC publishes three SEP scoreboards under the R2O2R framework:

| Scoreboard | Question | Sourced from |
|---|---|---|
| **A — onset probability** | Probability of an SEP event exceeding a given flux threshold within the forecast window | per-forecast `probabilities` array; an `all_clear` boolean maps to 0/1 |
| **B — peak flux prediction** | Most-likely peak flux value plus uncertainty at a given energy channel | per-forecast `peak_intensity` object |
| **C — event time profiles** | Time series of expected flux through an event, including onset, end, and threshold crossings | per-forecast `event_lengths`, `threshold_crossings`, optional `sep_profile` (external text file) |

All three are **views into the same per-model JSON envelope** — each
contributing model emits a `sep_forecast_submission` file (per CCMC's
`sep_json_writer.py` schema) that carries probabilities, peak intensity,
and event-profile data for one or more energy channels. The HELIOS
adapter walks the data tree, fetches per-model JSON, and projects each
`forecast` into A-, B-, and C-shaped `NormalizedRecord` rows according
to which fields are populated.

## URL layout (actual, verified live 2026-05)

CCMC's canonical machine-accessible mirror is the **ISWA data tree** —
*not* the interactive web apps at `sep.ccmc.gsfc.nasa.gov`, which are
SPAs that fetch via private AJAX endpoints we cannot rely on:

```
https://iswa.ccmc.gsfc.nasa.gov/iswa_data_tree/model/heliosphere/sep_scoreboard/
    <MODEL>/[<variant>/]<energy>/<YYYY>/<MM>/<filename>.json
```

Examples discovered live during adapter development:

* `UMASEP/v3_X/10MeV/2024/05/UMASEP10_prediction_2024_05_01_000516__2024_05_01_000920.json`
* `SEPSTER/Parker/2024/05/sepster_20240501_0636_0794_Parker_Spiral_iss_20240501_1547.json`
* `SEPSTER/Parker/2017/09/sepster_20170906_1000_0260.json` (September 2017 event — proposal Table 3-1 training event)

Per-model "variants" subdirectory layer (`v3_X`, `Parker`, `WSA-ENLIL`,
`1.X`, `2.X`, `3.X`, `LOS`, `VEC`, …) varies by model. The adapter
exposes `SCOREBOARD_MODELS` as a configurable registry; override via
the `models=` constructor kwarg to extend coverage.

The default registry (verified 2026-05): `UMASEP`, `SEPSTER`,
`SEPSTER2D`, `SAWS_ASPECS`, `SEPMOD`, `MagPy`, `SPRINTS-SEP`, `iPATH`.

> **Note on URLs the brief quoted as guesses**: the brief speculated
> at `https://ccmc.gsfc.nasa.gov/scoreboards/sep/scoreboards/A/`. That
> path **returns 404**. The actual machine-accessible API surface is
> the ISWA data tree described above. Live web-app URLs at
> `https://sep.ccmc.gsfc.nasa.gov/probability` (and `/intensity`,
> `/allclear`) are Single-Page Apps that load via private endpoints —
> not suitable for adapter use.

## HESPERIA REleASE exclusion

Per HELIOS proposal §3 T1 ref [30], **HESPERIA REleASE** requires a
separate licensing agreement for commercial use. The adapter enforces
this via three layered guards:

1. **Registry exclusion.** The default `SCOREBOARD_MODELS` does **not**
   include `RELEASE`, `RELEASE_PLUS`, `STEREO_RELEASE`, or
   `STEREO_RELEASE_PLUS`.
2. **Request-time path guard.** Every URL the adapter would request
   passes through `_assert_no_hesperia_release` which raises
   `ValueError` if the path contains `release` or `hesperia` (case-
   insensitive).
3. **Construction-time spec check.** `SepScoreboardsAdapter.__init__`
   eagerly validates the model registry — passing a custom `models=`
   list that references a forbidden directory raises at construction,
   not mid-fetch.

The repository's CI runs the regression test
`tests/test_sep_scoreboards.py::test_no_url_contains_release_or_hesperia`
which sweeps every URL a standard `fetch` would issue and asserts none
contains a forbidden token.

**What we still consume**: the *consensus aggregated scoreboard* output
— records from non-REleASE models in the registry. We never call a
REleASE-specific endpoint. The Scoreboard B and Scoreboard A consensus
views aggregated by CCMC's web apps include REleASE in some
consolidations, but the adapter consumes the model-level JSON
submissions directly, so REleASE never appears in lineage for any
record HELIOS emits.

## Rate limiting and retries

CCMC publishes no formal rate-limit policy for ISWA. The adapter
defaults to **3 RPS with burst=3** via `helios_connectors.ratelimit`,
and uses the shared `helios_connectors.http.request_with_retry` with
exponential backoff (1 s → 30 s, 4 attempts) on 429 / 5xx /
`httpx.TransportError`.

Empirically a `fetch_scoreboard_a(start=GANNON_START, end=GANNON_END)`
across the default 8-model registry hits roughly 240 listing URLs
(8 models × ≤5 energies × 6 months = 240) and a few hundred JSON files
in the populated months. At 3 RPS this completes in ~2 minutes; the
file cache (enabled by default) makes subsequent calls instant.

## Provenance lineage

Every record's `provenance.lineage` follows this order:

1. SPASE ID of the contributing model (when present in the envelope's
   `model.spase_id` field)
2. `model/<short_name>` (e.g. `model/UMASEP-10`)
3. The full ISWA file URL the data was fetched from
4. One entry per trigger event from the envelope's `triggers` array:
   `trigger/<kind>/<catalog_id_or_time>` (e.g.
   `trigger/cme/2024-05-08T16:00:00-CME-001`)

For example, a Scoreboard A record from UMASEP-10 for the 2024-05-10
Gannon event has lineage roughly:

```
('spase://CCMC/SimulationModel/UMASEP/v3',
 'model/UMASEP-10',
 'https://iswa.ccmc.gsfc.nasa.gov/iswa_data_tree/.../2024/05/UMASEP10_prediction_2024_05_10_193000__2024_05_10_193200.json',
 'trigger/cme/2024-05-08T16:00:00-CME-001',
 'trigger/flare/2024-05-10T15:35Z')
```

REleASE is **never** listed as a contributor in any HELIOS-emitted
lineage — that's the regression guarantee.

## Worked example: 2017 September 6 storm window

The September 6 / 10, 2017 event is one of three hold-out events in
the proposal's pre-registration (Table 3-1). To pull Scoreboard A onset
probabilities across the event:

```python
import asyncio
from datetime import datetime, timezone

from helios_connectors import SepScoreboardsAdapter

async def main() -> None:
    async with SepScoreboardsAdapter() as sb:
        async for rec in sb.fetch_scoreboard_a(
            start=datetime(2017, 9, 6, tzinfo=timezone.utc),
            end=datetime(2017, 9, 11, tzinfo=timezone.utc),
        ):
            print(
                rec.event_time.isoformat(),
                rec.value["model"],
                f"P={rec.value['probability']:.2f}",
                f"thr={rec.value['threshold']} {rec.value['threshold_units']}",
            )

asyncio.run(main())
```

The output is a per-model, per-issue-time table of onset probabilities;
HELIOS' downstream consensus layer (BMA fusion in
`helios-fusion-engine`) takes this as its input.

## Schema notes

A single per-model JSON envelope follows the shape from CCMC's
documented `sep_json_writer.py` (verified against the live example file
at `https://ccmc.gsfc.nasa.gov/static/files/SEPSB/sep_scoreboard_example_json_file.json`):

```json
{
  "sep_forecast_submission": {
    "model": { "short_name": "UMASEP-10", "spase_id": "spase://..." },
    "issue_time": "2024-05-10T19:30:00Z",
    "mode": "forecast",
    "triggers": [ { "cme": {...} }, { "flare": {...} } ],
    "forecasts": [
      {
        "energy_channel": { "min": 10, "max": -1, "units": "MeV" },
        "species": "proton",
        "location": "earth",
        "prediction_window": { "start_time": "...", "end_time": "..." },
        "peak_intensity": { "intensity": 3000.0, "units": "pfu", ... },   // → Scoreboard B
        "event_lengths": [ { "start_time": "...", "threshold_start": 1.0, ... } ],   // → Scoreboard C
        "threshold_crossings": [ { "crossing_time": "...", "threshold": 10.0 } ],   // → Scoreboard C
        "probabilities": [ { "probability_value": 0.85, "threshold": 10, ... } ],   // → Scoreboard A
        "all_clear": { "all_clear_boolean": false, "threshold": 10.0 },             // → Scoreboard A (fallback)
        "sep_profile": "filename.10MeV.txt"                                          // → Scoreboard C
      }
    ]
  }
}
```

**All three scoreboards share a single envelope shape** — A/B/C are
projections into different fields. Time strings are all ISO-8601 with
a `Z` UTC suffix in observed examples; the adapter's
`_maybe_isoparse` defensively handles either `Z` or `+00:00`
suffixes via `python-dateutil`. NaN-encoding: missing fields are simply
absent from the envelope (no sentinel value); the adapter treats
absent keys as `None`.

## Cross-references

* [Adapter pattern](../design.md)
* [`helios-provenance-spec`](https://github.com/577Industries/helios-provenance-spec)
  for the provenance schema
* CCMC ISEP project: <https://ccmc.gsfc.nasa.gov/scoreboards/sep/>
* `sep_json_writer.py` documentation:
  <https://ccmc.gsfc.nasa.gov/scoreboards/sep/using-json-helper-script-documentation/>
* HELIOS proposal §3 T1 — CCMC integration constraints
