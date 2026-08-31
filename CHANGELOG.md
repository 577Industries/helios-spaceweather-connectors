# Changelog

All notable changes to this project are documented here, following [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- **BREAKING / NOAA upstream migration (2026-08)**: NOAA SWPC retired the
  entire `/products/solar-wind/` product line (every variant 404s). The
  `plasma`/`mag` products of `SwpcAdapter` and the near-real-time leg of
  `DscovrAdapter` now consume the RTSW successors
  (`/json/rtsw/rtsw_wind_1m.json`, `/json/rtsw/rtsw_mag_1m.json`):
  list-of-dicts, newest-first, ~24 h deep (was 7 days —
  `SWPC_SOLARWIND_HOURS` documents the new depth), one record per
  (minute, observatory) with the prime stream flagged `active`. Adapters
  yield prime rows only, chronologically. Plasma `proton_*` fields map
  onto the long-standing `density`/`speed`/`temperature` value keys; mag
  field names were carried over by NOAA unchanged.
- **BREAKING**: `DscovrAdapter`'s near-real-time records are tagged with
  the new `SourceID.RTSW` instead of `SourceID.DSCOVR` — the feed's
  observatories are now SOLAR1/IMAP/ACE, so the DSCOVR instrument claim
  would be false. `SourceID.DSCOVR` remains on the PySPEDAS/NCEI archive
  leg (genuinely DSCOVR L2 CDFs). Consumers filtering realtime records on
  `SourceID.DSCOVR` must add `SourceID.RTSW`. Every RTSW-leg record (both
  adapters) carries the observing spacecraft in `value["observatory"]`.

### Fixed
- `SepScoreboardsAdapter` provenance now cites full ISWA URLs: lineage and
  `dataset_refs` previously carried client-relative paths (no host), which
  went unnoticed while UMASEP had no recent issuances and surfaced as live
  failures when 2026-08 activity produced records.
- `SepScoreboardsAdapter` now prefilters listing filenames by their
  embedded dates before downloading (fail-open for unrecognized names).
  Month directories hold every file for the month (13k+ during 2026-08's
  activity) and the issue-time filter only ran after download, so ANY
  window touching an active month crawled the whole month at 3 RPS —
  hours of runner time (the 08-30 nightly burned 2 h 33 m).
- Live-integration CI is capped (`timeout-minutes: 45`, per-test
  `pytest-timeout` budgets) and `test_live_iswa_recent` crawls 3 days
  instead of 30.

### Added
- `CddisGimAdapter` — NASA CDDIS Global Ionosphere Maps (vertical TEC at
  2-hour cadence on 2.5-by-5 deg grid). BUILD strategy: no maintained Python
  client with Earthdata Login + IONEX parsing existed. Optional `[earthdata]`
  extra installs `earthaccess` for the URS authentication handshake and
  `unlzw3` for the legacy `.Z` IONEX decompression. The custom IONEX parser
  is ~100 lines of pure Python (no `xarray` or `georinex` dep). Probes both
  the 2023-present long-form `.gz` filename and the pre-2023 legacy short-form
  `.Z` filename per request. Records carry the full 71x73 TEC grid (or a
  single bilinearly-interpolated point) with TECU units and full provenance
  including analysis-center lineage.
- Synthetic-Gannon IONEX fixture (`tests/fixtures/cddis_gim/synthetic_gannon_2024131.inx`)
  committed for offline tests; regenerator script kept alongside so the
  assumed analytic TEC distribution stays auditable. Real-CDDIS smoke test
  marked `@pytest.mark.live` for nightly runs once Earthdata creds are wired.
- 57 unit tests + 1 live test on `CddisGimAdapter`, 87% line+branch coverage.
  Critical Gannon-storm assertion: `fetch_tec_at_point(lat=40, lon=-83)`
  across 2024-05-10 yields peak TEC > 30 TECU around 20 UTC, consistent
  with the published Gannon-storm Midwest enhancement of >50 TECU.
- `SepScoreboardsAdapter` — CCMC's three SEP Scoreboards (A onset probability,
  B peak flux, C event time profiles) consumed as consensus aggregates. BUILD
  strategy (no Python client existed). HESPERIA REleASE explicitly excluded
  from request paths per the proposal's licensing constraint: triple-layered
  guard (registry exclusion + per-request path token check + construction-time
  spec validation) with a regression test that sweeps every URL the adapter
  would issue and asserts none contains `release` or `hesperia`.
- Recorded fixtures covering the September 2017 storm event (proposal Table 3-1
  training event) for downstream kill-gate eval, plus 2024-05 (Gannon-week)
  recent fixtures across UMASEP, SEPSTER, and a documentation-shape sample
  with all-three-board projections populated.
- 50 unit tests on `SepScoreboardsAdapter`, 90% line+branch coverage. Walks
  Apache mod_autoindex listings on the ISWA data tree (the actual
  machine-accessible mirror; the interactive `sep.ccmc.gsfc.nasa.gov` web apps
  are SPAs not suitable for adapter use). Default 3 RPS rate limit.
- `GoesAdapter` — GOES X-ray flux and integral proton flux. WRAP strategy with
  PySPEDAS for historical NCEI archive and NOAA SWPC near-real-time JSON for
  the last ~30 days. Intentional overlap with `SwpcAdapter` for proton flux;
  records distinguished by `source_id` and lineage.
- Adapter pattern foundation: `BaseAdapter` abstract class with async streaming `fetch()`, sync wrapper, common provenance emission.
- File cache (parquet, content-addressed) and async token-bucket rate limiter.
- Shared httpx client with NASA-etiquette User-Agent, dual-endpoint failover (api.nasa.gov ↔ kauai.ccmc.gsfc.nasa.gov), retry-with-backoff.
- `DonkiAdapter` — all 10 DONKI endpoints (CME, CMEAnalysis, FLR, SEP, GST, IPS, MPC, RBE, HSS, notifications) with intelligent linkages preserved as lineage.
- 9 real Gannon-week DONKI fixtures (`tests/fixtures/donki/`) as test corpus for downstream adapter agents.
- 66 unit tests + 1 live integration test, 94% coverage. Live test routes to kauai mirror by default to avoid NASA_API_KEY requirement in CI.
- `SwpcAdapter` — NOAA SWPC plasma, IMF, Kp, Dst, GOES protons, and 3-day SEP forecast. EXTEND strategy
  building on SunPy's index coverage. Transparent historical-archive fallback to
  GFZ Potsdam (Kp; CC-BY-4.0) and Kyoto WDC (Dst) for windows >30 days old, since
  NOAA SWPC's public archive only serves the last ~30 days.
- 7 real SWPC + archive fixtures (`tests/fixtures/swpc/`): real-time Kp, plasma, mag,
  GOES protons, 3-day forecast text, plus GFZ Kp for May 2024 and Kyoto Dst for May 2024.
- 42 unit tests + 1 live integration test on `SwpcAdapter`, 89% line+branch coverage.
  Critical regression test asserts that `fetch_kp(start=2024-05-08, end=2024-05-14)`
  (Gannon week) routes to GFZ and never hits the SWPC real-time endpoint.
- `DscovrAdapter` — DSCOVR L1 upstream solar-wind magnetometer + plasma. WRAP
  strategy with PySPEDAS for historical NCEI archive and NOAA SWPC near-real-time
  JSON for the last ~24-48 hours. Coordinate-frame ambiguity (GSE on PySPEDAS path,
  GSM on SWPC NRT path) is exposed via `record.value["frame"]`. Real-data Gannon-week
  smoke test: peak Bz = -59.16 nT on May 10 2024 in GSE frame.
- 38 unit + 1 live tests on `DscovrAdapter`, 92% line+branch coverage.

v0.1.0 alpha tag held until ≥3 adapters live (DONKI + at least two of SWPC/GOES/DSCOVR/Scoreboards/CDDIS). See Wave 2 dispatch in master plan.

See [GitHub releases](https://github.com/577Industries/helios-spaceweather-connectors/releases) for the canonical release notes when a tag ships.
