# Changelog

All notable changes to this project are documented here, following [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
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
