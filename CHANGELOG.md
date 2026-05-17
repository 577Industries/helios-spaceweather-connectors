# Changelog

All notable changes to this project are documented here, following [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Adapter pattern foundation: `BaseAdapter` abstract class with async streaming `fetch()`, sync wrapper, common provenance emission.
- File cache (parquet, content-addressed) and async token-bucket rate limiter.
- Shared httpx client with NASA-etiquette User-Agent, dual-endpoint failover (api.nasa.gov ↔ kauai.ccmc.gsfc.nasa.gov), retry-with-backoff.
- `DonkiAdapter` — all 10 DONKI endpoints (CME, CMEAnalysis, FLR, SEP, GST, IPS, MPC, RBE, HSS, notifications) with intelligent linkages preserved as lineage.
- 9 real Gannon-week DONKI fixtures (`tests/fixtures/donki/`) as test corpus for downstream adapter agents.
- 66 unit tests + 1 live integration test, 94% coverage. Live test routes to kauai mirror by default to avoid NASA_API_KEY requirement in CI.
- `DscovrAdapter` — DSCOVR L1 upstream solar-wind magnetometer + plasma. WRAP
  strategy with PySPEDAS for historical NCEI archive and NOAA SWPC near-real-time
  JSON for the last ~24-48 hours. Intentional overlap with `SwpcAdapter` for
  recent data; records distinguished by `source_id` and lineage.

v0.1.0 alpha tag held until ≥3 adapters live (DONKI + at least two of SWPC/GOES/DSCOVR/Scoreboards/CDDIS). See Wave 2 dispatch in master plan.

See [GitHub releases](https://github.com/577Industries/helios-spaceweather-connectors/releases) for the canonical release notes when a tag ships.
