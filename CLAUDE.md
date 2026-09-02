# CLAUDE.md — helios-spaceweather-connectors

Six adapters (DONKI, SEP Scoreboards A/B/C, SWPC, CDDIS GIM, GOES, DSCOVR) → one `NormalizedRecord` + feature-level provenance. Program context and gh identity rules: [helios-program/CLAUDE.md §2](https://github.com/577Industries/helios-program/blob/main/CLAUDE.md). Style, coverage, commits: [CONTRIBUTING.md](CONTRIBUTING.md). Adding an adapter: [docs/design.md](docs/design.md).

## Two suites — never conflate them

    pytest -m "not live"                      # PR gate: hermetic, fixtures only
    pytest -m live --no-cov --timeout=300     # live: real NASA/NOAA/CCMC; nightly 06:17 UTC + workflow_dispatch only

- **Live tests are drift detectors, not flaky tests.** A live failure means upstream changed (2026-08: NOAA retired `/products/solar-wind` — see [CHANGELOG.md](CHANGELOG.md)). Never `xfail`, `skip`, mute or loosen an assertion to get green: fix the adapter, re-capture the fixture, record it in CHANGELOG + the adapter's docs page.
- Fixtures in `tests/fixtures/<source>/` are real captures; note the capture date and any hand edit in the fixture docstring (pattern: `tests/conftest.py::swpc_plasma_fixture`).

## Timeouts (the 2026-08-30 nightly burned 2 h 33 m on one active ISWA month)

- Job cap `timeout-minutes: 45` in [.github/workflows/ci.yml](.github/workflows/ci.yml) is the backstop — don't raise it, fix the crawl.
- Per-test budget = **2.5× wall clock measured in an active-Sun period**, as `@pytest.mark.timeout(N)`; reference `tests/test_sep_scoreboards.py::test_live_iswa_recent` = 1800 s = 2.5 × 11 m 53 s (2026-08-31). Re-measure with `pytest -m live --no-cov --durations=0 -k <name>`; update the marker **and** its docstring (value + date) in the same PR.
- Crawl cost scales with *matched files*, not window length: keep `_filename_maybe_in_window` ahead of every download and keep live windows small (3 days, not 30).
- Hermetic tests never pace: `tests/test_sep_scoreboards.py` has a module-level autouse fixture that no-ops `RateLimiter.acquire` for non-`live` tests (before 2026-09-02 the suite was 26 min of `asyncio.sleep` — 735 token waits). Live tests keep the real limiter; `tests/test_ratelimit.py` tests the limiter itself.

## Gotchas

- `SourceID.RTSW` ≠ `SourceID.DSCOVR`: the realtime L1 feed is multi-observatory (SOLAR1/IMAP/ACE, spacecraft in `value["observatory"]`); `DSCOVR` survives only on the NCEI/PySPEDAS archive leg. Filtering realtime records on `DSCOVR` silently returns nothing — [docs/adapters/dscovr.md](docs/adapters/dscovr.md) § Routing rule, [docs/adapters/swpc.md](docs/adapters/swpc.md) § The 2026-08 RTSW migration.
- Provenance URLs must be absolute (host included) — relative ISWA paths passed unit tests for months and only surfaced as live failures.
- Release ≠ publish: `gh release create` fires [publish.yml](.github/workflows/publish.yml) → PyPI trusted publishing, which fails while `helios-provenance-spec` is a `git+https` direct reference in `pyproject.toml`.
- Version lives in `src/helios_connectors/__init__.py`; bump `CITATION.cff` `version:` in the same commit.
