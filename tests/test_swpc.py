"""Tests for the NOAA SWPC adapter.

Unit tests use captured fixtures from services.swpc.noaa.gov plus the
GFZ Potsdam and Kyoto WDC archives. The :mark.live integration test is
deselected by default; CI nightly runs ``pytest -m live`` while PRs run
``pytest -m "not live"``.

The critical regression test is :func:`test_fetch_kp_gannon_routes_to_archive`
which asserts that a Gannon-week (May 8-14, 2024) Kp query — months in
the past from any plausible test runtime — does NOT hit
services.swpc.noaa.gov and instead routes to the GFZ Potsdam archive.
This is the gotcha the brief surfaced: SWPC only serves the last ~30
days, and silent truncation of older requests would be a credibility hit.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest

from helios_connectors import SourceID, SwpcAdapter
from helios_connectors.adapters.swpc import (
    GFZ_KP_ARCHIVE_URL,
    SWPC_BASE_URL,
    SWPC_PRODUCTS,
    SWPC_REALTIME_DAYS,
    _coerce_float,
    _g_scale_from_kp,
    _needs_archive,
    _parse_3_day_forecast,
    _parse_gfz_kp,
    _parse_kyoto_dst,
)

GANNON_START = datetime(2024, 5, 8, tzinfo=UTC)
GANNON_END = datetime(2024, 5, 14, tzinfo=UTC)


# ---------------------------------------------------------------------------- #
# Pure helper tests
# ---------------------------------------------------------------------------- #


def test_needs_archive_recent_window_false() -> None:
    now = datetime.now(UTC)
    recent = now - timedelta(days=2)
    assert _needs_archive(recent) is False


def test_needs_archive_old_window_true() -> None:
    assert _needs_archive(GANNON_START) is True


def test_needs_archive_naive_datetime_treated_as_utc() -> None:
    very_old = datetime(2020, 1, 1)  # naive
    assert _needs_archive(very_old) is True


def test_coerce_float_handles_string() -> None:
    assert _coerce_float("2.40") == 2.40


def test_coerce_float_handles_numeric() -> None:
    assert _coerce_float(2.40) == 2.40
    assert _coerce_float(2) == 2.0


def test_coerce_float_handles_none() -> None:
    assert _coerce_float(None) is None
    assert _coerce_float("") is None
    assert _coerce_float("nan") is None
    assert _coerce_float("not-a-number") is None


@pytest.mark.parametrize(
    "kp,expected",
    [
        (0.0, "G0"),
        (4.9, "G0"),
        (5.0, "G1"),
        (5.99, "G1"),
        (6.0, "G2"),
        (7.0, "G3"),
        (8.0, "G4"),
        (8.999, "G4"),
        (9.0, "G5"),
        (9.5, "G5"),
    ],
)
def test_g_scale_from_kp(kp: float, expected: str) -> None:
    assert _g_scale_from_kp(kp) == expected


# ---------------------------------------------------------------------------- #
# GFZ + Kyoto parser tests against real fixtures
# ---------------------------------------------------------------------------- #


def test_parse_gfz_kp_gannon_g5_present(gfz_kp_archive_fixture: str) -> None:
    """The GFZ May 2024 fixture must contain the Gannon G5 Kp=9.0 sample."""
    samples = _parse_gfz_kp(gfz_kp_archive_fixture)
    assert len(samples) >= 31 * 8 - 5, f"expected ~248 samples for May 2024, got {len(samples)}"
    # Find the G5 sample at May 11 00-03UT
    g5 = next((s for s in samples if s[0] == datetime(2024, 5, 11, 0, 0, tzinfo=UTC)), None)
    assert g5 is not None, "May 11 00UT Kp sample not found"
    assert g5[1] == pytest.approx(9.0, abs=0.01), (
        f"expected Kp=9.0 (G5) at Gannon peak, got {g5[1]}"
    )


def test_parse_gfz_kp_skips_comments_and_negatives() -> None:
    text = (
        "# comment line\n"
        "# YYY MM DD ...\n"
        "\n"
        "2024 05 01 1 1.5 1 1 1.000 1.000 1.000 1.000 1.000 1.000 1.000 1.000 "
        "4 4 4 4 4 4 4 4 4 4 100.0 100.0 2\n"
        "2024 05 02 2 2.5 1 1 -1.000 -1.000 -1.000 -1.000 -1.000 -1.000 -1.000 -1.000 "
        "-1 -1 -1 -1 -1 -1 -1 -1 -1 -1 -1.0 -1.0 2\n"
    )
    samples = _parse_gfz_kp(text)
    # Day 1: 8 samples (all valid). Day 2: 0 samples (all -1 sentinels).
    assert len(samples) == 8
    assert all(s[0].day == 1 for s in samples)


def test_parse_kyoto_dst_gannon_peak(kyoto_dst_2405_fixture: str) -> None:
    """The Kyoto May 2024 fixture must contain Dst <= -400 nT during Gannon."""
    samples = _parse_kyoto_dst(kyoto_dst_2405_fixture, year=2024, month=5)
    assert len(samples) >= 24 * 28, f"expected ~720 hourly samples, got {len(samples)}"
    # Find the minimum Dst over May 10-11 (Gannon's superstorm phase)
    storm_window = [
        (ts, dst)
        for ts, dst in samples
        if datetime(2024, 5, 10, tzinfo=UTC) <= ts <= datetime(2024, 5, 12, tzinfo=UTC)
    ]
    min_dst = min(s[1] for s in storm_window)
    assert min_dst <= -400, f"expected Dst <= -400 nT during Gannon, got {min_dst}"


def test_parse_kyoto_dst_skips_sentinels() -> None:
    # Synthetic single-day line with one 9999 sentinel mid-day.
    line = "DST2405*15PPX120" + " " * 4 + "9999" + " 100" * 23 + "  50"
    samples = _parse_kyoto_dst(line, year=2024, month=5)
    # 23 valid hours + 1 sentinel skipped + we ignore the daily mean at col 116
    assert all(v != 9999 for _, v in samples)


# ---------------------------------------------------------------------------- #
# 3-day forecast parser
# ---------------------------------------------------------------------------- #


def test_parse_3_day_forecast_structure(swpc_sep_forecast_fixture: str) -> None:
    forecast = _parse_3_day_forecast(swpc_sep_forecast_fixture)
    assert "issued" in forecast
    assert isinstance(forecast["issued"], datetime)
    assert forecast["issued"].tzinfo is not None
    assert forecast["kp_breakdown"], "expected 8 UT bins of Kp breakdown"
    assert forecast["radiation_storm_probability"], "expected 3 days of S1+ probabilities"
    assert forecast["radio_blackout_probability"], "expected 3 days of R-blackout probabilities"


def test_parse_3_day_forecast_radiation_percent(swpc_sep_forecast_fixture: str) -> None:
    forecast = _parse_3_day_forecast(swpc_sep_forecast_fixture)
    radiation = forecast["radiation_storm_probability"]
    assert len(radiation) == 3
    for row in radiation:
        assert "date" in row and "percent" in row
        assert 0 <= row["percent"] <= 100


def test_parse_3_day_forecast_kp_breakdown_8_bins(swpc_sep_forecast_fixture: str) -> None:
    forecast = _parse_3_day_forecast(swpc_sep_forecast_fixture)
    bins = forecast["kp_breakdown"]
    assert len(bins) == 8
    expected_bins = {f"{h:02d}-{(h + 3) % 24:02d}UT" for h in range(0, 24, 3)}
    assert {row["ut_bin"] for row in bins} == expected_bins


def test_parse_3_day_forecast_missing_issued_returns_now() -> None:
    forecast = _parse_3_day_forecast("no issued header at all")
    # Should still return a datetime, falling back to now()
    assert isinstance(forecast["issued"], datetime)


# ---------------------------------------------------------------------------- #
# Mocked-transport adapter tests
# ---------------------------------------------------------------------------- #


def _swpc_mock_client(
    responses: dict[str, Any],
    *,
    requested_log: list[str] | None = None,
    base_url: str = SWPC_BASE_URL,
) -> httpx.AsyncClient:
    """AsyncClient with MockTransport returning canned responses by path."""

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if requested_log is not None:
            requested_log.append(str(request.url))
        for key, payload in responses.items():
            if path.endswith(key):
                if isinstance(payload, str):
                    return httpx.Response(200, text=payload)
                return httpx.Response(200, json=payload)
        return httpx.Response(404)

    return httpx.AsyncClient(base_url=base_url, transport=httpx.MockTransport(handler))


def _archive_mock_client(
    routes: dict[str, str],
    *,
    requested_log: list[str] | None = None,
) -> httpx.AsyncClient:
    """AsyncClient with MockTransport for the GFZ + Kyoto archive URLs.

    Keys in ``routes`` are partial path matches; values are response text.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        full_url = str(request.url)
        if requested_log is not None:
            requested_log.append(full_url)
        for needle, text in routes.items():
            if needle in full_url:
                return httpx.Response(200, text=text)
        return httpx.Response(404)

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


# ---------------------------------------------------------------------------- #
# Real-time Kp
# ---------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_fetch_kp_realtime_normalizes(
    swpc_kp_realtime_fixture: list[dict[str, Any]],
) -> None:
    """Real-time Kp fetcher must normalize records with provenance + G-scale."""
    # Derive the fixture's timestamp range and use a recent window that
    # captures it. The fixture's earliest entry must be inside the SWPC
    # real-time window (<30 days) for routing to stay on the realtime branch.
    fixture_ts = datetime.fromisoformat(swpc_kp_realtime_fixture[0]["time_tag"]).replace(tzinfo=UTC)
    # Build a window that surrounds the fixture timestamps. The window
    # start must be within the SWPC realtime window from "now" in real
    # wall-clock time so the routing stays on realtime. We set start to
    # the smaller of (now - 1 day, fixture_ts - 1 hour) — whichever keeps
    # the routing branch on realtime AND captures the fixture.
    now = datetime.now(UTC)
    start = min(now - timedelta(days=1), fixture_ts - timedelta(hours=1))
    end = max(now + timedelta(days=1), fixture_ts + timedelta(days=1))
    client = _swpc_mock_client({SWPC_PRODUCTS["kp"]: swpc_kp_realtime_fixture})
    async with SwpcAdapter(client=client, cache=False) as swpc:
        records = [r async for r in swpc.fetch_kp(start=start, end=end)]
    assert records, "expected non-empty Kp record stream"
    for rec in records:
        assert rec.source == SourceID.SWPC
        assert rec.record_type == "kp"
        assert rec.event_time.tzinfo is not None
        assert "kp" in rec.value
        assert "g_scale" in rec.value
        assert rec.provenance.model_id == "swpc/kp"
        assert rec.provenance.extra is not None
        assert rec.provenance.extra["lineage"] == ["swpc/kp"]


@pytest.mark.asyncio
async def test_fetch_kp_realtime_filters_outside_window(
    swpc_kp_realtime_fixture: list[dict[str, Any]],
) -> None:
    """Records outside [start, end] must be filtered out.

    We use a window that's recent enough to stay on the realtime branch
    (and thus exercise the realtime filter code path) but that doesn't
    overlap the fixture's actual timestamps.
    """
    client = _swpc_mock_client({SWPC_PRODUCTS["kp"]: swpc_kp_realtime_fixture})
    fixture_ts = datetime.fromisoformat(swpc_kp_realtime_fixture[0]["time_tag"]).replace(tzinfo=UTC)
    # Window 5 days after the fixture, still well within 30 days of "now".
    start = fixture_ts + timedelta(days=5)
    end = start + timedelta(hours=6)
    async with SwpcAdapter(client=client, cache=False) as swpc:
        records = [r async for r in swpc.fetch_kp(start=start, end=end)]
    # The fixture rows are all at 2026-05-10/11, which is before [start,end].
    # We should get zero records.
    assert records == []


# ---------------------------------------------------------------------------- #
# CRITICAL REGRESSION TEST: Gannon-week Kp must route to GFZ
# ---------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_fetch_kp_gannon_routes_to_archive(
    gfz_kp_archive_fixture: str,
    swpc_kp_realtime_fixture: list[dict[str, Any]],
) -> None:
    """Gannon-week Kp query MUST hit GFZ, NOT services.swpc.noaa.gov.

    This is the brief's critical regression test: SWPC's public archive
    only serves ~30 days; a Gannon-window query that silently truncated
    to the latest 30 days would be a credibility failure.
    """
    swpc_requests: list[str] = []
    archive_requests: list[str] = []

    swpc_client = _swpc_mock_client(
        {SWPC_PRODUCTS["kp"]: swpc_kp_realtime_fixture},
        requested_log=swpc_requests,
    )
    archive_client = _archive_mock_client(
        {GFZ_KP_ARCHIVE_URL: gfz_kp_archive_fixture},
        requested_log=archive_requests,
    )
    async with SwpcAdapter(client=swpc_client, archive_client=archive_client, cache=False) as swpc:
        records = [r async for r in swpc.fetch_kp(start=GANNON_START, end=GANNON_END)]

    # SWPC realtime endpoint MUST NOT have been called.
    assert not swpc_requests, (
        f"SWPC realtime endpoint should NOT be hit for Gannon-week query; "
        f"saw {len(swpc_requests)} requests: {swpc_requests}"
    )
    # GFZ archive endpoint MUST have been called.
    assert any(GFZ_KP_ARCHIVE_URL in u for u in archive_requests), (
        f"GFZ archive endpoint not hit; saw archive requests: {archive_requests}"
    )
    # Records returned must span Gannon week (~6 days * 8 bins + 1 = ~49).
    # End is 2024-05-14T00:00 so the 14th-day bins after midnight are excluded.
    assert len(records) >= 6 * 8
    assert all(r.source == SourceID.SWPC for r in records)
    assert all(r.record_type == "kp" for r in records)
    # Lineage must record the GFZ archive provider.
    assert all(
        r.provenance.extra is not None
        and any("GFZ" in segment for segment in r.provenance.extra["lineage"])
        for r in records
    )
    # Verify the G5 record is present and correctly labeled.
    g5 = [r for r in records if r.value["g_scale"] == "G5"]
    assert g5, "expected at least one G5 record during Gannon week"
    # Verify Kp peaks at 9.0
    peak = max(r.value["kp"] for r in records)
    assert peak == pytest.approx(9.0, abs=0.01)


@pytest.mark.asyncio
async def test_fetch_kp_archive_lineage_includes_gfz(
    gfz_kp_archive_fixture: str,
) -> None:
    """Records from the archive branch must carry GFZ Potsdam in lineage."""
    archive_client = _archive_mock_client({GFZ_KP_ARCHIVE_URL: gfz_kp_archive_fixture})
    async with SwpcAdapter(archive_client=archive_client, cache=False) as swpc:
        records = [
            r
            async for r in swpc.fetch_kp(
                start=datetime(2024, 5, 10, tzinfo=UTC),
                end=datetime(2024, 5, 11, tzinfo=UTC),
            )
        ]
    assert records
    for rec in records:
        assert rec.provenance.extra is not None
        assert "GFZ Potsdam/Kp_ap_Ap_SN_F107_since_1932.txt" in rec.provenance.extra["lineage"]
        assert rec.provenance.dataset_refs == [GFZ_KP_ARCHIVE_URL]


# ---------------------------------------------------------------------------- #
# Dst via Kyoto WDC
# ---------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_fetch_dst_gannon_via_kyoto(kyoto_dst_2405_fixture: str) -> None:
    """fetch_dst must produce hourly Dst records with Kyoto WDC lineage."""
    archive_client = _archive_mock_client({"dst_provisional/202405": kyoto_dst_2405_fixture})
    async with SwpcAdapter(archive_client=archive_client, cache=False) as swpc:
        records = [r async for r in swpc.fetch_dst(start=GANNON_START, end=GANNON_END)]
    assert records
    assert all(r.record_type == "dst" for r in records)
    assert all(r.value_units == "nT" for r in records)
    assert all(
        r.provenance.extra is not None
        and any("Kyoto WDC" in segment for segment in r.provenance.extra["lineage"])
        for r in records
    )
    # Min Dst during Gannon should be at most -400 nT
    min_dst = min(r.value["dst"] for r in records)
    assert min_dst <= -400


@pytest.mark.asyncio
async def test_fetch_dst_recent_uses_provisional(kyoto_dst_2405_fixture: str) -> None:
    """A 'recent' window (<30d) should hit provisional Kyoto, not final."""
    requested: list[str] = []
    archive_client = _archive_mock_client(
        {"dst_provisional": kyoto_dst_2405_fixture},
        requested_log=requested,
    )
    now = datetime.now(UTC)
    async with SwpcAdapter(archive_client=archive_client, cache=False) as swpc:
        # Set a window that hits the May 2024 fixture month so we get data,
        # but with a start date that's NOT-needs-archive: i.e. less than 30
        # days old. We'll fake this by setting start=now-1day; the mock will
        # still return the May 2024 file, but with a fake "now" the URL
        # tier-selection is what we're verifying, not the data content.
        _ = [r async for r in swpc.fetch_dst(start=now - timedelta(days=1), end=now)]
    # The provisional template must have been requested first.
    assert any("dst_provisional" in u for u in requested), requested
    assert not any("dst_final" in u for u in requested), requested


# ---------------------------------------------------------------------------- #
# Plasma + Mag (columnar)
# ---------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_fetch_plasma_columnar(
    swpc_plasma_fixture: list[list[Any]],
) -> None:
    """Plasma fetcher must parse header-as-first-row JSON arrays correctly."""
    client = _swpc_mock_client({SWPC_PRODUCTS["plasma"]: swpc_plasma_fixture})
    # Use a very wide window so all fixture rows pass the filter.
    async with SwpcAdapter(client=client, cache=False) as swpc:
        records = [
            r
            async for r in swpc.fetch_plasma(
                start=datetime(2020, 1, 1, tzinfo=UTC),
                end=datetime(2099, 1, 1, tzinfo=UTC),
            )
        ]
    assert records
    sample = records[0]
    assert sample.source == SourceID.SWPC
    assert sample.record_type == "plasma"
    # Expected float columns: density, speed, temperature
    assert "density" in sample.value
    assert "speed" in sample.value
    assert "temperature" in sample.value
    # Values must be parsed as floats from their string representation.
    assert isinstance(sample.value["density"], float)
    assert isinstance(sample.value["speed"], float)


@pytest.mark.asyncio
async def test_fetch_mag_columnar(swpc_mag_fixture: list[list[Any]]) -> None:
    client = _swpc_mock_client({SWPC_PRODUCTS["mag"]: swpc_mag_fixture})
    async with SwpcAdapter(client=client, cache=False) as swpc:
        records = [
            r
            async for r in swpc.fetch_mag(
                start=datetime(2020, 1, 1, tzinfo=UTC),
                end=datetime(2099, 1, 1, tzinfo=UTC),
            )
        ]
    assert records
    sample = records[0]
    assert sample.source == SourceID.SWPC
    assert sample.record_type == "mag"
    assert sample.value_units == "nT"
    assert "bz_gsm" in sample.value
    assert "bt" in sample.value


@pytest.mark.asyncio
async def test_fetch_plasma_archive_window_warns(
    swpc_plasma_fixture: list[list[Any]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """fetch_plasma with old start must log a deferral warning."""
    client = _swpc_mock_client({SWPC_PRODUCTS["plasma"]: swpc_plasma_fixture})
    caplog.set_level(logging.WARNING, logger="helios_connectors.adapters.swpc")
    async with SwpcAdapter(client=client, cache=False) as swpc:
        _ = [
            r
            async for r in swpc.fetch_plasma(
                start=GANNON_START, end=datetime(2099, 1, 1, tzinfo=UTC)
            )
        ]
    assert any("older than the SWPC real-time window" in r.getMessage() for r in caplog.records)


# ---------------------------------------------------------------------------- #
# GOES protons + SEP forecast
# ---------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_fetch_goes_protons_listdict(
    swpc_protons_fixture: list[dict[str, Any]],
) -> None:
    client = _swpc_mock_client({SWPC_PRODUCTS["goes_protons"]: swpc_protons_fixture})
    async with SwpcAdapter(client=client, cache=False) as swpc:
        records = [
            r
            async for r in swpc.fetch_goes_protons(
                start=datetime(2020, 1, 1, tzinfo=UTC),
                end=datetime(2099, 1, 1, tzinfo=UTC),
            )
        ]
    assert records
    sample = records[0]
    assert sample.source == SourceID.SWPC
    assert sample.record_type == "proton"
    assert sample.value_units == "pfu"
    assert "flux" in sample.value


@pytest.mark.asyncio
async def test_fetch_sep_forecast(swpc_sep_forecast_fixture: str) -> None:
    client = _swpc_mock_client({SWPC_PRODUCTS["sep_forecast"]: swpc_sep_forecast_fixture})
    async with SwpcAdapter(client=client, cache=False) as swpc:
        records = [r async for r in swpc.fetch_sep_forecast()]
    assert len(records) == 1
    rec = records[0]
    assert rec.source == SourceID.SWPC
    assert rec.record_type == "sep_forecast"
    assert rec.value_units == "percent"
    assert rec.value["radiation_storm_probability"]
    assert rec.event_time.tzinfo is not None
    assert rec.provenance.model_id == "swpc/sep_forecast"


# ---------------------------------------------------------------------------- #
# Unified fetch
# ---------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_unified_fetch_dispatches_across_products(
    swpc_kp_realtime_fixture: list[dict[str, Any]],
    swpc_plasma_fixture: list[list[Any]],
    swpc_mag_fixture: list[list[Any]],
    swpc_protons_fixture: list[dict[str, Any]],
    swpc_sep_forecast_fixture: str,
) -> None:
    client = _swpc_mock_client(
        {
            SWPC_PRODUCTS["kp"]: swpc_kp_realtime_fixture,
            SWPC_PRODUCTS["plasma"]: swpc_plasma_fixture,
            SWPC_PRODUCTS["mag"]: swpc_mag_fixture,
            SWPC_PRODUCTS["goes_protons"]: swpc_protons_fixture,
            SWPC_PRODUCTS["sep_forecast"]: swpc_sep_forecast_fixture,
        }
    )
    now = datetime.now(UTC)
    # Window covering both fixture timestamps and "now" while staying inside
    # the SWPC real-time window (else routing flips to GFZ archive).
    start = now - timedelta(days=SWPC_REALTIME_DAYS - 1)
    end = now + timedelta(days=365)
    async with SwpcAdapter(client=client, cache=False) as swpc:
        records = [
            r
            async for r in swpc.fetch(
                start=start,
                end=end,
                products=["kp", "plasma", "mag", "goes_protons", "sep_forecast"],
            )
        ]
    types = {r.record_type for r in records}
    # Each branch should at minimum produce some records (some Kp fixture
    # entries may be outside the window; that's fine, just need ≥1).
    assert "plasma" in types
    assert "mag" in types
    assert "proton" in types
    assert "sep_forecast" in types


@pytest.mark.asyncio
async def test_unified_fetch_unknown_product_raises() -> None:
    async with SwpcAdapter(cache=False) as swpc:
        agen = swpc.fetch(start=GANNON_START, end=GANNON_END, products=["bogus"])
        with pytest.raises(ValueError, match="unknown SWPC products"):
            await agen.__anext__()


# ---------------------------------------------------------------------------- #
# Lifecycle + rate limit
# ---------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_default_rate_limit_5_rps() -> None:
    async with SwpcAdapter(cache=False) as swpc:
        assert swpc._ratelimiter.config.rate_per_second == 5.0


@pytest.mark.asyncio
async def test_archive_rate_limit_1_rps() -> None:
    async with SwpcAdapter(cache=False) as swpc:
        assert swpc._archive_ratelimiter.config.rate_per_second == 1.0


@pytest.mark.asyncio
async def test_aclose_closes_archive_client() -> None:
    """aclose() must close the archive httpx client when owned by adapter."""
    swpc = SwpcAdapter(cache=False)
    archive_client = swpc._archive_client
    await swpc.aclose()
    assert archive_client.is_closed


@pytest.mark.asyncio
async def test_kp_realtime_bad_response_raises() -> None:
    """Non-list JSON response on Kp endpoint must raise DecodingError."""

    def handler(_req: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"oops": "scalar"})

    client = httpx.AsyncClient(base_url=SWPC_BASE_URL, transport=httpx.MockTransport(handler))
    now = datetime.now(UTC)
    async with SwpcAdapter(client=client, cache=False) as swpc:
        with pytest.raises(httpx.DecodingError):
            _ = [r async for r in swpc.fetch_kp(start=now - timedelta(days=1), end=now)]


@pytest.mark.asyncio
async def test_plasma_bad_response_raises() -> None:
    """Empty list response on plasma endpoint must raise DecodingError."""

    def handler(_req: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[])

    client = httpx.AsyncClient(base_url=SWPC_BASE_URL, transport=httpx.MockTransport(handler))
    now = datetime.now(UTC)
    async with SwpcAdapter(client=client, cache=False) as swpc:
        with pytest.raises(httpx.DecodingError):
            _ = [r async for r in swpc.fetch_plasma(start=now - timedelta(days=1), end=now)]


# ---------------------------------------------------------------------------- #
# Live integration test (off by default)
# ---------------------------------------------------------------------------- #


@pytest.mark.live
@pytest.mark.asyncio
async def test_live_swpc_kp_plasma_last_day() -> None:
    """Hit real services.swpc.noaa.gov for the last 24 hours of Kp + plasma."""
    now = datetime.now(UTC)
    async with SwpcAdapter(cache=False) as swpc:
        kp_records = [r async for r in swpc.fetch_kp(start=now - timedelta(days=1), end=now)]
        plasma_records = [
            r async for r in swpc.fetch_plasma(start=now - timedelta(hours=2), end=now)
        ]
    # We should get a handful of Kp readings (3-hourly) and many plasma readings (minutely).
    assert kp_records, "expected at least one Kp reading in the last 24h"
    assert plasma_records, "expected plasma readings in the last 2h"
