"""Tests for the CCMC SEP Scoreboards adapter.

Architecture under test:

* **Apache directory-listing walk** — mocked via ``httpx.MockTransport``;
  fixture listing in ``tests/fixtures/sep_scoreboards/listing-*.html``
  matches the real ISWA mod_autoindex shape.
* **Per-model JSON envelope retrieval** — mocked alongside the listing
  walks; JSON fixtures match the real
  ``sep_forecast_submission`` schema from CCMC's documentation example.

The critical regression test is :func:`test_no_url_contains_release_or_hesperia`
which sweeps every URL the adapter would request for a standard
``fetch_scoreboard_a`` call and asserts none contains a forbidden token.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest

from helios_connectors import SepScoreboardsAdapter, SourceID
from helios_connectors.adapters.sep_scoreboards import (
    ALL_SCOREBOARDS,
    FORBIDDEN_PATH_TOKENS,
    ISWA_BASE_URL,
    ISWA_SCOREBOARD_PREFIX,
    SCOREBOARD_MODELS,
    ScoreboardModelSpec,
    _assert_no_hesperia_release,
    _coerce_energy_channel,
    _coerce_issue_time,
    _ensure_utc,
    _expand_forecasts,
    _filename_maybe_in_window,
    _lineage_for,
    _maybe_float,
    _maybe_isoparse,
    _model_short_name,
    _months_in_window,
    _parse_listing,
    _record_id,
    _scoreboard_a_record,
    _scoreboard_b_record,
    _scoreboard_c_record,
    _spase_id,
)

FIXTURES = Path(__file__).parent / "fixtures" / "sep_scoreboards"

GANNON_START = datetime(2024, 5, 8, tzinfo=UTC)
GANNON_END = datetime(2024, 5, 14, tzinfo=UTC)

SEP2017_START = datetime(2017, 9, 6, tzinfo=UTC)
SEP2017_END = datetime(2017, 9, 11, tzinfo=UTC)


# ---------------------------------------------------------------------------- #
# fixtures
# ---------------------------------------------------------------------------- #


@pytest.fixture(scope="session")
def scoreboard_a_recent_payload() -> dict[str, Any]:
    return json.loads((FIXTURES / "scoreboard-a-recent.json").read_text())


@pytest.fixture(scope="session")
def scoreboard_b_recent_payload() -> dict[str, Any]:
    return json.loads((FIXTURES / "scoreboard-b-recent.json").read_text())


@pytest.fixture(scope="session")
def scoreboard_c_recent_payload() -> dict[str, Any]:
    return json.loads((FIXTURES / "scoreboard-c-recent.json").read_text())


@pytest.fixture(scope="session")
def scoreboard_a_sep2017_payload() -> dict[str, Any]:
    return json.loads((FIXTURES / "scoreboard-a-sep2017.json").read_text())


@pytest.fixture(scope="session")
def scoreboard_b_sep2017_payload() -> dict[str, Any]:
    return json.loads((FIXTURES / "scoreboard-b-sep2017.json").read_text())


@pytest.fixture(scope="session")
def listing_umasep_2024_05_html() -> str:
    return (FIXTURES / "listing-umasep-10-2024-05.html").read_text()


# ---------------------------------------------------------------------------- #
# pure helper tests
# ---------------------------------------------------------------------------- #


def test_ensure_utc_naive_treated_as_utc() -> None:
    ts = datetime(2024, 5, 10, 12, 0)
    assert _ensure_utc(ts).tzinfo is UTC


def test_ensure_utc_aware_converted() -> None:
    import datetime as _dt

    tz = _dt.timezone(_dt.timedelta(hours=3))
    ts = datetime(2024, 5, 10, 23, tzinfo=tz)
    out = _ensure_utc(ts)
    assert out.tzinfo is UTC
    assert out.hour == 20


def test_months_in_window_single_month() -> None:
    months = _months_in_window(datetime(2024, 5, 8), datetime(2024, 5, 14))
    assert months == [(2024, "05")]


def test_months_in_window_multi_month_year_boundary() -> None:
    months = _months_in_window(datetime(2023, 12, 28), datetime(2024, 2, 3))
    assert months == [(2023, "12"), (2024, "01"), (2024, "02")]


def test_maybe_float_handles_strings_and_bools() -> None:
    assert _maybe_float("1.5") == 1.5
    assert _maybe_float(1) == 1.0
    assert _maybe_float(True) is None
    assert _maybe_float("not a number") is None
    assert _maybe_float(None) is None


def test_maybe_isoparse_normalizes_utc() -> None:
    out = _maybe_isoparse("2017-09-06T10:00Z")
    assert out is not None
    assert out.tzinfo is UTC
    assert out.year == 2017


def test_maybe_isoparse_returns_none_on_bad_input() -> None:
    assert _maybe_isoparse(None) is None
    assert _maybe_isoparse("not a date") is None
    assert _maybe_isoparse(42) is None


def test_coerce_energy_channel_strict() -> None:
    assert _coerce_energy_channel({"min": 10, "max": -1, "units": "MeV"}) == (
        10.0,
        -1.0,
        "MeV",
    )
    assert _coerce_energy_channel(None) == (None, None, "MeV")
    assert _coerce_energy_channel("garbage") == (None, None, "MeV")


def test_coerce_issue_time_falls_back_to_now() -> None:
    out = _coerce_issue_time({})
    assert out.tzinfo is UTC


def test_coerce_issue_time_uses_issue_time(
    scoreboard_a_recent_payload: dict[str, Any],
) -> None:
    env = scoreboard_a_recent_payload["sep_forecast_submission"]
    ts = _coerce_issue_time(env)
    assert ts.year == 2024
    assert ts.month == 5


def test_coerce_issue_time_falls_back_to_prediction_window() -> None:
    env = {
        "forecasts": [
            {"prediction_window": {"start_time": "2024-05-10T19:30Z"}},
        ]
    }
    ts = _coerce_issue_time(env)
    assert ts == datetime(2024, 5, 10, 19, 30, tzinfo=UTC)


def test_model_short_name_and_spase(
    scoreboard_a_recent_payload: dict[str, Any],
) -> None:
    env = scoreboard_a_recent_payload["sep_forecast_submission"]
    assert _model_short_name(env) == "UMASEP-10"
    spase = _spase_id(env)
    assert spase is not None
    assert spase.startswith("spase://")


def test_model_short_name_unknown_when_missing() -> None:
    assert _model_short_name({}) == "unknown"
    assert _spase_id({}) is None


def test_filename_prefilter_keeps_in_window_umasep_style() -> None:
    start = datetime(2026, 8, 29, tzinfo=UTC)
    end = datetime(2026, 8, 31, tzinfo=UTC)
    fname = "UMASEP10_prediction_2026_08_30_000517__2026_08_30_000931.json"
    assert _filename_maybe_in_window(fname, start, end)


def test_filename_prefilter_drops_out_of_window() -> None:
    start = datetime(2026, 8, 29, tzinfo=UTC)
    end = datetime(2026, 8, 31, tzinfo=UTC)
    fname = "UMASEP10_prediction_2026_08_01_000517__2026_08_01_000931.json"
    assert not _filename_maybe_in_window(fname, start, end)


def test_filename_prefilter_keeps_boundary_via_pad() -> None:
    # end+1d pad: a file dated the day after the window still fetches.
    start = datetime(2026, 8, 29, tzinfo=UTC)
    end = datetime(2026, 8, 31, tzinfo=UTC)
    fname = "UMASEP10_prediction_2026_09_01_000000__2026_09_01_000100.json"
    assert _filename_maybe_in_window(fname, start, end)


def test_filename_prefilter_compact_sepster_style() -> None:
    start = datetime(2024, 5, 1, tzinfo=UTC)
    end = datetime(2024, 5, 2, tzinfo=UTC)
    assert _filename_maybe_in_window(
        "sepster_20240501_0636_0794_Parker_Spiral_iss_20240501_1547.json", start, end
    )
    assert not _filename_maybe_in_window(
        "sepster_20240401_0636_0794_Parker_Spiral_iss_20240401_1547.json", start, end
    )


def test_filename_prefilter_fails_open_without_dates() -> None:
    start = datetime(2026, 8, 29, tzinfo=UTC)
    end = datetime(2026, 8, 31, tzinfo=UTC)
    # No parseable date tokens -> must fetch (fail-open), including digit
    # runs that are not plausible dates.
    assert _filename_maybe_in_window("model_output_final.json", start, end)
    assert _filename_maybe_in_window("run_0774_9912_31.json", start, end)


def test_lineage_includes_source_url_model_and_triggers(
    scoreboard_c_recent_payload: dict[str, Any],
) -> None:
    env = scoreboard_c_recent_payload["sep_forecast_submission"]
    lineage = _lineage_for(env, "https://iswa.example/sep.json")
    assert "https://iswa.example/sep.json" in lineage
    assert any("UMASEP-10" in s for s in lineage)
    # triggers carry the DONKI CME catalog_id
    assert any("CME-001" in s for s in lineage)


def test_record_id_includes_scoreboard_model_energy(
    scoreboard_a_recent_payload: dict[str, Any],
) -> None:
    env = scoreboard_a_recent_payload["sep_forecast_submission"]
    rid = _record_id(env, "A", 10.0)
    assert rid.startswith("sep_scoreboard_a/UMASEP-10/10MeV/")


# ---------------------------------------------------------------------------- #
# HESPERIA REleASE exclusion (regression-critical)
# ---------------------------------------------------------------------------- #


def test_assert_no_hesperia_release_passes_clean_path() -> None:
    _assert_no_hesperia_release(
        "/iswa_data_tree/model/heliosphere/sep_scoreboard/UMASEP/v3_X/10MeV/2024/05/"
    )


def test_assert_no_hesperia_release_rejects_release() -> None:
    with pytest.raises(ValueError, match="release"):
        _assert_no_hesperia_release(
            "/iswa_data_tree/model/heliosphere/sep_scoreboard/RELEASE/Alert/"
        )


def test_assert_no_hesperia_release_rejects_release_plus() -> None:
    with pytest.raises(ValueError, match="release"):
        _assert_no_hesperia_release("/heliosphere/sep_scoreboard/RELEASE_PLUS/foo.json")


def test_assert_no_hesperia_release_rejects_hesperia() -> None:
    # Use a path without "release" so we hit the hesperia branch deterministically
    with pytest.raises(ValueError, match="hesperia"):
        _assert_no_hesperia_release("/some/HESPERIA_obs/file.json")


def test_assert_no_hesperia_release_is_case_insensitive() -> None:
    with pytest.raises(ValueError):
        _assert_no_hesperia_release("/heliosphere/Release/file.json")


def test_default_scoreboard_models_exclude_release() -> None:
    for spec in SCOREBOARD_MODELS:
        path = spec.base_path()
        for token in FORBIDDEN_PATH_TOKENS:
            assert token not in path.lower(), (
                f"default model registry contains forbidden token {token!r} via {path!r}"
            )


def test_adapter_rejects_release_spec_at_construction_time() -> None:
    bad = (ScoreboardModelSpec(name="RELEASE", variants=("Alert",), energies=("10MeV",)),)
    with pytest.raises(ValueError, match="release"):
        SepScoreboardsAdapter(client=httpx.AsyncClient(), cache=False, models=bad)


def test_no_url_contains_release_or_hesperia(
    scoreboard_a_recent_payload: dict[str, Any],
    listing_umasep_2024_05_html: str,
) -> None:
    """Sweep every URL the adapter would touch for a standard fetch.

    This is the regression test the proposal cares about. We
    instantiate the default adapter, drive a real fetch through a
    record-every-URL mock transport, then assert no path contains
    a forbidden token (case-insensitive).
    """

    requested_urls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requested_urls.append(str(request.url))
        # Returning empty listings keeps the walk short while still
        # exercising the full URL generation path.
        if request.url.path.endswith("/"):
            return httpx.Response(
                200,
                text='<html><body><a href="?C=N;O=D">Name</a></body></html>',
            )
        return httpx.Response(200, json=scoreboard_a_recent_payload)

    client = httpx.AsyncClient(
        base_url=ISWA_BASE_URL,
        transport=httpx.MockTransport(handler),
    )

    import asyncio as _asyncio

    async def runner() -> None:
        async with SepScoreboardsAdapter(client=client, cache=False) as sb:
            _ = [r async for r in sb.fetch(start=GANNON_START, end=GANNON_END)]

    _asyncio.run(runner())

    assert requested_urls, "expected some URL traffic"
    for url in requested_urls:
        lowered = url.lower()
        for token in FORBIDDEN_PATH_TOKENS:
            assert token not in lowered, (
                f"forbidden token {token!r} found in requested URL: {url!r}"
            )


# ---------------------------------------------------------------------------- #
# listing parser
# ---------------------------------------------------------------------------- #


def test_parse_listing_basic() -> None:
    html = """<html><body>
    <a href="?C=N;O=D">Name</a>
    <a href="/parent/">Parent Directory</a>
    <a href="subdir/">subdir</a>
    <a href="UMASEP10_prediction_2024_05_01_000516__2024_05_01_000920.json">file</a>
    <a href="UMASEP10_prediction_2024_05_01_001003__2024_05_01_001216.json">file2</a>
    </body></html>
    """
    subdirs, files = _parse_listing(html)
    assert subdirs == ["subdir"]
    assert len(files) == 2
    assert all(f.endswith(".json") for f in files)


def test_parse_listing_real_apache_index(listing_umasep_2024_05_html: str) -> None:
    subdirs, files = _parse_listing(listing_umasep_2024_05_html)
    # Trimmed fixture should still have many json files
    assert all(f.endswith(".json") for f in files)
    assert len(files) >= 5
    # No parent-dir or sort-control links pollute the lists
    assert "/" not in files
    for s in subdirs:
        assert not s.startswith("/")


# ---------------------------------------------------------------------------- #
# forecast expansion + per-board normalization
# ---------------------------------------------------------------------------- #


def test_expand_forecasts_parses_envelope(
    scoreboard_c_recent_payload: dict[str, Any],
) -> None:
    env = scoreboard_c_recent_payload["sep_forecast_submission"]
    forecasts = _expand_forecasts(env)
    assert len(forecasts) == 2
    assert forecasts[0].energy_min == 10.0
    assert forecasts[0].energy_max == -1.0
    assert forecasts[1].energy_min == 100.0


def test_expand_forecasts_empty_when_no_forecasts() -> None:
    assert _expand_forecasts({}) == []
    assert _expand_forecasts({"forecasts": "not a list"}) == []
    assert _expand_forecasts({"forecasts": [None, 42, "junk"]}) == []


def _adapter_for_normalize() -> SepScoreboardsAdapter:
    # Disposable adapter just to access `_emit_provenance` from the
    # per-board record helpers. We don't need a real client.
    return SepScoreboardsAdapter(
        client=httpx.AsyncClient(transport=httpx.MockTransport(lambda _r: httpx.Response(404))),
        cache=False,
    )


def test_scoreboard_a_record_from_probabilities(
    scoreboard_c_recent_payload: dict[str, Any],
) -> None:
    adapter = _adapter_for_normalize()
    env = scoreboard_c_recent_payload["sep_forecast_submission"]
    fc = _expand_forecasts(env)[0]
    rec = _scoreboard_a_record(
        adapter, fc, "https://iswa.example/sep.json", _ensure_utc(datetime(2024, 5, 10))
    )
    assert rec is not None
    assert rec.source is SourceID.SEP_SCOREBOARD_A
    assert rec.value["probability"] == 0.85
    assert rec.value["threshold"] == 10
    assert rec.value["scoreboard"] == "A"
    assert rec.value["model"] == "UMASEP-10"
    # The two-row probabilities array also lives in all_thresholds
    assert len(rec.value["all_thresholds"]) == 2
    assert rec.value_units == "probability"


def test_scoreboard_a_record_from_all_clear(
    scoreboard_a_recent_payload: dict[str, Any],
) -> None:
    """The UMASEP recent fixture has only an `all_clear` boolean (true).

    Scoreboard A must still produce a record with probability 0.0 (since
    all_clear=True ⇒ low onset probability).
    """

    adapter = _adapter_for_normalize()
    env = scoreboard_a_recent_payload["sep_forecast_submission"]
    fc = _expand_forecasts(env)[0]
    rec = _scoreboard_a_record(
        adapter, fc, "https://iswa.example/sep.json", _ensure_utc(datetime(2024, 5, 1))
    )
    assert rec is not None
    assert rec.value["probability"] == 0.0
    assert rec.value["from_all_clear"] is True


def test_scoreboard_a_record_returns_none_when_no_probability_data() -> None:
    adapter = _adapter_for_normalize()
    # An envelope with no probabilities and no all_clear → None
    env = {
        "model": {"short_name": "fake"},
        "forecasts": [
            {
                "energy_channel": {"min": 10, "max": -1, "units": "MeV"},
                "species": "proton",
                "location": "earth",
                "prediction_window": {
                    "start_time": "2024-05-10T00:00Z",
                    "end_time": "2024-05-11T00:00Z",
                },
                "peak_intensity": {"intensity": 1.0, "units": "pfu"},
            }
        ],
    }
    fc = _expand_forecasts(env)[0]
    rec = _scoreboard_a_record(
        adapter, fc, "https://iswa.example/sep.json", _ensure_utc(datetime(2024, 5, 10))
    )
    assert rec is None


def test_scoreboard_b_record_normalizes_intensity(
    scoreboard_b_recent_payload: dict[str, Any],
) -> None:
    adapter = _adapter_for_normalize()
    env = scoreboard_b_recent_payload["sep_forecast_submission"]
    forecasts = _expand_forecasts(env)
    # Pick the 10 MeV pfu forecast (index 1 in this fixture)
    target = next(f for f in forecasts if f.energy_min == 10.0 and f.energy_units == "MeV")
    rec = _scoreboard_b_record(
        adapter, target, "https://iswa.example/sep.json", _ensure_utc(datetime(2024, 5, 1))
    )
    assert rec is not None
    assert rec.source is SourceID.SEP_SCOREBOARD_B
    assert rec.value["intensity"] == pytest.approx(4.51)
    assert rec.value_units == "pfu"
    assert rec.value["scoreboard"] == "B"


def test_scoreboard_b_record_returns_none_without_peak_intensity() -> None:
    adapter = _adapter_for_normalize()
    env = {
        "model": {"short_name": "fake"},
        "forecasts": [
            {
                "energy_channel": {"min": 10, "max": -1, "units": "MeV"},
                "species": "proton",
                "location": "earth",
                "prediction_window": {
                    "start_time": "2024-05-10T00:00Z",
                    "end_time": "2024-05-11T00:00Z",
                },
                "probabilities": [
                    {"probability_value": 0.1, "threshold": 10, "threshold_units": "pfu"}
                ],
            }
        ],
    }
    fc = _expand_forecasts(env)[0]
    rec = _scoreboard_b_record(
        adapter, fc, "https://iswa.example/sep.json", _ensure_utc(datetime(2024, 5, 10))
    )
    assert rec is None


def test_scoreboard_c_record_from_event_lengths(
    scoreboard_c_recent_payload: dict[str, Any],
) -> None:
    adapter = _adapter_for_normalize()
    env = scoreboard_c_recent_payload["sep_forecast_submission"]
    fc = _expand_forecasts(env)[0]
    rec = _scoreboard_c_record(
        adapter, fc, "https://iswa.example/sep.json", _ensure_utc(datetime(2024, 5, 10))
    )
    assert rec is not None
    assert rec.source is SourceID.SEP_SCOREBOARD_C
    assert rec.value["onset_time"] is not None
    assert rec.value["crossing_time"] is not None
    assert rec.value["sep_profile"] == "gannon.10MeV.txt"
    assert rec.value["scoreboard"] == "C"


def test_scoreboard_c_record_returns_none_without_profile_data() -> None:
    adapter = _adapter_for_normalize()
    env = {
        "model": {"short_name": "fake"},
        "forecasts": [
            {
                "energy_channel": {"min": 10, "max": -1, "units": "MeV"},
                "species": "proton",
                "location": "earth",
                "prediction_window": {
                    "start_time": "2024-05-10T00:00Z",
                    "end_time": "2024-05-11T00:00Z",
                },
                "peak_intensity": {"intensity": 1.0, "units": "pfu"},
            }
        ],
    }
    fc = _expand_forecasts(env)[0]
    rec = _scoreboard_c_record(
        adapter, fc, "https://iswa.example/sep.json", _ensure_utc(datetime(2024, 5, 10))
    )
    assert rec is None


# ---------------------------------------------------------------------------- #
# End-to-end fetch with mocked transport
# ---------------------------------------------------------------------------- #


def _make_mock_transport(
    *,
    listing_html: str | None = None,
    json_payload: dict[str, Any] | None = None,
    fail_listings: bool = False,
) -> httpx.MockTransport:
    """Build a mock transport that serves listings for directory URLs
    (paths ending in ``/``) and JSON for file URLs."""

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/"):
            if fail_listings:
                return httpx.Response(404)
            if listing_html is not None and "/UMASEP/" in request.url.path:
                return httpx.Response(200, text=listing_html)
            # Default: empty listing
            return httpx.Response(200, text='<html><body><a href="?C=N;O=D">N</a></body></html>')
        if request.url.path.endswith(".json") and json_payload is not None:
            return httpx.Response(200, json=json_payload)
        return httpx.Response(404)

    return httpx.MockTransport(handler)


@pytest.mark.asyncio
async def test_fetch_scoreboard_a_end_to_end(
    listing_umasep_2024_05_html: str,
    scoreboard_a_recent_payload: dict[str, Any],
) -> None:
    transport = _make_mock_transport(
        listing_html=listing_umasep_2024_05_html,
        json_payload=scoreboard_a_recent_payload,
    )
    client = httpx.AsyncClient(base_url=ISWA_BASE_URL, transport=transport)
    # The recent fixture's issue_time is 2024-05-01, so use a full-month window
    month_start = datetime(2024, 5, 1, tzinfo=UTC)
    month_end = datetime(2024, 5, 31, tzinfo=UTC)
    async with SepScoreboardsAdapter(client=client, cache=False) as sb:
        records = [r async for r in sb.fetch_scoreboard_a(start=month_start, end=month_end)]
    assert records, "expected at least one Scoreboard A record"
    assert all(r.source is SourceID.SEP_SCOREBOARD_A for r in records)
    assert all(r.record_type == "onset_probability" for r in records)
    # Every record's lineage should reference the file URL we mocked —
    # as a FULL URL (host included): fetching uses client-relative paths,
    # but provenance must absolutize. This was a latent bug that only
    # surfaced once UMASEP actually had recent issuances (2026-08).
    for rec in records:
        assert rec.provenance.extra is not None
        lineage = rec.provenance.extra["lineage"]
        assert any("UMASEP10_prediction" in step for step in lineage), lineage
        assert any(ISWA_BASE_URL in step for step in lineage), lineage
        assert any(ISWA_BASE_URL in ref for ref in rec.provenance.dataset_refs)


@pytest.mark.asyncio
async def test_fetch_scoreboard_b_end_to_end(
    listing_umasep_2024_05_html: str,
    scoreboard_b_recent_payload: dict[str, Any],
) -> None:
    transport = _make_mock_transport(
        listing_html=listing_umasep_2024_05_html,
        json_payload=scoreboard_b_recent_payload,
    )
    client = httpx.AsyncClient(base_url=ISWA_BASE_URL, transport=transport)
    # The recent SEPSTER fixture issue_time is 2024-05-01T15:47:52Z
    month_start = datetime(2024, 5, 1, tzinfo=UTC)
    month_end = datetime(2024, 5, 31, tzinfo=UTC)
    async with SepScoreboardsAdapter(client=client, cache=False) as sb:
        records = [r async for r in sb.fetch_scoreboard_b(start=month_start, end=month_end)]
    assert records
    assert all(r.source is SourceID.SEP_SCOREBOARD_B for r in records)
    intensities = {r.value["intensity"] for r in records}
    assert any(v > 0 for v in intensities)


@pytest.mark.asyncio
async def test_fetch_scoreboard_c_end_to_end(
    listing_umasep_2024_05_html: str,
    scoreboard_c_recent_payload: dict[str, Any],
) -> None:
    transport = _make_mock_transport(
        listing_html=listing_umasep_2024_05_html,
        json_payload=scoreboard_c_recent_payload,
    )
    client = httpx.AsyncClient(base_url=ISWA_BASE_URL, transport=transport)
    # Full-month window: the mocked listing's filenames are dated
    # 2024-05-01, and the filename prefilter (correctly) skips files whose
    # embedded dates fall outside the query window — the old Gannon-week
    # window only worked because every listed file used to be fetched
    # regardless of its name.
    month_start = datetime(2024, 5, 1, tzinfo=UTC)
    month_end = datetime(2024, 5, 31, tzinfo=UTC)
    async with SepScoreboardsAdapter(client=client, cache=False) as sb:
        records = [r async for r in sb.fetch_scoreboard_c(start=month_start, end=month_end)]
    assert records
    assert all(r.source is SourceID.SEP_SCOREBOARD_C for r in records)
    assert all(r.record_type == "event_time_profile" for r in records)


@pytest.mark.asyncio
async def test_fetch_unified_returns_all_three_boards(
    listing_umasep_2024_05_html: str,
    scoreboard_c_recent_payload: dict[str, Any],
) -> None:
    """The C fixture has data for all three boards; the unified fetch
    should yield A, B, and C records for it."""

    transport = _make_mock_transport(
        listing_html=listing_umasep_2024_05_html,
        json_payload=scoreboard_c_recent_payload,
    )
    client = httpx.AsyncClient(base_url=ISWA_BASE_URL, transport=transport)
    # Full-month window: the mocked listing's filenames are dated
    # 2024-05-01 and must survive the filename prefilter.
    month_start = datetime(2024, 5, 1, tzinfo=UTC)
    month_end = datetime(2024, 5, 31, tzinfo=UTC)
    async with SepScoreboardsAdapter(client=client, cache=False) as sb:
        records = [r async for r in sb.fetch(start=month_start, end=month_end)]

    sources = {r.source for r in records}
    assert SourceID.SEP_SCOREBOARD_A in sources
    assert SourceID.SEP_SCOREBOARD_B in sources
    assert SourceID.SEP_SCOREBOARD_C in sources


@pytest.mark.asyncio
async def test_fetch_rejects_unknown_scoreboard() -> None:
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda _r: httpx.Response(404)))
    async with SepScoreboardsAdapter(client=client, cache=False) as sb:
        with pytest.raises(ValueError, match="unknown scoreboards"):
            _ = [
                r
                async for r in sb.fetch(
                    start=GANNON_START,
                    end=GANNON_END,
                    scoreboards=("D",),  # type: ignore[arg-type]
                )
            ]


@pytest.mark.asyncio
async def test_fetch_handles_404_listings_quietly() -> None:
    transport = _make_mock_transport(fail_listings=True)
    client = httpx.AsyncClient(base_url=ISWA_BASE_URL, transport=transport)
    async with SepScoreboardsAdapter(client=client, cache=False) as sb:
        records = [r async for r in sb.fetch_scoreboard_a(start=GANNON_START, end=GANNON_END)]
    assert records == []


@pytest.mark.asyncio
async def test_fetch_filters_envelopes_outside_window(
    listing_umasep_2024_05_html: str,
    scoreboard_a_recent_payload: dict[str, Any],
) -> None:
    """Envelope issue_time outside the window → zero records.

    The window is chosen so the listing's 2024-05-01 filenames PASS the
    filename prefilter (its ±1 day pad is date-granular) while the fixture
    envelope's issue_time (2024-05-01T00:09:20Z) falls BEFORE the window
    start — this test must exercise the downstream issue-time filter, not
    the filename one.
    """

    transport = _make_mock_transport(
        listing_html=listing_umasep_2024_05_html,
        json_payload=scoreboard_a_recent_payload,
    )
    client = httpx.AsyncClient(base_url=ISWA_BASE_URL, transport=transport)
    async with SepScoreboardsAdapter(client=client, cache=False) as sb:
        records = [
            r
            async for r in sb.fetch_scoreboard_a(
                start=datetime(2024, 5, 1, 12, 0, tzinfo=UTC),
                end=datetime(2024, 5, 2, tzinfo=UTC),
            )
        ]
    # Files are fetched (names date-match the window) but the envelope's
    # issue_time (00:09Z, before the 12:00Z start) filters every record.
    assert records == []


@pytest.mark.asyncio
async def test_fetch_september_2017_event(
    listing_umasep_2024_05_html: str,
    scoreboard_a_sep2017_payload: dict[str, Any],
) -> None:
    """Cross-check for the September 2017 event (proposal Table 3-1)."""

    # Reuse the real Apache-listing fixture's structure but rewrite its
    # embedded dates into the 2017-09 window — the filename prefilter
    # (correctly) skips files whose names date outside the query window.
    listing_2017_html = listing_umasep_2024_05_html.replace("2024_05_01", "2017_09_06")
    transport = _make_mock_transport(
        listing_html=listing_2017_html,
        json_payload=scoreboard_a_sep2017_payload,
    )
    client = httpx.AsyncClient(base_url=ISWA_BASE_URL, transport=transport)
    async with SepScoreboardsAdapter(client=client, cache=False) as sb:
        records = [r async for r in sb.fetch_scoreboard_a(start=SEP2017_START, end=SEP2017_END)]
    assert records
    # The SEPSTER fixture's all_clear flag (true) maps to probability 0.0
    assert all(r.value["probability"] == 0.0 for r in records)
    # The 2017-09-06 issue time must be in window
    assert all(SEP2017_START <= r.event_time <= SEP2017_END for r in records)


@pytest.mark.asyncio
async def test_fetch_skips_malformed_envelope() -> None:
    """An envelope without ``sep_forecast_submission`` key must be skipped."""

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/"):
            return httpx.Response(
                200,
                text=('<html><body><a href="?C=N;O=D">N</a><a href="foo.json">f</a></body></html>'),
            )
        return httpx.Response(200, json={"something_else": True})

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(base_url=ISWA_BASE_URL, transport=transport)
    async with SepScoreboardsAdapter(
        client=client,
        cache=False,
        models=(ScoreboardModelSpec(name="UMASEP", variants=("v3_X",), energies=("10MeV",)),),
    ) as sb:
        records = [r async for r in sb.fetch_scoreboard_a(start=GANNON_START, end=GANNON_END)]
    assert records == []


@pytest.mark.asyncio
async def test_fetch_skips_non_dict_response_body() -> None:
    """Empty or non-dict JSON shouldn't crash the adapter."""

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/"):
            return httpx.Response(
                200,
                text=('<html><body><a href="?C=N;O=D">N</a><a href="foo.json">f</a></body></html>'),
            )
        return httpx.Response(200, json=[1, 2, 3])

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(base_url=ISWA_BASE_URL, transport=transport)
    async with SepScoreboardsAdapter(
        client=client,
        cache=False,
        models=(ScoreboardModelSpec(name="UMASEP", variants=("v3_X",), energies=("10MeV",)),),
    ) as sb:
        records = [r async for r in sb.fetch_scoreboard_a(start=GANNON_START, end=GANNON_END)]
    assert records == []


@pytest.mark.asyncio
async def test_fetch_skips_non_json_body() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/"):
            return httpx.Response(
                200,
                text=('<html><body><a href="?C=N;O=D">N</a><a href="foo.json">f</a></body></html>'),
            )
        return httpx.Response(
            200, content=b"not json at all", headers={"content-type": "text/plain"}
        )

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(base_url=ISWA_BASE_URL, transport=transport)
    async with SepScoreboardsAdapter(
        client=client,
        cache=False,
        models=(ScoreboardModelSpec(name="UMASEP", variants=("v3_X",), energies=("10MeV",)),),
    ) as sb:
        records = [r async for r in sb.fetch_scoreboard_a(start=GANNON_START, end=GANNON_END)]
    assert records == []


# ---------------------------------------------------------------------------- #
# provenance-spec bridge
# ---------------------------------------------------------------------------- #


def test_to_helios_model_output_scoreboard_a(
    scoreboard_c_recent_payload: dict[str, Any],
) -> None:
    """The static helper must produce a valid helios-provenance-spec record."""

    adapter = _adapter_for_normalize()
    env = scoreboard_c_recent_payload["sep_forecast_submission"]
    fc = _expand_forecasts(env)[0]
    rec = _scoreboard_a_record(
        adapter, fc, "https://iswa.example/sep.json", _ensure_utc(datetime(2024, 5, 10))
    )
    assert rec is not None
    out = SepScoreboardsAdapter.to_helios_model_output(rec)
    assert out["record_type"] == "HeliosModelOutputRecord"
    assert out["schema_version"] == "0.1.0"
    assert isinstance(out["value"], (int, float))
    assert out["value_units"] == "probability"
    assert out["agent"]["name"] == "SepScoreboardsAdapter"
    assert "https://iswa.example/sep.json" in out["dataset_refs"]


def test_to_helios_model_output_scoreboard_b(
    scoreboard_b_recent_payload: dict[str, Any],
) -> None:
    adapter = _adapter_for_normalize()
    env = scoreboard_b_recent_payload["sep_forecast_submission"]
    forecasts = _expand_forecasts(env)
    target = next(f for f in forecasts if f.energy_min == 10.0 and f.energy_units == "MeV")
    rec = _scoreboard_b_record(
        adapter, target, "https://iswa.example/sep.json", _ensure_utc(datetime(2024, 5, 1))
    )
    assert rec is not None
    out = SepScoreboardsAdapter.to_helios_model_output(rec)
    assert out["value_units"] == "pfu"
    assert out["value"] == pytest.approx(4.51)


def test_to_helios_model_output_scoreboard_c(
    scoreboard_c_recent_payload: dict[str, Any],
) -> None:
    adapter = _adapter_for_normalize()
    env = scoreboard_c_recent_payload["sep_forecast_submission"]
    fc = _expand_forecasts(env)[0]
    rec = _scoreboard_c_record(
        adapter, fc, "https://iswa.example/sep.json", _ensure_utc(datetime(2024, 5, 10))
    )
    assert rec is not None
    out = SepScoreboardsAdapter.to_helios_model_output(rec)
    # Scoreboard C value is the ISO onset_time
    assert isinstance(out["value"], str)
    assert "2024-05-10" in out["value"]


# ---------------------------------------------------------------------------- #
# default rate limit
# ---------------------------------------------------------------------------- #


def test_default_rate_limit_is_3_rps() -> None:
    adapter = _adapter_for_normalize()
    cfg = adapter._default_rate_limit()
    assert cfg.rate_per_second == 3.0


# ---------------------------------------------------------------------------- #
# Live integration (deselected by default)
# ---------------------------------------------------------------------------- #


@pytest.mark.live
@pytest.mark.timeout(1800)
@pytest.mark.asyncio
async def test_live_iswa_recent() -> None:
    """Hit live ISWA and pull whatever's in the last 3 days for UMASEP.

    Window is 3 days (was 30): month directories hold every file for the
    month (13k+ during 2026-08's activity) and fetching runs at 3 RPS, so
    the crawl cost scales with matched files — the filename prefilter
    (``_filename_maybe_in_window``) plus this window keeps an active-Sun
    run to minutes where the unfiltered 30-day crawl burned 2.5 h in CI.
    The timeout above is 2.5x the measured active-period wall clock
    (11 m 53 s on 2026-08-31, the tail of a very active month), not the
    suite default; the job-level cap in ci.yml still bounds the whole run.
    """

    now = datetime.now(UTC)
    start = now - timedelta(days=3)
    async with SepScoreboardsAdapter(
        cache=False,
        models=(ScoreboardModelSpec(name="UMASEP", variants=("v3_X",), energies=("10MeV",)),),
    ) as sb:
        records = []
        async for rec in sb.fetch_scoreboard_a(start=start, end=now):
            records.append(rec)
            if len(records) >= 3:
                break
    # We don't require records — UMASEP may have no recent issuances —
    # but if there are any, the source must be A and lineage must
    # include the iswa URL.
    for rec in records:
        assert rec.source is SourceID.SEP_SCOREBOARD_A
        assert rec.provenance.extra is not None
        assert any(ISWA_BASE_URL.split("//", 1)[1] in s for s in rec.provenance.extra["lineage"])


# ---------------------------------------------------------------------------- #
# Shape/path constants used by docs
# ---------------------------------------------------------------------------- #


def test_iswa_constants_match_observed_layout() -> None:
    """Guard against accidental edits to the canonical URL constants."""

    assert ISWA_BASE_URL == "https://iswa.ccmc.gsfc.nasa.gov"
    assert ISWA_SCOREBOARD_PREFIX == "/iswa_data_tree/model/heliosphere/sep_scoreboard"
    assert ALL_SCOREBOARDS == ("A", "B", "C")
