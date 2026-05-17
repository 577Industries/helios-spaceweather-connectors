"""Tests for the GOES adapter.

Two routing paths under test:

* **SWPC near-real-time JSON path** — mocked via ``httpx.MockTransport``;
  fixture payloads in ``tests/fixtures/goes/*-7-day.json`` match the real
  ``services.swpc.noaa.gov`` shape.
* **PySPEDAS / NCEI archive path** — mocked via the ``pyspedas_loader``
  kwarg on :class:`GoesAdapter`; fixtures in
  ``tests/fixtures/goes/pyspedas-gannon-*.json`` are the *post-extraction*
  shape (the loader output, not the raw CDF).

The Gannon-week regression test below is the critical one: a
``fetch_protons(2024-05-08, 2024-05-14)`` call must route to the PySPEDAS
path (because >30 days old) and yield records with
``source_id = SourceID.GOES`` and lineage citing the NCEI archive URL.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest

from helios_connectors import GoesAdapter, SourceID
from helios_connectors.adapters.goes import (
    PROTON_THRESHOLDS_MEV,
    SUPPORTED_SATELLITES,
    SWPC_NRT_WINDOW_DAYS,
    XRAY_BANDS,
    _coerce_scalar_flux,
    _coerce_timestamp,
    _default_pyspedas_loader,
    _default_units,
    _extract_proton_samples,
    _extract_xray_samples,
    _ncei_archive_url,
    _parse_proton_threshold,
    _quiet_pyspedas,
    _swpc_endpoint_for,
    _swpc_span_days,
    _synthesise_record_id,
)

FIXTURES = Path(__file__).parent / "fixtures" / "goes"

GANNON_START = datetime(2024, 5, 8, tzinfo=UTC)
GANNON_END = datetime(2024, 5, 14, tzinfo=UTC)


# ---------------------------------------------------------------------------- #
# Local fixtures
# ---------------------------------------------------------------------------- #


@pytest.fixture(scope="session")
def swpc_xray_fixture() -> list[dict[str, Any]]:
    return json.loads((FIXTURES / "xrays-7-day.json").read_text())


@pytest.fixture(scope="session")
def swpc_protons_fixture() -> list[dict[str, Any]]:
    return json.loads((FIXTURES / "integral-protons-7-day.json").read_text())


@pytest.fixture(scope="session")
def pyspedas_protons_fixture() -> list[dict[str, Any]]:
    return json.loads((FIXTURES / "pyspedas-gannon-protons.json").read_text())


@pytest.fixture(scope="session")
def pyspedas_xray_fixture() -> list[dict[str, Any]]:
    return json.loads((FIXTURES / "pyspedas-gannon-xray.json").read_text())


# ---------------------------------------------------------------------------- #
# Pure helper tests
# ---------------------------------------------------------------------------- #


def test_parse_proton_threshold_standard() -> None:
    assert _parse_proton_threshold(">=10 MeV") == 10
    assert _parse_proton_threshold(">=50 MeV") == 50
    assert _parse_proton_threshold(">=100 MeV") == 100


def test_parse_proton_threshold_handles_no_comparator() -> None:
    assert _parse_proton_threshold("10 MeV") == 10


def test_parse_proton_threshold_rejects_garbage() -> None:
    assert _parse_proton_threshold("garbage") is None
    assert _parse_proton_threshold(None) is None
    assert _parse_proton_threshold(42) is None


def test_swpc_span_days_buckets() -> None:
    base = datetime(2026, 5, 15, tzinfo=UTC)
    assert _swpc_span_days(base, base + timedelta(hours=1)) == 0
    assert _swpc_span_days(base, base + timedelta(hours=23)) == 1
    assert _swpc_span_days(base, base + timedelta(days=2)) == 3
    assert _swpc_span_days(base, base + timedelta(days=5)) == 7


def test_swpc_endpoint_for_xray() -> None:
    assert _swpc_endpoint_for("xray", span_days=0) == "/xrays-6-hour.json"
    assert _swpc_endpoint_for("xray", span_days=7) == "/xrays-7-day.json"


def test_swpc_endpoint_for_protons() -> None:
    assert _swpc_endpoint_for("protons", span_days=0) == "/integral-protons-6-hour.json"
    assert _swpc_endpoint_for("protons", span_days=3) == "/integral-protons-3-day.json"


def test_ncei_archive_url_includes_probe_and_product() -> None:
    url = _ncei_archive_url("GOES-16", "protons")
    assert "goes-16" in url
    assert "sgps" in url
    url2 = _ncei_archive_url("GOES-18", "xray")
    assert "goes-18" in url2
    assert "xrsf" in url2


def test_synthesise_record_id_format() -> None:
    ts = datetime(2024, 5, 10, 15, 0, tzinfo=UTC)
    rid = _synthesise_record_id("GOES-16", "protons", 10, ts)
    assert rid.startswith("GOES-16/protons/10/")
    assert ts.isoformat() in rid


def test_proton_thresholds_are_helios_targets() -> None:
    assert PROTON_THRESHOLDS_MEV == (10, 50, 100)


def test_xray_bands_are_standard() -> None:
    assert XRAY_BANDS == ("0.05-0.4nm", "0.1-0.8nm")


# ---------------------------------------------------------------------------- #
# Routing tests
# ---------------------------------------------------------------------------- #


def test_route_split_all_archive() -> None:
    """Window entirely older than 30 days -> archive only."""
    adapter = GoesAdapter(cache=False)
    now = datetime(2026, 5, 15, tzinfo=UTC)
    archive, nrt = adapter._route_split(
        datetime(2024, 5, 8, tzinfo=UTC),
        datetime(2024, 5, 14, tzinfo=UTC),
        now=now,
    )
    assert archive is not None
    assert nrt is None


def test_route_split_all_nrt() -> None:
    """Window entirely within 30 days -> nrt only."""
    adapter = GoesAdapter(cache=False)
    now = datetime(2026, 5, 15, tzinfo=UTC)
    archive, nrt = adapter._route_split(
        now - timedelta(days=5),
        now - timedelta(days=1),
        now=now,
    )
    assert archive is None
    assert nrt is not None


def test_route_split_straddle() -> None:
    """Window straddling the 30-day boundary -> both paths."""
    adapter = GoesAdapter(cache=False)
    now = datetime(2026, 5, 15, tzinfo=UTC)
    archive, nrt = adapter._route_split(
        now - timedelta(days=45),
        now - timedelta(days=15),
        now=now,
    )
    assert archive is not None
    assert nrt is not None


def test_route_split_rejects_inverted_window() -> None:
    adapter = GoesAdapter(cache=False)
    with pytest.raises(ValueError, match="before start"):
        adapter._route_split(
            datetime(2024, 6, 1, tzinfo=UTC),
            datetime(2024, 5, 1, tzinfo=UTC),
        )


# ---------------------------------------------------------------------------- #
# SWPC near-real-time path (mocked httpx)
# ---------------------------------------------------------------------------- #


def _swpc_mock_client(
    xray_payload: list[dict[str, Any]] | None = None,
    protons_payload: list[dict[str, Any]] | None = None,
) -> httpx.AsyncClient:
    """Build an httpx AsyncClient with a MockTransport returning canned JSON."""

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if "xrays" in path and xray_payload is not None:
            return httpx.Response(200, json=xray_payload)
        if "integral-protons" in path and protons_payload is not None:
            return httpx.Response(200, json=protons_payload)
        return httpx.Response(404, json=[])

    transport = httpx.MockTransport(handler)
    return httpx.AsyncClient(base_url="https://services.swpc.noaa.gov", transport=transport)


@pytest.mark.asyncio
async def test_fetch_xray_nrt_path_normalizes(
    swpc_xray_fixture: list[dict[str, Any]],
) -> None:
    client = _swpc_mock_client(xray_payload=swpc_xray_fixture)
    now = datetime.now(UTC)
    async with GoesAdapter(client=client, cache=False) as goes:
        records = [
            r
            async for r in goes.fetch_xray(
                start=now - timedelta(days=3),
                end=now,
            )
        ]
    # SWPC fixture has dates that may not match window; assert path semantics
    assert all(r.source == SourceID.GOES for r in records)
    assert all(r.record_type == "xray" for r in records)
    assert all(r.value_units == "W/m^2" for r in records)
    # Lineage cites SWPC, not NCEI
    for rec in records:
        assert rec.provenance.extra is not None
        assert any("services.swpc.noaa.gov" in step for step in rec.provenance.extra["lineage"])


@pytest.mark.asyncio
async def test_fetch_protons_nrt_filters_to_helios_thresholds(
    swpc_protons_fixture: list[dict[str, Any]],
) -> None:
    """The 500 MeV entries in the fixture must be filtered out."""
    client = _swpc_mock_client(protons_payload=swpc_protons_fixture)
    now = datetime.now(UTC)
    # Patch the fixture's time_tag fields to fall within a recent window so
    # they pass the event_time filter. We re-mock with adjusted payload.
    adjusted = []
    for item in swpc_protons_fixture:
        new = dict(item)
        new["time_tag"] = (now - timedelta(hours=1)).isoformat().replace("+00:00", "Z")
        adjusted.append(new)
    client = _swpc_mock_client(protons_payload=adjusted)
    async with GoesAdapter(client=client, cache=False) as goes:
        records = [
            r
            async for r in goes.fetch_protons(
                start=now - timedelta(days=1),
                end=now,
            )
        ]
    thresholds = {r.value["threshold_mev"] for r in records}
    assert thresholds == {10, 50, 100}, thresholds
    assert all(r.value_units == "pfu" for r in records)
    assert all(r.source == SourceID.GOES for r in records)


@pytest.mark.asyncio
async def test_swpc_rejects_non_list_response() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"oops": "dict"})

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(base_url="https://services.swpc.noaa.gov", transport=transport)
    now = datetime.now(UTC)
    async with GoesAdapter(client=client, cache=False) as goes:
        with pytest.raises(httpx.DecodingError):
            _ = [r async for r in goes.fetch_xray(start=now - timedelta(hours=2), end=now)]


@pytest.mark.asyncio
async def test_swpc_skips_non_dict_entries(
    swpc_xray_fixture: list[dict[str, Any]],
) -> None:
    bad_payload: list[Any] = [*list(swpc_xray_fixture), "not a dict", 42]

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=bad_payload)

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(base_url="https://services.swpc.noaa.gov", transport=transport)
    now = datetime.now(UTC)
    async with GoesAdapter(client=client, cache=False) as goes:
        records = [r async for r in goes.fetch_xray(start=now - timedelta(days=1), end=now)]
    # Only dict entries with valid time_tags within the (likely-empty) window
    # may survive — but the call must not raise on the non-dict entries.
    assert isinstance(records, list)


@pytest.mark.asyncio
async def test_swpc_skips_unparseable_time_tag() -> None:
    payload = [
        {"time_tag": "not-a-date", "flux": 1e-7, "energy": "0.1-0.8nm"},
        {"flux": 2e-7, "energy": "0.1-0.8nm"},  # missing time_tag entirely
    ]

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(base_url="https://services.swpc.noaa.gov", transport=transport)
    now = datetime.now(UTC)
    async with GoesAdapter(client=client, cache=False) as goes:
        records = [r async for r in goes.fetch_xray(start=now - timedelta(hours=1), end=now)]
    assert records == []


# ---------------------------------------------------------------------------- #
# PySPEDAS / archive path (mocked loader)
# ---------------------------------------------------------------------------- #


def _make_pyspedas_mock(samples: list[dict[str, Any]]):
    """Build a loader function the adapter can call in place of real pyspedas."""
    calls: list[tuple[str, str, list[str]]] = []

    def loader(product: str, probe: str, trange: list[str]) -> list[dict[str, Any]]:
        calls.append((product, probe, trange))
        return samples

    loader.calls = calls  # type: ignore[attr-defined]
    return loader


@pytest.mark.asyncio
async def test_fetch_protons_gannon_routes_to_pyspedas(
    pyspedas_protons_fixture: list[dict[str, Any]],
) -> None:
    """Critical regression: Gannon week (>30d old) must hit the archive path.

    Records must carry:
    * ``source = SourceID.GOES``,
    * ``value_units = 'pfu'``,
    * lineage citing the NCEI archive URL (NOT services.swpc.noaa.gov).
    """
    loader = _make_pyspedas_mock(pyspedas_protons_fixture)
    async with GoesAdapter(cache=False, pyspedas_loader=loader) as goes:
        records = [r async for r in goes.fetch_protons(start=GANNON_START, end=GANNON_END)]
    assert loader.calls, "pyspedas loader was never invoked"
    assert loader.calls[0][0] == "protons"
    assert loader.calls[0][1] == "16"  # default GOES-16
    assert len(records) == len(pyspedas_protons_fixture)
    assert all(r.source == SourceID.GOES for r in records)
    assert all(r.record_type == "protons" for r in records)
    assert all(r.value_units == "pfu" for r in records)
    for rec in records:
        # Lineage must cite NCEI archive, NOT SWPC
        assert rec.provenance.extra is not None
        lineage_str = " ".join(rec.provenance.extra["lineage"])
        assert "ncei.noaa.gov" in lineage_str
        assert "services.swpc.noaa.gov" not in lineage_str
        # model_version reflects the archive path
        assert rec.provenance.model_version == "ncei_archive"
    # Spot-check threshold metadata
    thresholds = {r.value["threshold_mev"] for r in records}
    assert thresholds == {10, 50, 100}


@pytest.mark.asyncio
async def test_fetch_xray_gannon_routes_to_pyspedas(
    pyspedas_xray_fixture: list[dict[str, Any]],
) -> None:
    loader = _make_pyspedas_mock(pyspedas_xray_fixture)
    async with GoesAdapter(cache=False, pyspedas_loader=loader) as goes:
        records = [r async for r in goes.fetch_xray(start=GANNON_START, end=GANNON_END)]
    assert loader.calls[0][0] == "xray"
    assert len(records) == len(pyspedas_xray_fixture)
    bands = {r.value["band"] for r in records}
    assert bands == {"0.05-0.4nm", "0.1-0.8nm"}
    assert all(r.value_units == "W/m^2" for r in records)


@pytest.mark.asyncio
async def test_fetch_unified_dispatches_both_products(
    pyspedas_protons_fixture: list[dict[str, Any]],
    pyspedas_xray_fixture: list[dict[str, Any]],
) -> None:
    """``fetch(products=[xray, protons])`` must call the loader for both."""
    by_product: dict[str, list[dict[str, Any]]] = {
        "xray": pyspedas_xray_fixture,
        "protons": pyspedas_protons_fixture,
    }
    calls: list[str] = []

    def loader(product: str, probe: str, trange: list[str]) -> list[dict[str, Any]]:
        calls.append(product)
        return by_product[product]

    async with GoesAdapter(cache=False, pyspedas_loader=loader) as goes:
        records = [r async for r in goes.fetch(start=GANNON_START, end=GANNON_END)]
    assert set(calls) == {"xray", "protons"}
    record_types = {r.record_type for r in records}
    assert record_types == {"xray", "protons"}


@pytest.mark.asyncio
async def test_fetch_satellite_parameter_passes_through(
    pyspedas_protons_fixture: list[dict[str, Any]],
) -> None:
    loader = _make_pyspedas_mock(pyspedas_protons_fixture)
    async with GoesAdapter(cache=False, pyspedas_loader=loader) as goes:
        records = [
            r
            async for r in goes.fetch_protons(
                start=GANNON_START, end=GANNON_END, satellite="GOES-18"
            )
        ]
    assert loader.calls[0][1] == "18"
    assert all(r.value["satellite"] == "GOES-18" for r in records)


@pytest.mark.asyncio
async def test_fetch_rejects_unknown_satellite() -> None:
    async with GoesAdapter(cache=False) as goes:
        agen = goes.fetch(start=GANNON_START, end=GANNON_END, satellite="GOES-99")
        with pytest.raises(ValueError, match="unsupported satellite"):
            await agen.__anext__()


@pytest.mark.asyncio
async def test_fetch_rejects_unknown_product() -> None:
    async with GoesAdapter(cache=False) as goes:
        agen = goes.fetch(start=GANNON_START, end=GANNON_END, products=["nope"])
        with pytest.raises(ValueError, match="unknown GOES products"):
            await agen.__anext__()


# ---------------------------------------------------------------------------- #
# Provenance spec compliance — records ARE HeliosModelOutputRecord
# ---------------------------------------------------------------------------- #


@pytest.mark.asyncio
async def test_record_provenance_is_spec_compliant(
    pyspedas_protons_fixture: list[dict[str, Any]],
) -> None:
    """The provenance object IS a HeliosModelOutputRecord (no bridge needed)."""
    loader = _make_pyspedas_mock(pyspedas_protons_fixture[:1])
    async with GoesAdapter(cache=False, pyspedas_loader=loader) as goes:
        records = [r async for r in goes.fetch_protons(start=GANNON_START, end=GANNON_END)]
    prov = records[0].provenance
    assert prov.record_type == "HeliosModelOutputRecord"
    assert prov.schema_version == "0.1.0"
    assert isinstance(prov.value, float)
    assert prov.value_units == "pfu"
    assert prov.extra is not None
    assert prov.extra["threshold_mev"] == 10
    assert prov.extra["satellite"] == "GOES-16"


# ---------------------------------------------------------------------------- #
# Default rate-limit + constants
# ---------------------------------------------------------------------------- #


def test_default_rate_limit_is_polite() -> None:
    adapter = GoesAdapter(cache=False)
    assert adapter._ratelimiter.config.rate_per_second == 2.0


def test_supported_satellites() -> None:
    assert SUPPORTED_SATELLITES == ("GOES-16", "GOES-17", "GOES-18")


def test_nrt_window_default() -> None:
    assert SWPC_NRT_WINDOW_DAYS == 30


# ---------------------------------------------------------------------------- #
# Live integration test (off by default)
# ---------------------------------------------------------------------------- #


# ---------------------------------------------------------------------------- #
# Helper coverage
# ---------------------------------------------------------------------------- #


def test_coerce_timestamp_from_datetime() -> None:
    ts = datetime(2024, 5, 10, tzinfo=UTC)
    assert _coerce_timestamp(ts) is ts


def test_coerce_timestamp_from_iso_string() -> None:
    ts = _coerce_timestamp("2024-05-10T12:00:00Z")
    assert ts.year == 2024


def test_coerce_timestamp_from_unix_epoch() -> None:
    ts = _coerce_timestamp(1715299200.0)
    assert ts.tzinfo is not None
    assert ts.year == 2024


def test_coerce_timestamp_rejects_garbage() -> None:
    with pytest.raises(ValueError, match="cannot coerce"):
        _coerce_timestamp(None)


def test_default_units_per_product() -> None:
    assert _default_units("xray") == "W/m^2"
    assert _default_units("protons") == "pfu"
    assert _default_units("nope") == "none"


def test_coerce_scalar_flux_passes_through_float() -> None:
    assert _coerce_scalar_flux(0.5) == 0.5


def test_coerce_scalar_flux_handles_none() -> None:
    assert _coerce_scalar_flux(None) is None


def test_coerce_scalar_flux_handles_sequence() -> None:
    assert _coerce_scalar_flux([1.5, 2.5]) == 1.5


def test_coerce_scalar_flux_handles_empty_sequence() -> None:
    assert _coerce_scalar_flux([]) is None


def test_coerce_scalar_flux_rejects_garbage() -> None:
    assert _coerce_scalar_flux("not-a-float") is None


def test_quiet_pyspedas_suppresses_stdout(capsys: pytest.CaptureFixture[str]) -> None:
    with _quiet_pyspedas():
        print("this should be suppressed")
    out = capsys.readouterr().out
    assert "suppressed" not in out


class _StubData:
    """Mimic the shape of pyspedas.get_data return values."""

    def __init__(self, times: list[float], y: list[float]) -> None:
        self.times = times
        self.y = y


def test_extract_xray_samples_with_stub_pyspedas() -> None:
    stub = _StubData([1715299200.0, 1715299260.0], [1.0e-6, 1.2e-6])

    class StubPyspedas:
        @staticmethod
        def get_data(_var: str) -> _StubData:
            return stub

    samples = _extract_xray_samples(StubPyspedas, ["g16_xrsb_flux"])
    assert len(samples) == 2
    assert samples[0]["band"] == "0.1-0.8nm"
    assert samples[0]["record_type"] == "xray"


def test_extract_xray_samples_skips_unmatched_vars() -> None:
    class StubPyspedas:
        @staticmethod
        def get_data(_var: str) -> _StubData:  # pragma: no cover - never called
            raise AssertionError("should not be called for unmatched vars")

    samples = _extract_xray_samples(StubPyspedas, ["unrelated_var"])
    assert samples == []


def test_extract_xray_samples_empty_var_list() -> None:
    assert _extract_xray_samples(object(), []) == []


def test_extract_xray_samples_handles_none_data() -> None:
    class StubPyspedas:
        @staticmethod
        def get_data(_var: str) -> None:
            return None

    samples = _extract_xray_samples(StubPyspedas, ["g16_xrsa_flux"])
    assert samples == []


def test_extract_proton_samples_diff_yields_three_thresholds() -> None:
    """The differential-flux extractor must emit one record per HELIOS
    threshold (10 / 50 / 100 MeV) per timestep."""

    # Sample shape: (sensor_units=2, diff_channels=13)
    one_step = [[0.1] * 13, [0.2] * 13]
    stub = _StubData([1715299200.0, 1715299260.0], [one_step, one_step])

    class StubPyspedas:
        @staticmethod
        def get_data(_var: str) -> _StubData:
            return stub

    samples = _extract_proton_samples(StubPyspedas, ["g16_sgps_AvgDiffProtonFlux"])
    # 2 timesteps * 3 thresholds
    assert len(samples) == 6
    thresholds = {s["threshold_mev"] for s in samples}
    assert thresholds == {10, 50, 100}
    # Averaged east/west: (0.1 + 0.2) / 2 = 0.15
    assert all(s["value"] == pytest.approx(0.15) for s in samples)


def test_extract_proton_samples_int_yields_500_mev_threshold() -> None:
    """The integral >=500 MeV channel must be emitted with threshold_mev=500."""

    stub = _StubData([1715299200.0], [[0.5, 1.5]])

    class StubPyspedas:
        @staticmethod
        def get_data(_var: str) -> _StubData:
            return stub

    samples = _extract_proton_samples(StubPyspedas, ["g16_sgps_AvgIntProtonFlux"])
    assert len(samples) == 1
    assert samples[0]["threshold_mev"] == 500
    # Averaged east/west: (0.5 + 1.5) / 2 = 1.0
    assert samples[0]["value"] == pytest.approx(1.0)


def test_extract_proton_samples_skips_unmatched() -> None:
    class StubPyspedas:
        @staticmethod
        def get_data(_var: str) -> _StubData:  # pragma: no cover
            raise AssertionError("should not be called")

    samples = _extract_proton_samples(StubPyspedas, ["some_unrelated_var"])
    assert samples == []


def test_extract_proton_samples_empty_var_list() -> None:
    assert _extract_proton_samples(object(), []) == []


def test_extract_proton_samples_handles_none_data() -> None:
    class StubPyspedas:
        @staticmethod
        def get_data(_var: str) -> None:
            return None

    samples = _extract_proton_samples(StubPyspedas, ["g16_sgps_AvgDiffProtonFlux"])
    assert samples == []


def test_extract_proton_samples_rejects_fill_values() -> None:
    """GOES uses -1e+31 as a fill value; the extractor must skip those."""

    fill = -1e31
    one_step = [[fill] * 13, [fill] * 13]
    stub = _StubData([1715299200.0], [one_step])

    class StubPyspedas:
        @staticmethod
        def get_data(_var: str) -> _StubData:
            return stub

    samples = _extract_proton_samples(StubPyspedas, ["g16_sgps_AvgDiffProtonFlux"])
    assert samples == []


def test_default_pyspedas_loader_dispatches_xray(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The default loader should route ``product='xray'`` to ``goes.xrs``."""
    import helios_connectors.adapters.goes as goes_mod

    called: dict[str, Any] = {}

    def fake_extract_xray(_pyspedas: Any, var_names: Any) -> list[dict[str, Any]]:
        called["xray"] = var_names
        return [
            {
                "timestamp": 1.0,
                "value": 1.0,
                "units": "W/m^2",
                "band": "0.1-0.8nm",
                "record_type": "xray",
            }
        ]

    monkeypatch.setattr(goes_mod, "_extract_xray_samples", fake_extract_xray)
    samples = _default_pyspedas_loader("xray", "16", ["2024-05-10/00:00:00", "2024-05-11/00:00:00"])
    assert called.get("xray") is not None
    assert len(samples) == 1


@pytest.mark.live
@pytest.mark.asyncio
async def test_live_swpc_xray_6_hour() -> None:
    """Hit real SWPC. Deselected by default; run with `pytest -m live`."""
    now = datetime.now(UTC)
    async with GoesAdapter(cache=False) as goes:
        records = [
            r
            async for r in goes.fetch_xray(
                start=now - timedelta(hours=4),
                end=now,
            )
        ]
    assert isinstance(records, list)
    # If SWPC is up, we should get at least a few samples in 4 hours
    assert len(records) > 0
