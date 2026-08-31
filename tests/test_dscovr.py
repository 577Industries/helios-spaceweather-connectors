"""Tests for the DSCOVR adapter.

Recorded fixtures live under ``tests/fixtures/dscovr/``:

- ``rtsw_wind_1m.json`` / ``rtsw_mag_1m.json`` — captured NOAA SWPC RTSW
  JSON (2026-08-31) for the near-real-time path: newest-first,
  multi-observatory (SOLAR1/IMAP/ACE) with ``active`` prime flags.
- ``plasma-7-day.json`` / ``mag-7-day.json`` — the retired columnar
  products shape (Gannon-era capture), kept for the legacy branch of
  ``_parse_swpc_csv_json``.
- ``gannon-week-mag-tplot.json`` / ``gannon-week-plasma-tplot.json`` —
  synthetic recordings of the PySPEDAS ``get_data`` output shape for the
  May 2024 Gannon storm window. We mock pyspedas at the function boundary
  so unit tests never hit the network or require CDF parsing.

The live integration test (``@pytest.mark.live``) is deselected by default;
the PR job runs ``pytest -m "not live"``.
"""

from __future__ import annotations

import json
import logging
from collections import namedtuple
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import pytest

from helios_connectors.adapters.dscovr import (
    DSCOVR_PRODUCTS,
    SWPC_BASE_URL,
    SWPC_MAG_URL_PATH,
    SWPC_PLASMA_URL_PATH,
    DscovrAdapter,
    _coerce_float,
    _coerce_timestamp,
    _dataset_ref_for,
    _filter_rows,
    _parse_swpc_csv_json,
    _pyspedas_trange,
    _tplot_to_mag_rows,
    _tplot_to_plasma_rows,
)
from helios_connectors.schema import SourceID

FIXTURES_ROOT = Path(__file__).parent / "fixtures" / "dscovr"

GANNON_START = datetime(2024, 5, 8, tzinfo=UTC)
GANNON_END = datetime(2024, 5, 14, tzinfo=UTC)

# Mimic the pyspedas tplot dataclass shape; we only need .times and .y
_TplotData = namedtuple("_TplotData", ["times", "y"])


# ---------------------------------------------------------------------------- #
# fixtures
# ---------------------------------------------------------------------------- #


@pytest.fixture(scope="session")
def dscovr_plasma_swpc_fixture() -> list[dict[str, Any]]:
    """RTSW wind capture (2026-08-31, newest-first, multi-observatory).

    The oldest active row's ``proton_density`` is nulled by hand so the
    None-handling path stays covered.
    """
    return json.loads((FIXTURES_ROOT / "rtsw_wind_1m.json").read_text())


@pytest.fixture(scope="session")
def dscovr_mag_swpc_fixture() -> list[dict[str, Any]]:
    """RTSW mag capture (2026-08-31, newest-first, multi-observatory)."""
    return json.loads((FIXTURES_ROOT / "rtsw_mag_1m.json").read_text())


@pytest.fixture(scope="session")
def dscovr_legacy_columnar_plasma() -> list[list[Any]]:
    """Retired /products/solar-wind columnar capture (Gannon-era).

    Kept to prove ``_parse_swpc_csv_json`` still converts the legacy
    header-first array-of-arrays shape.
    """
    return json.loads((FIXTURES_ROOT / "plasma-7-day.json").read_text())


@pytest.fixture(scope="session")
def dscovr_gannon_mag_tplot() -> dict[str, Any]:
    return json.loads((FIXTURES_ROOT / "gannon-week-mag-tplot.json").read_text())


@pytest.fixture(scope="session")
def dscovr_gannon_plasma_tplot() -> dict[str, Any]:
    return json.loads((FIXTURES_ROOT / "gannon-week-plasma-tplot.json").read_text())


# ---------------------------------------------------------------------------- #
# pure-helper unit tests
# ---------------------------------------------------------------------------- #


def test_coerce_float_handles_strings() -> None:
    assert _coerce_float("12.5") == pytest.approx(12.5)


def test_coerce_float_handles_swpc_fill_value() -> None:
    """SWPC's -99999.9 sentinel should map to None."""
    assert _coerce_float("-99999.9") is None
    assert _coerce_float(-99999.9) is None


def test_coerce_float_handles_none_and_garbage() -> None:
    assert _coerce_float(None) is None
    assert _coerce_float("not-a-number") is None
    assert _coerce_float([1, 2, 3]) is None


def test_coerce_float_handles_bool() -> None:
    assert _coerce_float(True) == pytest.approx(1.0)
    assert _coerce_float(False) == pytest.approx(0.0)


def test_coerce_float_rejects_huge_values() -> None:
    """1e30 is a CDF fill sentinel; do not accept as physical."""
    assert _coerce_float(1.0e35) is None


def test_coerce_timestamp_swpc_format() -> None:
    raw = {"time_tag": "2024-05-10 16:36:00.000"}
    ts = _coerce_timestamp(raw)
    assert ts == datetime(2024, 5, 10, 16, 36, 0, tzinfo=UTC)


def test_coerce_timestamp_iso() -> None:
    raw = {"timestamp": "2024-05-10T16:36:00Z"}
    ts = _coerce_timestamp(raw)
    assert ts == datetime(2024, 5, 10, 16, 36, 0, tzinfo=UTC)


def test_coerce_timestamp_unix_epoch() -> None:
    raw = {"time": 1715357760.0}
    ts = _coerce_timestamp(raw)
    assert ts == datetime(2024, 5, 10, 16, 16, 0, tzinfo=UTC)


def test_coerce_timestamp_fallback_to_now(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.WARNING)
    before = datetime.now(UTC)
    ts = _coerce_timestamp({})
    assert ts >= before
    assert any("no parseable timestamp" in m for m in caplog.messages)


def test_parse_swpc_csv_json_normal(
    dscovr_legacy_columnar_plasma: list[list[Any]],
) -> None:
    rows = _parse_swpc_csv_json(dscovr_legacy_columnar_plasma)
    assert len(rows) == 10
    assert rows[0]["time_tag"] == "2024-05-10 16:36:00.000"
    assert "density" in rows[0]
    assert "speed" in rows[0]


def test_parse_swpc_csv_json_accepts_dict_form() -> None:
    """Some future SWPC feeds may ship list-of-dicts; we accept both."""
    rows = _parse_swpc_csv_json([{"time_tag": "x", "density": "1"}])
    assert rows == [{"time_tag": "x", "density": "1"}]


def test_parse_swpc_csv_json_empty_payload() -> None:
    assert _parse_swpc_csv_json([]) == []
    assert _parse_swpc_csv_json(None) == []


def test_parse_swpc_csv_json_missing_header_raises() -> None:
    with pytest.raises(httpx.DecodingError, match="header"):
        _parse_swpc_csv_json([[1, 2, 3], [4, 5, 6]])  # numeric "header" rejected


def test_parse_swpc_csv_json_drops_malformed_rows() -> None:
    payload = [["a", "b"], ["x", "y"], ["only-one"], "garbage"]
    rows = _parse_swpc_csv_json(payload)
    assert rows == [{"a": "x", "b": "y"}]


def test_filter_rows_inclusive_bounds() -> None:
    rows = [
        {"time_tag": "2024-05-10 12:00:00"},
        {"time_tag": "2024-05-10 16:00:00"},
        {"time_tag": "2024-05-11 00:00:00"},
    ]
    out = _filter_rows(
        rows,
        start=datetime(2024, 5, 10, 12, 0, tzinfo=UTC),
        end=datetime(2024, 5, 10, 23, 59, tzinfo=UTC),
        time_key="time_tag",
    )
    assert len(out) == 2


def test_filter_rows_drops_unparseable() -> None:
    rows = [{"time_tag": "not-a-time"}, {"time_tag": "2024-05-10 12:00:00"}]
    out = _filter_rows(
        rows,
        start=datetime(2024, 5, 10, tzinfo=UTC),
        end=datetime(2024, 5, 11, tzinfo=UTC),
        time_key="time_tag",
    )
    assert len(out) == 1


def test_dataset_ref_for_routes() -> None:
    assert "ngdc.noaa.gov" in _dataset_ref_for("pyspedas", "mag")
    assert "ngdc.noaa.gov" in _dataset_ref_for("pyspedas", "plasma")
    assert "services.swpc.noaa.gov" in _dataset_ref_for("swpc", "mag")
    assert "services.swpc.noaa.gov" in _dataset_ref_for("swpc", "plasma")


def test_pyspedas_trange_format() -> None:
    ts = datetime(2024, 5, 10, 16, 36, 0, tzinfo=UTC)
    assert _pyspedas_trange(ts) == "2024-05-10/16:36:00"


def test_pyspedas_trange_naive_treated_as_utc() -> None:
    ts = datetime(2024, 5, 10, 16, 36, 0)
    assert _pyspedas_trange(ts) == "2024-05-10/16:36:00"


def test_tplot_to_mag_rows_attaches_bt(dscovr_gannon_mag_tplot: dict[str, Any]) -> None:
    data = _TplotData(times=dscovr_gannon_mag_tplot["times"], y=dscovr_gannon_mag_tplot["y"])
    rows = _tplot_to_mag_rows(data, frame="GSE")
    assert len(rows) == 10
    # Bt must be the L2 magnitude of (bx, by, bz)
    expected_bt = (12.45**2 + 8.32**2 + 21.41**2) ** 0.5
    assert rows[0]["bt"] == pytest.approx(expected_bt, rel=1e-6)
    assert rows[0]["frame"] == "GSE"


def test_tplot_to_mag_rows_handles_missing_data() -> None:
    assert _tplot_to_mag_rows(_TplotData(times=None, y=None), frame="GSE") == []


def test_tplot_to_plasma_rows_aligns_three_vars(
    dscovr_gannon_plasma_tplot: dict[str, Any],
) -> None:
    times = dscovr_gannon_plasma_tplot["times"]
    np_data = _TplotData(times=times, y=dscovr_gannon_plasma_tplot["np_y"])
    v_data = _TplotData(times=times, y=dscovr_gannon_plasma_tplot["v_y"])
    temp_data = _TplotData(times=times, y=dscovr_gannon_plasma_tplot["temp_y"])
    rows = _tplot_to_plasma_rows(np_data=np_data, v_data=v_data, temp_data=temp_data)
    assert len(rows) == 8
    assert rows[0]["Np"] == pytest.approx(12.45)
    # Speed = sqrt(vx^2 + vy^2 + vz^2)
    expected_speed = (610.0**2 + 23.4**2 + 18.1**2) ** 0.5
    assert rows[0]["v"] == pytest.approx(expected_speed, rel=1e-6)
    assert rows[0]["vx_gse"] == pytest.approx(-610.0)


def test_tplot_to_plasma_rows_without_v_or_temp() -> None:
    np_data = _TplotData(times=[1715357760.0], y=[12.0])
    rows = _tplot_to_plasma_rows(np_data=np_data, v_data=None, temp_data=None)
    assert len(rows) == 1
    assert "v" not in rows[0]
    assert rows[0]["THERMAL_TEMP"] is None


def test_tplot_to_plasma_rows_empty_when_no_np() -> None:
    assert _tplot_to_plasma_rows(np_data=None, v_data=None, temp_data=None) == []


# ---------------------------------------------------------------------------- #
# adapter integration tests with mocked httpx + mocked pyspedas
# ---------------------------------------------------------------------------- #


def _mock_swpc_client(
    *,
    plasma: list[dict[str, Any]] | None = None,
    mag: list[dict[str, Any]] | None = None,
) -> httpx.AsyncClient:
    """Build an httpx AsyncClient that serves canned SWPC RTSW payloads."""

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if plasma is not None and path == SWPC_PLASMA_URL_PATH:
            return httpx.Response(200, json=plasma)
        if mag is not None and path == SWPC_MAG_URL_PATH:
            return httpx.Response(200, json=mag)
        return httpx.Response(404, json=[])

    transport = httpx.MockTransport(handler)
    return httpx.AsyncClient(base_url=SWPC_BASE_URL, transport=transport)


@pytest.mark.asyncio
async def test_swpc_path_explicit_backend(
    dscovr_mag_swpc_fixture: list[dict[str, Any]],
) -> None:
    """Forcing backend='swpc' should hit the RTSW endpoint, prime rows only."""
    client = _mock_swpc_client(mag=dscovr_mag_swpc_fixture)
    async with DscovrAdapter(client=client, cache=False) as dscovr:
        records = [
            r
            async for r in dscovr.fetch_mag(
                start=datetime(2026, 8, 31, 16, 0, tzinfo=UTC),
                end=datetime(2026, 8, 31, 17, 0, tzinfo=UTC),
                backend="swpc",
            )
        ]
    n_active = sum(1 for row in dscovr_mag_swpc_fixture if row.get("active"))
    assert len(records) == n_active
    # The near-real-time leg is RTSW-tagged post-2026-08; DSCOVR is the
    # archive leg's identity only.
    assert all(r.source == SourceID.RTSW for r in records)
    assert all(r.record_type == "mag" for r in records)
    # SWPC mag is published in GSM
    assert all(r.value["frame"] == "GSM" for r in records)
    assert all(r.value["observatory"] in {"SOLAR1", "IMAP", "ACE"} for r in records)
    # Chronological despite the feed being newest-first; oldest active row
    # in the 2026-08-31 capture is 16:07:00 with bz_gsm = -1.88 nT.
    assert records[0].event_time == datetime(2026, 8, 31, 16, 7, 0, tzinfo=UTC)
    assert records[0].value["bz"] == pytest.approx(-1.88)


@pytest.mark.asyncio
async def test_swpc_plasma_fetch_normalizes(
    dscovr_plasma_swpc_fixture: list[dict[str, Any]],
) -> None:
    client = _mock_swpc_client(plasma=dscovr_plasma_swpc_fixture)
    async with DscovrAdapter(client=client, cache=False) as dscovr:
        records = [
            r
            async for r in dscovr.fetch_plasma(
                start=datetime(2026, 8, 31, 16, 0, tzinfo=UTC),
                end=datetime(2026, 8, 31, 17, 0, tzinfo=UTC),
                backend="swpc",
            )
        ]
    n_active = sum(1 for row in dscovr_plasma_swpc_fixture if row.get("active"))
    assert len(records) == n_active
    assert all(r.source == SourceID.RTSW for r in records)
    # RTSW proton_* keys must normalize onto density/speed/temperature.
    # Oldest active capture row (16:03:00, the hand-nulled one): density
    # None, speed 433.0 km/s, temperature 122388 K.
    assert records[0].value["density"] is None
    assert records[0].value["speed"] == pytest.approx(433.0)
    assert records[0].value["temperature"] == pytest.approx(122388.0)
    assert records[0].value["observatory"] == "SOLAR1"
    # A null upstream field must surface as None, never a fill sentinel.
    assert any(r.value["density"] is None for r in records)


@pytest.mark.asyncio
async def test_swpc_lineage_cites_services_swpc_noaa(
    dscovr_mag_swpc_fixture: list[dict[str, Any]],
) -> None:
    """SWPC-backed records must lineage-cite services.swpc.noaa.gov directly."""
    client = _mock_swpc_client(mag=dscovr_mag_swpc_fixture)
    async with DscovrAdapter(client=client, cache=False) as dscovr:
        records = [
            r
            async for r in dscovr.fetch_mag(
                start=datetime(2026, 8, 31, 16, 0, tzinfo=UTC),
                end=datetime(2026, 8, 31, 17, 0, tzinfo=UTC),
                backend="swpc",
            )
        ]
    assert records
    for rec in records:
        assert any("services.swpc.noaa.gov" in ref for ref in rec.provenance.dataset_refs)
        # model_id names the adapter product line and is deliberately stable
        # across the RTSW migration (the source tag is what changed).
        assert rec.provenance.model_id == "dscovr/mag"


@pytest.mark.asyncio
async def test_pyspedas_path_routes_for_historical_window(
    dscovr_gannon_mag_tplot: dict[str, Any],
) -> None:
    """The critical regression test: Gannon week mag fetch routes to PySPEDAS
    and returns DSCOVR-tagged records with dramatic southward Bz."""

    times = dscovr_gannon_mag_tplot["times"]
    y = dscovr_gannon_mag_tplot["y"]
    fake_tplot = _TplotData(times=times, y=y)

    fake_pyspedas_mag = MagicMock(return_value=["dsc_h0_mag_B1GSE"])
    fake_get_data = MagicMock(return_value=fake_tplot)

    # Patch the lazy imports inside _load_mag_archive
    fake_mod = MagicMock()
    fake_mod.mag = fake_pyspedas_mag
    fake_tplot_mod = MagicMock()
    fake_tplot_mod.get_data = fake_get_data

    with patch.dict(
        "sys.modules",
        {
            "pyspedas.projects.dscovr": fake_mod,
            "pyspedas.tplot_tools": fake_tplot_mod,
        },
    ):
        async with DscovrAdapter(cache=False) as dscovr:
            records = [
                r
                async for r in dscovr.fetch_mag(
                    start=GANNON_START, end=GANNON_END, backend="pyspedas"
                )
            ]

    assert len(records) == 10
    assert all(r.source == SourceID.DSCOVR for r in records)
    # PySPEDAS path emits GSE-frame samples
    assert all(r.value["frame"] == "GSE" for r in records)
    # Lineage cites the NCEI archive
    for rec in records:
        assert any("ngdc.noaa.gov" in ref for ref in rec.provenance.dataset_refs)

    # Peak southward Bz should be ~-49.82 (last sample) — the canonical
    # G5-storm IMF signature.
    bz_values = [r.value["bz"] for r in records]
    peak_bz = min(bz_values)
    assert peak_bz == pytest.approx(-49.82, rel=1e-6)
    assert peak_bz < -25  # Sanity: deep negative excursion required for G5


@pytest.mark.asyncio
async def test_pyspedas_plasma_path(
    dscovr_gannon_plasma_tplot: dict[str, Any],
) -> None:
    """Historical plasma fetch routes through pyspedas.dscovr.fc."""

    times = dscovr_gannon_plasma_tplot["times"]
    np_tplot = _TplotData(times=times, y=dscovr_gannon_plasma_tplot["np_y"])
    v_tplot = _TplotData(times=times, y=dscovr_gannon_plasma_tplot["v_y"])
    temp_tplot = _TplotData(times=times, y=dscovr_gannon_plasma_tplot["temp_y"])

    fake_pyspedas_fc = MagicMock(
        return_value=["dsc_h1_fc_Np", "dsc_h1_fc_V_GSE", "dsc_h1_fc_THERMAL_TEMP"]
    )

    def fake_get(var: str) -> Any:
        return {
            "dsc_h1_fc_Np": np_tplot,
            "dsc_h1_fc_V_GSE": v_tplot,
            "dsc_h1_fc_THERMAL_TEMP": temp_tplot,
        }[var]

    fake_get_data = MagicMock(side_effect=fake_get)

    fake_mod = MagicMock()
    fake_mod.fc = fake_pyspedas_fc
    fake_tplot_mod = MagicMock()
    fake_tplot_mod.get_data = fake_get_data

    with patch.dict(
        "sys.modules",
        {
            "pyspedas.projects.dscovr": fake_mod,
            "pyspedas.tplot_tools": fake_tplot_mod,
        },
    ):
        async with DscovrAdapter(cache=False) as dscovr:
            records = [
                r
                async for r in dscovr.fetch_plasma(
                    start=GANNON_START, end=GANNON_END, backend="pyspedas"
                )
            ]
    assert len(records) == 8
    assert all(r.source == SourceID.DSCOVR for r in records)
    # Speed should exceed 600 km/s — the G5 hallmark
    speeds = [r.value["speed"] for r in records if r.value.get("speed") is not None]
    assert max(speeds) > 700
    # vx_gse is propagated through as a separate field
    assert records[0].value["vx_gse"] == pytest.approx(-610.0)


@pytest.mark.asyncio
async def test_pyspedas_empty_var_list_returns_no_records() -> None:
    """If PySPEDAS produces no variables (NCEI down, bad trange), don't crash."""

    fake_mod = MagicMock()
    fake_mod.mag = MagicMock(return_value=[])
    fake_tplot_mod = MagicMock()
    fake_tplot_mod.get_data = MagicMock()

    with patch.dict(
        "sys.modules",
        {
            "pyspedas.projects.dscovr": fake_mod,
            "pyspedas.tplot_tools": fake_tplot_mod,
        },
    ):
        async with DscovrAdapter(cache=False) as dscovr:
            records = [
                r
                async for r in dscovr.fetch_mag(
                    start=GANNON_START, end=GANNON_END, backend="pyspedas"
                )
            ]
    assert records == []


@pytest.mark.asyncio
async def test_auto_backend_selects_swpc_for_recent_window() -> None:
    """The auto-routing rule: a request whose start is within the threshold
    routes to SWPC, not PySPEDAS."""
    async with DscovrAdapter(cache=False) as dscovr:
        chosen = dscovr._choose_backend(
            start=datetime.now(UTC) - timedelta(hours=6),
            end=datetime.now(UTC),
        )
    assert chosen == "swpc"


@pytest.mark.asyncio
async def test_auto_backend_selects_pyspedas_for_historical_window() -> None:
    """The auto-routing rule: a request whose start is older than the threshold
    routes to PySPEDAS / NCEI archive."""
    async with DscovrAdapter(cache=False) as dscovr:
        chosen = dscovr._choose_backend(start=GANNON_START, end=GANNON_END)
    assert chosen == "pyspedas"


@pytest.mark.asyncio
async def test_unified_fetch_dispatches_both_products(
    dscovr_plasma_swpc_fixture: list[dict[str, Any]],
    dscovr_mag_swpc_fixture: list[dict[str, Any]],
) -> None:
    """fetch(products=...) should call both endpoints when both requested."""
    client = _mock_swpc_client(
        plasma=dscovr_plasma_swpc_fixture,
        mag=dscovr_mag_swpc_fixture,
    )
    async with DscovrAdapter(client=client, cache=False) as dscovr:
        records = [
            r
            async for r in dscovr.fetch(
                start=datetime(2026, 8, 31, 16, 0, tzinfo=UTC),
                end=datetime(2026, 8, 31, 17, 0, tzinfo=UTC),
                products=["mag", "plasma"],
                backend="swpc",
            )
        ]
    sources = {r.source for r in records}
    record_types = {r.record_type for r in records}
    assert sources == {SourceID.RTSW}
    assert "mag" in record_types
    assert "plasma" in record_types


@pytest.mark.asyncio
async def test_unified_fetch_unknown_product_raises() -> None:
    async with DscovrAdapter(cache=False) as dscovr:
        agen = dscovr.fetch(
            start=GANNON_START,
            end=GANNON_END,
            products=["mag", "nope"],
        )
        with pytest.raises(ValueError, match="unknown DSCOVR products"):
            await agen.__anext__()


@pytest.mark.asyncio
async def test_unified_fetch_unknown_backend_raises() -> None:
    async with DscovrAdapter(cache=False) as dscovr:
        agen = dscovr.fetch(
            start=GANNON_START,
            end=GANNON_END,
            backend="postgres",
        )
        with pytest.raises(ValueError, match="unknown backend"):
            await agen.__anext__()


@pytest.mark.asyncio
async def test_default_rate_limit_5_rps() -> None:
    """SWPC etiquette: 5 RPS default."""
    async with DscovrAdapter(cache=False) as dscovr:
        assert dscovr._ratelimiter.config.rate_per_second == 5.0


def test_dscovr_products_constant() -> None:
    assert set(DSCOVR_PRODUCTS) == {"mag", "plasma"}


def test_intentional_overlap_with_swpc_documented() -> None:
    """Coordination invariant: DscovrAdapter records use SourceID.DSCOVR,
    never SourceID.SWPC, even when the data physically came from SWPC.
    This keeps the instrument-tagged vs operator-tagged stream split
    downstream — per-product detail lives on ``record_type``."""
    assert DscovrAdapter.source_id == SourceID.DSCOVR
    assert SourceID.DSCOVR != SourceID.SWPC


# ---------------------------------------------------------------------------- #
# live integration test (off by default)
# ---------------------------------------------------------------------------- #


@pytest.mark.live
@pytest.mark.asyncio
async def test_live_swpc_last_six_hours() -> None:
    """Hit the real SWPC RTSW JSON for the last 6 h. Deselected by default."""
    end = datetime.now(UTC)
    start = end - timedelta(hours=6)
    async with DscovrAdapter(cache=False) as dscovr:
        mag_records = [r async for r in dscovr.fetch_mag(start=start, end=end, backend="swpc")]
        plasma_records = [
            r async for r in dscovr.fetch_plasma(start=start, end=end, backend="swpc")
        ]
    assert len(mag_records) > 0, "expected some mag samples in the last 6 h"
    assert len(plasma_records) > 0, "expected some plasma samples in the last 6 h"
    # The near-real-time leg is RTSW-tagged (multi-observatory feed), and
    # every record names its observing spacecraft.
    for rec in mag_records + plasma_records:
        assert rec.source == SourceID.RTSW
        assert isinstance(rec.value.get("observatory"), str)
