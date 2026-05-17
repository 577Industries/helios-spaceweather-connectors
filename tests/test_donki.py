"""Tests for the DONKI adapter.

Unit tests use recorded fixtures captured during the May 2024 Gannon
storm. The live integration test is marked ``@pytest.mark.live`` and is
deselected by default; the CI nightly job runs ``pytest -m live`` and
the PR job runs ``pytest -m "not live"``.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

import httpx
import pytest

from helios_connectors import DonkiAdapter, SourceID
from helios_connectors.adapters.donki import (
    DONKI_EVENT_TYPES,
    DONKI_KAUAI_BASE_URL,
    _coerce_activity_id,
    _coerce_event_time,
    _coerce_lineage,
    _fmt_date,
)

GANNON_START = datetime(2024, 5, 8, tzinfo=UTC)
GANNON_END = datetime(2024, 5, 15, tzinfo=UTC)


# ---------------------------------------------------------------------------- #
# pure helper unit tests
# ---------------------------------------------------------------------------- #


def test_fmt_date_naive_treated_as_utc() -> None:
    assert _fmt_date(datetime(2024, 5, 10)) == "2024-05-10"


def test_fmt_date_aware_converted_to_utc() -> None:
    # 2024-05-10T23:00 in UTC+3 → 20:00 UTC; same date in UTC
    import datetime as _dt

    tz = _dt.timezone(_dt.timedelta(hours=3))
    ts = datetime(2024, 5, 10, 23, 0, tzinfo=tz)
    assert _fmt_date(ts) == "2024-05-10"


def test_coerce_activity_id_per_endpoint(
    donki_cme_fixture: list[dict[str, Any]],
    donki_flr_fixture: list[dict[str, Any]],
    donki_gst_fixture: list[dict[str, Any]],
    donki_sep_fixture: list[dict[str, Any]],
    donki_cme_analysis_fixture: list[dict[str, Any]],
) -> None:
    assert _coerce_activity_id("CME", donki_cme_fixture[0]) == donki_cme_fixture[0]["activityID"]
    assert _coerce_activity_id("FLR", donki_flr_fixture[0]) == donki_flr_fixture[0]["flrID"]
    assert _coerce_activity_id("GST", donki_gst_fixture[0]) == donki_gst_fixture[0]["gstID"]
    assert _coerce_activity_id("SEP", donki_sep_fixture[0]) == donki_sep_fixture[0]["sepID"]
    # CMEAnalysis has only the parent CME identifier; we synthesize from that
    assert (
        _coerce_activity_id("CMEAnalysis", donki_cme_analysis_fixture[0])
        == donki_cme_analysis_fixture[0]["associatedCMEID"]
    )


def test_coerce_activity_id_synthesized_when_missing() -> None:
    raw = {"startTime": "2024-05-10T12:00Z"}
    aid = _coerce_activity_id("CME", raw)
    assert aid.startswith("CME-")


def test_coerce_event_time_utc(donki_cme_fixture: list[dict[str, Any]]) -> None:
    ts = _coerce_event_time("CME", donki_cme_fixture[0])
    assert ts.tzinfo is not None
    assert ts.utcoffset().total_seconds() == 0  # type: ignore[union-attr]


def test_coerce_event_time_falls_back_to_now() -> None:
    ts = _coerce_event_time("CME", {})  # no timestamp fields
    # Should still be timezone-aware and approximately recent
    assert ts.tzinfo is not None
    assert ts <= datetime.now(UTC)


def test_coerce_lineage_from_linked_events(donki_gst_fixture: list[dict[str, Any]]) -> None:
    """The Gannon G5 GST must lineage-trace back to the originating CMEs."""
    gannon = next(g for g in donki_gst_fixture if g.get("gstID", "").startswith("2024-05-10"))
    lineage = _coerce_lineage("GST", gannon)
    assert len(lineage) >= 5  # 5 CMEs + at least one IPS
    assert all(isinstance(x, str) for x in lineage)
    assert any("CME" in x for x in lineage)


def test_coerce_lineage_cme_analysis_uses_parent(
    donki_cme_analysis_fixture: list[dict[str, Any]],
) -> None:
    """CMEAnalysis records carry no linkedEvents; lineage must use associatedCMEID."""
    analysis = donki_cme_analysis_fixture[0]
    lineage = _coerce_lineage("CMEAnalysis", analysis)
    assert lineage[0] == analysis["associatedCMEID"]


def test_coerce_lineage_empty_when_missing() -> None:
    assert _coerce_lineage("CME", {}) == ()


# ---------------------------------------------------------------------------- #
# adapter integration tests with mocked transport
# ---------------------------------------------------------------------------- #


def _mock_client(
    responses: dict[str, list[dict[str, Any]]],
    *,
    base_url: str = "https://api.nasa.gov",
    require_api_key: bool = True,
) -> httpx.AsyncClient:
    """Build an AsyncClient with a MockTransport returning canned responses.

    ``responses`` keys are DONKI event type slugs; the corresponding
    value is the JSON array we should return on the matching URL.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if require_api_key:
            assert "api_key" in request.url.params, f"missing api_key: {request.url}"
        assert "startDate" in request.url.params, f"missing startDate: {request.url}"
        assert "endDate" in request.url.params, f"missing endDate: {request.url}"
        for kind, payload in responses.items():
            if path.endswith(f"/{kind}"):
                return httpx.Response(200, json=payload)
        return httpx.Response(404, json=[])

    transport = httpx.MockTransport(handler)
    return httpx.AsyncClient(base_url=base_url, transport=transport)


@pytest.mark.asyncio
async def test_fetch_cme_normalizes(donki_cme_fixture: list[dict[str, Any]]) -> None:
    client = _mock_client({"CME": donki_cme_fixture})
    async with DonkiAdapter(client=client, cache=False) as donki:
        records = [r async for r in donki.fetch_cme(start=GANNON_START, end=GANNON_END)]
    assert len(records) == len(donki_cme_fixture)
    assert all(r.source == SourceID.DONKI for r in records)
    assert all(r.record_type == "CME" for r in records)
    assert all(r.event_time.tzinfo is not None for r in records)


@pytest.mark.asyncio
async def test_fetch_flr_lineage_present_when_linked(
    donki_flr_fixture: list[dict[str, Any]],
) -> None:
    """In the Gannon window, *some* FLR records have linkedEvents and others don't.

    The adapter must populate lineage when DONKI provides it (forward
    pointers from a flare to its downstream CME/SEP) and emit an empty
    tuple otherwise — never crash on a ``linkedEvents: null`` payload.
    """

    client = _mock_client({"FLR": donki_flr_fixture})
    async with DonkiAdapter(client=client, cache=False) as donki:
        records = [r async for r in donki.fetch_flr(start=GANNON_START, end=GANNON_END)]
    # All FLRs parse, none crash; some have lineage, others don't
    with_lineage = [r for r in records if r.provenance.lineage]
    without_lineage = [r for r in records if not r.provenance.lineage]
    assert len(records) == len(donki_flr_fixture)
    assert with_lineage, "expected at least one FLR with linked CME/SEP in Gannon window"
    assert without_lineage, "expected at least one FLR without linkedEvents"
    # Linked FLRs should point at downstream CMEs / SEPs
    for rec in with_lineage:
        kinds_linked = {item.split("-")[-2] for item in rec.provenance.lineage}
        assert kinds_linked & {"CME", "SEP", "IPS"}, kinds_linked


@pytest.mark.asyncio
async def test_fetch_gst_gannon_lineage(donki_gst_fixture: list[dict[str, Any]]) -> None:
    """The Gannon GST record must propagate full lineage through the adapter.

    This is the strongest demonstration of provenance: the May 10 G5
    geomagnetic storm traces back through DONKI's intelligent linkages
    to the 5 originating CMEs and an upstream IPS.
    """

    client = _mock_client({"GST": donki_gst_fixture})
    async with DonkiAdapter(client=client, cache=False) as donki:
        records = [r async for r in donki.fetch_gst(start=GANNON_START, end=GANNON_END)]
    gannon_rec = next(r for r in records if r.provenance.id.startswith("2024-05-10"))
    assert len(gannon_rec.provenance.lineage) >= 5
    # Verify the CMEs and IPS are both represented
    assert any("CME" in linked for linked in gannon_rec.provenance.lineage)
    assert any("IPS" in linked for linked in gannon_rec.provenance.lineage)
    # The record_id matches the GST activityID
    assert gannon_rec.provenance.id == "2024-05-10T15:00:00-GST-001"


@pytest.mark.asyncio
async def test_unified_fetch_dispatches_concurrently(
    donki_cme_fixture: list[dict[str, Any]],
    donki_flr_fixture: list[dict[str, Any]],
    donki_gst_fixture: list[dict[str, Any]],
) -> None:
    """fetch(types=[...]) should hit each endpoint and merge into one stream."""

    requested: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        requested.append(path)
        if path.endswith("/CME"):
            return httpx.Response(200, json=donki_cme_fixture)
        if path.endswith("/FLR"):
            return httpx.Response(200, json=donki_flr_fixture)
        if path.endswith("/GST"):
            return httpx.Response(200, json=donki_gst_fixture)
        return httpx.Response(200, json=[])

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(base_url="https://api.nasa.gov", transport=transport)
    async with DonkiAdapter(client=client, cache=False) as donki:
        records = [
            r
            async for r in donki.fetch(
                start=GANNON_START, end=GANNON_END, types=["CME", "FLR", "GST"]
            )
        ]
    types = {r.record_type for r in records}
    assert types == {"CME", "FLR", "GST"}
    assert any(p.endswith("/CME") for p in requested)
    assert any(p.endswith("/FLR") for p in requested)
    assert any(p.endswith("/GST") for p in requested)


@pytest.mark.asyncio
async def test_unified_fetch_unknown_type_raises() -> None:
    async with DonkiAdapter(cache=False) as donki:
        agen = donki.fetch(start=GANNON_START, end=GANNON_END, types=["NOPE"])
        with pytest.raises(ValueError, match="unknown DONKI event types"):
            await agen.__anext__()


@pytest.mark.asyncio
async def test_fetch_handles_dict_response_singleton() -> None:
    """DONKI occasionally returns a single dict instead of a list. We must cope."""

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "messageID": "20240510-AL-001",
                "messageType": "Report",
                "messageIssueTime": "2024-05-10T12:00Z",
            },
        )

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(base_url="https://api.nasa.gov", transport=transport)
    async with DonkiAdapter(client=client, cache=False) as donki:
        records = [r async for r in donki.fetch_notifications(start=GANNON_START, end=GANNON_END)]
    assert len(records) == 1
    assert records[0].record_type == "notifications"


@pytest.mark.asyncio
async def test_kauai_base_url_omits_api_key(
    donki_cme_fixture: list[dict[str, Any]],
) -> None:
    """When using kauai endpoint, requests should not include api_key."""

    def handler(request: httpx.Request) -> httpx.Response:
        assert "api_key" not in request.url.params, (
            f"kauai path should not carry api_key: {request.url}"
        )
        assert "/DONKI/WS/get/CME" in request.url.path
        return httpx.Response(200, json=donki_cme_fixture)

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(base_url=DONKI_KAUAI_BASE_URL, transport=transport)
    async with DonkiAdapter(base_url=DONKI_KAUAI_BASE_URL, client=client, cache=False) as donki:
        records = [r async for r in donki.fetch_cme(start=GANNON_START, end=GANNON_END)]
    assert len(records) == len(donki_cme_fixture)


@pytest.mark.asyncio
async def test_api_key_never_logged(
    donki_cme_fixture: list[dict[str, Any]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The api_key value must not appear in any log record."""

    secret = "test-secret-do-not-leak"
    client = _mock_client({"CME": donki_cme_fixture})
    caplog.set_level(logging.DEBUG, logger="helios_connectors")
    async with DonkiAdapter(api_key=secret, client=client, cache=False) as donki:
        _ = [r async for r in donki.fetch_cme(start=GANNON_START, end=GANNON_END)]
    for record in caplog.records:
        assert secret not in record.getMessage(), "API key leaked into log output"


def test_event_types_constant_coverage() -> None:
    """Sanity: the canonical list covers every per-endpoint convenience method."""
    expected = {
        "CME",
        "CMEAnalysis",
        "FLR",
        "SEP",
        "GST",
        "IPS",
        "MPC",
        "RBE",
        "HSS",
        "notifications",
    }
    assert set(DONKI_EVENT_TYPES) == expected


@pytest.mark.asyncio
async def test_demo_key_default_rate_limit() -> None:
    """DEMO_KEY adapters should have a stricter default rate limit."""
    async with DonkiAdapter(api_key="DEMO_KEY", cache=False) as donki:
        assert donki._ratelimiter.config.rate_per_second == 1.0


@pytest.mark.asyncio
async def test_real_key_default_rate_limit() -> None:
    async with DonkiAdapter(api_key="not-demo", cache=False) as donki:
        assert donki._ratelimiter.config.rate_per_second == 10.0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "endpoint,method_name",
    [
        ("CME", "fetch_cme"),
        ("CMEAnalysis", "fetch_cme_analysis"),
        ("FLR", "fetch_flr"),
        ("SEP", "fetch_sep"),
        ("GST", "fetch_gst"),
        ("IPS", "fetch_ips"),
        ("MPC", "fetch_mpc"),
        ("RBE", "fetch_rbe"),
        ("HSS", "fetch_hss"),
        ("notifications", "fetch_notifications"),
    ],
)
async def test_per_endpoint_convenience_methods(
    request: pytest.FixtureRequest,
    endpoint: str,
    method_name: str,
    donki_notifications_fixture: list[dict[str, Any]],
) -> None:
    """Each ``fetch_<kind>`` method must hit the right endpoint and stream records."""
    # Source the right fixture by endpoint
    fixture_lookup = {
        "CME": "donki_cme_fixture",
        "CMEAnalysis": "donki_cme_analysis_fixture",
        "FLR": "donki_flr_fixture",
        "SEP": "donki_sep_fixture",
        "GST": "donki_gst_fixture",
        "IPS": "donki_ips_fixture",
        "MPC": "donki_mpc_fixture",
        "RBE": "donki_rbe_fixture",
        "HSS": None,  # no Gannon HSS events; pass empty list
        "notifications": "donki_notifications_fixture",
    }
    fixture_name = fixture_lookup[endpoint]
    payload: list[dict[str, Any]] = request.getfixturevalue(fixture_name) if fixture_name else []

    requested_paths: list[str] = []

    def handler(req: httpx.Request) -> httpx.Response:
        requested_paths.append(req.url.path)
        return httpx.Response(200, json=payload)

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(base_url="https://api.nasa.gov", transport=transport)
    async with DonkiAdapter(client=client, cache=False) as donki:
        method = getattr(donki, method_name)
        records = [r async for r in method(start=GANNON_START, end=GANNON_END)]
    assert any(f"/DONKI/{endpoint}" in p for p in requested_paths)
    assert len(records) == len(payload)
    for rec in records:
        assert rec.record_type == endpoint
        assert rec.source == SourceID.DONKI


def test_fetch_sync_wrapper(
    donki_cme_fixture: list[dict[str, Any]],
) -> None:
    """fetch_sync() returns a list from outside an event loop."""
    client = _mock_client({"CME": donki_cme_fixture})
    donki = DonkiAdapter(client=client, cache=False)
    records = donki.fetch_sync(start=GANNON_START, end=GANNON_END, types=["CME"])
    assert len(records) == len(donki_cme_fixture)


@pytest.mark.asyncio
async def test_unexpected_response_type_raises(caplog: pytest.LogCaptureFixture) -> None:
    """Non-list, non-dict responses should surface as a decoding error."""

    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json="surprise-string-body")

    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(base_url="https://api.nasa.gov", transport=transport)
    async with DonkiAdapter(client=client, cache=False) as donki:
        with pytest.raises(httpx.DecodingError):
            _ = [r async for r in donki.fetch_cme(start=GANNON_START, end=GANNON_END)]


# ---------------------------------------------------------------------------- #
# live integration test (off by default)
# ---------------------------------------------------------------------------- #


@pytest.mark.live
@pytest.mark.asyncio
async def test_live_donki_one_day_window() -> None:
    """Hit the real DONKI API on a 1-day window. Deselected by default."""
    # Use the kauai endpoint to avoid api.nasa.gov key requirements during CI.
    async with DonkiAdapter(base_url=DONKI_KAUAI_BASE_URL, cache=False) as donki:
        records = [
            r
            async for r in donki.fetch_cme(
                start=datetime(2024, 5, 10, tzinfo=UTC),
                end=datetime(2024, 5, 11, tzinfo=UTC),
            )
        ]
    assert isinstance(records, list)
    # The Gannon window has many CMEs; if we get zero, something is wrong.
    assert len(records) > 0
