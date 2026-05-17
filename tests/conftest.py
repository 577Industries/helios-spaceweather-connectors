"""Shared pytest fixtures for helios-connectors tests."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

FIXTURES_ROOT = Path(__file__).parent / "fixtures" / "donki"
SWPC_FIXTURES_ROOT = Path(__file__).parent / "fixtures" / "swpc"


@pytest.fixture(scope="session")
def swpc_fixtures_root() -> Path:
    """Return the directory holding SWPC fixtures (real + archive)."""
    return SWPC_FIXTURES_ROOT


@pytest.fixture(scope="session")
def swpc_kp_realtime_fixture() -> list[dict[str, object]]:
    """Real SWPC Kp planetary index response (current month snapshot)."""
    return json.loads((SWPC_FIXTURES_ROOT / "kp-realtime.json").read_text())


@pytest.fixture(scope="session")
def swpc_plasma_fixture() -> list[list[object]]:
    """Real SWPC plasma-7-day response (header + ~1 hour of rows)."""
    return json.loads((SWPC_FIXTURES_ROOT / "plasma-7-day.json").read_text())


@pytest.fixture(scope="session")
def swpc_mag_fixture() -> list[list[object]]:
    """Real SWPC mag-7-day response (header + ~1 hour of rows)."""
    return json.loads((SWPC_FIXTURES_ROOT / "mag-7-day.json").read_text())


@pytest.fixture(scope="session")
def swpc_protons_fixture() -> list[dict[str, object]]:
    """Real SWPC GOES integral-protons-7-day response (40 rows)."""
    return json.loads((SWPC_FIXTURES_ROOT / "goes-protons-7-day.json").read_text())


@pytest.fixture(scope="session")
def swpc_sep_forecast_fixture() -> str:
    """Real SWPC 3-day forecast text product."""
    return (SWPC_FIXTURES_ROOT / "sep-forecast.txt").read_text()


@pytest.fixture(scope="session")
def gfz_kp_archive_fixture() -> str:
    """GFZ Potsdam Kp archive slice for May 2024 (31 days)."""
    return (SWPC_FIXTURES_ROOT / "gfz-kp-2024-05.txt").read_text()


@pytest.fixture(scope="session")
def kyoto_dst_2405_fixture() -> str:
    """Kyoto WDC provisional Dst for May 2024."""
    return (SWPC_FIXTURES_ROOT / "kyoto-dst-2405.html").read_text()


@pytest.fixture(scope="session")
def donki_fixtures_root() -> Path:
    """Return the directory holding DONKI JSON fixtures."""
    return FIXTURES_ROOT


@pytest.fixture(scope="session")
def donki_cme_fixture() -> list[dict[str, object]]:
    """Real DONKI CME response captured during the May 2024 Gannon storm."""
    return json.loads((FIXTURES_ROOT / "CME_sample.json").read_text())


@pytest.fixture(scope="session")
def donki_flr_fixture() -> list[dict[str, object]]:
    """Real DONKI FLR response captured during the May 2024 Gannon storm."""
    return json.loads((FIXTURES_ROOT / "FLR_sample.json").read_text())


@pytest.fixture(scope="session")
def donki_gst_fixture() -> list[dict[str, object]]:
    """Real DONKI GST response captured during the May 2024 Gannon storm.

    Contains the Gannon G5 entry (2024-05-10T15:00:00-GST-001) with
    rich ``linkedEvents`` traceback to 5 originating CMEs plus an IPS.
    """
    return json.loads((FIXTURES_ROOT / "GST_sample.json").read_text())


@pytest.fixture(scope="session")
def donki_sep_fixture() -> list[dict[str, object]]:
    """Real DONKI SEP response captured during the May 2024 Gannon storm."""
    return json.loads((FIXTURES_ROOT / "SEP_sample.json").read_text())


@pytest.fixture(scope="session")
def donki_cme_analysis_fixture() -> list[dict[str, object]]:
    """Real DONKI CMEAnalysis response from the Gannon storm window."""
    return json.loads((FIXTURES_ROOT / "CMEAnalysis_sample.json").read_text())


@pytest.fixture(scope="session")
def donki_ips_fixture() -> list[dict[str, object]]:
    """Real DONKI IPS response from the Gannon storm window."""
    return json.loads((FIXTURES_ROOT / "IPS_sample.json").read_text())


@pytest.fixture(scope="session")
def donki_mpc_fixture() -> list[dict[str, object]]:
    """Real DONKI MPC response from the Gannon storm window."""
    return json.loads((FIXTURES_ROOT / "MPC_sample.json").read_text())


@pytest.fixture(scope="session")
def donki_rbe_fixture() -> list[dict[str, object]]:
    """Real DONKI RBE response from the Gannon storm window."""
    return json.loads((FIXTURES_ROOT / "RBE_sample.json").read_text())


@pytest.fixture(scope="session")
def donki_notifications_fixture() -> list[dict[str, object]]:
    """Real DONKI notifications response from the Gannon storm window."""
    return json.loads((FIXTURES_ROOT / "notifications_sample.json").read_text())
