"""Shared pytest fixtures for helios-connectors tests."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

FIXTURES_ROOT = Path(__file__).parent / "fixtures" / "donki"


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
