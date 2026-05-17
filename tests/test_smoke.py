"""Smoke tests: import the package and confirm version is well-formed."""
from __future__ import annotations

import re

import helios_connectors


def test_version_is_semver() -> None:
    assert re.match(r"^\d+\.\d+\.\d+", helios_connectors.__version__)


def test_package_imports_clean() -> None:
    assert hasattr(helios_connectors, "__version__")
