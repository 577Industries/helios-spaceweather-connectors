"""Per-source adapter modules.

Each upstream data source has its own submodule here. The shared base
class lives in :mod:`helios_connectors.adapters.base`. Concrete adapters
import :class:`~helios_connectors.adapters.base.BaseAdapter` and
implement the source-specific ``fetch_*`` methods.
"""

from __future__ import annotations

from .base import BaseAdapter
from .donki import DonkiAdapter
from .dscovr import DscovrAdapter
from .goes import GoesAdapter
from .swpc import SwpcAdapter

__all__ = ["BaseAdapter", "DonkiAdapter", "DscovrAdapter", "GoesAdapter", "SwpcAdapter"]
