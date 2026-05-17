# helios-spaceweather-connectors

[![CI](https://github.com/577Industries/helios-spaceweather-connectors/actions/workflows/ci.yml/badge.svg)](https://github.com/577Industries/helios-spaceweather-connectors/actions/workflows/ci.yml) [![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![PyPI](https://img.shields.io/pypi/v/helios-spaceweather-connectors.svg)](https://pypi.org/project/helios-spaceweather-connectors/)

> Production-grade Python adapters for six space-weather data sources (NASA DONKI, CCMC SEP Scoreboards A/B/C, NOAA SWPC, NASA CDDIS GIMs, GOES, DSCOVR), normalized to a common feature schema with feature-level provenance per helios-provenance-spec.

## Status

This repository is part of the **HELIOS** program — a NASA SBIR Phase I effort by
577 Industries Inc. supporting subtopic SPWX.1.S26A (Advanced Data-Driven
Applications for Space Weather R2O2R). See proposal §2 Obj. 1 + §3 T1.

| Adapter | Strategy | Status |
|---|---|---|
| NASA DONKI | BUILD | **v0.1 — shipped** |
| CCMC SEP Scoreboards A/B/C | BUILD | Planned (v0.2) |
| NOAA SWPC | EXTEND | Planned (v0.2) |
| NASA CDDIS GIMs | BUILD | Planned (v0.3) |
| GOES X-ray + proton | WRAP | Planned (v0.3) |
| DSCOVR | WRAP | Planned (v0.3) |

See [`docs/index.md`](docs/index.md) for the full adapter survey and
[`docs/design.md`](docs/design.md) for the framework conventions.

## Quickstart

```bash
pip install helios-spaceweather-connectors
```

```python
from datetime import UTC, datetime

from helios_connectors import DonkiAdapter

async with DonkiAdapter() as donki:
    async for rec in donki.fetch_gst(
        start=datetime(2024, 5, 8, tzinfo=UTC),
        end=datetime(2024, 5, 15, tzinfo=UTC),
    ):
        print(rec.event_time, rec.provenance.id, rec.provenance.lineage)
```

The May 2024 Gannon G5 storm record carries an 8-deep lineage tracing
back through its originating CMEs — see [`docs/adapters/donki.md`](docs/adapters/donki.md)
and [`examples/donki_quickstart.ipynb`](examples/donki_quickstart.ipynb).

## Documentation

- **Master plan**: see [`helios-program`](https://github.com/577Industries/helios-program) (private; internal team)
- **Specification**: docs published at the project's docs site when available
- **Provenance**: every output traces to its upstream model and transformation chain
  via [`helios-provenance-spec`](https://github.com/577Industries/helios-provenance-spec)

## License

Apache 2.0 — see [LICENSE](LICENSE).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Substantive changes should be discussed in an issue first.

## Citation

```bibtex
@software{helios_helios_spaceweather_connectors,
  author       = {Waweru, Thomas and 577 Industries Inc.},
  title        = { helios-spaceweather-connectors: Production-grade Python adapters for six space-weather data sources (NASA DONKI, CCMC SEP Scoreboards A/B/C, NOAA SWPC, NASA CDDIS GIMs, GOES, DSCOVR), normalized to a common feature schema with feature-level provenance per helios-provenance-spec },
  year         = {2026},
  publisher    = {577 Industries Inc.},
  url          = {https://github.com/577Industries/helios-spaceweather-connectors},
}
```

## Related

- **HELIOS program**: [`helios-program`](https://github.com/577Industries/helios-program) — master plan, proposal companion document, orchestration scripts.
- **Wave 1 review pack**: [Artifact B foundation review pack](https://github.com/577Industries/helios-program/blob/main/specs/2026-05-17-B-connectors-foundation-review-pack.md) — adapter-pattern design notes and the 7 DONKI API quirks documented for follow-up adapter agents.
- **Provenance schema**: [`helios-provenance-spec`](https://github.com/577Industries/helios-provenance-spec) — the JSON Schema and pydantic models this package emits for every fetched record.
- **Downstream consumer**: [`helios-fusion-engine`](https://github.com/577Industries/helios-fusion-engine) — uses these adapters to fuse multi-source space-weather signals.
