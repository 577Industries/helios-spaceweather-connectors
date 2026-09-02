<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/577Industries/.github/main/brand/out/wordmark-dark.svg">
  <img alt="577 Industries" height="44" src="https://raw.githubusercontent.com/577Industries/.github/main/brand/out/wordmark-light.svg">
</picture>

# helios-spaceweather-connectors

`HELIOS` · [program overview](https://github.com/577Industries#helios--calibrated-heliophysics-fusion)

**Production-grade Python adapters for NASA DONKI, CCMC SEP Scoreboards, NOAA SWPC, CDDIS GIMs, GOES, and DSCOVR — normalized to a common schema with feature-level provenance.**

[![ci](https://img.shields.io/github/actions/workflow/status/577Industries/helios-spaceweather-connectors/ci.yml?style=flat-square&label=ci)](https://github.com/577Industries/helios-spaceweather-connectors/actions/workflows/ci.yml) [![release](https://img.shields.io/github/v/release/577Industries/helios-spaceweather-connectors?style=flat-square)](https://github.com/577Industries/helios-spaceweather-connectors/releases) [![license](https://img.shields.io/badge/license-Apache_2.0-blue?style=flat-square)](LICENSE) [![docs](https://img.shields.io/badge/docs-live-009688?style=flat-square)](https://577industries.github.io/helios-spaceweather-connectors/)

## Status

This repository is part of the **HELIOS** program — a NASA SBIR Phase I effort by
577 Industries Inc. supporting subtopic SPWX.1.S26A (Advanced Data-Driven
Applications for Space Weather R2O2R). See proposal §2 Obj. 1 + §3 T1.

| Adapter | Strategy | Status |
|---|---|---|
| NASA DONKI | BUILD | **v0.1 — shipped** |
| CCMC SEP Scoreboards A/B/C | BUILD | **v0.2 — shipped** |
| NOAA SWPC | EXTEND | **v0.2 — shipped** (RTSW feeds since 2026-08) |
| NASA CDDIS GIMs | BUILD | **v0.2.1 — shipped** |
| GOES X-ray + proton | WRAP | **v0.2.1 — shipped** |
| DSCOVR | WRAP | **v0.2.1 — shipped** (archive leg; realtime is `SourceID.RTSW`) |

See [`docs/index.md`](docs/index.md) for the full adapter survey and
[`docs/design.md`](docs/design.md) for the framework conventions.

## Quickstart

```bash
# Not on PyPI yet (the publish step is blocked on a dependency that PyPI refuses); install from the tag:
pip install "helios-spaceweather-connectors @ git+https://github.com/577Industries/helios-spaceweather-connectors@v0.2.1"
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
