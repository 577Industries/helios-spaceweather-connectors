# helios-spaceweather-connectors

[![CI](https://github.com/577-Industries/helios-spaceweather-connectors/actions/workflows/ci.yml/badge.svg)](https://github.com/577-Industries/helios-spaceweather-connectors/actions/workflows/ci.yml) [![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![PyPI](https://img.shields.io/pypi/v/helios-spaceweather-connectors.svg)](https://pypi.org/project/helios-spaceweather-connectors/)

> Production-grade Python adapters for six space-weather data sources (NASA DONKI, CCMC SEP Scoreboards A/B/C, NOAA SWPC, NASA CDDIS GIMs, GOES, DSCOVR), normalized to a common feature schema with feature-level provenance per helios-provenance-spec.

## Status

This repository is part of the **HELIOS** program — a NASA SBIR Phase I effort by
577 Industries Inc. supporting subtopic SPWX.1.S26A (Advanced Data-Driven
Applications for Space Weather R2O2R). See proposal §2 Obj. 1 + §3 T1 of the proposal.

**Initial scaffolding committed 2026-05-17. Implementation in progress.**
Open issues to comment on the design or propose contributions.

## Quickstart

```bash
pip install helios-spaceweather-connectors
```

```python
import helios_connectors
print(helios_connectors.__version__)
```

## Documentation

- **Master plan**: see [`helios-program`](https://github.com/577-Industries/helios-program) (private; internal team)
- **Specification**: docs published at the project's docs site when available
- **Provenance**: every output traces to its upstream model and transformation chain
  via [`helios-provenance-spec`](https://github.com/577-Industries/helios-provenance-spec)

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
  url          = {https://github.com/577-Industries/helios-spaceweather-connectors},
}
```
