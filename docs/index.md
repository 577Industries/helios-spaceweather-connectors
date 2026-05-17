# helios-spaceweather-connectors

Production-grade Python adapters for the space-weather data sources that
feed the HELIOS fusion engine. Each adapter normalizes its upstream
representation into a common `NormalizedRecord` shape with full
feature-level provenance per the
[`helios-provenance-spec`](https://github.com/577Industries/helios-provenance-spec)
contract.

## Adapter status

| Source | Strategy | Status | Module |
|---|---|---|---|
| NASA DONKI (CME, FLR, SEP, GST, IPS, MPC, RBE, HSS, notifications) | BUILD | **v0.1 — shipped** | [`adapters.donki`](adapters/donki.md) |
| CCMC SEP Scoreboards A / B / C | BUILD | Planned (v0.2) | `adapters.sep_scoreboard` |
| NOAA SWPC (Kp, plasma, mag, SEP forecast JSON) | EXTEND (SunPy where possible) | Planned (v0.2) | `adapters.swpc` |
| NASA CDDIS GIMs (IONEX) | BUILD | Planned (v0.3) | `adapters.cddis_gim` |
| GOES X-ray + proton flux | WRAP (PySPEDAS / SWPC NRT JSON) | Planned (v0.3) | `adapters.goes` |
| DSCOVR upstream solar wind | WRAP (PySPEDAS / NOAA NRT JSON) | Planned (v0.3) | `adapters.dscovr` |

Strategy reference (from the master plan landscape survey):

- **BUILD** — no existing maintained client; we implement from scratch.
- **EXTEND** — partial SunPy / community client; we add the gaps.
- **WRAP** — mature upstream client; we provide a thin facade that
  emits `NormalizedRecord` objects with provenance.

Excluded by license: HESPERIA REleASE (commercial use restricted per
proposal ref [30]); model outputs flow in via CCMC SEP Scoreboards
rather than direct adapter integration.

## Quickstart

```bash
pip install helios-spaceweather-connectors
```

```python
from datetime import datetime, timezone

from helios_connectors import DonkiAdapter

async with DonkiAdapter() as donki:
    async for rec in donki.fetch_flr(
        start=datetime(2024, 5, 8, tzinfo=timezone.utc),
        end=datetime(2024, 5, 15, tzinfo=timezone.utc),
    ):
        print(rec.event_time, rec.value.get("classType"), rec.provenance.id)
```

## Design

See [Adapter pattern](design.md) for the framework conventions every
adapter follows, the streaming-async-first interface, the provenance
model, and how to add a new source.

## See also

- [HELIOS program master plan](https://github.com/577Industries/helios-program)
  (private; internal team)
- [`helios-provenance-spec`](https://github.com/577Industries/helios-provenance-spec)
  — JSON Schema for the provenance model
- [`helios-fusion-engine`](https://github.com/577Industries/helios-fusion-engine)
  — downstream consumer (BMA fusion + calibration)
- NASA SBIR Phase I proposal — see proposal §2 Obj. 1 and §3 T1
