"""Generate a synthetic-but-realistic IONEX fixture for unit tests.

Produces an IONEX 1.0 file with the standard IGS combined grid:

* 71 latitudes (87.5 → -87.5 step -2.5)
* 73 longitudes (-180 → +180 step 5.0)
* 13 maps (00 UT … 24 UT step 2h)

TEC values are synthesized via a smooth analytic field roughly mimicking
the 2024-05-10 Gannon-storm peak TEC distribution over the U.S.
Midwest: a Gaussian bump centred at (40N, -83E) reaching ~65 TECU at
20:00 UT, on top of a diurnally-modulated background reaching ~15 TECU
elsewhere. This is NOT a substitute for the real CDDIS file, but it
lets the parser + adapter + smoke test run without Earthdata credentials.

To regenerate::

    python tests/fixtures/cddis_gim/_generate_synthetic.py

The script is checked in alongside the fixture so reviewers can audit
exactly what synthetic distribution the tests assert against.
"""

from __future__ import annotations

import gzip
import math
from datetime import UTC, datetime, timedelta
from pathlib import Path

LAT1 = 87.5
LAT2 = -87.5
DLAT = -2.5
LON1 = -180.0
LON2 = 180.0
DLON = 5.0
N_LAT = int(round(abs((LAT2 - LAT1) / DLAT))) + 1  # 71
N_LON = int(round(abs((LON2 - LON1) / DLON))) + 1  # 73
EXPONENT = -1  # values stored as TECU * 10
N_MAPS = 13
DATE = datetime(2024, 5, 10, 0, 0, 0, tzinfo=UTC)  # DOY 131
INTERVAL_SEC = 7200
HEIGHT_KM = 450.0


def _tec_at(lat: float, lon: float, hour_utc: float) -> float:
    """Return synthetic TEC at (lat, lon) for the given UTC hour.

    Diurnal modulation: a cosine peaked at local-time 14:00 (LT = UT +
    lon/15). Gannon bump: a 2D Gaussian centred on (40N, -83E) at 20:00
    UT, sigma ~15 deg lat, ~25 deg lon, peak amplitude 50 TECU above
    background.
    """

    local_hour = (hour_utc + lon / 15.0) % 24.0
    diurnal = 12.0 + 8.0 * math.cos(2 * math.pi * (local_hour - 14.0) / 24.0)
    # Background drops with |lat| (rough sech^2 of geomagnetic latitude).
    background = diurnal * math.exp(-((lat / 35.0) ** 2))
    # Gannon enhancement: only material between 16 and 24 UTC, peak at 20 UTC.
    storm_envelope = max(0.0, math.exp(-((hour_utc - 20.0) ** 2) / 4.0))
    storm = (
        50.0
        * storm_envelope
        * math.exp(-((lat - 40.0) ** 2) / (2 * 15.0**2))
        * math.exp(-((lon + 83.0) ** 2) / (2 * 25.0**2))
    )
    return background + storm


def _fmt_epoch(t: datetime) -> str:
    # 6 right-justified 6-wide ints: YYYY MM DD HH MM SS
    return (
        f"{t.year:6d}{t.month:6d}{t.day:6d}"
        f"{t.hour:6d}{t.minute:6d}{t.second:6d}"
    )


def _write_label(body: str, label: str) -> str:
    # IONEX puts the label starting at column 61 (0-indexed 60).
    body = body.ljust(60)[:60]
    return body + label


def build_ionex() -> str:
    lines: list[str] = []
    lines.append(_write_label(f"{'1.0':>8s}{'I':>12s}", "IONEX VERSION / TYPE"))
    lines.append(_write_label("HELIOS Synthetic   v0.2  HELIOS-test  20240511 000000 UTC", "PGM / RUN BY / DATE"))
    lines.append(_write_label("Synthetic Gannon-storm fixture for helios-connectors", "DESCRIPTION"))
    lines.append(_write_label(_fmt_epoch(DATE), "EPOCH OF FIRST MAP"))
    last = DATE + timedelta(seconds=INTERVAL_SEC * (N_MAPS - 1))
    lines.append(_write_label(_fmt_epoch(last), "EPOCH OF LAST MAP"))
    lines.append(_write_label(f"{INTERVAL_SEC:6d}", "INTERVAL"))
    lines.append(_write_label(f"{N_MAPS:6d}", "# OF MAPS IN FILE"))
    lines.append(_write_label("    SPHE", "MAPPING FUNCTION"))
    lines.append(_write_label(f"{10.0:8.1f}", "ELEVATION CUTOFF"))
    lines.append(_write_label("Spherical harmonic", "OBSERVABLES USED"))
    lines.append(_write_label(f"{HEIGHT_KM:8.1f}{HEIGHT_KM:8.1f}{0.0:8.1f}", "HGT1 / HGT2 / DHGT"))
    lines.append(_write_label(f"{LAT1:8.1f}{LAT2:8.1f}{DLAT:8.1f}", "LAT1 / LAT2 / DLAT"))
    lines.append(_write_label(f"{LON1:8.1f}{LON2:8.1f}{DLON:8.1f}", "LON1 / LON2 / DLON"))
    lines.append(_write_label(f"{EXPONENT:6d}", "EXPONENT"))
    lines.append(_write_label("", "END OF HEADER"))

    for map_idx in range(N_MAPS):
        epoch = DATE + timedelta(seconds=INTERVAL_SEC * map_idx)
        lines.append(_write_label(f"{map_idx + 1:6d}", "START OF TEC MAP"))
        lines.append(_write_label(_fmt_epoch(epoch), "EPOCH OF CURRENT MAP"))
        for lat_idx in range(N_LAT):
            lat = LAT1 + lat_idx * DLAT
            # The body of the LAT/LON1/LON2/DLON/H line:
            body = (
                f"{lat:8.1f}{LON1:8.1f}{LON2:8.1f}{DLON:8.1f}{HEIGHT_KM:8.1f}"
            )
            lines.append(_write_label(body, "LAT/LON1/LON2/DLON/H"))
            # 73 integers, 16 per row, width 5.
            ints: list[int] = []
            for lon_idx in range(N_LON):
                lon = LON1 + lon_idx * DLON
                tec = _tec_at(lat, lon, epoch.hour + epoch.minute / 60.0)
                ints.append(int(round(tec * 10**(-EXPONENT))))
            for chunk_start in range(0, N_LON, 16):
                chunk = ints[chunk_start:chunk_start + 16]
                row = "".join(f"{v:5d}" for v in chunk)
                lines.append(row)
        lines.append(_write_label(f"{map_idx + 1:6d}", "END OF TEC MAP"))

    lines.append(_write_label("", "END OF FILE"))
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    out_dir = Path(__file__).parent
    txt = build_ionex()
    plain = out_dir / "synthetic_gannon_2024131.inx"
    plain.write_text(txt)
    gz_path = out_dir / "synthetic_gannon_2024131.inx.gz"
    gz_path.write_bytes(gzip.compress(txt.encode("ascii")))
    print(f"wrote {plain} ({plain.stat().st_size} bytes)")
    print(f"wrote {gz_path} ({gz_path.stat().st_size} bytes)")
