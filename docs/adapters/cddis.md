# NASA CDDIS Global Ionosphere Maps (`CddisGimAdapter`)

The Crustal Dynamics Data Information System (CDDIS, NASA GSFC) is the
authoritative archive for GNSS data products, including the IGS Global
Ionosphere Maps (GIMs). HELIOS uses GIMs as the input for the §2 Obj. 4
ionospheric-forecasting model (TFT) and for the Gannon-storm v2
retrospective in `gannon-storm-rtk-analysis`.

This adapter wraps NASA CDDIS's IONEX GIM products into the HELIOS
adapter shape: an async streaming `fetch()` plus per-product helpers,
each yielding `NormalizedRecord` objects with full provenance.

## What it gives you

Vertical Total Electron Content (TEC) in **TECU**, at **2-hour** cadence,
on the standard IGS grid (typically **71 latitudes × 73 longitudes**,
2.5°×5° spacing covering the whole globe). Choose between:

* **Whole-map records** via `fetch_tec_maps()` — every record carries
  the full 71×73 grid in `record.value["tec_grid"]`.
* **Single-point time series** via `fetch_tec_at_point()` — bilinear
  interpolation at one (lat, lon).
* The unified `fetch(products=["tec_maps"])` for parity with the other
  HELIOS adapters.

## Quick start

```python
from datetime import datetime, UTC
from helios_connectors import CddisGimAdapter

# Single-station TEC time series across the Gannon week.
async with CddisGimAdapter() as cddis:
    async for rec in cddis.fetch_tec_at_point(
        start=datetime(2024, 5, 9, tzinfo=UTC),
        end=datetime(2024, 5, 12, tzinfo=UTC),
        lat=40.0,
        lon=-83.0,  # Columbus, OH
    ):
        print(rec.event_time, rec.value["tec"], "TECU")
```

You will see the storm-day enhancement clearly: quiet-day TEC over
Columbus runs 5–15 TECU; the Gannon-storm peak hits **>50 TECU** on
2024-05-10 around 20:00 UTC and remains elevated through 2024-05-11.

## Authentication setup

CDDIS requires a NASA **Earthdata Login** (URS) account. One-time setup:

1. Register at <https://urs.earthdata.nasa.gov/>.
2. Sign in, go to *Applications → Authorized Apps*, and authorize the
   **NASA GESDISC DATA ARCHIVE** application. (This is the application
   that fronts CDDIS access on URS.)
3. Export credentials in your shell:

   ```bash
   export NASA_EARTHDATA_USER="your-username"
   export NASA_EARTHDATA_PASS="your-password"
   ```

   Or populate `~/.netrc` (preferred when using the `earthaccess`
   library):

   ```
   machine urs.earthdata.nasa.gov login your-username password your-password
   ```

If credentials are missing, the adapter raises a `RuntimeError` with
the setup instructions on first download attempt — never on
construction (so test suites that don't actually hit CDDIS still pass
without credentials).

### `earthaccess` vs manual httpx

The URS authentication handshake is **fiddly** — it involves an OAuth
redirect chain across several hosts. The `earthaccess` library handles
this cleanly. Install via the optional `[earthdata]` extra:

```bash
pip install 'helios-spaceweather-connectors[earthdata]'
```

This pulls in:

* `earthaccess>=0.10` — clean URS auth + `requests` session reuse.
* `unlzw3>=0.2.2` — needed for pre-2023 `.Z` (Unix-compress) IONEX
  files. 2023-present files use `.gz` (handled by stdlib).

If `earthaccess` isn't installed, the adapter falls back to a manual
httpx `BasicAuth` against the CDDIS endpoint. This works (CDDIS
supports HTTP Basic over the URS-bound user via its proxyauth flow)
but is less robust on transient redirect failures. The manual fallback
is the default in CI.

You can force either path via the `use_earthaccess` constructor kwarg
(`True` to require, `False` to disable, `None` to pick best-available).

## Analysis center options

CDDIS publishes GIMs from several IGS analysis centers. The `center`
kwarg selects which one to pull:

| slug | name |
|------|------|
| `igsg` *(default)* | **IGS combined** — the weighted-average of all contributors, with outlier rejection. Gold standard. |
| `jplg` | Jet Propulsion Laboratory |
| `codg` | CODE (University of Bern) |
| `esag` | European Space Agency |
| `upcg` | Universitat Politecnica de Catalunya |

For HELIOS fusion work, use `igsg` unless you specifically need
per-center variation as a feature. The IGS combined product is the
operational standard.

## Cache layout and lazy-fetch policy

The full 24+-year CDDIS GIM archive is a **few terabytes** uncompressed.
The adapter is built to never download more than the user asked for:

* **One IONEX file = one UTC day** (13 maps, 00 UT through 24 UT).
* Files are cached under
  `HELIOS_CACHE_ROOT/cddis/<year>/<doy>/<center>_<year>_<doy>.ionex`
  (decompressed) alongside the original compressed file.
* `HELIOS_CACHE_ROOT` defaults to `~/.cache/helios-connectors`; override
  via the env var or the `cache_root` constructor kwarg.
* **Lazy-fetch always**: no pre-warming. The adapter walks the
  requested date range one UTC day at a time, hits the cache first, and
  downloads only on miss.

Approximate footprints:

| window | rough size |
|--------|-----------|
| 1 day  | ~50–200 KB compressed; ~200–500 KB decompressed |
| 1 week | ~1–2 MB |
| 1 month | ~5–10 MB |
| 1 year | ~50–75 MB |
| Full archive (1998–present) | ~few GB |

You can safely cache the full Gannon week (~2 MB) on every CI runner.

## Filename conventions

CDDIS uses **two** filename conventions:

* **Pre-2023** legacy short form:
  `<year>/<doy>/<center><doy>0.<yy>i.Z`
  e.g. `2018/100/igsg1000.18i.Z`

* **2023-present** long form (RINEX-3-style):
  `<year>/<doy>/<CENTER>0OPSFIN_<YYYY><DOY>0000_01D_02H_GIM.INX.gz`
  e.g. `2024/131/IGS0OPSFIN_20241310000_01D_02H_GIM.INX.gz`

The adapter probes both URLs (long-form-first for 2023+, legacy-first
for older years) and uses whichever returns 200. Both decompression
formats are handled.

## IONEX format brief

IONEX 1.0 / 1.1 is a fixed-width ASCII format. The official spec is at
<https://files.igs.org/pub/data/format/ionex1.pdf>. Key header records
the parser consumes:

* `EPOCH OF FIRST MAP` / `EPOCH OF LAST MAP` — window bounds.
* `INTERVAL` — seconds between maps (typically 7200).
* `# OF MAPS IN FILE` — usually 13.
* `LAT1 / LAT2 / DLAT` — latitude grid (87.5 / -87.5 / -2.5).
* `LON1 / LON2 / DLON` — longitude grid (-180 / 180 / 5.0).
* `EXPONENT` — TEC values are integers, scaled by 10**EXPONENT TECU
  (typically -1 → values are tenths of a TECU).
* `END OF HEADER`.

Each map block is delimited by `START OF TEC MAP` /
`END OF TEC MAP`. Per-latitude rows start with a `LAT/LON1/LON2/DLON/H`
line followed by integer TEC values, width-5, 16 per continuation
line.

The HELIOS parser is **~100 lines of pure Python** (no `xarray` or
`georinex` heavyweight dep). RMS-map blocks are silently skipped.
Sentinel cells (`9999`) become `float('nan')`; the bilinear sampler
falls back to the mean of valid corners when one or more is NaN.

## Worked example: Columbus, OH during the Gannon week

Columbus, OH (40.0°N, 83.0°W) sat near the eastern footprint of the
mid-latitude trough during the May 2024 Gannon G5 storm. Quiet-day
vertical TEC over central Ohio is typically 5–15 TECU mid-afternoon
local time; the storm pushed peak vertical TEC over the U.S. Midwest
**above 50 TECU**, with the most dramatic enhancement on the May 10
afternoon/evening local time (i.e. roughly 18:00–24:00 UTC).

```python
from datetime import datetime, UTC
from helios_connectors import CddisGimAdapter

async with CddisGimAdapter() as cddis:
    columbus_tec = [
        (rec.event_time, rec.value["tec"])
        async for rec in cddis.fetch_tec_at_point(
            start=datetime(2024, 5, 9, tzinfo=UTC),
            end=datetime(2024, 5, 12, tzinfo=UTC),
            lat=40.0,
            lon=-83.0,
        )
    ]

peak = max(columbus_tec, key=lambda t: t[1])
print(f"Peak TEC at Columbus: {peak[1]:.1f} TECU at {peak[0]}")
# → typically prints something like:
#   Peak TEC at Columbus: 54.3 TECU at 2024-05-10 20:00:00+00:00
```

This is the workhorse query for the §2 Obj. 4 TFT input pipeline and
for the Gannon v2 RTK analysis SPP corrections.

## Rate-limit and etiquette

CDDIS publishes **no documented rate limit**, but heavy bursts from a
single IP trigger throttling in practice. The adapter defaults to
**2 RPS** with a burst of 4 — well inside the practical threshold and
matching the HELIOS-program standard for "no documented limit"
sources.

The adapter never logs credentials. URS cookies are kept in memory
only (not persisted to disk) when the manual path is used; the
`earthaccess` path may write a netrc-style token cache per its own
config.

## Provenance shape

Every record carries:

* `source = SourceID.CDDIS_GIM`
* `record_type = "tec_map"` or `"tec_point"`
* `value_units = "TECU"`
* `provenance.model_id = "cddis/ionex/<center>"`
* `provenance.dataset_refs = (<filename or URL>,)`
* `provenance.lineage = (<analysis-center name>, <source URL>)`

For the future swap to the real `helios-provenance-spec` v0.1
`HeliosModelOutputRecord` shape, use the static
`CddisGimAdapter.to_helios_model_output(record)` converter. Whole-map
records flatten to the spatial-mean TEC in `value` and the full grid
in `extra`; point records emit the scalar TEC directly.
