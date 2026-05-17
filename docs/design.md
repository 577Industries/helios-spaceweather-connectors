# Adapter pattern

This page documents the shared framework every adapter in this package
implements. The goal is that adding a new source — SWPC, GIM, GOES,
DSCOVR — is a matter of subclassing `BaseAdapter` and filling in the
HTTP details, *not* re-deciding lifecycle, rate-limiting, caching, or
provenance semantics.

## High-level shape

```
┌─────────────────────────────────────────────────────────────┐
│                       BaseAdapter (ABC)                     │
│                                                             │
│  source_id   : SourceID  (class var, must be set)           │
│  fetch()     : AsyncIterator[NormalizedRecord] (abstract)   │
│  fetch_sync(): list[NormalizedRecord]                       │
│  _emit_provenance() -> ProvenanceRecord                     │
│  _ratelimiter / _cache / _client / _owns_client             │
└──────────────────────────┬──────────────────────────────────┘
                           │
            ┌──────────────┼──────────────┐
            │              │              │
   ┌─────────────┐  ┌─────────────┐  ┌─────────────┐
   │ DonkiAdapter│  │ SwpcAdapter │  │ GoesAdapter │
   │ (v0.1)      │  │ (v0.2)      │  │ (v0.3)      │
   └─────────────┘  └─────────────┘  └─────────────┘
```

Every adapter exposes:

- An **async streaming `fetch(start, end, **kwargs)`** returning an
  `AsyncIterator[NormalizedRecord]`. This is the production interface;
  downstream pipelines can apply backpressure by consuming records one
  at a time.
- A **sync `fetch_sync(start, end, **kwargs)`** convenience wrapper
  that drains the async iterator into a list. Useful for notebooks and
  one-shot scripts. Refuses to run inside an active event loop.
- Source-specific endpoint conveniences (e.g. `fetch_cme`,
  `fetch_gst`) that delegate to `fetch()` with the right kwargs.
- A `source_id: ClassVar[SourceID]` so downstream code can dispatch on
  the source without string matching.

## The `NormalizedRecord` contract

```python
@dataclass
class NormalizedRecord:
    source: SourceID            # which upstream system produced this
    record_type: str            # source-local discriminator (e.g. "CME")
    event_time: datetime        # UTC
    value: dict[str, Any]       # JSON-serializable science payload
    value_units: str            # e.g. "pfu", "nT", "none"
    provenance: ProvenanceRecord
    raw: dict[str, Any]         # the unmodified upstream response
```

Every adapter produces these. The shape is deliberately wider than any
single source needs so we can support both scalar (e.g. Kp index) and
structured (e.g. CME with linked flares) payloads in `value`.

## Provenance: feature-level lineage

```python
@dataclass(frozen=True)
class ProvenanceRecord:
    id: str                  # globally unique
    schema_version: str
    model_id: str            # e.g. "donki/CME", "swpc/kp-3-hour"
    dataset_refs: tuple[str, ...]
    timestamp: datetime
    value: Any
    value_units: str
    ingestion_timestamp: datetime  # when this adapter observed the upstream
    lineage: tuple[str, ...] # upstream events / models contributing
```

For DONKI, `lineage` is populated from the `linkedEvents` array
(intelligent linkages). For other sources, `lineage` carries whatever
upstream identifier chain makes the record reproducible — e.g. for a
SEP Scoreboard A entry, the lineage tuple is the list of model IDs
contributing to that fused row.

This is a **placeholder** type in `helios_connectors.schema` that will
be replaced by an import from `helios-provenance` once that package's
v0.1 schema is pinned. The swap is a single-line change because the
field shape was designed to match.

## Concurrency model

- The base class wraps every adapter in an async context manager so
  the underlying `httpx.AsyncClient` is closed cleanly even if the
  caller bails out mid-iteration.
- Per-adapter rate limiters (`RateLimitConfig` token bucket) live
  *inside* the adapter instance. Two adapters on different sources
  never share a limiter; concurrent calls to a slow source can't
  back-pressure a fast one.
- For the unified DONKI `fetch(types=[...])` call we use
  `asyncio.gather` to issue per-endpoint calls concurrently. Each call
  passes through the same rate limiter, so the *aggregate* rate stays
  inside CCMC's limit even when the underlying HTTP calls overlap.

## Caching

- Default cache lives under `~/.cache/helios-connectors/<source>/...`.
- Override via the `HELIOS_CACHE_ROOT` env var.
- Keyed by `(source_id, sorted(query_params))` → SHA-256 fingerprint.
- Contents are pandas DataFrames serialized to parquet via pyarrow,
  so any DuckDB / Polars / pandas user can inspect them out of band.
- Pass `cache=False` on construction to disable; pass an explicit
  `FileCache` instance to use a custom one.

## HTTP and retry

- Every adapter uses `helios_connectors.http.make_client()` for its
  `AsyncClient`. This sets the User-Agent NASA APIs prefer, a 30s
  total timeout, and a connection pool capped at 20.
- `request_with_retry()` retries `httpx.TransportError` and HTTP 429 /
  5xx with exponential backoff (1s → 30s, 4 attempts).
- API keys are passed as query parameters per NASA convention and are
  *never* logged. `safe_log_params` allowlists which params are
  included in debug logs.

## Adding a new adapter — the recipe

1. **Pick a `SourceID`** — add a member to
   `helios_connectors.schema.SourceID`.
2. **Subclass `BaseAdapter`** in `helios_connectors.adapters.<name>`.
3. **Set `source_id`** as a `ClassVar[SourceID]`.
4. **Override `_default_rate_limit`** if the source needs a non-10-RPS
   default. NOAA SWPC: 5 RPS. DEMO_KEY-gated calls: 1 RPS.
5. **Implement `fetch()`** — it must be an async generator. Use
   `await self._ratelimiter.acquire()` before every outbound call.
6. **Build each `NormalizedRecord` with `self._emit_provenance()`**
   so timestamps are UTC-normalized and IDs are stable.
7. **Tests**: capture a handful of real upstream responses into
   `tests/fixtures/<source>/`, write a `test_<source>.py` modeled on
   `test_donki.py`, mark a live integration test with
   `@pytest.mark.live`.
8. **Docs**: add `docs/adapters/<source>.md` and update the table in
   `docs/index.md`.
9. **Add to `helios_connectors.adapters.__init__`** so the adapter is
   importable from `helios_connectors`.

The next four adapters (SEP Scoreboards, SWPC, CDDIS GIMs, GOES /
DSCOVR wrappers) are designed to be cloned from `DonkiAdapter`'s
shape; if you find yourself wanting to break the base class to make
your adapter fit, that's a signal the base class needs to widen,
not that your adapter is a special case.
