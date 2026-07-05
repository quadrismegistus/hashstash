# Changelog

## Unreleased

Serialization, engines, profiling, and hardening. Several items change stored
bytes or default behavior — noted **BREAKING** below (cached values are
regenerable, so these are safe to adopt; they just change where/how data is
stored, not its correctness).

### Serializer
- **Comprehensive type coverage** — numpy scalars/`datetime64`/`timedelta64`/
  structured arrays, pandas `Timestamp`/`Timedelta`/`Period`/`NaT`/`Categorical`/
  the full `Index` family, nullable/categorical/tz-aware `Series`, `enum`/
  `IntEnum`, `zoneinfo`, and dict subclasses now round-trip. Guarded by a
  `hypothesis` property test over the whole type zoo.
- **BREAKING** — the DataFrame serializer was rewritten column-wise (preserves
  every column's exact dtype; the old `df.values` path degraded e.g. `Int64` to
  `object`). Non-finite floats and numpy `inf`/`nan` are now stored via a tagged
  form (previously corrupted to `null`/`None`).
- ~3.2× faster serialize (JSON-native fast-path) and ~2.8× faster deserialize
  (symmetric fast-path).

### Engines
- **jsonl** gained a key→offset index: random `get` is now an O(1) seek instead
  of an O(file) scan (~80 ms → ~1 ms at 100 KB).
- **BREAKING** — networked engines (redis, mongo, remote fsspec) now default to
  `safe=True` reads (untrusted-writer protection); pass `safe=False` to opt back
  into code-executing deserialization.
- **BREAKING** — `DEFAULT_B64` is now `False`. Binary-capable engines
  (pairtree/lmdb/leveldb/diskcache/duckdb/memory/...) no longer pay base64's
  ~33% size + CPU overhead; text-only engines (jsonl/redis/mongo/shelve) still
  force it on. Because `b64` is part of the on-disk path, existing caches sit at
  their old path after upgrade (not found, not corrupted).
- **dataframe engine** now preserves dtypes. It was blanket-`str()`-ing every
  cell before a feather/parquet write and re-inferring types on read, which
  silently dropped nullable `Int64`/`boolean`, datetime, and categorical dtypes.
  Now typed columns are written natively (arrow preserves them) and only object
  columns are coerced; re-inference is limited to the text formats (csv/json)
  that actually need it.
- **lmdb** auto-grow ceiling is now configurable via `max_map_size` (default
  256 GB), alongside the existing `map_size`; both survive serialization.
- Value envelopes now carry an explicit format version (`_fv`) for future
  migrations.

### GraphStash
- Edge queries gained an **edge-property equality index**: `edges_where(field=value)`
  now narrows sources via a secondary index (like the existing rel index) instead
  of scanning every source. Range/other operators still apply within the narrowed
  set. Built lazily, maintained on add, invalidated on remove.

### Async
- `arun()` now has exception-caching parity with sync `run()`: for `async def`
  functions it honors `_cache_exceptions`/`_exception_ttl` (negative-caches a
  failed await), and it re-raises a still-valid cached exception on a hit
  instead of leaking the marker as a value.

### Parallel map
- **BREAKING** — a `StashMap` (from `stash.map`) now **iterates and indexes the
  computed values** (like builtin `map` / `pmap`): `list(sm)`, `for x in sm`,
  `sm[0]`, `sm[1:3]` all return results. The `StashMapRun` wrapper objects moved
  to `.runs` (`.results`/`.items()`/`.values()` are unchanged). Previously
  iterating yielded the wrappers, which surprised everyone.
- Progress bars auto-silence when output isn't a TTY (pipes/CI/redirected),
  instead of spamming non-interactive runs.

### Profiling & docs
- `HashStashProfiler.compare_serializers`, `scripts/bench_serializers.py`,
  `scripts/bench_engines.py`, and `BENCHMARKS.md`.
- Fixed and modernized the profiler plot methods; README figures are regenerated
  from them (isolated-I/O engine biplot, all "lower = faster"), plus a
  comparison-to-alternatives table and a per-component reference.

### Infrastructure
- Tag-based releases; broader engine test coverage (duckdb/leveldb/fsspec in the
  shared contract battery); CI hardened against a flaky spawn-multiprocessing
  hang (`timeout-minutes`, `pytest-timeout`, `pytest-rerunfailures`).
