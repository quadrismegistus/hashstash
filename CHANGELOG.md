# Changelog

## Unreleased

Serialization, engines, profiling, and hardening. Several items change stored
bytes or default behavior — noted **BREAKING** below (cached values are
regenerable, so these are safe to adopt; they just change where/how data is
stored, not its correctness).

### Security
- **Fixed an arbitrary-code-execution bypass in `safe=True` mode** (found by a
  pre-1.0 red-team). `_deserialize_custom` dispatched `CUSTOM_DESERIALIZERS[addr]`
  — keyed by the payload's own `__py__` — before the code-type refusals, and four
  registered addresses (`function`/`type`/`object`/`ReusableGenerator`) route to
  `exec`/`compile` reconstruction, so a 2-key payload (`{"__py__":"function",
  "__source__":...}`) executed code under `safe=True` / `HASHSTASH_SAFE=1` through
  every read path (`get`/`[]`/`get_all`/`items`/`values`/`assemble_df`). The
  dispatch now refuses those code-reconstructing deserializers in safe mode. Also
  hardened the numpy deserializer to reject an object dtype from a byte buffer
  (a memory-corruption primitive). Regression tests in
  `tests/test_safe_mode_bypass.py`. If you rely on `safe=True` for untrusted
  caches, upgrade.

### Reliability & ergonomics (pre-1.0 review)
Driven by real production feedback + fresh-user review passes.
- **BREAKING (quieter): logging is now WARNING-level on stderr, not INFO on
  stdout.** hashstash chatter no longer interleaves with your program's stdout
  (you can `2>/dev/null` it), and caught/cached exceptions no longer shout —
  one failure passing through the wrapped get/deserialize layers used to ERROR-
  log 6×. Set `HASHSTASH_LOG=INFO` (or `logging.getLogger('hashstash')
  .setLevel(...)`) for the play-by-play. Source lookups for REPL/notebook
  functions are cached (no per-call spam or slow first hit).
- **Unrecognized constructor kwargs now warn** instead of silently vanishing
  (a `dir=`/`ttll=`/`compres=` typo used to send data to the default cache or
  disable a setting with no signal). `name=` is accepted as an alias for
  `dbname=`.
- **BREAKING: `items()`/`values()` default to latest-per-key**, consistent with
  `len()` and `stash[key]`. In `append_mode` they used to yield every version,
  so DataFrames built from `items()` double-counted rewritten keys. Pass
  `all_results=True` for the full history.
- **fix(jsonl): `items()`/`values()` no longer leak TTL-expired entries** (the
  logical read view now applies the stash's ttl like the get-path; `keys()`/
  `len()` stay the raw maintenance view, matching memory/sqlite). Flat-mode
  error messages now advise `flat=False` for datetime/bytes values instead of
  the unactionable "pass b64=True".
- `len()` documents its lazy-TTL semantics (physical count; expired entries are
  hidden by `in`/`get`/iteration but counted until overwritten).
- **lmdb** env registry is keyed by realpath, so two spellings of the same
  directory share one handle (was a double-open corruption hazard).
- New `stash.filter_keys(model="x", ...)` — convenience scan for dict keys
  containing given field=value pairs.
- `dir(hashstash)` is curated to the public surface (was leaking `os`/`sys`/
  `json`/... from the internal star-imports); `from hashstash import *` is
  unchanged.
- **Callables no longer silently corrupt.** Closures that capture variables and
  `functools.partial` used to serialize fine but come back as broken callables
  that raised only at call time (`NameError` / returned the `partial` class).
  Closures now restore their free-variable cells (incl. recursive self-refs) and
  `partial` round-trips its func/args/keywords; a genuinely un-round-trippable
  case (empty closure cell) fails loudly at *store* time with an actionable
  message.
- **~60× faster first `serialize()`/`encode_hash()` per process** (~310 ms →
  ~5 ms): backend availability is probed lazily and cached per backend, so
  constructing a `Config` no longer eagerly imports every optional dependency
  (pandas/blosc/duckdb/pymongo/redis/cbor2/msgpack/…) just to serialize.
- **`stash.map` handles spawn footguns gracefully.** With `num_proc>1`, a
  function that can't be safely reconstructed in a worker (interactive REPL /
  `python -c` / piped stdin, unguarded module top-level, or source-unretrievable
  callable) now falls back to `num_proc=1` with one actionable warning instead of
  crashing with `BrokenProcessPool` / a bootstrap `RuntimeError` / a worker
  `KeyError`. `num_proc=1` no longer round-trips the function through
  serialization, so REPL/exec-defined callables work serially.
- **BREAKING: `stash.map` defaults to `num_proc=1` (serial).** Parallel maps use
  a spawn pool that re-imports `__main__`, so the old parallel-by-default
  (`cpus-2`) crashed an unguarded top-level `map()` in a script. Serial-by-
  default needs no `if __name__ == "__main__"` guard and works everywhere; opt
  into parallelism with `num_proc=N` (and guard the script). Maps that relied on
  the parallel default now run serially unless they pass `num_proc`.
- **`StashMapRun.was_cached`** — public read-only flag for whether an item came
  from cache (the "per-item cache status" the README described).
- `stash.map` warns once when `num_proc>1` is combined with `engine='memory'`
  without ultradict (worker results can't reach the process-local parent).
- **dataframe engine (feather/parquet): stop silently stringifying object
  columns.** list-of-primitive and dict columns now store natively via Arrow and
  round-trip as real lists/dicts; only genuinely unserializable columns fall back
  to `str()`, per column. `assemble_df` renders readable keys (`_key = "Animal
  1"`) instead of bytes.
- `prune(dry_run=False)` no longer raises `KeyError` on a TTL-expired key (it
  deleted through the TTL-aware `has()`; now deletes the physical entry directly).
- Closures whose source can't be retrieved (REPL / `python -c` / exec) now fail
  loudly with an actionable `ValueError` at read time instead of a cryptic
  `NameError` (module-level and notebook closures round-trip fine).
- `stash.stats` always reports all four keys (`hits`/`misses`/`sets`/`deletes`)
  and documents that `run`/`@stashed_result` count on the function's sub-stash.
  `repr(stash)` now appends a compact summary of any non-zero counters.
- `__dir__`/lazy-probe/logging changes are behavior-only; stored bytes for
  existing caches are unaffected.

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
- **BREAKING** — dropped the pandas/polars `MetaDataFrame` wrapper. The
  `dataframe` engine and `assemble_df()` now return **plain pandas DataFrames**
  instead of a wrapper object; a polars DataFrame passed as a value is converted
  to pandas on store. This removes ~600 lines (the `__getattr__` re-wrapping and
  serialize-to-compare `__eq__`) and makes the return type predictable. Polars is
  no longer a first-class backend (it was opt-in and untested through a stash).
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

### DataFrames
- `dataframe`-engine stashes gain `stash.sql(query)` / `stash.duckdb()`: with
  `io_engine='parquet'`, run DuckDB SQL across all cached DataFrames in place
  (no deserialization), returning a pandas DataFrame. The stash stays a plain
  key-value cache underneath.

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
