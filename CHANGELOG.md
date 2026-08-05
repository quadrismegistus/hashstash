# Changelog

## Unreleased

## 1.2.0 — 2026-08-05

Minor rather than patch, on the same principle as 1.1.0: the version number is
signalling a semantic change, not diff size. Every item below is a bug fix, but
one of them changes observable behaviour — a sub-stash you created with `sub()`
now **survives** `parent.clear()` called on the handle that created it, where it
was previously destroyed. If you relied on a parent's `clear()` to cascade into
sub-stashes, call `sub.clear()` yourself. Memoized `stashed_result` caches are
unaffected: they still clear with their parent, and now do so from any handle
rather than only the one that registered them.

The theme is silent failure. Every bug here — data destroyed by a routine call,
an argument accepted and ignored, a corrupt row taken as a whole-stash outage, a
destructive call that did nothing — reported success while doing the wrong
thing. Several were found while auditing hashstash as the store for a
money-critical batch ledger; the rest by an adversarial review of that first
round of fixes, which caught one regression the fixes themselves introduced.

### Fixed

- **`clear()` no longer destroys sub-stashes.** `sub()` nests a child inside the parent's *param folder* (`<root>/<dbname>/pairtree.hashstash.lz4/<sub dbname>/...`), and `clear()` removed that whole folder — so clearing a cache silently deleted every sub-stash under it, including from a handle that never created them and so never had them in `self.children`. `clear()` now removes only this stash's own storage (everything named for `self.filename`: `data.db`, the `data.db/` tree, `data.db-wal`, `data.db.dat/.dir/.bak`, `data.db.lock`, `data.jsonl.compact.<pid>`) and drops the param folder only if nothing else is left in it. The JSONL engine already behaved this way — the two engines disagreed about whether `clear()` was destructive to siblings. The `fsspec` engine, which overrides `clear()`, had the identical bug and is fixed the same way; `memory`, whose override skipped the cascade entirely, now clears registered children like everything else.
  - **`clear()` now cascades to function-result stashes only**, and does it on disk rather than through `self.children`. Cascading to every registered child made the result depend on *which handle* called it: the handle that created a sub-stash destroyed it, while a fresh handle (a CLI run, another process — the normal way anyone clears a cache) spared it. Same folder, same call, opposite outcome. Now a `sub()` you made survives either way, and `stashed_result` memoized results are removed either way — previously a fresh handle left them stale, so a "cleared" cache still returned memoized values. `GraphStash`'s sub-stashes are likewise no longer collateral; `graph.clear()` clears them explicitly.
  - Worth knowing regardless: a sub-stash still lives *inside* the parent's directory tree, so anything that removes that tree by other means (`rm -rf`, a cleanup script, `tmp()`) still takes children with it. For data with a different lifetime than the parent cache — a ledger, an audit log, anything you would be unhappy to lose with a cache clear — use a separate `HashStash` at a sibling `root_dir` rather than `sub()`.
- **`clear()` with a file-style `root_dir` did nothing at all.** `HashStash(root_dir='/x/mydata.db')` removed only the literal path — but `dbm` appends its own suffix, so for `shelve` that file never exists: `clear()` deleted nothing, reported success, and a fresh handle still read every entry. A destructive call that silently no-ops is worse than one that over-reaches, because nothing tells you. It now also removes a closed list of sidecar suffixes (`.db`, `.dat`, `.dir`, `.bak`, `.wal`, `-wal`, `-shm`, `.lock`), which covers sqlite/duckdb write-ahead logs in the same branch. Deliberately a fixed list rather than the prefix sweep used for a param folder we own: here the directory belongs to the user and holds unrelated files, so `mydata.db.backup` must survive — and does.
- **`sub(dbname=...)` naming the parent's own storage file is now refused.** A child nests under the parent's param folder, so `sub(dbname='data.db')` placed it *inside* `<param>/data.db/` — the parent's own pairtree tree. The parent's directory walk then found the child's entries and silently merged the two keyspaces: a parent holding 2 entries reported `len() == 7` and returned the child's keys from `keys()`, whether or not `clear()` was ever called. This raises `ValueError` now, rather than remapping the path, which would relocate data invisibly. Pass an explicit `root_dir` if you want a child with that name elsewhere.
- **`sub(engine=...)` now actually switches engine.** `sub()` built the child with `self.__class__(**kwargs)`, which bypasses the engine registry, so `pairtree_stash.sub(dbname='x', engine='jsonl')` returned another `PairtreeHashStash`. The argument was accepted and silently ignored, handing back the wrong storage format with no error. When the requested engine differs from the parent's, `sub()` now dispatches through the `HashStash` factory. `append_mode`, `dbname`, and the rest were always honoured and still are.
  - **If you passed `engine=` to `sub()` before, that data is now at a different path.** It was written under the *parent's* param folder (`ledger/pairtree.hashstash.lz4/`) because the argument was ignored; the same call now resolves to `ledger/jsonl.hashstash.raw/`. The old data is intact but invisible to the new call — reach it by opening the old path explicitly, or move it with `migrate()`.
  - `safe` is no longer inherited across an engine switch. It is engine-*derived*: networked engines (redis/mongo) default to `safe=True` because their writer may be untrusted, but `sub()` handed down the parent's already-materialized `False`, which won over that default. A sub-stash moved onto a shared Mongo/Redis silently got code-capable deserialization of other writers' payloads. Pass `safe=` explicitly to override.
  - `sub()` across engines no longer emits a spurious "ignoring unrecognized argument(s) `['host', 'port']`" warning telling the caller to check for typos they did not make (the arguments were inherited from a redis/mongo parent, not typed).
- **A torn final line in a JSONL stash no longer costs two rows.** A row is written by a single open/write/close, so a killed process cannot tear one — but a power loss can leave the last line truncated with no newline, and the next append then concatenated onto it, so **one** torn line silently destroyed **two** rows: its own and the next one written. Appends now terminate a stray unterminated line first (logging a warning), confining the loss to the torn row. Cost is ~10µs per append, constant in file size (~5% on the full `set()` path).
- **JSONL reads no longer depend on which JSON packages are installed.** `iter_jsonl()` delegated to `orjsonl.stream()` whenever that undeclared optional package happened to be importable, and `stream()` raises `JSONDecodeError` on the first malformed row and takes the whole read with it. So on a machine with `orjsonl` installed, one torn line broke `items()`, `values()`, and — worst — `compact()`, the call that repairs the file, while the engine's own index scan skipped bad rows on the same file. Reads are now a single tolerant line-by-line pass using **stdlib `json`**, which matters for more than tolerance: rows are written with `json.dumps`, and `json.loads` is its exact inverse, while every faster parser is *stricter* than the writer. `orjson` rejects the `NaN`/`Infinity` tokens `json.dumps` emits by default, rejects lone surrogates and nesting past 1024, and — worst, because nothing raises — decodes an integer above `2**64-1` to a float, so `2**70` reads back as `1.18e21`. Since `compact()` rebuilds the file from this function, any such row was not merely hidden on read but **permanently deleted or rewritten** by the maintenance call. This also keeps the whole engine on one parser, so `get()`/`len()` and `items()`/`values()`/`compact()` can never disagree about which rows exist.
  - A badly corrupted file now logs one warning naming the first bad line plus a total, rather than one warning per line (100k bad lines previously produced ~19MB of identical log text and a 35× slowdown).
- **A JSONL line that is valid JSON but not a row no longer bricks the stash.** Tolerating an unparseable line is worth nothing if the callers don't survive it: every consumer indexed `row['__key__']` directly, so one line of `123`, `null`, `"abc"`, `[1,2,3]`, or `{"__value__": "x"}` raised `KeyError`/`TypeError` out of `get()`, `len()`, `keys()`, `items()`, `values()`, `set()` — **and out of `compact()`**, the documented repair path. The stash could not be read, written, or repaired. Malformed rows are now skipped with a warning wherever rows are consumed. A row missing `__value__` outside flat mode is likewise skipped rather than materializing a phantom entry.

### Note on concurrency (unchanged behaviour, now documented)

Appends are safe across processes on one machine: the JSONL engine holds a cross-process file lock for every write, and the pairtree engine writes one atomically-renamed file per version. Verified with 8 concurrent processes appending to the same key (200/200 versions kept on both engines) and with `SIGKILL` mid-write (100% of completed writes readable).

What is **not** covered is a read-then-decide-then-write sequence: the lock spans the write, not your decision. Two processes can both read "absent" and both act. Wrap the whole critical section in the stash's own per-key lock:

```python
with stash.key_lock(key):
    if stash.get(key) is None:
        stash[key] = expensive_or_billable_operation()
```

`key_lock()` is a local advisory file lock — it coordinates processes on one machine, not across hosts or NFS. Note also that no engine calls `fsync`, so a machine crash or power loss can still lose recently written rows sitting in the page cache.

## 1.1.0 — 2026-07-26

Minor rather than patch: this changes the stored address of a class of cache key
for the second time in this lineage (0.5 was the first). The diff is small; the
version number is signalling the address change, not the diff size. If your keys
contain dicts with non-string keys, existing entries need `legacy_read=True` or
`migrate()` to be visible after upgrading.

### Dict key ordering: what changed in 0.5, and what was still broken until now

**If you are upgrading a cache written by hashstash < 0.5, read this.** Before
0.5, a dict used as a cache key was addressed **in its insertion order**:
`{'model': m, 'prompt': p}` and `{'prompt': p, 'model': m}` were two *different*
cache entries. Since 0.5 (commit `d7bbbab`), keys serialize with `sort_keys=True`,
so equal dicts address to the same entry regardless of construction order.

That was a deliberate fix — the old behaviour caused silent misses and duplicate
recomputation — but it was **never stated plainly**: this changelog begins at
1.0.0, and the 1.0.1 entry below refers to it only as "a latent key-address
instability", which does not mention dicts or ordering. At least one downstream
project lost a day to exactly that gap. Concretely:

- A pre-0.5 entry is visible to 0.5+ **only if** the call site that wrote it
  happened to build the dict in alphabetical order. Otherwise it still exists, still
  enumerates via `keys()`, and silently fails to resolve.
- Sets and frozensets were worse than insertion-order sensitive before 0.5: they
  serialized in `PYTHONHASHSEED`-dependent iteration order, so a key containing a
  set was not stable **across runs of the same interpreter**. A pre-0.5 cache with
  set-containing keys is not reliably re-addressable at all; migrate it.
- 0.5+ does **not** read pre-0.5 addresses natively. `legacy_read=True` reaches
  them with a decode-and-match scan; `migrate()` re-addresses them permanently.

### Fixed

- **Dicts with non-string keys now canonicalize on the key path.** `sort_keys=True`
  sorts JSON *object* keys, but a dict with non-string keys (or one holding a
  reserved `__py__`/`__pytype__`/`__data__` marker) serializes to a JSON *list* of
  `[key, value]` pairs, and `sort_keys` does not reorder array elements. So
  `{1: 'a', 2: 'b'}` and `{2: 'b', 1: 'a'}` were **still** two different cache
  entries on 1.0.1 — the 0.5 fix never reached this path. The same applied to dict
  subclasses (`Counter`, `defaultdict`) via the reducer's `__dictitems__`. These
  lists are now sorted by their serialized form, matching how set elements have
  been ordered since 0.5.
  - `OrderedDict` is deliberately **excluded**: its `__eq__` is order-sensitive, so
    two differently-ordered `OrderedDict`s are *unequal* keys and collapsing them to
    one address would be a false hit — worse than the miss being fixed. This is
    decided from the **live type** (`type(obj).__eq__ is dict.__eq__`), not from the
    serialized type address, so `OrderedDict` *subclasses* are excluded too. Types
    that keep their ordering in reduce state rather than in their items (e.g.
    `bson.SON`) are excluded on the same grounds.
  - Values are unaffected: a dict value's insertion order is observable on
    round-trip and is still preserved. Only keys canonicalize.
  - `keys()` now returns non-string-keyed dict keys in **canonical order**, not the
    order they were stored in — `stash[{2:'b', 1:'a'}] = 1` enumerates back as
    `{1:'a', 2:'b'}`. They are `==`-equal, so lookups are unaffected, but code that
    reads `list(key.items())` positionally will see a different order.
  - *This changes the address of affected keys.* If you have stored keys containing
    non-string-keyed dicts, recover them with `legacy_read=True` or `migrate()`.
- **Partial key-encoding drift now warns.** `items()` warned only when *no* key
  resolved, so a stash where (say) 20% of entries were unaddressable looked
  perfectly healthy — the more dangerous shape, since total drift at least
  announces itself. It now warns on any shortfall. The check counts **keys that
  resolved**, not values yielded: `get_all()` returns every stored version, so on an
  append-mode stash the value count routinely exceeds the key count and a
  value-based comparison would stay silent exactly where there is most history to
  lose (100 keys, 50 resolvable, 3 versions each → 150 values > 100 keys).
- **An explicit `before`/`after` window is no longer mistaken for drift.**
  `items(after=...)` that legitimately filtered out every entry used to emit the
  "written by an OLDER hashstash" warning.
- **`values()` now applies the time filter it was given.** It accepted `**kwargs`
  and silently dropped them, so `values(after=X)` and `values_l(after=X)` returned
  everything while `items(after=X)` filtered correctly — the same query spelled two
  ways gave different answers.

## 1.0.1 — 2026-07-05

Fixes a release-critical backward-compat break found by production consumers on
real pre-1.0 caches.

- **Recover caches written by an older hashstash.** The cache-key encoding drifted
  across versions, so a pre-1.0 cache would *enumerate* (`keys()`/`len()` work)
  but `get()`/`in`/`items()` silently missed every entry — `encode_key(key)` now
  hashes to a different address than where the value was stored. Fresh 1.0 caches
  were never affected. Recovery:
  - **`stash.migrate(dest=..., dry_run=True/False)`** now reads via the raw stored
    entries (so it recovers a drifted cache, which the old `items()`-based migrate
    could not), returns a `{'total','migrated','failed','dest'}` report to diff
    against expected counts, and `dry_run=True` counts without writing (safe on a
    huge stash). It re-appends every stored version, so an append-mode source's
    **edit history is preserved** (into an append-mode dest). `dest` may be a path
    string/`Path` — a stash is built there **inheriting the source layout**
    (engine/serializer/compress/b64) so the data reads back the same way; a
    non-stash, non-path `dest` now raises `TypeError` up front instead of silently
    failing every write. The report includes `first_error`, and an all-failed run
    warns loudly. If it finds nothing but a sibling layout dir has data, it warns
    that the source was opened with the wrong `b64`/`compress`/engine kwargs (the
    layout is encoded in the path).
    *(Return type changed from the dest stash to the report dict —
    `report['dest']` holds the destination.)*
  - **`legacy_read=True`** on a stash falls back to a decode-and-match read of the
    old-format entry — covering `get()`, `key in stash`, and `items()` (so the
    common `if key in stash: stash[key]` hot path works). Read-only — it never
    rewrites, so a large stash is never churned.
  - **`stash.iter_recovered()`** streams `(key, value)` for every stored version.
  - **Loud warning:** `items()` now warns when keys enumerate but nothing resolves
    (the drift signature) instead of silently looking empty. These data-integrity
    warnings go out on both the logger (stays loud under `filterwarnings('ignore')`)
    and as a catchable `HashStashWarning` (for `warnings.catch_warnings`).
- **lmdb: no more teardown `TypeError`.** Abandoning a `keys()`/`items()` generator
  mid-iteration could raise "catching classes that do not inherit from
  BaseException" during interpreter shutdown (the `MapResizedError` handler
  resolved the class via attribute lookup on a half-cleared module); the exception
  classes are now bound to locals.
- Docs: a relative `root_dir` resolves under `~/.cache/hashstash` (not the CWD) —
  now called out explicitly.

Upgrading from a pre-1.0 cache: `report = old.migrate(dest=new, dry_run=True)` to
count, then `dry_run=False` to recover. Or open with `legacy_read=True`. Note the
`b64` default is `False` in 1.0 and is part of the on-disk path, so a cache
written under the old `b64=True` default also needs `b64=True` (or migration).

You may see **fewer keys after migrating** — that's a fix, not data loss. Some
older caches wrote logically-identical keys under different addresses (a latent
key-address instability that caused silent misses and duplicate re-computation —
specifically, dict keys were addressed by insertion order before 0.5; see the
Unreleased section above);
1.0's deterministic canonical-key addressing collapses those duplicates. All
stored *versions* are preserved — only the redundant addresses merge. (A
production migration of 34 stashes saw one stash go from 24,856 to 23,056 keys,
with all 24,894 stored versions intact.)

## 1.0.0 — 2026-07-05

First stable release. Consolidates the serializer type-coverage + speed work,
the engine hardening (jsonl O(1) index, engine-aware safe defaults, LMDB config),
GraphStash edge-property indexing, async exception caching, the MetaDataFrame
removal (plain-pandas return types), and an extensive pre-1.0 review round — a
security red-team, cross-version portability, networked-engine and concurrency
stress tests — that fixed a critical `safe=True` code-execution bypass and a set
of concurrency and ergonomics bugs. See the sections below.

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

### Concurrency (pre-1.0 stress review)
- **pairtree same-key concurrent writes no longer lose the value.** Two writers
  each pruned "all versions except the one I wrote", deleting each other's file
  and leaving the key valueless (12–25% of same-key races) with `len` disagreeing
  with `has`. Prune now keeps the newest version file (which all writers agree
  on), so a value always survives.
- **pairtree concurrent read+write no longer raises.** A version file pruned
  between the exists-check and the read now reads as a clean miss instead of a
  `TypeError`/`FileNotFoundError`.
- **lmdb no longer loses writes when another process grows the map.**
  `MapResizedError` is now handled with `set_mapsize(0)`+retry (adopt the grown
  map) instead of reopening at the stale size and dropping the write.
- **append-mode is now atomic under concurrency on the KV engines.** The
  read-modify-write of an append was unlocked on `needs_lock=False` engines
  (lmdb/redis/mongo/diskcache/duckdb/leveldb/fsspec/memory), so concurrent
  appends lost versions (lmdb kept ~330/2400); it now holds a per-key lock (one
  machine; multi-host redis/mongo appends can still race and are documented).
- **`stash.map` no longer re-runs a hard-crashed item in the parent.** A worker
  segfault/OOM mid-item surfaces a clear error instead of recomputing in-process
  (which double-ran side effects and could take the parent down). Submit-time
  pool failures, where the item never ran, still degrade to in-process compute.
- `DEFAULT_APPEND_MODE` is now `False`, matching the actual default (the constant
  said `True` but the `__init__` default shadowed it).

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
