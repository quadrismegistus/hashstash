# HashStash roadmap

Ideas that are proposed but **not yet implemented**, roughly ordered by
value-per-effort. Contributions welcome — open an issue to discuss before
starting anything substantial.

## Open

### GraphStash edge-property indexing
v0.9 added a `rel` index (`edges_where(rel=...)` visits only matching sources).
Arbitrary edge-*property* predicates (`weight__gt=...`) still scan within that
narrowed set. A per-property index (sorted structures for range operators)
would speed property-heavy queries — a larger lift with careful
maintenance-on-write.

### Optimize `_serialize_custom`
The recursive Python conversion to a JSON-safe structure dominates
serialization time — it, not the final `json`/`orjson` dumps, is the real
bottleneck (orjson's 10x dumps win only shows through as ~1.2x end-to-end
because of it). A fast path that detects already-JSON-native values and hands
them straight to the encoder — carefully, since tuples/non-str-keys/non-finite
floats need the full path — would unlock much larger gains.

### Async exception caching
`@stashed_result` over `async def` works, and exception caching works for sync
`run`/`get_set`, but `arun` doesn't yet negative-cache. Thread
`_cache_exceptions` through the async path.

### LMDB `map_size` cap tuning
Auto-grow is in (doubles on `MapFullError`, capped at 256 GB). The cap could be
configurable per stash for very large stores.

---

## Delivered

- **v0.6** — full audit fix-set (~60 issues).
- **v0.7** — TTL, single-flight, stats, per-call invalidation.
- **v0.8** — safe deserialization mode, msgpack serializer, fsspec engine, async
  API, `GraphStash.batch()`, JSONL `compact()`, `max_entries` eviction.
- **v0.9** — GraphStash `rel` index, exception/negative caching, DuckDB engine,
  LevelDB engine (plyvel), LMDB auto-grow, orjson value acceleration, cbor2
  serializer.
