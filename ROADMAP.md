# HashStash roadmap

Ideas that are proposed but **not yet implemented**, roughly ordered by
value-per-effort. Contributions welcome — open an issue to discuss before
starting anything substantial.

## Near-term

### GraphStash query indexing
`GraphStash.batch()` optimized the *write* side of the graph, but the read
side (`edges_where`, `edges`, `num_edges`) still does an O(V+E) full scan on
every query. Add secondary indexes — by `rel` and by edge property — so
queries touch only matching edges instead of walking the whole graph.
*Half-finished: the write-batching landed in v0.8; the query index did not.*

### Exception / negative caching
Cache function **failures** (raised exceptions, or a sentinel "not found") so
an expensive call that legitimately fails or returns nothing isn't
re-executed on every invocation. Would extend `stash.run` / `get_set` with an
opt-in `cache_exceptions=` (with its own shorter TTL, typically). Listed as an
API gap in the original audit; never built.

## Engines

### DuckDB engine
An indexed, SQL-queryable engine with native DataFrame assembly — could
eventually subsume the `dataframe` engine and make `assemble_df` fast at
scale. Needs the `duckdb` dependency and real integration testing.

### LMDB auto-grow
`map_size` is configurable (and survives pickling) as of the audit work, but
LMDB still hard-fails with `MapFullError` at the limit. Catch `MapFullError`,
grow the map, and retry the transaction so long-lived stores don't wedge.

### RocksDB / LevelDB engine
A `plyvel`-based alternative to LMDB without a fixed `map_size`. Lower
priority — LMDB already covers most of this niche.

## Serializers

### orjson value acceleration
Use `orjson` to speed up the `hashstash` serializer's **value** path.
Important constraint: **key** bytes must remain stdlib-`json` canonical
(`sort_keys=True`) so cache-key hashes never depend on which JSON library is
installed — otherwise the same key hashes differently across environments.

### cbor2 serializer
Another compact, data-only binary format alongside `msgpack`. Nice-to-have;
`msgpack` already fills the fast-data-only slot (and pairs with `safe=True`).

---

*Delivered in the v0.6–v0.8 cycle (for context): the full audit fix-set
(v0.6), then TTL / single-flight / stats / invalidation (v0.7), then safe
deserialization mode, the msgpack serializer, the fsspec engine, an async
API, `GraphStash.batch()`, JSONL `compact()`, and `max_entries` eviction
(v0.8).*
