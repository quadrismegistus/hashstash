# Serializer benchmarks

Baseline serialize/deserialize throughput and output size for each installed
serializer, on a JSON-native payload (`dict`) and a full-path payload (`mixed`:
tuples, sets, bytes, datetimes). Regenerate with:

```bash
python scripts/bench_serializers.py --sizes 1000 10000 100000 --iterations 25
```

**These numbers are wall-clock and machine-dependent.** Use them for *relative*
comparison (serializer vs serializer, fast-path vs full-path) and to catch
regressions — not as absolute guarantees. `dict` exercises the hashstash
serializer's JSON-native fast-path; `mixed` forces its full path.

## Baseline (Apple M-series, Python 3.12; ~25 iterations, mean)

| serializer | data type | payload | serialize | deserialize | encoded size |
|---|---|---:|---:|---:|---:|
| hashstash | dict | 1,000 B | 18 µs | 28 µs | 1,847 B |
| hashstash | dict | 10,000 B | 80 µs | 127 µs | 10,478 B |
| hashstash | dict | 100,000 B | 795 µs | 1,129 µs | 100,666 B |
| hashstash | mixed | 1,000 B | 194 µs | 230 µs | 3,779 B |
| hashstash | mixed | 10,000 B | 318 µs | 338 µs | 12,438 B |
| hashstash | mixed | 100,000 B | 1,668 µs | 1,754 µs | 102,615 B |
| pickle | dict | 100,000 B | 419 µs | 232 µs | 86,275 B |
| pickle | mixed | 100,000 B | 430 µs | 235 µs | 86,759 B |
| msgpack | dict | 100,000 B | 363 µs | 212 µs | 82,446 B |
| msgpack | mixed | 100,000 B | — | — | *unsupported (sets)* |
| cbor2 | dict | 100,000 B | 794 µs | 320 µs | 82,468 B |
| cbor2 | mixed | 100,000 B | 799 µs | 817 µs | 82,840 B |
| jsonpickle | dict | 100,000 B | 1,995 µs | 3,171 µs | 105,182 B |
| jsonpickle | mixed | 100,000 B | 2,082 µs | 3,147 µs | 106,299 B |

(A few sizes elided for brevity — run the script for the full sweep.)

## What the numbers say

- **pickle** is the fastest all-rounder and handles every type — but it is not
  portable across Python versions and is unsafe to load from untrusted sources.
- **msgpack** is the fastest on JSON-native data and the most compact, but is
  **data-only**: it cannot encode sets/arbitrary objects (`mixed` is
  unsupported). Great for trusted, JSON-shaped, cross-language caches.
- **cbor2** is data-only like msgpack but broader (it encodes the `mixed`
  payload), compact, and a good `safe=True` pairing.
- **hashstash** (the default) is competitive with pickle/msgpack on JSON-native
  data thanks to the fast-path, roughly ~2x their time on `dict` and slower on
  `mixed` (the full recursive path) — but it is the only serializer here that
  round-trips *everything* (functions, lambdas, custom objects, numpy/pandas)
  and produces portable, canonical cache keys.
- **jsonpickle** is the slowest across the board.

## Choosing

- Trusted local cache, need to cache arbitrary Python objects → **hashstash**
  (default).
- Trusted, JSON-shaped, want max speed/compactness → **msgpack**.
- Shared/untrusted cache → **`safe=True`** with **cbor2** or **msgpack**
  (data-only serializers can't execute code on load).
- Never cache across Python versions with **pickle**.

# Engine benchmarks

Engine cost is driven by payload **size** and access pattern, not by which
serializer produced the bytes. Regenerate with:

```bash
python scripts/bench_engines.py --sizes 1000 10000 100000 --spot-check
```

## Size sweep (serializer=hashstash, ms per op; Apple M-series)

| engine | get @ 1 KB | get @ 100 KB |
|---|---:|---:|
| memory / lmdb / leveldb | ~0.05 | ~2.75 |
| diskcache / pairtree / duckdb / dataframe | ~0.1 | ~3.0 |
| sqlite / shelve | ~0.3–0.5 | ~3.3 |
| jsonl | ~0.05 | ~0.95 |

Writes: lmdb / leveldb / memory are fastest; the SQL engines (sqlite, duckdb)
and file-per-key engines (pairtree, dataframe) carry more per-op overhead.

**jsonl** now holds a key→offset index (built incrementally by folding
newly-appended rows), so a random `get` is an O(1) seek to the row instead of a
full-file scan — read latency no longer grows with the log size (it was ~80 ms
per get at 100 KB before the index). Its flat mode also stores JSON-native dict
values directly, so those reads skip the serializer entirely, which is why its
get can beat the keyed engines here.

## Engine × serializer are separable

`get` time for one 50 KB payload, across engine × serializer:

| engine | hashstash | pickle | msgpack |
|---|---:|---:|---:|
| memory | 1.37 | 0.19 | 0.32 |
| lmdb | 1.40 | 0.20 | 0.33 |
| sqlite | 1.91 | 0.68 | 0.82 |

Down a column (fix serializer, vary engine) the spread is small (~1.4×); across
a row (fix engine, vary serializer) it is ~7×. So `get` is dominated by the
serializer's **deserialize**, and the engine adds a smaller, roughly-additive
I/O term. That means engine and serializer can be measured **independently** and
composed — a full N×M grid is unnecessary. The two cases that genuinely need
their own measurement are jsonl (flat mode stores JSON-native values directly,
bypassing the serializer) and the `dataframe` engine (stores DataFrames natively
via feather/parquet, also bypassing the serializer).
