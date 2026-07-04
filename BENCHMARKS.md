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
