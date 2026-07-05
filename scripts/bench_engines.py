#!/usr/bin/env python
"""Benchmark storage engines and print Markdown tables.

Engine cost is driven by payload SIZE and access pattern, not by which
serializer produced the bytes — so this sweeps set/get time across payload
sizes per engine (a size curve) rather than a full engine x serializer grid.
Two behaviours a flat benchmark hides are made explicit:

  * jsonl `get` re-scans the whole log (O(file size) per lookup) — the size
    sweep shows it blowing up while keyed engines stay flat.
  * a small engine x serializer spot-check confirms `get` time is dominated by
    deserialize (the serializer), i.e. engine and serializer are separable.

Usage:
    python scripts/bench_engines.py                      # default sweep
    python scripts/bench_engines.py --sizes 1000 100000 --n 200
    python scripts/bench_engines.py --spot-check
"""
import argparse
import logging
import tempfile
import time


def _payload(target_bytes):
    """A JSON-native dict roughly `target_bytes` when serialized."""
    rows, approx = [], 0
    i = 0
    while approx < target_bytes:
        rows.append({"id": i, "name": f"item-{i}", "score": i * 1.5, "ok": i % 2 == 0})
        approx += 60
        i += 1
    return {"rows": rows}


def _bench(stash, payload, n):
    t = time.perf_counter()
    for i in range(n):
        stash[f"k{i}"] = payload
    set_ms = (time.perf_counter() - t) / n * 1000
    t = time.perf_counter()
    for i in range(n):
        _ = stash[f"k{i}"]
    get_ms = (time.perf_counter() - t) / n * 1000
    return set_ms, get_ms


def _new_stash(engine, serializer, **kw):
    from hashstash import HashStash
    return HashStash(engine=engine, serializer=serializer,
                     root_dir=tempfile.mkdtemp(prefix="hs-eng-"), **kw)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--sizes", type=int, nargs="+", default=[1_000, 10_000, 100_000])
    ap.add_argument("--n", type=int, default=150, help="keys per measurement")
    ap.add_argument("--engines", nargs="+", default=None)
    ap.add_argument("--serializer", default="hashstash")
    ap.add_argument("--spot-check", action="store_true",
                    help="also run a small engine x serializer grid to show get≈deserialize")
    args = ap.parse_args()

    logging.getLogger("hashstash").setLevel(logging.CRITICAL + 1)
    import warnings
    warnings.filterwarnings("ignore")

    from hashstash.config import get_working_engines

    engines = args.engines or [
        e for e in get_working_engines() if e not in ("redis", "mongo", "fsspec")
    ]

    print(f"# engine size sweep (serializer={args.serializer}, n={args.n} keys)\n")
    print("| engine | payload | set (ms) | get (ms) |")
    print("|---|---:|---:|---:|")
    for size in args.sizes:
        payload = _payload(size)
        for engine in sorted(engines):
            try:
                stash = _new_stash(engine, args.serializer)
                stash.clear()
                set_ms, get_ms = _bench(stash, payload, args.n)
                print(f"| {engine} | {size:,} B | {set_ms:.3f} | {get_ms:.3f} |")
            except Exception as e:  # engine missing deps / server
                print(f"| {engine} | {size:,} B | *{type(e).__name__}* | — |")
        print("| | | | |")

    if args.spot_check:
        print("\n# spot-check: get(ms) across engine x serializer "
              "(get is dominated by deserialize, not engine I/O)\n")
        sers = ["hashstash", "pickle", "msgpack"]
        engs = [e for e in ("memory", "lmdb", "sqlite") if e in engines]
        payload = _payload(50_000)
        print("| engine | " + " | ".join(sers) + " |")
        print("|---|" + "---:|" * len(sers))
        for engine in engs:
            cells = []
            for ser in sers:
                try:
                    stash = _new_stash(engine, ser)
                    stash.clear()
                    _, get_ms = _bench(stash, payload, args.n)
                    cells.append(f"{get_ms:.3f}")
                except Exception as e:
                    cells.append(f"*{type(e).__name__}*")
            print(f"| {engine} | " + " | ".join(cells) + " |")


if __name__ == "__main__":
    main()
