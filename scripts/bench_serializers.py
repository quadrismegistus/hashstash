#!/usr/bin/env python
"""Benchmark hashstash serializers and print a Markdown table.

Runs HashStashProfiler.compare_serializers over a size sweep and both a
JSON-native ('dict') and a full-path ('mixed') payload, then prints mean
serialize/deserialize time and output size per (serializer, size, data type).
Serializers that cannot encode a payload (e.g. msgpack on sets) are marked
"unsupported" rather than dropped.

Usage:
    python scripts/bench_serializers.py                  # default sweep
    python scripts/bench_serializers.py --sizes 1000 100000 --iterations 50
    python scripts/bench_serializers.py --csv results.csv

The numbers are wall-clock and machine-dependent — use them for RELATIVE
comparison (serializer vs serializer, fast-path vs full-path) and to spot
regressions, not as absolute guarantees. Regenerate BENCHMARKS.md from this.
"""
import argparse
import logging


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--sizes", type=int, nargs="+", default=[1_000, 10_000, 100_000])
    ap.add_argument("--iterations", type=int, default=30)
    ap.add_argument(
        "--serializers", nargs="+", default=None,
        help="default: all installed serializers",
    )
    ap.add_argument("--data-types", nargs="+", default=["dict", "mixed"])
    ap.add_argument("--csv", default=None, help="also write raw rows to this CSV path")
    args = ap.parse_args()

    logging.getLogger("hashstash").setLevel(logging.CRITICAL + 1)
    import warnings

    warnings.filterwarnings("ignore")

    from hashstash.config import get_working_serializers
    from hashstash.profilers.engine_profiler import HashStashProfiler

    serializers = args.serializers or get_working_serializers()

    df = HashStashProfiler.compare_serializers(
        serializers=serializers,
        sizes=tuple(args.sizes),
        iterations=args.iterations,
        data_types=tuple(args.data_types),
    )
    d = df.df if hasattr(df, "df") else df

    if args.csv:
        d.to_csv(args.csv, index=False)
        print(f"# raw rows written to {args.csv}\n")

    unsupported = set()
    if "Unsupported" in d.columns:
        for _, r in d[d["Unsupported"] == True].iterrows():  # noqa: E712
            unsupported.add((r["Serializer"], r["Data Type"], int(r["Size (B)"])))
        d = d[d["Unsupported"].isna()]

    grp = d.groupby(["Serializer", "Data Type", "Size (B)"])
    print("| serializer | data type | payload | serialize | deserialize | encoded size |")
    print("|---|---|---:|---:|---:|---:|")
    for (serializer, dtype, size), rows in grp:
        ser_us = rows["Serialize Time (s)"].mean() * 1e6
        deser_us = rows["Deserialize Time (s)"].mean() * 1e6
        out_b = rows["Serialized Size (B)"].mean()
        print(
            f"| {serializer} | {dtype} | {int(size):,} B "
            f"| {ser_us:,.0f} µs | {deser_us:,.0f} µs | {out_b:,.0f} B |"
        )
    for serializer, dtype, size in sorted(unsupported):
        print(f"| {serializer} | {dtype} | {size:,} B | — | — | *unsupported* |")


if __name__ == "__main__":
    main()
