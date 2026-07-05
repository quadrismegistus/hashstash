#!/usr/bin/env python
"""Regenerate the README profile figures (serializers, engines, encodings).

Built on the working benchmark primitives (compare_serializers + direct engine
and encoding sweeps), not the legacy run_profiles pipeline. Skips the combined
engine x serializer x encoding figure by design.

    python scripts/make_readme_figures.py

Server engines (redis/mongo) are included only if a local server is reachable.
"""
import argparse
import logging
import tempfile
import time
import warnings
from pathlib import Path

FIG_DIR = Path(__file__).resolve().parent.parent / "figures"


def _payload(target_bytes):
    rows, approx, i = [], 0, 0
    while approx < target_bytes:
        rows.append({"id": i, "name": f"item-{i}", "score": i * 1.5, "ok": i % 2 == 0})
        approx += 60
        i += 1
    return {"rows": rows}


# --- serializers -------------------------------------------------------------

def fig_serializers(size, iterations):
    import pandas as pd
    import plotnine as p9
    from hashstash.config import get_working_serializers
    from hashstash.profilers.engine_profiler import HashStashProfiler

    df = HashStashProfiler.compare_serializers(
        serializers=get_working_serializers(),
        sizes=(size,), iterations=iterations, data_types=("dict", "mixed"),
    )
    d = df.df if hasattr(df, "df") else df
    d = d[d.get("Unsupported").isna()] if "Unsupported" in d.columns else d
    fig = (
        d.groupby(["Serializer", "Data Type"]).median(numeric_only=True).reset_index()
        .melt(
            id_vars=["Serializer", "Data Type", "Raw Size (B)", "Serialized Size (B)"],
            value_vars=["Serialize Time (s)", "Deserialize Time (s)"],
            value_name="Time (s)", var_name="Operation",
        )
    )
    fig["Operation"] = fig["Operation"].str.replace(" Time (s)", "", regex=False)
    fig["Operation"] = pd.Categorical(fig["Operation"], categories=["Serialize", "Deserialize"])
    fig["Serialized Size (KB)"] = fig["Serialized Size (B)"] / 1024
    fig["Rate (MB/s)"] = fig["Raw Size (B)"] / fig["Time (s)"] / 1024 / 1024
    p9.options.figure_size = (9, 5)
    g = (
        p9.ggplot(fig, p9.aes(x="Serialized Size (KB)", y="Rate (MB/s)", label="Serializer", color="Serializer"))
        + p9.facet_grid(". ~ Operation")
        + p9.geom_point(p9.aes(shape="Data Type"), size=3)
        + p9.geom_text(nudge_y=0.06, size=8)
        + p9.theme_classic()
        + p9.scale_y_log10()
        + p9.labs(x="Serialized size (KB)", y="Throughput (MB/s, higher = faster)",
                  title="Comparing serializers (all installed)")
    )
    _save(g, "fig.comparing_serializers_size_speed.png")


# --- engines -----------------------------------------------------------------

def fig_engines(size, n, serializer):
    import pandas as pd
    import plotnine as p9
    from hashstash import HashStash
    from hashstash.config import get_working_engines

    payload = _payload(size)
    rows = []
    for engine in sorted(get_working_engines()):
        if engine == "fsspec":
            root = f"memory://fig-{engine}"
        else:
            root = tempfile.mkdtemp(prefix=f"hs-fig-{engine}-")
        try:
            s = HashStash(engine=engine, serializer=serializer, root_dir=root)
            s.clear()
            t = time.perf_counter()
            for i in range(n):
                s[f"k{i}"] = payload
            set_ms = (time.perf_counter() - t) / n * 1000
            t = time.perf_counter()
            for i in range(n):
                _ = s[f"k{i}"]
            get_ms = (time.perf_counter() - t) / n * 1000
            rows.append({"Engine": engine, "Operation": "Set", "ms": set_ms})
            rows.append({"Engine": engine, "Operation": "Get", "ms": get_ms})
        except Exception as e:
            print(f"  skipping engine {engine}: {type(e).__name__}")
    d = pd.DataFrame(rows)
    p9.options.figure_size = (8, 6)
    g = (
        p9.ggplot(d, p9.aes(x="reorder(Engine, -ms)", y="ms", fill="Operation"))
        + p9.geom_col(position="dodge")
        + p9.coord_flip()
        + p9.theme_classic()
        + p9.labs(x="", y="ms per op (lower = faster)",
                  title=f"Comparing engines (serializer={serializer}, {size // 1000} KB values)")
    )
    _save(g, "fig.comparing_engines.png")


# --- encodings ---------------------------------------------------------------

def fig_encodings(size):
    import pandas as pd
    import plotnine as p9
    from hashstash import HashStash
    from hashstash.config import get_working_compressers

    payload = _payload(size)
    base = HashStash(engine="memory", root_dir=tempfile.mkdtemp())
    serialized = base.serialize(payload)
    raw_stash = HashStash(engine="memory", compress=False, b64=False, root_dir=tempfile.mkdtemp())
    raw_kb = len(raw_stash.encode_value(serialized)) / 1024
    rows = []
    for comp in get_working_compressers():
        try:
            s = HashStash(engine="memory", compress=comp, b64=True, root_dir=tempfile.mkdtemp())
            enc, dec, n = 0.0, 0.0, 30
            encoded = None
            for _ in range(n):
                t = time.perf_counter(); encoded = s.encode_value(serialized); enc += time.perf_counter() - t
                t = time.perf_counter(); s.decode_value(encoded); dec += time.perf_counter() - t
            size_kb = len(encoded) / 1024
            rate = len(serialized) / ((enc + dec) / n) / 1024 / 1024
            rows.append({"Encoding": comp, "Encoded Size (KB)": size_kb, "Rate (MB/s)": rate})
        except Exception as e:
            print(f"  skipping compressor {comp}: {type(e).__name__}")
    d = pd.DataFrame(rows)
    p9.options.figure_size = (8, 6)
    g = (
        p9.ggplot(d, p9.aes(x="Encoded Size (KB)", y="Rate (MB/s)", label="Encoding", color="Encoding"))
        + p9.geom_point(size=3) + p9.geom_text(nudge_y=0.04, size=9)
        + p9.geom_vline(xintercept=raw_kb, linetype="dashed", color="gray")
        + p9.annotate("text", x=raw_kb, y=d["Rate (MB/s)"].min(), label=f"raw = {raw_kb:.0f} KB",
                      color="gray", ha="left")
        + p9.theme_classic() + p9.scale_y_log10()
        + p9.labs(x="Encoded size (KB, smaller = better)", y="Encode+decode throughput (MB/s)",
                  title="Comparing encodings / compressors")
    )
    _save(g, "fig.comparing_encodings_size_speed.png")


def _save(g, name):
    FIG_DIR.mkdir(parents=True, exist_ok=True)
    out = FIG_DIR / name
    g.save(out, dpi=140, verbose=False)
    print(f"wrote {out}")


def main():
    warnings.filterwarnings("ignore")
    logging.getLogger("hashstash").setLevel(logging.CRITICAL + 1)
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--size", type=int, default=100_000)
    ap.add_argument("--iterations", type=int, default=20)
    ap.add_argument("--n", type=int, default=100)
    ap.add_argument("--serializer", default="hashstash")
    ap.add_argument("--only", nargs="+", choices=["serializers", "engines", "encodings"])
    args = ap.parse_args()
    only = args.only or ["serializers", "engines", "encodings"]
    if "serializers" in only:
        fig_serializers(args.size, args.iterations)
    if "engines" in only:
        fig_engines(args.size, args.n, args.serializer)
    if "encodings" in only:
        fig_encodings(args.size)


if __name__ == "__main__":
    main()
