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

# label repel (needs adjustText): spread colliding labels apart with thin leader
# lines so every serializer/engine/encoding stays readable
REPEL = {
    "expand_points": (1.6, 1.6),
    "arrowprops": {"arrowstyle": "-", "color": "gray", "alpha": 0.4, "lw": 0.5},
}


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
    # label one point per serializer (the 'dict' payload) with repel; shape marks
    # dict vs mixed; color reinforces the serializer
    lab = fig[fig["Data Type"] == "dict"]
    g = (
        p9.ggplot(fig, p9.aes(x="Serialized Size (KB)", y="Rate (MB/s)", color="Serializer"))
        + p9.facet_grid(". ~ Operation")
        + p9.geom_point(p9.aes(shape="Data Type"), size=3.5, alpha=0.9)
        + p9.geom_text(lab, p9.aes(label="Serializer"), size=8, adjust_text=REPEL, show_legend=False)
        + p9.theme_classic()
        + p9.scale_y_log10()
        + p9.guides(color=False)
        + p9.labs(x="Serialized size (KB, smaller = better)",
                  y="Throughput (MB/s, higher = faster)",
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

    def _ms(fn, reps):
        t = time.perf_counter()
        for i in range(reps):
            fn(i)
        return (time.perf_counter() - t) / reps * 1000

    rows = []
    for engine in sorted(get_working_engines()):
        root = f"memory://fig-{engine}" if engine == "fsspec" else tempfile.mkdtemp(prefix=f"hs-fig-{engine}-")
        try:
            s = HashStash(engine=engine, serializer=serializer, root_dir=root)
            s.clear()
            ev = s.encode_value(payload)
            # ISOLATE engine I/O from ser/deser: subtract the serialize+encode
            # cost from a full set, and the decode+deserialize cost from a full
            # get (these are what the profiler times as separate operations). The
            # full time is ~85-95% ser/deser and hides the real engine differences.
            enc = _ms(lambda i: (s.encode_value(payload), s.encode_key(f"k{i}")), n)
            dec = _ms(lambda i: s.decode_value(ev), n)
            set_full = _ms(lambda i: s.__setitem__(f"k{i}", payload), n)
            get_full = _ms(lambda i: s[f"k{i}"], n)
            rows.append({"Engine": engine,
                         "Set I/O (ms)": max(set_full - enc, 0.0),
                         "Get I/O (ms)": max(get_full - dec, 0.0)})
        except Exception as e:
            print(f"  skipping engine {engine}: {type(e).__name__}")
    d = pd.DataFrame(rows)
    # ISOLATED write vs read I/O (serialize/deserialize subtracted out). Now the
    # engines actually spread (~25x): memory/lmdb near the origin, the SQL engines
    # far out. Dashed diagonal is set==get I/O.
    p9.options.figure_size = (8, 7)
    g = (
        p9.ggplot(d, p9.aes(x="Set I/O (ms)", y="Get I/O (ms)"))
        + p9.geom_abline(slope=1, intercept=0, linetype="dashed", color="gray", alpha=0.5)
        + p9.geom_point(p9.aes(color="Engine"), size=3, show_legend=False)
        + p9.geom_text(p9.aes(label="Engine", color="Engine"), size=8, adjust_text=REPEL, show_legend=False)
        + p9.theme_classic()
        + p9.labs(x="Write I/O — ms per set, ser/enc removed (lower = faster)",
                  y="Read I/O — ms per get, deser/dec removed (lower = faster)",
                  title=f"Comparing engines: pure I/O ({size // 1000} KB values, serializer={serializer})")
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
    mb = len(serialized) / 1024 / 1024
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
            rows.append({"Encoding": comp, "Encoded Size (KB)": size_kb,
                         "Operation": "Encode", "Rate (MB/s)": mb / (enc / n)})
            rows.append({"Encoding": comp, "Encoded Size (KB)": size_kb,
                         "Operation": "Decode", "Rate (MB/s)": mb / (dec / n)})
        except Exception as e:
            print(f"  skipping compressor {comp}: {type(e).__name__}")
    d = pd.DataFrame(rows)
    d["Operation"] = pd.Categorical(d["Operation"], categories=["Encode", "Decode"])
    p9.options.figure_size = (9, 5)
    # biplot faceted by operation, mirroring the serializer figure: encoded size
    # (compression) vs throughput. The dashed line marks the uncompressed size.
    g = (
        p9.ggplot(d, p9.aes(x="Encoded Size (KB)", y="Rate (MB/s)", color="Encoding"))
        + p9.facet_grid(". ~ Operation")
        + p9.geom_vline(xintercept=raw_kb, linetype="dashed", color="gray", alpha=0.6)
        + p9.geom_point(size=3.5, alpha=0.9)
        + p9.geom_text(p9.aes(label="Encoding"), size=8, adjust_text=REPEL, show_legend=False)
        + p9.theme_classic() + p9.scale_y_log10()
        + p9.guides(color=False)
        + p9.labs(x=f"Encoded size (KB, smaller = better; dashed = raw {raw_kb:.0f} KB)",
                  y="Throughput (MB/s, higher = faster)",
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
