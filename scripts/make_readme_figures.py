#!/usr/bin/env python
"""Regenerate the README profile figures from the library's own plot methods.

Thin wrapper over HashStashProfiler.plot_serializers / plot_engines /
plot_encodings (the single source of truth) — it just points them at the repo's
figures/ dir and uses a lighter iteration count than the library defaults.

    python scripts/make_readme_figures.py
    python scripts/make_readme_figures.py --only engines --iterations 200

Server engines (redis/mongo) appear in the engine figure only if a local server
is reachable (e.g. `docker run -d -p 6379:6379 redis`). The combined
engine x serializer x encoding figure is intentionally not regenerated.
"""
import argparse
import logging
import warnings
from pathlib import Path

FIG_DIR = Path(__file__).resolve().parent.parent / "figures"


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--iterations", type=int, default=100)
    ap.add_argument("--serializer", default="hashstash",
                    help="serializer for the engine I/O figure")
    ap.add_argument("--only", nargs="+", choices=["serializers", "engines", "encodings"])
    args = ap.parse_args()

    warnings.filterwarnings("ignore")
    logging.getLogger("hashstash").setLevel(logging.CRITICAL + 1)

    from hashstash.profilers.engine_profiler import HashStashProfiler as P

    common = dict(iterations=args.iterations, num_proc=1, num_procs=[1],
                  progress=False, progress_inner=False)
    only = args.only or ["serializers", "engines", "encodings"]

    if "serializers" in only:
        P.plot_serializers(filename=str(FIG_DIR / "fig.comparing_serializers_size_speed.png"), **common)
        print("wrote fig.comparing_serializers_size_speed.png")
    if "engines" in only:
        P.plot_engines(filename=str(FIG_DIR / "fig.comparing_engines.png"),
                       serializer=args.serializer, **common)
        print("wrote fig.comparing_engines.png")
    if "encodings" in only:
        P.plot_encodings(filename=str(FIG_DIR / "fig.comparing_encodings_size_speed.png"), **common)
        print("wrote fig.comparing_encodings_size_speed.png")


if __name__ == "__main__":
    main()
