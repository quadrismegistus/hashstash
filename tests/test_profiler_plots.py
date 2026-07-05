"""Guards for the profiler plot/run_profiles fixes: the plot methods are
classmethods that render without crashing, and run_profiles tolerates a
serializer that can't handle a data type instead of crashing the whole run."""
import inspect

import pytest

pytest.importorskip("pandas")
pytest.importorskip("numpy")

from hashstash.profilers.engine_profiler import HashStashProfiler


def test_plot_methods_are_classmethods():
    # plot_serializers / plot_encodings were missing @classmethod (called with
    # cls unbound -> TypeError: missing positional arg)
    for name in ("plot", "plot_engines", "plot_serializers", "plot_encodings", "plot_all"):
        attr = inspect.getattr_static(HashStashProfiler, name)
        assert isinstance(attr, classmethod), f"{name} must be a classmethod"


def test_run_profiles_skips_incompatible_serializer_datatype():
    """msgpack can't serialize a DataFrame; run_profiles must skip that combo
    (not crash), while keeping the combos that work."""
    df = HashStashProfiler.run_profiles(
        iterations=2, size=2000,
        engines=["memory"], serializers=["hashstash", "msgpack"],
        compress=[False], b64=[True], append_mode=[False],
        num_procs=[1], num_proc=1, data_types=["dict", "pandas_df"],
        progress=False, progress_inner=False,
    )
    counts = df.groupby(["Serializer", "Data Type"]).size().to_dict()
    assert counts.get(("hashstash", "dict"), 0) > 0
    assert counts.get(("hashstash", "pandas_df"), 0) > 0      # hashstash handles DataFrames
    assert counts.get(("msgpack", "dict"), 0) > 0
    assert ("msgpack", "pandas_df") not in counts            # skipped, not crashed


def test_plot_methods_render(tmp_path, monkeypatch):
    """The plot methods render end-to-end at a tiny scale (exercises the
    classmethod fix and the loess-singularity guard in plot())."""
    pytest.importorskip("plotnine")
    import hashstash.profilers.engine_profiler as ep

    small = dict(iterations=6, size=2000, compress=[False], b64=[True],
                 num_procs=[1], num_proc=1, progress=False, progress_inner=False)
    monkeypatch.setattr(ep, "opts_serializers",
                        {**ep.opts_serializers, **small, "engines": ["memory"],
                         "serializers": ["hashstash", "pickle"]})
    monkeypatch.setattr(ep, "opts_encoders",
                        {**ep.opts_encoders, **small, "engines": ["memory"], "serializers": ["pickle"]})
    monkeypatch.setattr(ep, "opts_engines",
                        {**ep.opts_engines, **small, "engines": ["memory", "sqlite"], "serializers": ["pickle"]})
    for name in ("plot_serializers", "plot_encodings", "plot_engines"):
        getattr(HashStashProfiler, name)(filename=str(tmp_path / f"{name}.png"))
