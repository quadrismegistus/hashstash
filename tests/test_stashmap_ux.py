"""UX guards for stash.map / StashMap:

  * StashMapRun.was_cached: a public read-only "did this come from cache?" flag
    (the README advertised per-item cache status but only a private
    _needed_computing existed).
  * graceful degradation under spawn multiprocessing when the target function's
    source can't be retrieved (REPL / exec-defined) instead of a cryptic
    KeyError / BrokenProcessPool.
  * a one-time warning when num_proc>1 is combined with a process-local
    engine='memory' (no ultradict), where worker results never reach the parent.

Functions are module-level so spawn workers can reconstruct them.
"""
import sys
import tempfile

import pytest

from hashstash import HashStash

pmapmod = sys.modules["hashstash.utils.pmap"]


def _square(x):
    return x * x


@pytest.fixture
def persistent_stash(tmp_path):
    # a real on-disk engine so per-item results survive between two map() calls
    return HashStash(root_dir=str(tmp_path / "cache"))


def test_was_cached_false_when_freshly_computed(persistent_stash):
    m = persistent_stash.map(
        _square, objects=[1, 2, 3], num_proc=1, progress=False, stash_map=False
    )
    assert list(m) == [1, 4, 9]
    # nothing was in the cache the first time round
    assert [r.was_cached for r in m.runs] == [False, False, False]


def test_was_cached_true_on_cached_remap(persistent_stash):
    first = persistent_stash.map(
        _square, objects=[1, 2, 3], num_proc=1, progress=False, stash_map=False
    )
    assert list(first) == [1, 4, 9]
    assert not any(r.was_cached for r in first.runs)

    # identical map again -> every item is served from the per-item cache
    second = persistent_stash.map(
        _square, objects=[1, 2, 3], num_proc=1, progress=False, stash_map=False
    )
    assert list(second) == [1, 4, 9]  # works after iteration
    assert [r.was_cached for r in second.runs] == [True, True, True]


def test_was_cached_true_on_default_stash_map_remap(persistent_stash):
    """The default stash.map (stash_map=True) short-circuits a re-map by returning
    the whole stashed StashMap; its runs are cache hits too."""
    first = persistent_stash.map(_square, objects=[1, 2, 3], num_proc=1, progress=False)
    assert list(first) == [1, 4, 9]
    assert not any(r.was_cached for r in first.runs)

    second = persistent_stash.map(_square, objects=[1, 2, 3], num_proc=1, progress=False)
    assert list(second) == [1, 4, 9]
    assert [r.was_cached for r in second.runs] == [True, True, True]


def test_was_cached_is_read_only(persistent_stash):
    m = persistent_stash.map(
        _square, objects=[1], num_proc=1, progress=False, stash_map=False
    )
    list(m)
    with pytest.raises(AttributeError):
        m.runs[0].was_cached = True


def test_source_unavailable_func_falls_back_and_does_not_crash():
    """A function whose source can't be retrieved (here: exec-defined) used to
    fail cryptically under num_proc>1. It must transparently fall back to
    num_proc=1 and still produce correct results."""
    ns = {}
    exec("def _dyn(x):\n    return x + 100", ns)
    dyn = ns["_dyn"]  # inspect.getsource() can't recover this -> empty source

    stash = HashStash(root_dir=tempfile.mkdtemp())
    m = stash.map(dyn, objects=[1, 2, 3], num_proc=2, progress=False, stash_map=False)
    assert list(m) == [101, 102, 103]  # no BrokenProcessPool / KeyError
    assert m.num_proc == 1  # degraded to serial


def test_map_fallback_reason_none_for_importable_module_func():
    # a normally-importable module-level function is spawn-safe: no fallback
    assert pmapmod._map_fallback_reason(_square) is None


def test_memory_engine_without_ultradict_warns_under_num_proc(monkeypatch):
    """num_proc>1 + engine='memory' without ultradict is process-local, so worker
    results never reach the parent. Emit exactly one warning about it."""
    monkeypatch.setattr(pmapmod, "_ultradict_available", lambda: False)

    warnings = []
    # spy on the warning call directly so the assertion is independent of the
    # hashstash logger level (other test modules raise it to CRITICAL)
    monkeypatch.setattr(pmapmod.log, "warning", lambda msg, *a, **k: warnings.append(msg))

    stash = HashStash(engine="memory", root_dir=tempfile.mkdtemp())
    # preload/precompute off: the warning fires in __init__, no need to actually
    # spawn workers for this assertion
    m = stash.map(
        _square,
        objects=[1, 2, 3],
        num_proc=2,
        progress=False,
        stash_map=False,
        preload=False,
        precompute=False,
    )

    # a spawn-safe module-level function stays parallel (num_proc not reduced)...
    assert m.num_proc == 2
    # ...and the memory/ultradict warning fired exactly once
    memory_warnings = [w for w in warnings if "engine='memory'" in w]
    assert len(memory_warnings) == 1


def test_memory_engine_no_warning_with_ultradict(monkeypatch):
    monkeypatch.setattr(pmapmod, "_ultradict_available", lambda: True)

    warnings = []
    monkeypatch.setattr(pmapmod.log, "warning", lambda msg, *a, **k: warnings.append(msg))

    stash = HashStash(engine="memory", root_dir=tempfile.mkdtemp())
    stash.map(
        _square,
        objects=[1, 2, 3],
        num_proc=2,
        progress=False,
        stash_map=False,
        preload=False,
        precompute=False,
    )

    assert not any("engine='memory'" in w for w in warnings)
