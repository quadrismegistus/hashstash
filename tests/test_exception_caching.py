"""Exception / negative caching: cache function failures so they aren't
re-executed, opt-in via _cache_exceptions (run/@stashed_result) or
cache_exceptions (get_set)."""
import time

import pytest

from hashstash import HashStash, stashed_result
from hashstash.engines.base import HashStashCachedError


# module-level (closure-free) so cache identity is stable across calls; the call
# log lives in a module global that isn't part of the function's identity
_calls = []


def _always_fails(x):
    _calls.append(x)
    raise ValueError(f"boom-{x}")


def _fails_then(x):
    _calls.append(x)
    raise KeyError("missing")


def setup_function():
    _calls.clear()


# --- default behaviour: failures are NOT cached -------------------------------


def test_without_flag_reexecutes_on_failure(tmp_path):
    stash = HashStash(root_dir=str(tmp_path / "c"))
    with pytest.raises(ValueError):
        stash.run(_always_fails, 1)
    with pytest.raises(ValueError):
        stash.run(_always_fails, 1)
    assert len(_calls) == 2  # re-executed each time


# --- opt-in: failures ARE cached and re-raised --------------------------------


def test_cached_exception_not_reexecuted(tmp_path):
    stash = HashStash(root_dir=str(tmp_path / "c"))
    with pytest.raises(ValueError, match="boom-1"):
        stash.run(_always_fails, 1, _cache_exceptions=True)
    # second call re-raises the cached exception WITHOUT calling the function
    with pytest.raises(ValueError, match="boom-1"):
        stash.run(_always_fails, 1, _cache_exceptions=True)
    assert len(_calls) == 1


def test_cached_exception_preserves_type(tmp_path):
    stash = HashStash(root_dir=str(tmp_path / "c"))
    with pytest.raises(KeyError):
        stash.run(_fails_then, 1, _cache_exceptions=True)
    # a caller catching the ORIGINAL type still catches the cached one
    try:
        stash.run(_fails_then, 1, _cache_exceptions=True)
        assert False, "should have raised"
    except KeyError:
        pass
    assert len(_calls) == 1


def test_cached_exception_expires(tmp_path):
    stash = HashStash(root_dir=str(tmp_path / "c"))
    with pytest.raises(ValueError):
        stash.run(_always_fails, 2, _cache_exceptions=True, _exception_ttl=0.3)
    time.sleep(0.6)
    # expired -> recomputed (raises again, function called a second time)
    with pytest.raises(ValueError):
        stash.run(_always_fails, 2, _cache_exceptions=True, _exception_ttl=0.3)
    assert len(_calls) == 2


def test_force_bypasses_cached_exception(tmp_path):
    stash = HashStash(root_dir=str(tmp_path / "c"))
    with pytest.raises(ValueError):
        stash.run(_always_fails, 3, _cache_exceptions=True)
    with pytest.raises(ValueError):
        stash.run(_always_fails, 3, _cache_exceptions=True, _force=True)
    assert len(_calls) == 2  # _force re-executes


def _flaky(x):
    # fails the first time, succeeds after the cached exception is cleared
    _calls.append(x)
    if len(_calls) == 1:
        raise RuntimeError("first-call-fails")
    return x * 10


def test_recovers_after_exception_ttl(tmp_path):
    stash = HashStash(root_dir=str(tmp_path / "c"))
    with pytest.raises(RuntimeError):
        stash.run(_flaky, 5, _cache_exceptions=True, _exception_ttl=0.3)
    time.sleep(0.6)
    assert stash.run(_flaky, 5, _cache_exceptions=True, _exception_ttl=0.3) == 50
    # and now the success is cached
    assert stash.run(_flaky, 5, _cache_exceptions=True) == 50
    assert len(_calls) == 2


# --- @stashed_result and get_set ---------------------------------------------


def test_stashed_result_cache_exceptions(tmp_path):
    stash = HashStash(root_dir=str(tmp_path / "c"))

    @stash.stashed_result
    def f(x):
        _calls.append(x)
        raise ValueError("nope")

    with pytest.raises(ValueError):
        f(1, _cache_exceptions=True)
    with pytest.raises(ValueError):
        f(1, _cache_exceptions=True)
    assert len(_calls) == 1


def test_get_set_cache_exceptions(tmp_path):
    stash = HashStash(root_dir=str(tmp_path / "c"))

    def setter():
        _calls.append(1)
        raise RuntimeError("setter failed")

    with pytest.raises(RuntimeError):
        stash.get_set("k", setter, cache_exceptions=True)
    with pytest.raises(RuntimeError):
        stash.get_set("k", setter, cache_exceptions=True)
    assert len(_calls) == 1


# --- safe mode compatibility --------------------------------------------------


def test_cached_exception_works_under_safe_mode(tmp_path):
    """The cached-exception marker is plain data, so negative caching works even
    with safe=True (unlike caching a serialized exception object would)."""
    stash = HashStash(root_dir=str(tmp_path / "c"), safe=True)
    with pytest.raises(ValueError):
        stash.run(_always_fails, 7, _cache_exceptions=True)
    with pytest.raises(ValueError):
        stash.run(_always_fails, 7, _cache_exceptions=True)
    assert len(_calls) == 1


# --- non-importable exception falls back gracefully ---------------------------


class _LocalError(Exception):
    pass


def _raise_local(x):
    _calls.append(x)
    raise _LocalError("local")


def test_non_importable_exception_falls_back(tmp_path):
    stash = HashStash(root_dir=str(tmp_path / "c"))
    with pytest.raises(_LocalError):
        stash.run(_raise_local, 1, _cache_exceptions=True)
    # the locally-defined class can't be reimported in-process by qualname, so
    # the cached re-raise falls back to HashStashCachedError (carrying the info)
    with pytest.raises((_LocalError, HashStashCachedError)):
        stash.run(_raise_local, 1, _cache_exceptions=True)
    assert len(_calls) == 1
