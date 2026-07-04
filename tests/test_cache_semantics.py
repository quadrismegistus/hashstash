"""Tests for the 0.7.0 cache-semantics features: TTL, single-flight,
statistics, and per-call invalidation."""
import os
import subprocess
import sys
import time
from datetime import timedelta

import pytest

from hashstash import HashStash, stashed_result

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# --- TTL ---------------------------------------------------------------------


# TTL timing note: never assert "still present within a SHORT ttl" — the
# microsecond set->read gap can stretch on a loaded CI runner, so that direction
# flakes. Presence checks use a generous ttl (can't expire in the test's
# lifetime); expiry checks sleep well past a short ttl (the reliable direction).

_LONG_TTL = 3600      # presence: effectively never expires during the test
_SHORT_TTL = 0.3      # expiry: paired with a >=3x sleep
_EXPIRE_SLEEP = 1.0


def test_ttl_present_within_window(tmp_path):
    stash = HashStash(root_dir=str(tmp_path / "c"), ttl=_LONG_TTL)
    stash["k"] = "v"
    assert stash["k"] == "v"
    assert "k" in stash


def test_ttl_expires_reads(tmp_path):
    stash = HashStash(root_dir=str(tmp_path / "c"), ttl=_SHORT_TTL)
    stash["k"] = "v"
    time.sleep(_EXPIRE_SLEEP)
    assert stash.get("k") is None
    assert "k" not in stash
    with pytest.raises(KeyError):
        stash["k"]


def test_ttl_accepts_timedelta(tmp_path):
    stash = HashStash(root_dir=str(tmp_path / "c"), ttl=timedelta(milliseconds=300))
    assert stash.ttl == pytest.approx(0.3)
    stash["k"] = "v"
    time.sleep(_EXPIRE_SLEEP)
    assert stash.get("k") is None


def test_ttl_rejects_nonpositive(tmp_path):
    with pytest.raises(ValueError):
        HashStash(root_dir=str(tmp_path / "c"), ttl=0)


@pytest.mark.parametrize("engine", ["pairtree", "sqlite", "jsonl", "memory"])
def test_ttl_across_engines(engine, tmp_path):
    if engine == "sqlite":
        pytest.importorskip("sqlitedict")
    # presence via a generous-ttl stash, expiry via a short-ttl stash — both
    # robust directions, both exercising this engine
    present = HashStash(engine=engine, root_dir=str(tmp_path / f"{engine}_p"), ttl=_LONG_TTL)
    present["k"] = {"payload": 1}
    assert present["k"] == {"payload": 1}

    expiring = HashStash(engine=engine, root_dir=str(tmp_path / f"{engine}_e"), ttl=_SHORT_TTL)
    expiring["k"] = {"payload": 1}
    time.sleep(_EXPIRE_SLEEP)
    assert expiring.get("k") is None
    assert "k" not in expiring


def test_ttl_refreshes_on_rewrite(tmp_path):
    # append_mode so both versions coexist; a fresh write must restart the clock.
    # generous margins: v1 written at 0, v2 at ~1.0s; read at ~1.5s with ttl=1.0
    # -> v1 (age 1.5) expired, v2 (age 0.5) present.
    stash = HashStash(root_dir=str(tmp_path / "c"), ttl=1.0, append_mode=True)
    stash["k"] = "v1"
    time.sleep(1.0)
    stash["k"] = "v2"  # fresh write restarts the clock
    time.sleep(0.5)
    assert stash["k"] == "v2"
    # only the fresh version survives the ttl window
    assert stash.get_all("k") == ["v2"]


def _tick(x):
    _tick.calls.append(x)
    return len(_tick.calls)


_tick.calls = []


def test_ttl_run_recomputes_after_expiry(tmp_path):
    _tick.calls = []
    stash = HashStash(root_dir=str(tmp_path / "c"), ttl=1.0)
    assert stash.run(_tick, 5) == 1
    assert stash.run(_tick, 5) == 1  # cached (back-to-back, well within ttl)
    time.sleep(1.5)
    assert stash.run(_tick, 5) == 2  # expired -> recomputed
    assert len(_tick.calls) == 2


def test_ttl_prune_still_sees_expired(tmp_path):
    stash = HashStash(root_dir=str(tmp_path / "c"), ttl=0.3)
    stash["k1"] = 1
    stash["k2"] = 2
    time.sleep(0.6)
    # reads say absent...
    assert stash.get("k1") is None
    # ...but prune can still find and reclaim the expired entries
    assert stash.prune(older_than=timedelta(seconds=0.05), dry_run=True) == 2


def test_ttl_inherited_by_function_stash(tmp_path):
    stash = HashStash(root_dir=str(tmp_path / "c"), ttl=123)
    fstash = stash.sub_function_results(_tick)
    assert fstash.ttl == 123


# --- single-flight -----------------------------------------------------------


def test_single_flight_across_processes(tmp_path):
    """N concurrent processes missing the same key must execute the function
    once, not N times."""
    pytest.importorskip("sqlitedict")
    root = str(tmp_path / "cache")
    marker_dir = tmp_path / "executions"
    marker_dir.mkdir()

    worker = (
        "import sys, os, time, uuid\n"
        f"sys.path.insert(0, {REPO_ROOT!r})\n"
        "from hashstash import HashStash\n"
        f"stash = HashStash(engine='sqlite', root_dir={root!r})\n"
        "def slow(x):\n"
        f"    open(os.path.join({str(marker_dir)!r}, uuid.uuid4().hex), 'w').close()\n"
        "    time.sleep(0.5)\n"
        "    return x * 2\n"
        "print(stash.run(slow, 21))\n"
    )
    procs = [
        subprocess.Popen(
            [sys.executable, "-c", worker],
            env={**os.environ, "PYTHONPATH": REPO_ROOT},
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        for _ in range(4)
    ]
    outs = []
    for p in procs:
        out, err = p.communicate(timeout=120)
        assert p.returncode == 0, err.decode()
        # hashstash may log to stdout (e.g. "could not get source code" for a
        # -c-defined function); the result is the last line
        outs.append(out.decode().strip().splitlines()[-1])

    assert outs == ["42"] * 4
    executions = len(list(marker_dir.iterdir()))
    assert executions == 1, f"expected 1 execution, got {executions}"


def test_single_flight_opt_out(tmp_path):
    """_single_flight=False must skip the key lock entirely (single caller
    semantics are unchanged either way)."""
    stash = HashStash(root_dir=str(tmp_path / "c"))
    _tick.calls = []
    assert stash.run(_tick, 9, _single_flight=False) == 1
    assert stash.run(_tick, 9, _single_flight=False) == 1  # cached
    assert len(_tick.calls) == 1


def test_get_set_single_flight_threads(tmp_path):
    """Concurrent threads missing the same key run the setter once."""
    import threading

    pytest.importorskip("sqlitedict")
    stash = HashStash(engine="sqlite", root_dir=str(tmp_path / "c"))
    calls = []
    results = []

    def setter():
        calls.append(1)
        time.sleep(0.3)
        return "computed"

    def worker():
        results.append(stash.get_set("k", setter))

    threads = [threading.Thread(target=worker) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert results == ["computed"] * 5
    assert len(calls) == 1


# --- stats -------------------------------------------------------------------


def test_stats_counts(tmp_path):
    stash = HashStash(root_dir=str(tmp_path / "c"))
    assert stash.stats == {}
    stash["a"] = 1
    stash["b"] = 2
    _ = stash["a"]
    _ = stash.get("missing")
    _ = stash.get("missing2")
    del stash["b"]

    stats = stash.stats
    assert stats["sets"] == 2
    assert stats["hits"] == 1
    assert stats["misses"] >= 2  # 'in'/delete checks may add hits, never fewer misses
    assert stats["deletes"] == 1

    stash.reset_stats()
    assert stash.stats == {}


def test_stats_via_stashed_result(tmp_path):
    stash = HashStash(root_dir=str(tmp_path / "c"))
    _tick.calls = []
    fn = stash.stashed_result(_tick)
    fn(1)
    fn(1)
    fstats = fn.stash.stats
    assert fstats["sets"] == 1
    assert fstats["hits"] >= 1
    assert fstats["misses"] >= 1


# --- invalidate --------------------------------------------------------------


def test_invalidate_function_call_signature(tmp_path):
    stash = HashStash(root_dir=str(tmp_path / "c"))
    _tick.calls = []
    fn = stash.stashed_result(_tick)

    assert fn(7) == 1
    assert fn(7) == 1  # cached
    assert fn.invalidate(7) is True
    assert fn(7) == 2  # recomputed
    assert fn.invalidate(7) is True
    assert fn.invalidate(7) is False  # nothing cached now


def test_invalidate_plain_key(tmp_path):
    stash = HashStash(root_dir=str(tmp_path / "c"))
    stash["k"] = "v"
    assert stash.invalidate("k") is True
    assert "k" not in stash
    assert stash.invalidate("k") is False
    with pytest.raises(TypeError):
        stash.invalidate("k", extra="arg")
