"""Regression guards for two networked-engine bugs found in the pre-1.0 review:
BUG 1 - stash.map's StashMap cache was unreadable on a safe-mode stash (it
embeds the mapped function, which safe deserialize refuses).
BUG 2 - single-flight lock files landed at a URL-derived path, materializing a
bogus ./memory:/ or ./s3:/ lock tree in the working directory for fsspec."""
import os
import tempfile

import pytest

from hashstash import HashStash


def _square(x):
    return x * x


def _incr(x):
    return x + 1


def test_map_on_safe_stash_is_readable():
    # BUG 1: on a safe stash, a repeat map() and keys()/items() must not raise
    # SafeDeserializationError from the cached StashMap embedding the function.
    s = HashStash(engine="pairtree", safe=True, root_dir=tempfile.mkdtemp())
    s.clear()
    assert list(s.map(_square, objects=[1, 2, 3], progress=False)) == [1, 4, 9]
    # repeat map (would have been a cache read of the unreadable StashMap)
    assert list(s.map(_square, objects=[1, 2, 3], progress=False)) == [1, 4, 9]
    # iterating the stash must not blow up either
    assert list(s.keys()) is not None
    list(s.items())


def test_fsspec_single_flight_does_not_leak_lock_dir_to_cwd(tmp_path, monkeypatch):
    # BUG 2: run() uses the single-flight key_lock; for an fsspec URL root the
    # lock path must be local, not `./memory:/...` in the process cwd.
    pytest.importorskip("fsspec")
    monkeypatch.chdir(tmp_path)  # run in an empty dir so a leak is obvious
    s = HashStash(engine="fsspec", root_dir="memory://cache", dbname="lk", clear=True)
    s.run(_incr, 1)  # exercises the single-flight lock path
    leaked = [d for d in os.listdir(".") if d.startswith(("memory:", "s3:", "gcs:"))]
    assert leaked == [], f"single-flight leaked lock dirs into cwd: {leaked}"
