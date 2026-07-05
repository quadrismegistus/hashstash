"""Regression guards for the concurrency bugs found in the pre-1.0 stress review.
These run real subprocesses (spawn), so they're a bit slow; kept modest to catch
the races without dominating CI."""
import tempfile
from concurrent.futures import ProcessPoolExecutor

import pytest

from hashstash import HashStash


# module-level workers (spawn re-imports this module); root passed explicitly
# because a spawn worker does NOT inherit main-process globals.
def _pairtree_same_key_writer(args):
    root, i = args
    s = HashStash(engine="pairtree", root_dir=root, dbname="same")
    for n in range(30):
        s["K"] = f"w{i}-{n}"
    return 1


def _lmdb_appender(args):
    root, i = args
    s = HashStash(engine="lmdb", append_mode=True, root_dir=root, dbname="app")
    for n in range(50):
        s["K"] = f"w{i}-{n}"
    return 1


def test_pairtree_same_key_concurrent_write_never_loses_value():
    # BUG 1/2: concurrent same-key writes must never leave the key valueless
    # (the prune race deleted every version; reads must not crash either).
    root = tempfile.mkdtemp()
    HashStash(engine="pairtree", root_dir=root, dbname="same").clear()
    with ProcessPoolExecutor(max_workers=6) as ex:
        list(ex.map(_pairtree_same_key_writer, [(root, i) for i in range(6)]))
    s = HashStash(engine="pairtree", root_dir=root, dbname="same")
    assert "K" in s
    val = s.get("K", "__MISSING__")
    assert val != "__MISSING__"
    assert isinstance(val, str) and val.startswith("w")  # a real written value
    # len agrees with membership (the race also caused len!=has)
    assert len(s) == 1


def test_lmdb_append_mode_concurrent_keeps_all_versions():
    pytest.importorskip("lmdb")
    # BUG 4: append read-modify-write is now locked on needs_lock=False engines,
    # so concurrent appends don't lose versions to a lost update.
    root = tempfile.mkdtemp()
    HashStash(engine="lmdb", append_mode=True, root_dir=root, dbname="app").clear()
    n_workers, per = 4, 50
    with ProcessPoolExecutor(max_workers=n_workers) as ex:
        list(ex.map(_lmdb_appender, [(root, i) for i in range(n_workers)]))
    s = HashStash(engine="lmdb", append_mode=True, root_dir=root, dbname="app")
    kept = len(s.get_all("K") or [])
    assert kept == n_workers * per, f"lost append versions: {kept}/{n_workers*per}"
