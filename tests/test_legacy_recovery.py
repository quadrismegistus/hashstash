"""Recovery of caches written by an older hashstash whose key encoding differs.

The cache-key encoding (the serialized/canonical form of a key) drifted across
versions, so a pre-1.0 cache enumerates (keys() decodes, len() is right) but
get()/`in`/items() silently miss — encode_key(key) now hashes to a different
address than where the entry was stored. We simulate that drift by patching
encode_key AFTER writing (stored under the old form, read under the new one) and
assert the recovery paths read through it."""
import logging
import tempfile

import pytest

from hashstash import HashStash

ENGINES = ["lmdb", "pairtree", "sqlite", "memory"]


def _drifted_stash(engine, n=15):
    """Write n entries, then make encode_key produce a different address than
    what is stored — reproducing a cross-version key-encoding change."""
    if engine == "lmdb":
        pytest.importorskip("lmdb")
    root = tempfile.mkdtemp()
    s = HashStash(engine=engine, root_dir=root)
    s.clear()
    for i in range(n):
        s[{"model": "gpt", "idx": i}] = {"result": i * 10}
    orig = s.encode_key
    s.encode_key = lambda k: orig(k) + b"DRIFT"
    return s, root, orig


@pytest.mark.parametrize("engine", ENGINES)
def test_break_is_reproduced(engine):
    s, _root, _orig = _drifted_stash(engine)
    # keys still enumerate, but the value is unaddressable via the new encoding
    assert sum(1 for _ in s.keys()) == 15
    assert s.get({"model": "gpt", "idx": 3}, "MISS") == "MISS"
    assert sum(1 for _ in s.items()) == 0


@pytest.mark.parametrize("engine", ENGINES)
def test_iter_recovered_reads_through_drift(engine):
    s, _root, _orig = _drifted_stash(engine)
    rec = {tuple(sorted(k.items())): v for k, v in s.iter_recovered()}
    assert len(rec) == 15
    assert rec[(("idx", 7), ("model", "gpt"))] == {"result": 70}


@pytest.mark.parametrize("engine", ENGINES)
def test_migrate_recovers_into_fresh_stash(engine):
    s, _root, _orig = _drifted_stash(engine)
    dry = s.migrate(dry_run=True)
    assert dry["migrated"] == 15 and dry["failed"] == 0
    tgt = HashStash(engine="memory", root_dir=tempfile.mkdtemp())
    tgt.clear()
    rep = s.migrate(dest=tgt, dry_run=False)
    assert rep["migrated"] == 15 and rep["failed"] == 0
    assert tgt.get({"model": "gpt", "idx": 7}) == {"result": 70}  # normal read works


@pytest.mark.parametrize("engine", ENGINES)
def test_legacy_read_flag_reads_through_drift(engine):
    s, root, orig = _drifted_stash(engine)
    s2 = HashStash(engine=engine, root_dir=root, dbname=s.dbname, legacy_read=True)
    s2.encode_key = lambda k: orig(k) + b"DRIFT"
    assert s2.get({"model": "gpt", "idx": 7}, "MISS") == {"result": 70}
    assert s2.get({"model": "gpt", "idx": 999}, "MISS") == "MISS"  # genuine miss


def test_items_warns_loudly_when_unaddressable(caplog):
    s, _root, _orig = _drifted_stash("memory")
    with caplog.at_level(logging.WARNING, logger="hashstash"):
        list(s.items())
    assert any("OLDER hashstash" in r.message for r in caplog.records)


def test_no_false_warning_on_a_healthy_stash(caplog):
    s = HashStash(engine="memory", root_dir=tempfile.mkdtemp())
    s.clear()
    s[{"a": 1}] = "v"
    with caplog.at_level(logging.WARNING, logger="hashstash"):
        assert list(s.items()) == [({"a": 1}, "v")]
    assert not any("OLDER hashstash" in r.message for r in caplog.records)
