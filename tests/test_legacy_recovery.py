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

from hashstash import HashStash, HashStashWarning

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


@pytest.mark.parametrize("engine", ENGINES)
def test_legacy_read_covers_contains_and_items(engine):
    # the real hot path is `if key in stash: stash[key]` — legacy_read must cover
    # __contains__ and items(), not just __getitem__
    s, root, orig = _drifted_stash(engine)
    s2 = HashStash(engine=engine, root_dir=root, dbname=s.dbname, legacy_read=True)
    s2.encode_key = lambda k: orig(k) + b"DRIFT"
    assert {"model": "gpt", "idx": 5} in s2
    assert {"model": "gpt", "idx": 999} not in s2
    items = dict((tuple(sorted(k.items())), v) for k, v in s2.items())
    assert len(items) == 15
    assert items[(("idx", 5), ("model", "gpt"))] == {"result": 50}


@pytest.mark.parametrize("engine", ["pairtree", "lmdb"])
def test_migrate_preserves_append_history(engine):
    if engine == "lmdb":
        pytest.importorskip("lmdb")
    root = tempfile.mkdtemp()
    src = HashStash(engine=engine, root_dir=root, append_mode=True)
    src.clear()
    src[{"k": 1}] = "v1"
    src[{"k": 1}] = "v2"  # two versions of one key
    orig = src.encode_key
    src.encode_key = lambda k: orig(k) + b"DRIFT"  # drift
    dest = HashStash(engine=engine, root_dir=tempfile.mkdtemp(), append_mode=True)
    dest.clear()
    rep = src.migrate(dest=dest, dry_run=False)
    # both versions migrated (total counts raw entries: pairtree=2 version files,
    # lmdb=1 envelope holding both — so assert on migrated + the preserved history)
    assert rep["migrated"] == 2
    assert dest.get_all({"k": 1}, all_results=True) == ["v1", "v2"]  # history kept


@pytest.mark.parametrize("engine", ["lmdb", "pairtree"])
def test_migrate_accepts_path_dest_inheriting_layout(engine):
    # migrate(dest="/path") must build a stash there inheriting the SOURCE layout
    # (engine/serializer/compress/b64), not silently fail on the str
    if engine == "lmdb":
        pytest.importorskip("lmdb")
    s, _root, orig = _drifted_stash(engine)
    destpath = tempfile.mkdtemp()
    rep = s.migrate(dest=destpath, dry_run=False)
    assert rep["migrated"] == 15 and rep["failed"] == 0
    assert rep["first_error"] is None
    dest = rep["dest"]
    assert dest.engine == engine  # inherited, not the default engine
    assert dest.get({"model": "gpt", "idx": 3}) == {"result": 30}


def test_migrate_rejects_non_stash_dest():
    # a non-stash, non-path dest used to be accepted, then every set() failed and
    # was swallowed to {migrated:0, failed:N} — now it errors up front
    s = HashStash(engine="memory", root_dir=tempfile.mkdtemp())
    s.clear()
    s[{"a": 1}] = "v"
    with pytest.raises(TypeError, match="HashStash or a path"):
        s.migrate(dest=12345)


def test_migrate_report_includes_first_error_key():
    s = HashStash(engine="memory", root_dir=tempfile.mkdtemp())
    s.clear()
    s[{"a": 1}] = "v"
    rep = s.migrate(dest=HashStash(engine="memory", root_dir=tempfile.mkdtemp()))
    assert "first_error" in rep and rep["first_error"] is None


def test_migrate_warns_when_layout_kwargs_wrong(caplog):
    # the layout (b64/compress/...) is in the dirname; opening with the wrong
    # kwargs resolves to an empty sibling path — warn instead of silent total=0
    root = tempfile.mkdtemp()
    good = HashStash(engine="pairtree", root_dir=root, compress="lz4", b64=True)
    good.clear()
    good[{"a": 1}] = "x"
    wrong = HashStash(engine="pairtree", root_dir=root, compress="lz4", b64=False)
    with caplog.at_level(logging.WARNING, logger="hashstash"):
        rep = wrong.migrate(dry_run=True)
    assert rep["total"] == 0
    assert any("sibling layout" in r.message for r in caplog.records)


def test_items_warns_loudly_when_unaddressable(caplog):
    import warnings

    s, _root, _orig = _drifted_stash("memory")
    with caplog.at_level(logging.WARNING, logger="hashstash"):
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            list(s.items())
    # loud on the logger (survives filterwarnings('ignore')) ...
    assert any("OLDER hashstash" in r.message for r in caplog.records)
    # ... and catchable programmatically via a typed warning
    assert any(issubclass(x.category, HashStashWarning) for x in w)


def test_no_false_warning_on_a_healthy_stash(caplog):
    s = HashStash(engine="memory", root_dir=tempfile.mkdtemp())
    s.clear()
    s[{"a": 1}] = "v"
    with caplog.at_level(logging.WARNING, logger="hashstash"):
        assert list(s.items()) == [({"a": 1}, "v")]
    assert not any("OLDER hashstash" in r.message for r in caplog.records)
