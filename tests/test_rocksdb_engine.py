"""RocksDBHashStash: LevelDB-backed key-value engine via plyvel."""
import time

import pytest

# plyvel needs the system LevelDB C library; skip cleanly if it isn't installed
plyvel = pytest.importorskip("plyvel")

from hashstash import HashStash


@pytest.fixture
def stash(tmp_path):
    s = HashStash(engine="rocksdb", root_dir=str(tmp_path / "rockscache"))
    s.clear()
    yield s
    s.clear()


def test_roundtrip(stash):
    stash["k"] = {"a": [1, 2, 3], "nested": {"x": True}}
    assert stash["k"] == {"a": [1, 2, 3], "nested": {"x": True}}


def test_contains_and_len(stash):
    assert len(stash) == 0
    stash["a"] = 1
    stash["b"] = 2
    assert "a" in stash
    assert "missing" not in stash
    assert len(stash) == 2


def test_keys_values_items(stash):
    stash["a"] = 10
    stash["b"] = 20
    assert sorted(stash.keys()) == ["a", "b"]
    assert sorted(stash.values()) == [10, 20]
    assert sorted(stash.items()) == [("a", 10), ("b", 20)]


def test_delete(stash):
    stash["k"] = "v"
    del stash["k"]
    assert "k" not in stash
    with pytest.raises(KeyError):
        del stash["k"]


def test_overwrite(stash):
    stash["k"] = 1
    stash["k"] = 2
    assert stash["k"] == 2
    assert len(stash) == 1


def test_append_mode(tmp_path):
    stash = HashStash(
        engine="rocksdb",
        root_dir=str(tmp_path / "appendcache"),
        append_mode=True,
    )
    stash.clear()
    stash["k"] = 1
    stash["k"] = 2
    assert stash.get_all("k") == [1, 2]
    stash.clear()


def test_persists_across_instances(tmp_path):
    root = str(tmp_path / "persistcache")
    a = HashStash(engine="rocksdb", root_dir=root)
    a.clear()
    a["k"] = "durable"
    a.close()  # drop the process-global handle so b reopens from disk
    b = HashStash(engine="rocksdb", root_dir=root)
    assert b["k"] == "durable"
    b.clear()


def test_ttl_expiry(tmp_path):
    stash = HashStash(engine="rocksdb", root_dir=str(tmp_path / "ttlcache"), ttl=0.2)
    stash.clear()
    stash["k"] = "v"
    assert stash["k"] == "v"
    time.sleep(0.3)
    assert stash.get("k") is None
    stash.clear()


def test_clear(stash):
    stash["a"] = 1
    stash["b"] = 2
    assert len(stash) == 2
    stash.clear()
    assert len(stash) == 0
    assert "a" not in stash


def test_rocksdb_in_working_engines():
    from hashstash.config import get_working_engines

    assert "rocksdb" in get_working_engines()
