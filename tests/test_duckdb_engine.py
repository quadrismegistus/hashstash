"""DuckDBHashStash: key-value engine backed by a single DuckDB BLOB table."""
import time

import pytest

pytest.importorskip("duckdb")

from hashstash import HashStash


@pytest.fixture
def stash(tmp_path):
    s = HashStash(engine="duckdb", root_dir=f"{tmp_path}/duckcache")
    s.clear()
    yield s
    s.clear()


def test_roundtrip_dict(stash):
    stash["k"] = {"a": [1, 2, 3], "nested": {"x": True}}
    assert stash["k"] == {"a": [1, 2, 3], "nested": {"x": True}}


def test_roundtrip_list_and_nested(stash):
    stash["lst"] = [1, "two", 3.0, [4, 5]]
    assert stash["lst"] == [1, "two", 3.0, [4, 5]]
    stash["deep"] = {"a": {"b": {"c": [{"d": 1}]}}}
    assert stash["deep"] == {"a": {"b": {"c": [{"d": 1}]}}}


def test_roundtrip_bytes(stash):
    stash["raw"] = b"\x00\x01\x02binary\xff"
    assert stash["raw"] == b"\x00\x01\x02binary\xff"


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


def test_delete_missing_raises(stash):
    with pytest.raises(KeyError):
        stash.delete("never-set")


def test_overwrite(stash):
    stash["k"] = 1
    stash["k"] = 2
    assert stash["k"] == 2
    assert len(stash) == 1


def test_append_mode(tmp_path):
    stash = HashStash(
        engine="duckdb", root_dir=f"{tmp_path}/appendcache", append_mode=True
    )
    stash.clear()
    stash["k"] = 1
    stash["k"] = 2
    assert stash.get_all("k") == [1, 2]
    assert len(stash) == 1


def test_persists_across_instances(tmp_path):
    root = f"{tmp_path}/persistcache"
    a = HashStash(engine="duckdb", root_dir=root)
    a.clear()
    a["k"] = "durable"
    a.close()
    b = HashStash(engine="duckdb", root_dir=root)
    assert b["k"] == "durable"
    b.clear()


def test_ttl_expiry(tmp_path):
    stash = HashStash(engine="duckdb", root_dir=f"{tmp_path}/ttlcache", ttl=0.3)
    stash.clear()
    stash["k"] = "v"
    time.sleep(0.6)
    assert stash.get("k") is None
    assert "k" not in stash
    stash.clear()


def test_clear(stash):
    stash["a"] = 1
    stash["b"] = 2
    assert len(stash) == 2
    stash.clear()
    assert len(stash) == 0
    assert "a" not in stash


def test_duckdb_in_working_engines():
    from hashstash.config import get_working_engines

    assert "duckdb" in get_working_engines()
