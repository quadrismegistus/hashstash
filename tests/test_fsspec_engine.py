"""FsspecHashStash: pairtree layout over fsspec filesystems."""
import uuid

import pytest

fsspec = pytest.importorskip("fsspec")

from hashstash import HashStash


@pytest.fixture(params=["memory", "file"])
def stash(request, tmp_path):
    if request.param == "memory":
        # unique root per test so the process-global memory fs doesn't leak
        root = f"memory://fsspec-test-{uuid.uuid4().hex}"
    else:
        root = f"file://{tmp_path}/fscache"
    s = HashStash(engine="fsspec", root_dir=root)
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
    root = f"file://{tmp_path}/appendcache"
    stash = HashStash(engine="fsspec", root_dir=root, append_mode=True)
    stash.clear()
    stash["k"] = 1
    stash["k"] = 2
    assert stash.get_all("k") == [1, 2]


def test_persists_across_instances(tmp_path):
    root = f"file://{tmp_path}/persistcache"
    a = HashStash(engine="fsspec", root_dir=root)
    a.clear()
    a["k"] = "durable"
    b = HashStash(engine="fsspec", root_dir=root)
    assert b["k"] == "durable"
    b.clear()


def test_ttl_on_fsspec(tmp_path):
    import time

    root = f"file://{tmp_path}/ttlcache"
    stash = HashStash(engine="fsspec", root_dir=root, ttl=0.2)
    stash.clear()
    stash["k"] = "v"
    assert stash["k"] == "v"
    time.sleep(0.3)
    assert stash.get("k") is None


def test_fsspec_url_root_dir_preserved():
    stash = HashStash(engine="fsspec", root_dir="memory://keep-this-url/sub")
    assert stash.root_dir == "memory://keep-this-url/sub"
    assert "://" in stash.path


def test_fsspec_in_working_engines():
    from hashstash.config import get_working_engines

    assert "fsspec" in get_working_engines()
