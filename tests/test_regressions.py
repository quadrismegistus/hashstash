"""Regression tests for bugs found in the 2026-07 repository audit.

Grouped by fix stage; each test names the defect it guards against.
"""
import os
import subprocess
import sys

import pytest

from hashstash import HashStash
from hashstash.engines.base import BaseHashStash
from hashstash.utils.encodings import encode_compressed

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# --- Stage 1: data loss & global state --------------------------------------


def test_clear_file_style_root_dir_spares_siblings(tmp_path):
    """clear() on a file-style root_dir must not rmtree the parent directory."""
    parent = tmp_path / "parent"
    parent.mkdir()
    precious = parent / "precious.txt"
    precious.write_text("precious data")

    stash = HashStash(root_dir=str(parent / "mycache.db"))
    stash["k"] = "v"
    stash.clear()

    assert precious.exists()
    assert parent.exists()


def test_clear_standard_layout_removes_only_stash_dir(tmp_path):
    stash = HashStash(root_dir=str(tmp_path / "cache"))
    stash["k"] = "v"
    stash.clear()
    assert not os.path.exists(stash.path_dirname)
    assert tmp_path.exists()


def test_import_does_not_mutate_global_state():
    """import hashstash must not install warnings filters (it used to blanket-ignore
    every warning in the host app) nor set the multiprocessing start method (the old
    code caught the wrong exception type and would crash on Windows)."""
    code = (
        "import warnings, multiprocessing as mp\n"
        "n_before = len(warnings.filters)\n"
        "import hashstash\n"
        "assert len(warnings.filters) == n_before, 'import added warnings filters'\n"
        "assert mp.get_start_method(allow_none=True) is None, 'import set mp start method'\n"
    )
    env = {**os.environ, "PYTHONPATH": REPO_ROOT}
    result = subprocess.run(
        [sys.executable, "-c", code], env=env, capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr


def test_set_propagates_write_errors(tmp_path):
    """A failed write must raise, not silently drop the entry (was: bare log.error)."""

    class FailingDB(dict):
        def __setitem__(self, key, value):
            raise OSError("disk full")

    class FailingStash(BaseHashStash):
        engine = "failing"
        needs_lock = False

        def get_db(self):
            return FailingDB()

    stash = FailingStash(root_dir=str(tmp_path / "cache"))
    with pytest.raises(OSError):
        stash["k"] = "v"


def test_compression_failure_raises(monkeypatch):
    """Compression errors must raise, not silently store uncompressed bytes that are
    recorded as compressed (which made the entry permanently unreadable)."""
    import zlib

    def boom(data):
        raise RuntimeError("boom")

    monkeypatch.setattr(zlib, "compress", boom)
    with pytest.raises(RuntimeError):
        encode_compressed(b"payload", "zlib")


def test_diskcache_does_not_silently_evict(tmp_path):
    """diskcache's default 1 GB least-recently-stored eviction must be disabled:
    no other engine silently drops entries."""
    pytest.importorskip("diskcache")
    stash = HashStash(engine="diskcache", root_dir=str(tmp_path / "dc"))
    with stash.db as db:
        assert db.eviction_policy == "none"


def test_pairtree_delete_method(tmp_path):
    """delete() on the default engine used to raise NotImplementedError (only
    __delitem__ was overridden)."""
    stash = HashStash(engine="pairtree", root_dir=str(tmp_path / "pt"))
    stash["a"] = 1
    stash.delete("a")
    assert "a" not in stash
    with pytest.raises(KeyError):
        stash.delete("a")


def _redis_available():
    try:
        import redis
    except ImportError:
        return False
    try:
        redis.Redis(host="localhost", port=6379, socket_connect_timeout=0.5).ping()
        return True
    except Exception:
        return False


# --- Stage 2: cache-key stability & serializer correctness ------------------


def test_dict_key_order_does_not_change_cache_key(tmp_path):
    """Equal dicts with different insertion order must hit the same cache entry."""
    stash = HashStash(root_dir=str(tmp_path / "cache"))
    stash[{"a": 1, "b": 2}] = "hit"
    assert stash[{"b": 2, "a": 1}] == "hit"


def test_kwargs_order_does_not_change_function_key(tmp_path):
    stash = HashStash(root_dir=str(tmp_path / "cache"))
    k1 = stash.encode_key((["f"], {"a": 1, "b": 2}))
    k2 = stash.encode_key((["f"], {"b": 2, "a": 1}))
    assert k1 == k2


def test_set_serialization_is_deterministic_across_hash_seeds():
    """Sets used to serialize in PYTHONHASHSEED-dependent order, changing the
    cache key every interpreter restart."""
    code = (
        "from hashstash import serialize\n"
        "print(serialize({'gamma', 'alpha', 'x', 'y', 'beta', 'z'}, serializer='hashstash'))\n"
    )
    outs = set()
    for seed in ("0", "1", "2"):
        env = {**os.environ, "PYTHONPATH": REPO_ROOT, "PYTHONHASHSEED": seed}
        result = subprocess.run(
            [sys.executable, "-c", code], env=env, capture_output=True, text=True
        )
        assert result.returncode == 0, result.stderr
        outs.add(result.stdout)
    assert len(outs) == 1, f"set serialization varies with hash seed: {outs}"


@pytest.mark.parametrize(
    "value",
    [
        {1: "one", 2: "two"},
        {1.5: "float-key", True: "bool-key"},
        {(1, 2): "tuple-key"},
        {"__py__": "not.an.address", "__data__": "just a plain dict"},
        {"__pytype__": "dict"},
    ],
)
def test_dict_key_types_roundtrip(value, tmp_path):
    """Non-string dict keys were silently JSON-coerced to strings (or crashed for
    tuple keys); dicts containing reserved marker keys were misdeserialized."""
    stash = HashStash(root_dir=str(tmp_path / "cache"))
    stash["k"] = value
    assert stash["k"] == value


def test_datetime_roundtrip(tmp_path):
    """datetime used to fail at write time: __reduce__ args contain raw bytes that
    json.dumps rejected."""
    import datetime

    value = datetime.datetime(2020, 1, 1, 12, 0)
    stash = HashStash(root_dir=str(tmp_path / "cache"))
    stash["dt"] = value
    assert stash["dt"] == value


def test_complex_roundtrip(tmp_path):
    """complex used to silently serialize to null (bare __reduce__ raises on
    C-types; the exception was swallowed)."""
    stash = HashStash(root_dir=str(tmp_path / "cache"))
    stash["c"] = complex(1, 2)
    assert stash["c"] == complex(1, 2)


class EmptyPayload:
    """Importable class whose to_dict() payload is empty (falsy)."""

    def __init__(self, label="init"):
        self.label = label

    def to_dict(self):
        return {}

    @classmethod
    def from_dict(cls, data):
        return cls(label="from_dict")


def test_empty_data_object_roundtrips_to_instance():
    """An object whose to_dict() is empty used to deserialize to the class object
    itself instead of an instance (falsy __data__ check)."""
    from hashstash import deserialize, serialize

    out = deserialize(
        serialize(EmptyPayload(), serializer="hashstash"), serializer="hashstash"
    )
    assert isinstance(out, EmptyPayload), f"expected instance, got {out!r}"


def test_unserializable_object_raises_not_none():
    """Objects that cannot be reduced must raise, not silently become null."""
    import threading

    from hashstash import serialize

    with pytest.raises(Exception):
        serialize(threading.Lock(), serializer="hashstash")


@pytest.mark.skipif(not _redis_available(), reason="no local redis server")
def test_redis_clear_scoped_to_namespace():
    """clear() used to flushdb() the whole numbered Redis db, wiping other stashes
    (and any other application data) that hash to the same db number."""
    from hashstash.engines.redis import get_db_number

    # find two dbnames that collide on the same numbered redis db
    name_a = "regression_clear_a"
    name_b = next(
        f"regression_clear_b{i}"
        for i in range(1000)
        if get_db_number(f"regression_clear_b{i}") == get_db_number(name_a)
    )
    a = HashStash(engine="redis", dbname=name_a)
    b = HashStash(engine="redis", dbname=name_b)
    try:
        a["k"] = "va"
        b["k"] = "vb"
        a.clear()
        assert a.get("k") is None
        assert b.get("k") == "vb"
    finally:
        a.clear()
        b.clear()
