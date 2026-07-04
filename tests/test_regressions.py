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


# --- Stage 3: None-sentinel, function identity, wrappers/run() --------------


def test_stored_none_is_not_a_miss(tmp_path):
    """Storing None used to make __getitem__ raise KeyError and get_set re-run
    its setter on every call."""
    stash = HashStash(root_dir=str(tmp_path / "cache"))
    stash["k"] = None
    assert "k" in stash
    assert stash["k"] is None

    calls = []

    def setter():
        calls.append(1)
        return None

    assert stash.get_set("none-key", setter) is None
    assert stash.get_set("none-key", setter) is None
    assert len(calls) == 1


def _returns_none(x):
    _returns_none.calls.append(x)
    return None


_returns_none.calls = []


def test_none_returning_function_cached(tmp_path):
    stash = HashStash(root_dir=str(tmp_path / "cache"))
    assert stash.run(_returns_none, 1) is None
    assert stash.run(_returns_none, 1) is None
    assert len(_returns_none.calls) == 1


def test_closures_do_not_share_cache(tmp_path):
    """Closures from the same factory shared one cache key: run(make_adder(100), 5)
    used to return make_adder(1)'s cached 6."""
    stash = HashStash(root_dir=str(tmp_path / "cache"))

    def make_adder(n):
        def adder(x):
            return x + n

        return adder

    assert stash.run(make_adder(1), 5) == 6
    assert stash.run(make_adder(100), 5) == 105


def test_lambdas_do_not_share_cache(tmp_path):
    stash = HashStash(root_dir=str(tmp_path / "cache"))
    add_one = lambda x: x + 1
    times_ten = lambda x: x * 10
    assert stash.run(add_one, 5) == 6
    assert stash.run(times_ten, 5) == 50


class SimplePoint:
    def __init__(self, x):
        self.x = x


def test_stashed_result_with_object_first_arg(tmp_path):
    """Any object-valued first argument used to be misclassified as 'self',
    raising AttributeError and corrupting the decorator's closure."""
    stash = HashStash(root_dir=str(tmp_path / "cache"))

    @stash.stashed_result
    def scale(point, factor):
        return point.x * factor

    assert scale(SimplePoint(2), 3) == 6
    assert scale(SimplePoint(5), 3) == 15


def test_run_with_kwarg_named_default(tmp_path):
    """User kwargs used to leak into stash.get(), colliding with its parameters."""
    stash = HashStash(root_dir=str(tmp_path / "cache"))

    def render(x, default="D"):
        return f"{x}-{default}"

    assert stash.run(render, 1, default="Z") == "1-Z"


def test_run_builtin(tmp_path):
    """run() on builtins used to crash (no writable __dict__)."""
    stash = HashStash(root_dir=str(tmp_path / "cache"))
    assert stash.run(len, [1, 2, 3]) == 3


def _mul(x, y=1):
    return x * y


def test_map_key_includes_common_kwargs(tmp_path):
    """Two maps differing only in common kwargs used to share one stashed map."""
    stash = HashStash(root_dir=str(tmp_path / "cache"))
    r1 = list(stash.map(_mul, objects=[1, 2], y=2, num_proc=1, progress=False))
    r2 = list(stash.map(_mul, objects=[1, 2], y=3, num_proc=1, progress=False))
    assert [r.result for r in r1] == [2, 4]
    assert [r.result for r in r2] == [3, 6]


def test_relative_dir_path_resolves_from_cwd(tmp_path, monkeypatch):
    """Directory-style relative paths used to silently nest under
    ~/.cache/hashstash instead of resolving from the current directory."""
    monkeypatch.chdir(tmp_path)
    stash = HashStash(root_dir="./mycache")
    assert stash.root_dir == str(tmp_path / "mycache")
    stash2 = HashStash(root_dir=os.path.join("data", "cache"))
    assert stash2.root_dir == str(tmp_path / "data" / "cache")


def test_bare_name_nests_under_config_root():
    from hashstash.config import Config

    stash = HashStash(root_dir="bare_name_regression")
    assert stash.root_dir == os.path.join(Config().root_dir, "bare_name_regression")


# --- Stage 4: real locking & connection pool --------------------------------


def test_lock_is_reentrant(tmp_path):
    """Nested `with stash:` used to make the inner exit release the outer lock."""
    pytest.importorskip("sqlitedict")
    stash = HashStash(engine="sqlite", root_dir=str(tmp_path / "cache"))
    with stash:
        with stash:
            stash["k"] = 1
        stash["k2"] = 2  # still holding the outer lock
    assert stash["k"] == 1
    assert stash["k2"] == 2


@pytest.mark.skipif(os.name == "nt", reason="flock check is POSIX-only")
def test_lock_excludes_other_processes(tmp_path):
    """The old Manager-based lock was per-process: two independent processes never
    shared it. The file lock must actually be held across process boundaries."""
    pytest.importorskip("sqlitedict")
    stash = HashStash(engine="sqlite", root_dir=str(tmp_path / "cache"))
    stash["seed"] = 1  # create dirs + lock file
    lock_path = stash.path + ".lock"

    probe = (
        "import fcntl, sys\n"
        f"f = open({lock_path!r}, 'a+b')\n"
        "try:\n"
        "    fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)\n"
        "    print('ACQUIRED')\n"
        "except OSError:\n"
        "    print('LOCKED')\n"
    )
    with stash:
        held = subprocess.run(
            [sys.executable, "-c", probe], capture_output=True, text=True
        )
    released = subprocess.run(
        [sys.executable, "-c", probe], capture_output=True, text=True
    )
    assert held.stdout.strip() == "LOCKED", held.stderr
    assert released.stdout.strip() == "ACQUIRED", released.stderr


def test_concurrent_appends_no_lost_updates(tmp_path):
    """Append-mode was an unlocked read-modify-write: concurrent appenders read
    the same old envelope and one append vanished."""
    pytest.importorskip("sqlitedict")
    root = str(tmp_path / "cache")
    n_procs, n_appends = 4, 5

    worker = (
        "import sys\n"
        f"sys.path.insert(0, {REPO_ROOT!r})\n"
        "from hashstash import HashStash\n"
        f"stash = HashStash(engine='sqlite', root_dir={root!r}, append_mode=True)\n"
        f"for i in range({n_appends}):\n"
        "    stash.set('k', (int(sys.argv[1]), i))\n"
    )
    procs = [
        subprocess.Popen(
            [sys.executable, "-c", worker, str(n)],
            env={**os.environ, "PYTHONPATH": REPO_ROOT},
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        for n in range(n_procs)
    ]
    for p in procs:
        _, err = p.communicate(timeout=120)
        assert p.returncode == 0, err.decode()

    stash = HashStash(engine="sqlite", root_dir=root, append_mode=True)
    versions = stash.get_all("k")
    assert len(versions) == n_procs * n_appends


def test_connection_pool_thread_safety(tmp_path):
    """The pool was an unsynchronized dict: concurrent threads could clobber and
    leak each other's connections."""
    import threading

    pytest.importorskip("sqlitedict")
    stash = HashStash(engine="sqlite", root_dir=str(tmp_path / "cache"))
    errors = []

    def work(tid):
        try:
            for i in range(20):
                stash[f"{tid}-{i}"] = i
                assert stash[f"{tid}-{i}"] == i
        except Exception as e:  # pragma: no cover - failure path
            errors.append(e)

    threads = [threading.Thread(target=work, args=(t,)) for t in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    assert len(stash) == 8 * 20


# --- Stage 5: engine fixes ---------------------------------------------------


def test_unknown_engine_raises():
    """Unknown engines used to silently fall back to pairtree."""
    with pytest.raises(ValueError):
        HashStash(engine="ldmb")  # typo of lmdb


def test_jsonl_sees_other_writers(tmp_path):
    """The JSONL keyset loaded once and never refreshed: a long-lived reader never
    saw keys written by another instance/process."""
    root = str(tmp_path / "cache")
    reader = HashStash(engine="jsonl", root_dir=root)
    assert reader.get("k") is None  # loads (empty) keyset

    writer = HashStash(engine="jsonl", root_dir=root)
    writer["k"] = "written elsewhere"

    assert "k" in reader
    assert reader.get("k") == "written elsewhere"


def test_jsonl_overwrite_semantics_match_other_engines(tmp_path):
    """With append_mode=False, jsonl.get_all returned every historical row while
    every other engine returned only the latest."""
    jstash = HashStash(engine="jsonl", root_dir=str(tmp_path / "j"), append_mode=False)
    jstash["k"] = 1
    jstash["k"] = 2
    assert jstash.get_all("k") == [2]
    assert jstash.items_l() == [("k", 2)]
    # chaining contract: clear() returns self like the base engine
    assert jstash.clear() is jstash


def test_lmdb_two_instances_same_path(tmp_path):
    """Two stash objects on one path used to open the LMDB environment twice in
    one process ('environment already open', issue #9)."""
    pytest.importorskip("lmdb")
    root = str(tmp_path / "cache")
    a = HashStash(engine="lmdb", root_dir=root)
    b = HashStash(engine="lmdb", root_dir=root)
    a["k"] = "va"
    assert b["k"] == "va"
    b["k2"] = "vb"
    assert a["k2"] == "vb"
    a.close()


def test_pairtree_same_microsecond_appends_both_survive(tmp_path):
    """Version filenames were bare microsecond timestamps: two writers in the same
    microsecond silently overwrote each other. Filenames now carry a pid suffix."""
    stash = HashStash(engine="pairtree", root_dir=str(tmp_path / "pt"), append_mode=True)
    stash["k"] = "v1"
    stash["k"] = "v2"
    path = stash._get_path(stash.encode_key("k"))
    versions = [f for f in os.listdir(path) if not f.startswith(".")]
    assert len(versions) == 2
    assert all("." in f for f in versions)  # pid suffix present
    assert stash.get_all("k") == ["v1", "v2"]


def test_dataframe_engine_set_contract(tmp_path):
    """DataFrameHashStash.set() violated the base signature (no append param) and
    never honored append_mode=False for dataframe values."""
    pd = pytest.importorskip("pandas")
    stash = HashStash(engine="dataframe", root_dir=str(tmp_path / "df"), append_mode=False)

    df1 = pd.DataFrame({"a": [1, 2]})
    df2 = pd.DataFrame({"a": [3, 4]})
    stash.set("k", df1, append=None)  # base-contract signature
    stash.set("k", df2)

    path = stash._get_path(stash.encode_key("k"))
    versions = [f for f in os.listdir(path) if not f.startswith(".")]
    assert len(versions) == 1  # overwrite semantics honored

    out = stash.get("k")
    got = out.df if hasattr(out, "df") else out
    assert list(got["a"]) == [3, 4]

    # non-dataframe values still work through the pairtree path
    stash["plain"] = {"x": 1}
    assert stash["plain"] == {"x": 1}


def test_shelve_engine_roundtrip(tmp_path):
    """shelve existed in code but was never tested anywhere."""
    stash = HashStash(engine="shelve", root_dir=str(tmp_path / "sh"))
    stash["k"] = {"nested": [1, 2, 3]}
    assert stash["k"] == {"nested": [1, 2, 3]}
    del stash["k"]
    assert "k" not in stash


def test_memory_engine_works_without_ultradict(tmp_path, monkeypatch):
    """memory is listed as a builtin engine but hard-required the optional
    ultradict package at first use; it now degrades to a process-local dict."""
    import hashstash.engines.memory as mem

    monkeypatch.setattr(mem, "SHARED_MEMORY_CACHE", None)
    real_import = __import__

    def no_ultradict(name, *args, **kwargs):
        if name == "UltraDict":
            raise ImportError("simulated missing ultradict")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr("builtins.__import__", no_ultradict)
    stash = HashStash(engine="memory", root_dir=str(tmp_path / "mem"))
    stash["k"] = "v"
    assert stash["k"] == "v"
    monkeypatch.setattr(mem, "SHARED_MEMORY_CACHE", None)


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
