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
    assert r1 == [2, 4]   # StashMap iterates values now (was run wrappers)
    assert r2 == [3, 6]


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


# --- Stage 7: test isolation -------------------------------------------------


def test_default_stash_is_isolated_from_user_cache(isolated_default_root):
    """The conftest isolation fixture used to patch a throwaway Config instance:
    HashStash() still wrote to the real ~/.cache/hashstash during tests."""
    stash = HashStash()
    assert stash.root_dir.startswith(isolated_default_root)
    assert not stash.root_dir.startswith(os.path.expanduser("~/.cache/hashstash"))


# --- Stage 6: GraphStash -----------------------------------------------------


@pytest.fixture
def graph(tmp_path):
    stash = HashStash(root_dir=str(tmp_path / "g"))
    return stash.graph()


def test_graph_edges_where_mixed_types_no_typeerror(graph):
    """One edge storing a string weight used to TypeError every weight__gt query."""
    graph.add_edge("a", "b", weight=5)
    graph.add_edge("a", "c", weight="heavy")
    graph.add_edge("a", "d", tag=7)
    hits = graph.edges_where(weight__gt=1)
    assert [(s, d) for s, d, r, p in hits] == [("a", "b")]
    assert graph.edges_where(tag__contains="x") == []  # int prop: non-match, not crash


def test_graph_node_returns_copy(graph):
    """node() used to hand out the live cache dict: caller mutation silently
    diverged the cache from disk."""
    graph.add_node("a", role="original")
    props = graph.node("a")
    props["role"] = "HACKED"
    assert graph.node("a")["role"] == "original"


def test_graph_edges_of_returns_copies(graph):
    graph.add_edge("a", "b", w=1)
    edges = graph.edges_of("a")
    edges[0][2]["w"] = 999
    assert graph.edge("a", "b")["w"] == 1


def test_graph_preload_warms_sink_nodes(graph):
    """preload() iterated out-stash keys to warm the in-cache, so pure sink nodes
    stayed cold."""
    graph.add_edge("a", "b")
    g2 = graph._stash.graph()
    g2.preload()
    assert "b" in g2._cache_in


def test_graph_edges_between_returns_parallel_edges(graph):
    graph.add_edge("a", "b", rel="knows", since=2020)
    graph.add_edge("a", "b", rel="knows", since=2024)
    graph.add_edge("a", "b", rel="likes")
    knows = graph.edges_between("a", "b", rel="knows")
    assert sorted(p["since"] for r, p in knows) == [2020, 2024]
    assert len(graph.edges_between("a", "b")) == 3


def test_graph_edges_where_rel_none_means_rel_none(graph):
    """edges_where(rel=None) was a no-op filter returning every edge."""
    graph.add_edge("a", "b")  # rel None
    graph.add_edge("a", "c", rel="k")
    hits = graph.edges_where(rel=None)
    assert [(s, d) for s, d, r, p in hits] == [("a", "b")]
    assert len(graph.edges_where()) == 2


def test_graph_absent_prop_ne_semantics(graph):
    graph.add_edge("a", "b", color="red")
    graph.add_edge("a", "c")  # no color
    hits = graph.edges_where(color__ne="blue")
    assert sorted(d for s, d, r, p in hits) == ["b", "c"]


def test_graph_interleaved_write_query_stays_fresh(graph):
    """Every write used to nuke the key caches, and queries after writes could
    miss just-written edges if caches went stale in the other direction."""
    graph.add_edge("a", "b", n=1)
    assert len(graph.edges_where(n__gte=1)) == 1
    graph.add_edge("a", "c", n=2)
    assert len(graph.edges_where(n__gte=1)) == 2
    graph.add_edge("z", "a", n=3)  # brand-new source node
    assert len(graph.edges_where(n__gte=1)) == 3


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


# --- Stage 5: canonical key ordering & partial-drift detection ---------------
# Reported by a downstream consumer who lost a day to insertion-order-sensitive
# dict keys across a 0.4.0 -> 1.0.1 upgrade.


@pytest.mark.parametrize(
    "a,b,label",
    [
        ({1: "a", 2: "b"}, {2: "b", 1: "a"}, "int-keyed dict"),
        ({(1, 2): "a", (3, 4): "b"}, {(3, 4): "b", (1, 2): "a"}, "tuple-keyed dict"),
        ({"a": 1, 1: "a"}, {1: "a", "a": 1}, "mixed-key dict"),
        ({"__py__": 1, "z": 2}, {"z": 2, "__py__": 1}, "dict w/ reserved key"),
        ({"k": {1: "a", 2: "b"}}, {"k": {2: "b", 1: "a"}}, "nested int-keyed dict"),
    ],
)
def test_non_str_keyed_dicts_canonicalize_on_the_key_path(a, b, label):
    """Dicts with non-string (or reserved) keys serialize to a JSON LIST of pairs,
    which json.dumps(sort_keys=True) does NOT reorder — so two equal dicts used as
    cache keys addressed to two different entries, silently."""
    from hashstash import serialize

    assert serialize(a, as_string=True, sort_keys=True) == serialize(
        b, as_string=True, sort_keys=True
    ), label


def test_reordered_non_str_key_hits_the_same_stash_entry(tmp_path):
    """End-to-end: the reproducer the consumer sent back."""
    stash = HashStash(root_dir=str(tmp_path), engine="memory", b64=True)
    stash[{"x": {1: "a", 2: "b"}}] = "hit"
    assert stash.get({"x": {2: "b", 1: "a"}}) == "hit"


def test_dict_subclasses_canonicalize_except_ordered_dict(tmp_path):
    """Counter/defaultdict compare order-insensitively, so their keys must
    canonicalize. OrderedDict.__eq__ IS order-sensitive: collapsing two
    differently-ordered OrderedDicts to one address would be a false HIT, which is
    worse than the miss being fixed here."""
    from collections import Counter, OrderedDict, defaultdict

    stash = HashStash(root_dir=str(tmp_path), engine="memory", b64=True)

    d1, d2 = defaultdict(int), defaultdict(int)
    d1["a"], d1["b"] = 1, 2
    d2["b"], d2["a"] = 2, 1
    assert stash.encode_key(d1) == stash.encode_key(d2)
    assert stash.encode_key(Counter({"a": 1, "b": 2})) == stash.encode_key(
        Counter({"b": 2, "a": 1})
    )

    assert OrderedDict([("a", 1), ("b", 2)]) != OrderedDict([("b", 2), ("a", 1)])
    assert stash.encode_key(OrderedDict([("a", 1), ("b", 2)])) != stash.encode_key(
        OrderedDict([("b", 2), ("a", 1)])
    )


def test_value_path_preserves_dict_insertion_order():
    """Canonicalization is for KEYS only — a value's insertion order is observable
    on round-trip and must survive."""
    from hashstash import deserialize, serialize

    assert list(deserialize(serialize({2: "b", 1: "a"})).keys()) == [2, 1]


def test_list_items_are_never_reordered():
    """__listitems__ (and plain lists) carry semantic order; only dict entries sort."""
    from hashstash import deserialize, serialize

    assert deserialize(serialize([3, 1, 2], sort_keys=True)) == [3, 1, 2]
    assert deserialize(serialize(({"b": 1}, [3, 1, 2]), sort_keys=True)) == (
        {"b": 1},
        [3, 1, 2],
    )


def _strand_at_legacy_address(stash, key):
    """Move an entry to the address a PRE-canonical hashstash would have written it
    to (insertion-order serialization, no sort_keys). The entry still decodes and
    still enumerates via keys() — it just no longer resolves, which is exactly the
    drift signature a 0.4.0-written cache presents to 1.0.x.

    Note the two things that do NOT work: deleting the entry removes it from keys()
    too (n_keys drops, so no drift exists), and mangling the stored address makes
    keys() raise on decode. Go through the engine-agnostic _get/_set/_del
    primitives rather than stash.db — some backends (lmdb) expose an Environment
    there, which does not support item assignment.

    Caveats for anyone reusing this to test drift monitoring against real drift:
    - it assumes one envelope per key, so it does not transfer to pairtree, which
      stores one file per stored version (see the parametrize list below);
    - pairtree also does not implement BaseHashStash._get at all — it overrides
      get_all and the path-based read, so the base primitive raises
      NotImplementedError there. Dormant in normal operation (nothing reaches that
      call site), but it surfaces the moment you drive the raw primitives, which is
      exactly what this helper does."""
    canonical = stash.encode_key(key)
    legacy = stash.encode(stash.serialize(key), as_string=stash.string_keys)
    assert legacy != canonical, "key must not already be in canonical order"
    stash._set(legacy, stash._get(canonical))
    stash._del(canonical)


# pairtree is deliberately absent: it stores one file per stored version rather
# than one envelope per key, so relocating a raw entry to a legacy address is not
# the same operation there. The warning itself is engine-independent (it lives in
# BaseHashStash.items), so these three cover it.
@pytest.mark.parametrize("engine", ["lmdb", "sqlite", "memory"])
def test_partial_drift_warns_even_when_most_keys_resolve(engine, tmp_path):
    """items() warned only when NOTHING resolved, so a stash with (say) 40% of its
    entries unaddressable looked perfectly healthy. Partial drift is the more
    dangerous shape precisely because it does not announce itself."""
    from hashstash.engines.base import HashStashWarning

    if engine == "lmdb":
        pytest.importorskip("lmdb")
    stash = HashStash(root_dir=str(tmp_path), engine=engine, b64=True)
    stash.clear()
    keys = [{"b": i, "a": i} for i in range(5)]  # non-alphabetical insertion order
    for i, k in enumerate(keys):
        stash[k] = i

    for k in keys[:2]:
        _strand_at_legacy_address(stash, k)

    assert len(stash) == 5  # all five still enumerate
    with pytest.warns(HashStashWarning, match="PARTIAL"):
        resolved = list(stash.items())
    assert len(resolved) == 3


def test_append_mode_versions_do_not_mask_unresolvable_keys(tmp_path):
    """The predicate must compare KEYS to keys, not values to keys: get_all returns
    every stored version, so on an append-mode stash the value count exceeds the key
    count and a naive `n_yield < n_keys` goes quiet exactly where there is most
    history to lose."""
    from hashstash.engines.base import HashStashWarning

    stash = HashStash(
        root_dir=str(tmp_path), engine="memory", b64=True, append_mode=True
    )
    keys = [{"b": i, "a": i} for i in range(4)]
    for i, k in enumerate(keys):
        for version in range(3):
            stash[k] = f"{i}v{version}"

    for k in keys[:2]:
        _strand_at_legacy_address(stash, k)

    n_keys = len(stash)
    with pytest.warns(HashStashWarning, match="PARTIAL"):
        pairs = list(stash.items(all_results=True))

    assert n_keys == 4
    # the exact case a `n_yield < n_keys` predicate would have stayed silent on:
    # 6 values yielded from 2 resolvable keys still outnumbers the 4 stored keys
    assert len(pairs) == 6 > n_keys
    assert len({tuple(sorted(k.items())) for k, v in pairs}) == 2


def test_explicit_time_window_is_not_mistaken_for_drift(tmp_path):
    """A before/after filter legitimately hides entries — it must not trip the
    drift warning."""
    import warnings

    stash = HashStash(root_dir=str(tmp_path), engine="memory", b64=True)
    for i in range(3):
        stash[{"i": i}] = i

    with warnings.catch_warnings():
        warnings.simplefilter("error")
        assert list(stash.items(after=2**31)) == []
