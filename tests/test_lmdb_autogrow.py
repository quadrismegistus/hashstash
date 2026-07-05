"""LMDB auto-grow on MapFullError.

LMDB has a fixed map_size and raises lmdb.MapFullError once the map fills. The
store used to wedge; now write transactions double self.map_size, call
env.set_mapsize on the shared environment, and retry — so writes keep succeeding
past the initial map_size.
"""
import pytest

pytest.importorskip("lmdb")

from hashstash import HashStash


def test_lmdb_autogrows_on_mapfull(tmp_path):
    # 64 KB is tiny for LMDB — a few hundred KB of values forces several grows.
    initial_map_size = 1024 * 64
    stash = HashStash(
        engine="lmdb",
        root_dir=str(tmp_path / "m"),
        map_size=initial_map_size,
    )
    stash.clear()
    assert stash.map_size == initial_map_size

    n = 300
    # ~1.5 KB per value -> ~450 KB of data, well past a 64 KB map.
    expected = {f"key-{i}": f"{i}:" + ("x" * 1500) for i in range(n)}

    for k, v in expected.items():
        stash[k] = v  # must not raise MapFullError; grows the map instead

    # The map must have grown past where it started.
    assert stash.map_size > initial_map_size

    # Every write is durable and reads back byte-for-byte.
    for k, v in expected.items():
        assert stash[k] == v
    assert len(stash) == n

    stash.close()


def test_lmdb_grown_map_persists_for_new_instance(tmp_path):
    """After a grow, a fresh stash on the same path keeps reading the data and
    can keep writing — the grown env is shared per path (issue #9)."""
    root = str(tmp_path / "m2")
    initial_map_size = 1024 * 64
    a = HashStash(engine="lmdb", root_dir=root, map_size=initial_map_size)
    a.clear()

    payload = "y" * 1500
    for i in range(300):
        a[f"k{i}"] = payload
    assert a.map_size > initial_map_size

    # Second instance on the same path shares the grown environment.
    b = HashStash(engine="lmdb", root_dir=root, map_size=initial_map_size)
    assert b["k0"] == payload
    assert b["k299"] == payload
    b["extra"] = payload  # still writable through the shared, grown env
    assert a["extra"] == payload
    a.close()


def test_lmdb_grow_respects_cap(tmp_path):
    """Growth stops at max_map_size and re-raises rather than looping forever."""
    import lmdb

    stash = HashStash(engine="lmdb", root_dir=str(tmp_path / "m3"), map_size=1024 * 64)
    stash.clear()
    stash.max_map_size = stash.map_size  # already at the cap -> no room to grow
    with pytest.raises(lmdb.MapFullError):
        stash._grow_map()
    stash.close()


def test_lmdb_max_map_size_is_configurable(tmp_path):
    """max_map_size can be set at construction and survives to_dict serialization
    (so the cap the user chose is honored by rebuilt/pickled stashes)."""
    stash = HashStash(
        engine="lmdb",
        root_dir=str(tmp_path / "m4"),
        map_size=1024 * 64,
        max_map_size=1024 * 64 * 4,   # a 4x-of-initial custom ceiling
    )
    assert stash.max_map_size == 1024 * 64 * 4
    rebuilt = HashStash(**stash.to_dict())
    assert rebuilt.max_map_size == 1024 * 64 * 4

    # a default stash gets the 256 GB ceiling from constants
    from hashstash.constants import DEFAULT_LMDB_MAX_MAP_SIZE
    assert HashStash(engine="lmdb", root_dir=str(tmp_path / "m5")).max_map_size == DEFAULT_LMDB_MAX_MAP_SIZE
