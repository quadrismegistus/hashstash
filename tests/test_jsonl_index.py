"""jsonl key->offset index: correct across overwrite/append/delete/compact and
concurrent-writer-style appends, and reads seek instead of scanning the log."""
import tempfile

import pytest

from hashstash import HashStash


def _stash(**kw):
    return HashStash(engine="jsonl", root_dir=tempfile.mkdtemp(), **kw)


def test_overwrite_reads_latest():
    s = _stash(flat=False)
    s.clear()
    s["k"] = {"v": 1}
    s["k"] = {"v": 2}
    s["k"] = {"v": 3}
    assert s["k"] == {"v": 3}


def test_delete_then_readd():
    s = _stash(flat=False)
    s.clear()
    s["a"] = 1
    del s["a"]
    assert s.get("a") is None
    assert "a" not in s
    s["a"] = 2
    assert s["a"] == 2


def test_append_mode_keeps_versions_via_offsets():
    s = _stash(flat=False, append_mode=True)
    s.clear()
    s["k"] = 1
    s["k"] = 2
    s["k"] = 3
    assert s.get_all("k") == [1, 2, 3]
    del s["k"]
    assert s.get_all("k") is None          # delete resets version history
    s["k"] = 9
    assert s.get_all("k") == [9]


def test_unicode_offsets_are_byte_exact():
    # multibyte values stress byte-offset correctness (text-mode tell would be
    # an opaque cookie; the index uses true binary offsets)
    s = _stash(flat=False)
    s.clear()
    for i in range(30):
        s[f"k{i}"] = {"text": "héllo☃" * (i + 1), "i": i}
    for i in range(30):
        assert s[f"k{i}"] == {"text": "héllo☃" * (i + 1), "i": i}


def test_keys_reflect_pending_writes():
    # _keys must fold in rows written since the last scan (the index is no longer
    # updated at write time)
    s = _stash(flat=False)
    s.clear()
    s["key1"] = 1
    s["key2"] = 2
    assert set(s.keys()) == {"key1", "key2"}


def test_compact_rebuilds_offsets():
    s = _stash(flat=False)
    s.clear()
    for i in range(40):
        s[f"k{i}"] = i
    for i in range(40):
        s[f"k{i}"] = i * 100        # overwrite -> dead rows
    s.compact()
    for i in range(40):
        assert s[f"k{i}"] == i * 100   # reads use the post-compact offsets


def test_second_reader_sees_first_writers_rows():
    # a fresh stash on the same file must fold in rows it never wrote (its scan
    # is the source of truth, offsets are byte-exact across processes/handles)
    d = tempfile.mkdtemp()
    w = HashStash(engine="jsonl", root_dir=d, flat=False)
    w.clear()
    w["x"] = {"n": 1}
    w["y"] = {"n": 2}
    r = HashStash(engine="jsonl", root_dir=d, flat=False)
    assert r["x"] == {"n": 1}
    assert r["y"] == {"n": 2}
    assert set(r.keys()) == {"x", "y"}


def test_get_does_not_scale_with_log_size():
    """Random get should be ~O(1): reading one key from a 500-key log must not be
    dramatically slower than from a 50-key log (the old scan was O(file))."""
    import time

    def median_get_ms(n_keys):
        s = _stash(flat=False)
        s.clear()
        payload = {"blob": "x" * 2000}
        for i in range(n_keys):
            s[f"k{i}"] = payload
        # time reads of the FIRST key (worst case for a top-of-file scan)
        t = time.perf_counter()
        for _ in range(50):
            _ = s["k0"]
        return (time.perf_counter() - t) / 50 * 1000

    small = median_get_ms(50)
    large = median_get_ms(500)
    # O(n) scan would make the 10x-larger log ~10x slower; allow generous slack
    assert large < small * 4 + 1.0, f"get scales with log size: {small:.3f} -> {large:.3f} ms"
