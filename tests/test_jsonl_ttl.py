"""TTL correctness for the jsonl engine + flat-mode value error messages.

BUG B5: jsonl items()/values()/iteration used to leak TTL-expired entries even
though get()/`in` correctly hid them. items() folded ``before``/``after`` but
never folded in ``self.ttl`` the way the get-path (get_all -> _ttl_after) does.

Note on scope: the logical read view (items/values, and everything that routes
through items) must hide expired entries so it stays consistent with get()/`in`.
keys()/list()/__len__ are intentionally the *raw* storage view -- that is what
prune()/eviction/compact() use to physically reclaim expired rows, and it is
exactly how the memory and sqlite engines behave. These tests therefore assert
jsonl matches the memory engine on both the logical view (items empty) and the
raw view (keys still lists the key), rather than requiring keys()/list() to also
go empty (which would strand expired rows on disk forever).
"""
import tempfile
import time
from datetime import datetime

import pytest

from hashstash import HashStash


TTL = 0.5
EXPIRE_PAUSE = 0.8  # comfortably longer than TTL to avoid timing flakiness


def _jsonl(**kw):
    return HashStash(engine="jsonl", root_dir=tempfile.mkdtemp(), dbname="t", **kw)


def _memory(**kw):
    return HashStash(engine="memory", root_dir=tempfile.mkdtemp(), dbname="t", **kw)


# --------------------------------------------------------------------------- #
# BUG B5: expired entries never leak through the logical read view
# --------------------------------------------------------------------------- #

@pytest.mark.parametrize("flat", [True, False])
def test_expired_entry_hidden_from_items_and_consistent_with_get(flat):
    s = _jsonl(ttl=TTL, flat=flat)
    s.clear()
    s["http://u"] = {"data": 1}
    time.sleep(EXPIRE_PAUSE)

    # get-path already hid it (this was always correct)
    assert "http://u" not in s
    assert s.get("http://u", "EXP") == "EXP"

    # the logical read view must agree -- previously it leaked the expired entry
    assert list(s.items()) == []
    assert list(s.values()) == []
    assert s.items_l() == []
    assert dict(s.copy()) == {}


@pytest.mark.parametrize("flat", [True, False])
def test_nonexpired_entry_still_visible(flat):
    s = _jsonl(ttl=TTL, flat=flat)
    s.clear()
    s["http://u"] = {"data": 1}

    # read immediately -- well within the ttl window
    assert "http://u" in s
    assert s.get("http://u") == {"data": 1}
    assert list(s.items()) == [("http://u", {"data": 1})]
    assert list(s.values()) == [{"data": 1}]
    assert list(s.keys()) == ["http://u"]


@pytest.mark.parametrize("flat", [True, False])
def test_mixed_expired_and_fresh(flat):
    s = _jsonl(ttl=TTL, flat=flat)
    s.clear()
    s["old"] = {"data": "old"}
    time.sleep(EXPIRE_PAUSE)
    s["new"] = {"data": "new"}  # written after "old" expired

    # only the fresh entry shows in the logical view
    assert list(s.items()) == [("new", {"data": "new"})]
    assert "old" not in s
    assert "new" in s


def test_no_ttl_keeps_everything_visible():
    """Regression guard: with no ttl set, items() must not filter anything."""
    s = _jsonl()  # no ttl
    s.clear()
    s["a"] = {"data": 1}
    s["b"] = {"data": 2}
    time.sleep(EXPIRE_PAUSE)
    assert dict(s.items()) == {"a": {"data": 1}, "b": {"data": 2}}


# --------------------------------------------------------------------------- #
# keys()/list()/len are the raw maintenance view -- must match memory/sqlite
# --------------------------------------------------------------------------- #

def test_keys_are_raw_view_matching_memory_engine():
    """keys()/list()/__len__ intentionally still expose expired entries so
    prune()/eviction/compact can reclaim them -- exactly like memory/sqlite."""
    js = _jsonl(ttl=TTL)
    mem = _memory(ttl=TTL)
    for s in (js, mem):
        s.clear()
        s["http://u"] = {"data": 1}
    time.sleep(EXPIRE_PAUSE)

    # logical view: both hide the expired entry
    assert list(js.items()) == list(mem.items()) == []
    assert ("http://u" in js) == ("http://u" in mem) == False

    # raw view: both still list the key (so it can be physically reclaimed)
    assert list(js.keys()) == list(mem.keys()) == ["http://u"]
    assert list(js) == list(mem) == ["http://u"]
    assert len(js) == len(mem) == 1


def test_prune_can_still_see_expired_entries():
    """Because keys() stays raw, prune() can still see a ttl-expired row (if
    keys() filtered ttl, prune would match 0 and could never reclaim it). This
    matches memory/sqlite exactly.

    (The subsequent physical deletion has a separate, pre-existing base-engine
    quirk -- delete() -> has() is ttl-aware and raises KeyError for the expired
    key on every engine, memory/sqlite/jsonl alike -- so this test only asserts
    the reclaim *visibility* that the jsonl fix is responsible for preserving.)
    """
    from datetime import timedelta

    js = _jsonl(ttl=TTL)
    mem = _memory(ttl=TTL)
    for s in (js, mem):
        s.clear()
        s["k"] = {"data": 1}
    time.sleep(EXPIRE_PAUSE)
    js_matched = js.prune(older_than=timedelta(seconds=0), dry_run=True)
    mem_matched = mem.prune(older_than=timedelta(seconds=0), dry_run=True)
    assert js_matched == mem_matched == 1


# --------------------------------------------------------------------------- #
# Papercut: accurate flat-mode value error messages
# --------------------------------------------------------------------------- #

def test_flat_mode_datetime_value_gives_actionable_error():
    """Storing a non-JSON-native value in flat mode used to raise a bare
    'Object of type datetime is not JSON serializable'. The message now points
    at flat=False (b64/serializer changes cannot help here)."""
    s = _jsonl()  # flat=True by default
    s.clear()
    with pytest.raises(TypeError) as ei:
        s["k"] = {"ts": datetime.now()}
    msg = str(ei.value)
    assert "flat=False" in msg
    assert "JSON-native" in msg
    # nothing was written
    assert list(s.keys()) == []


def test_flat_mode_binary_serializer_message_points_at_flat_false():
    """In flat mode b64 is forced False, so the old 'Pass b64=True' advice was
    unactionable. The flat-mode message now recommends flat=False instead."""
    with pytest.raises(ValueError) as ei:
        HashStash(engine="jsonl", root_dir=tempfile.mkdtemp(), dbname="t",
                  serializer="pickle")  # flat=True default
    msg = str(ei.value)
    assert "flat=False" in msg
    assert "Pass b64=True" not in msg


def test_nonflat_binary_serializer_message_unchanged():
    """The non-flat path still legitimately advises b64=True."""
    with pytest.raises(ValueError) as ei:
        HashStash(engine="jsonl", root_dir=tempfile.mkdtemp(), dbname="t",
                  serializer="pickle", flat=False)
    assert "Pass b64=True" in str(ei.value)
