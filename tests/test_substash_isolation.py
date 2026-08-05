"""Sub-stash isolation and JSONL torn-line tolerance.

Guards four defects found while auditing hashstash for use as a money-critical
batch ledger (a sidecar stash recording which provider batches were submitted):

1. clear() on a parent destroyed sub-stashes nested in its param folder.
2. sub(engine=...) silently ignored the engine and returned the parent's class.
3. iter_jsonl() delegated to orjsonl when importable, which raises on the first
   malformed row — breaking compact(), the call that repairs the file.
4. A torn final line swallowed the NEXT appended row as well as its own.
"""
import json
import os

import pytest

from hashstash import HashStash
from hashstash.engines.jsonl import JSONLHashStash
from hashstash.utils.misc import iter_jsonl


# --- 1. clear() must not destroy sub-stashes --------------------------------


def test_parent_clear_spares_substash(tmp_path):
    """A sub-stash is a separate store that merely lives nearby: clearing the
    parent must not delete it, even from a handle that never created it."""
    parent = HashStash(engine="pairtree", root_dir=str(tmp_path), dbname="annotations")
    parent["a"] = 1
    ledger = parent.sub(dbname="batch_ledger", append_mode=True)
    ledger["cid1"] = {"batch_id": "b1", "status": "closed"}

    # a different handle (another process, no children registry) clears the cache
    HashStash(engine="pairtree", root_dir=str(tmp_path), dbname="annotations").clear()

    reopened = HashStash(
        engine="pairtree", root_dir=str(tmp_path), dbname="annotations"
    ).sub(dbname="batch_ledger", append_mode=True)
    assert reopened.get("cid1") == {"batch_id": "b1", "status": "closed"}


def test_parent_clear_still_clears_own_data(tmp_path):
    parent = HashStash(engine="pairtree", root_dir=str(tmp_path), dbname="annotations")
    parent["a"] = 1
    parent.sub(dbname="ledger")["cid1"] = "x"
    parent.clear()
    assert len(parent) == 0
    assert parent.get("a") is None


def test_parent_clear_removes_param_dir_when_no_substash(tmp_path):
    """With nothing else in it, clear() still takes the param folder away."""
    stash = HashStash(engine="pairtree", root_dir=str(tmp_path), dbname="solo")
    stash["a"] = 1
    stash.clear()
    assert not os.path.exists(stash.path_dirname)


def test_clear_removes_sidecar_files(tmp_path):
    """Engines that write sidecars next to the data file (shelve's .dat/.dir/
    .bak, sqlite's -wal) must be cleared too, not just `filename` itself."""
    stash = HashStash(engine="pairtree", root_dir=str(tmp_path), dbname="side")
    stash["a"] = 1
    sidecar = os.path.join(stash.path_dirname, stash.filename + ".dat")
    with open(sidecar, "w") as f:
        f.write("x")
    stash.clear()
    assert not os.path.exists(sidecar)


def test_clear_cascades_to_registered_children(tmp_path):
    """Function-result stashes are registered children and still get cleared."""
    parent = HashStash(engine="pairtree", root_dir=str(tmp_path), dbname="p")
    child = parent.sub(dbname="child")
    child["k"] = "v"
    parent.clear()
    assert child.get("k") is None


# --- 2. sub(engine=...) must actually switch engine -------------------------


def test_sub_honors_engine_override(tmp_path):
    parent = HashStash(engine="pairtree", root_dir=str(tmp_path), dbname="p")
    sub = parent.sub(dbname="ledger", engine="jsonl", append_mode=True)
    assert isinstance(sub, JSONLHashStash)
    assert sub.engine == "jsonl"
    assert sub.filename == "data.jsonl"  # not the parent's data.db
    sub["k"] = {"v": 1}
    assert sub.get("k") == {"v": 1}


def test_sub_without_engine_keeps_parent_class(tmp_path):
    parent = HashStash(engine="pairtree", root_dir=str(tmp_path), dbname="p")
    sub = parent.sub(dbname="child")
    assert type(sub) is type(parent)


# --- 3 & 4. torn final line -------------------------------------------------


def _torn(tmp_path, dbname):
    stash = HashStash(
        engine="jsonl", root_dir=str(tmp_path), dbname=dbname, append_mode=True
    )
    stash["A"] = {"v": 1}
    stash["B"] = {"v": 2}
    with open(stash.path, "a") as f:
        f.write('{"__key__": "C", "sta')  # killed mid-write: no trailing newline
    return HashStash(
        engine="jsonl", root_dir=str(tmp_path), dbname=dbname, append_mode=True
    )


def test_torn_line_does_not_break_other_keys(tmp_path):
    stash = _torn(tmp_path, "t1")
    assert stash.get("A") == {"v": 1}
    assert stash.get("B") == {"v": 2}
    assert len(stash) == 2


def test_torn_line_does_not_swallow_next_append(tmp_path):
    """The torn row loses itself and nothing else: the next write must land."""
    stash = _torn(tmp_path, "t2")
    stash["D"] = {"v": 4}
    reopened = HashStash(
        engine="jsonl", root_dir=str(tmp_path), dbname="t2", append_mode=True
    )
    assert reopened.get("D") == {"v": 4}
    assert reopened.get("A") == {"v": 1}
    assert len(reopened) == 3


def test_iter_jsonl_skips_torn_line(tmp_path):
    stash = _torn(tmp_path, "t3")
    rows = list(iter_jsonl(stash.path))
    assert len(rows) == 2
    assert {r["__key__"] for r in rows} == {"A", "B"}


def test_items_and_values_survive_torn_line(tmp_path):
    stash = _torn(tmp_path, "t4")
    assert len(stash.items_l()) == 2
    assert len(list(stash.values())) == 2


def test_compact_repairs_torn_line(tmp_path):
    """compact() is the repair path; it must not be the thing that breaks."""
    stash = _torn(tmp_path, "t5")
    stash.compact()
    with open(stash.path) as f:
        raw = f.read()
    assert raw.endswith("\n")
    for line in raw.splitlines():
        json.loads(line)  # every surviving row parses
    stash["D"] = {"v": 4}
    assert stash.get("D") == {"v": 4}
    assert stash.get("A") == {"v": 1}


def test_iter_jsonl_tolerates_torn_line_with_orjsonl_installed(tmp_path):
    """iter_jsonl must not depend on whether orjsonl happens to be importable:
    the old fast path raised on the first bad row, so the same file was readable
    or fatal depending on the environment."""
    pytest.importorskip("orjsonl")
    stash = _torn(tmp_path, "t6")
    assert len(list(iter_jsonl(stash.path))) == 2
    assert len(stash.items_l()) == 2
    stash.compact()
