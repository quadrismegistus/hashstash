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


@pytest.mark.parametrize("engine", ["shelve", "sqlite", "jsonl", "duckdb"])
def test_clear_removes_real_sidecar_files(engine, tmp_path):
    """Engines that write sidecars next to the data file (shelve's dbm suffixes,
    sqlite/duckdb's -wal, the .lock) must be cleared too, not just `filename`.
    Uses whatever the engine really writes rather than a fabricated name."""
    pytest.importorskip(engine) if engine in ("duckdb",) else None
    stash = HashStash(engine=engine, root_dir=str(tmp_path), dbname="side")
    for i in range(5):
        stash[f"k{i}"] = i
    param_dir = stash.path_dirname
    before = set(os.listdir(param_dir))
    assert any(e != stash.filename for e in before), f"no sidecars written: {before}"
    stash.clear()
    left = set(os.listdir(param_dir)) if os.path.exists(param_dir) else set()
    assert left == set(), f"clear() left {left}"


def test_clear_spares_user_sub_from_the_creating_handle(tmp_path):
    """The handle that created a sub-stash must not destroy it either. Cascading
    to every child made clear() depend on which handle called it: same folder,
    same call, opposite outcome."""
    parent = HashStash(engine="pairtree", root_dir=str(tmp_path), dbname="ann")
    ledger = parent.sub(dbname="batch_ledger", append_mode=True)
    ledger["cid1"] = {"batch_id": "b1", "status": "closed"}
    parent.clear()
    assert ledger.get("cid1") == {"batch_id": "b1", "status": "closed"}


@pytest.mark.parametrize("same_handle", [True, False])
def test_clear_always_clears_stashed_result(tmp_path, same_handle):
    """Memoized results are the stash's OWN data under a namespace it owns, so
    they must clear from any handle — a CLI run clearing a cache is the normal
    case, and it used to leave stale memoized values behind."""
    root = str(tmp_path)
    parent = HashStash(engine="pairtree", root_dir=root, dbname="fn")

    @parent.stashed_result
    def slow(x):
        return x * 2

    slow(3)
    assert list(slow.stash.items())
    clearer = parent if same_handle else HashStash(
        engine="pairtree", root_dir=root, dbname="fn"
    )
    clearer.clear()
    assert list(slow.stash.items()) == []


@pytest.mark.parametrize("engine", ["pairtree", "jsonl", "memory", "sqlite"])
def test_clear_cascades_to_function_stashes_not_user_subs(engine):
    """The split: a function-result stash is this stash's own memoized data and
    clears with it; a sub() the caller made is a separate store and survives.
    Covers memory too, which has no directory to sweep — there the in-process
    cascade is the only mechanism, and its clear() override skipped it."""
    import tempfile

    parent = HashStash(engine=engine, root_dir=tempfile.mkdtemp(), dbname="p")

    @parent.stashed_result
    def slow(x):
        return x * 2

    slow(3)
    user_sub = parent.sub(dbname="child")
    user_sub["k"] = "v"

    parent.clear()

    assert list(slow.stash.items()) == [], "function results survived clear()"
    assert user_sub.get("k") == "v", "clear() destroyed a caller's sub-stash"


def test_fsspec_parent_clear_spares_substash(tmp_path):
    """The fsspec engine overrides clear() and had the same bug as the base:
    rmtree of path_dirname takes every sub-stash nested inside it."""
    pytest.importorskip("fsspec")
    root = str(tmp_path)
    parent = HashStash(engine="fsspec", root_dir=root, dbname="ledger")
    parent["a"] = 1
    sub = parent.sub(dbname="batch2026", append_mode=True)
    sub["row1"] = {"amount": 100}

    HashStash(engine="fsspec", root_dir=root, dbname="ledger").clear()

    reopened = HashStash(engine="fsspec", root_dir=root, dbname="ledger").sub(
        dbname="batch2026", append_mode=True
    )
    assert reopened.get("row1") == {"amount": 100}


def test_fsspec_clear_still_clears_own_data(tmp_path):
    pytest.importorskip("fsspec")
    stash = HashStash(engine="fsspec", root_dir=str(tmp_path), dbname="own")
    stash["a"] = 1
    stash.sub(dbname="child")["k"] = "v"
    stash.clear()
    assert len(stash) == 0
    assert stash.get("a") is None


def test_sub_dbname_colliding_with_storage_file_is_refused(tmp_path):
    """A child nests under the parent's param folder, so dbname='data.db' puts
    it inside the parent's own storage. The parent's walk then finds the child's
    entries and silently merges the keyspaces: len() and keys() report both."""
    parent = HashStash(engine="pairtree", root_dir=str(tmp_path), dbname="p")
    parent["a"] = 1
    parent["b"] = 2
    with pytest.raises(ValueError, match="collides"):
        parent.sub(dbname=parent.filename)
    assert len(parent) == 2
    assert sorted(map(str, parent.keys())) == ["a", "b"]


def test_sub_dbname_collision_refused_on_nested_path(tmp_path):
    parent = HashStash(engine="jsonl", root_dir=str(tmp_path), dbname="p")
    with pytest.raises(ValueError, match="collides"):
        parent.sub(dbname=f"{parent.filename}/deeper")


def test_sub_dbname_collision_allowed_with_explicit_root_dir(tmp_path):
    """With an explicit root_dir the child is not nested, so the name is fine."""
    parent = HashStash(engine="pairtree", root_dir=str(tmp_path), dbname="p")
    sub = parent.sub(root_dir=str(tmp_path / "elsewhere"), dbname=parent.filename)
    sub["k"] = "v"
    assert sub.get("k") == "v"
    assert len(parent) == 0


# --- file-style root_dir: clear() must actually clear -----------------------


@pytest.mark.parametrize("engine", ["shelve", "sqlite", "pairtree"])
def test_file_style_root_dir_clear_actually_clears(engine, tmp_path):
    """With root_dir naming a file, clear() removed only the literal path. For
    shelve that is a file dbm never creates, so clear() silently removed nothing
    and reported success while a fresh handle still read every entry."""
    target = str(tmp_path / "mydata.db")
    stash = HashStash(engine=engine, root_dir=target)
    stash["a"] = 1
    stash["b"] = 2
    assert stash._owns_dir is False
    stash.clear()
    fresh = HashStash(engine=engine, root_dir=target)
    assert len(fresh) == 0
    assert fresh.get("a") is None


def test_file_style_root_dir_clear_spares_unrelated_files(tmp_path):
    """The reason the sweep here is a closed suffix list and not a prefix glob:
    path_dirname is the user's own directory, shared with unrelated files."""
    target = str(tmp_path / "mydata.db")
    keep = tmp_path / "mydata.db.backup"
    keep.write_text("precious")
    other = tmp_path / "notes.txt"
    other.write_text("also precious")

    stash = HashStash(engine="shelve", root_dir=target)
    stash["a"] = 1
    stash.clear()

    assert keep.read_text() == "precious"
    assert other.read_text() == "also precious"


# --- 2. sub(engine=...) must actually switch engine -------------------------


def test_sub_honors_engine_override(tmp_path):
    parent = HashStash(engine="pairtree", root_dir=str(tmp_path), dbname="p")
    sub = parent.sub(dbname="ledger", engine="jsonl", append_mode=True)
    assert isinstance(sub, JSONLHashStash)
    assert sub.engine == "jsonl"
    sub["k"] = {"v": 1}
    assert sub.get("k") == {"v": 1}
    # the child really is stored in the child engine's format
    assert sub.path.endswith("data.jsonl")
    assert json.loads(open(sub.path).readline())["__key__"] == "k"


def test_sub_without_engine_keeps_parent_class(tmp_path):
    parent = HashStash(engine="pairtree", root_dir=str(tmp_path), dbname="p")
    sub = parent.sub(dbname="child")
    assert type(sub) is type(parent)


def test_sub_to_networked_engine_keeps_safe_default(tmp_path):
    """safe is engine-derived, not user config. Networked engines default to
    safe=True because their writer may be untrusted; inheriting the parent's
    materialized False would silently hand a shared-server child code-capable
    deserialization of other writers' payloads."""
    direct = HashStash(engine="mongo", root_dir=str(tmp_path), dbname="d")
    sub = HashStash(engine="pairtree", root_dir=str(tmp_path), dbname="p").sub(
        dbname="c", engine="mongo"
    )
    assert sub.safe == direct.safe is True


def test_sub_honors_explicit_safe_override(tmp_path):
    sub = HashStash(engine="pairtree", root_dir=str(tmp_path), dbname="p").sub(
        dbname="c", engine="mongo", safe=False
    )
    assert sub.safe is False


def test_sub_across_engines_warns_nothing(tmp_path):
    """The caller typed no stray argument: inherited engine-specific params
    (redis/mongo host+port) must not trigger the typo warning."""
    import warnings

    parent = HashStash(engine="redis", root_dir=str(tmp_path), dbname="p")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        parent.sub(dbname="c", engine="jsonl")
    assert [str(w.message) for w in caught] == []


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


@pytest.mark.parametrize(
    "bad_line",
    [
        pytest.param('{"__value__": "x"}', id="object-without-key"),
        pytest.param("123", id="bare-int"),
        pytest.param("null", id="bare-null"),
        pytest.param('"abc"', id="bare-string"),
        pytest.param("[1, 2, 3]", id="bare-list"),
    ],
)
def test_valid_json_that_is_not_a_row_does_not_brick_the_stash(tmp_path, bad_line):
    """A line can parse as JSON and still not be a row. Every consumer indexed
    row['__key__'] directly, so one such line raised out of get/len/keys/items/
    values/set AND out of compact() — the repair path — leaving the stash
    unreadable, unwritable and unrepairable. Tolerating a torn line is worth
    nothing if the callers don't survive it."""
    stash = HashStash(
        engine="jsonl", root_dir=str(tmp_path), dbname="shape", append_mode=True
    )
    stash["A"] = {"v": 1}
    with open(stash.path, "a") as f:
        f.write(bad_line + "\n")

    reopened = HashStash(
        engine="jsonl", root_dir=str(tmp_path), dbname="shape", append_mode=True
    )
    assert reopened.get("A") == {"v": 1}
    assert len(reopened) == 1
    assert list(reopened.keys()) == ["A"]
    assert reopened.items_l() == [("A", {"v": 1})]
    assert len(list(reopened.values())) == 1
    reopened["D"] = {"v": 4}          # still writable
    reopened.compact()                # still repairable
    assert reopened.get("D") == {"v": 4}
    assert reopened.get("A") == {"v": 1}


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


# --- 5. the reader must be the exact inverse of the writer ------------------
#
# Rows are written with json.dumps. A stricter parser (orjson) rejects or
# silently alters values json.dumps happily emits, and compact() rebuilds the
# file from iter_jsonl — so anything the reader can't parse is not just hidden
# on read, it is permanently deleted or rewritten by the maintenance call.


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(float("inf"), id="inf"),
        pytest.param(float("-inf"), id="-inf"),
        pytest.param(2 ** 70, id="int>2**64"),
        pytest.param(-(2 ** 70), id="-int>2**64"),
        pytest.param(2 ** 64 - 1, id="max-uint64"),
        pytest.param("\ud800", id="lone-surrogate"),
    ],
)
def test_values_survive_read_and_compact(tmp_path, value):
    stash = HashStash(
        engine="jsonl", root_dir=str(tmp_path), dbname="inv", append_mode=True
    )
    stash["k"] = {"v": value}
    assert stash.get("k") == {"v": value}
    assert stash.items_l() == [("k", {"v": value})]
    stash.compact()
    assert stash.get("k") == {"v": value}, "compact() destroyed or altered the row"
    assert len(stash) == 1


def test_nan_survives_read_and_compact(tmp_path):
    """NaN needs its own test: NaN != NaN, so == comparison can't be used."""
    import math

    stash = HashStash(
        engine="jsonl", root_dir=str(tmp_path), dbname="nan", append_mode=True
    )
    stash["k"] = {"v": float("nan")}
    assert math.isnan(stash.get("k")["v"])
    assert len(stash.items_l()) == 1
    stash.compact()
    assert math.isnan(stash.get("k")["v"]), "compact() destroyed the NaN row"


def test_big_int_not_downcast_to_float(tmp_path):
    """orjson decodes >2**64-1 to a float WITHOUT raising, so a fallback parser
    can't catch it: the value is silently wrong and compact() makes it stick."""
    stash = HashStash(
        engine="jsonl", root_dir=str(tmp_path), dbname="bigint", append_mode=True
    )
    stash["k"] = {"v": 2 ** 70}
    stash.compact()
    got = stash.get("k")["v"]
    assert isinstance(got, int), f"exact int became {type(got).__name__}"
    assert got == 2 ** 70


def test_deeply_nested_value_survives_compact(tmp_path):
    """orjson caps nesting at 1024; json.dumps writes deeper without complaint.

    Depth must exceed 1024 for the guard to mean anything, but Python < 3.12
    cannot json-encode that deep under the default recursion limit — the value
    cannot be written there at all, so there is no round-trip to protect. Probe
    the interpreter rather than hard-coding a version check.
    """
    obj = cur = {}
    for _ in range(1200):
        cur["x"] = {}
        cur = cur["x"]
    stash = HashStash(
        engine="jsonl", root_dir=str(tmp_path), dbname="deep", append_mode=True
    )
    # Gate on the real write, not a separate json.dumps probe: the probe can
    # differ from the engine's own call by a frame or two and skip when the
    # write would have worked (or vice versa). If the value cannot be stored on
    # this interpreter, there is no round-trip to protect.
    try:
        stash["k"] = {"v": obj}
    except RecursionError:
        pytest.skip("interpreter cannot json-encode depth 1200 at the default limit")
    assert len(stash.items_l()) == 1
    stash.compact()
    assert stash.get("k") is not None, "compact() destroyed the deeply nested row"


def test_len_agrees_with_items_on_every_written_value(tmp_path):
    """The index scan and iter_jsonl must use the same parser: if they diverge,
    len() and items() disagree about which rows exist on the same file."""
    stash = HashStash(
        engine="jsonl", root_dir=str(tmp_path), dbname="agree", append_mode=True
    )
    for i, value in enumerate(
        [float("nan"), float("inf"), 2 ** 70, "\ud800", 1.5, "plain"]
    ):
        stash[f"k{i}"] = {"v": value}
    assert len(stash) == len(stash.items_l()) == 6


def test_corrupt_file_does_not_warn_per_line(tmp_path, caplog):
    """One warning plus a total, not one per line: a badly corrupted file used
    to emit a warning for every row (~19MB of identical text on 100k lines)."""
    stash = HashStash(
        engine="jsonl", root_dir=str(tmp_path), dbname="spam", append_mode=True
    )
    stash["A"] = {"v": 1}
    with open(stash.path, "a") as f:
        for _ in range(500):
            f.write("{not json\n")
    with caplog.at_level("WARNING"):
        rows = list(iter_jsonl(stash.path))
    assert len(rows) == 1
    warnings = [r for r in caplog.records if "unparseable" in r.getMessage()]
    assert len(warnings) <= 2, f"{len(warnings)} warnings for 500 bad lines"


def test_iter_jsonl_tolerates_torn_line_with_orjsonl_installed(tmp_path):
    """iter_jsonl must not depend on whether orjsonl happens to be importable:
    the old fast path raised on the first bad row, so the same file was readable
    or fatal depending on the environment."""
    pytest.importorskip("orjsonl")
    stash = _torn(tmp_path, "t6")
    assert len(list(iter_jsonl(stash.path))) == 2
    assert len(stash.items_l()) == 2
    stash.compact()
