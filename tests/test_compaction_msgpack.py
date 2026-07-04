"""JSONL compaction and the msgpack serializer."""
import datetime
import os

import pytest

from hashstash import HashStash, deserialize, serialize


# --- JSONL compaction --------------------------------------------------------


def test_jsonl_compact_shrinks_file_overwrite(tmp_path):
    stash = HashStash(engine="jsonl", root_dir=str(tmp_path / "j"), append_mode=False)
    for i in range(50):
        stash["k"] = i  # 50 appended rows, 1 live
    size_before = os.path.getsize(stash.path)
    lines_before = sum(1 for _ in open(stash.path))
    assert lines_before == 50

    stash.compact()

    lines_after = sum(1 for _ in open(stash.path))
    assert lines_after == 1
    assert os.path.getsize(stash.path) < size_before
    assert stash["k"] == 49  # value preserved


def test_jsonl_compact_keeps_all_versions_append_mode(tmp_path):
    stash = HashStash(engine="jsonl", root_dir=str(tmp_path / "j"), append_mode=True)
    stash["k"] = 1
    stash["k"] = 2
    stash["k"] = 3
    stash.compact()
    assert stash.get_all("k") == [1, 2, 3]


def test_jsonl_compact_drops_deleted_keys(tmp_path):
    stash = HashStash(engine="jsonl", root_dir=str(tmp_path / "j"), append_mode=False)
    stash["keep"] = "v"
    stash["gone"] = "x"
    del stash["gone"]
    lines_before = sum(1 for _ in open(stash.path))
    assert lines_before == 3  # keep, gone, tombstone

    stash.compact()

    lines_after = sum(1 for _ in open(stash.path))
    assert lines_after == 1
    assert "keep" in stash
    assert "gone" not in stash
    assert stash["keep"] == "v"


def test_jsonl_compact_visible_to_fresh_instance(tmp_path):
    root = str(tmp_path / "j")
    a = HashStash(engine="jsonl", root_dir=root, append_mode=False)
    a["k"] = 1
    a["k"] = 2
    a.compact()
    b = HashStash(engine="jsonl", root_dir=root, append_mode=False)
    assert b["k"] == 2
    assert len(b) == 1


def test_jsonl_compact_noop_on_missing_file(tmp_path):
    stash = HashStash(engine="jsonl", root_dir=str(tmp_path / "j"))
    assert stash.compact() is stash  # no file yet, no error


# --- msgpack serializer ------------------------------------------------------


msgpack = pytest.importorskip("msgpack")


@pytest.mark.parametrize(
    "value",
    [
        {"a": 1, "b": [1, 2, 3]},
        [1, 2.5, "three", True, None],
        {"nested": {"deep": {"list": [1, 2, {"x": "y"}]}}},
        b"raw bytes",
        "unicode: ☃",
        {"ints": list(range(100))},
    ],
)
def test_msgpack_roundtrip(value):
    blob = serialize(value, serializer="msgpack")
    assert isinstance(blob, (bytes, str))
    assert deserialize(blob, serializer="msgpack") == value


def test_msgpack_datetime(tmp_path):
    stash = HashStash(root_dir=str(tmp_path / "c"), serializer="msgpack")
    dt = datetime.datetime(2020, 1, 1, 12, 0, tzinfo=datetime.timezone.utc)
    stash["when"] = dt
    assert stash["when"] == dt


def test_msgpack_stash_end_to_end(tmp_path):
    stash = HashStash(root_dir=str(tmp_path / "c"), serializer="msgpack")
    assert stash.serializer == "msgpack"
    stash["data"] = {"user": "alice", "scores": [1, 2, 3]}
    assert stash["data"] == {"user": "alice", "scores": [1, 2, 3]}


def test_msgpack_is_a_working_serializer():
    from hashstash.config import get_working_serializers

    assert "msgpack" in get_working_serializers()


def test_msgpack_compact_vs_hashstash(tmp_path):
    """msgpack should produce a smaller blob than the JSON-based custom serializer
    for plain data (it's a binary format)."""
    value = {"records": [{"id": i, "name": f"item{i}"} for i in range(200)]}
    mp = serialize(value, serializer="msgpack")
    hs = serialize(value, serializer="hashstash")
    mp_len = len(mp if isinstance(mp, bytes) else mp.encode())
    hs_len = len(hs if isinstance(hs, bytes) else hs.encode())
    assert mp_len < hs_len
