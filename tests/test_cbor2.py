"""The cbor2 serializer (data-only, fast, compact — mirrors msgpack)."""
import datetime

import pytest

from hashstash import HashStash, deserialize, serialize


cbor2 = pytest.importorskip("cbor2")


@pytest.mark.parametrize(
    "value",
    [
        {"a": 1, "b": [1, 2, 3]},
        [1, 2.5, "three", True, None],
        {"nested": {"deep": {"list": [1, 2, {"x": "y"}]}}},
        b"raw bytes",
        "unicode: ☃",
        {"ints": list(range(100))},
        datetime.datetime(2020, 1, 1, 12, 0, tzinfo=datetime.timezone.utc),
    ],
)
def test_cbor2_roundtrip(value):
    blob = serialize(value, serializer="cbor2")
    assert isinstance(blob, (bytes, str))
    assert deserialize(blob, serializer="cbor2") == value


def test_cbor2_datetime(tmp_path):
    stash = HashStash(root_dir=str(tmp_path / "c"), serializer="cbor2")
    dt = datetime.datetime(2020, 1, 1, 12, 0, tzinfo=datetime.timezone.utc)
    stash["when"] = dt
    assert stash["when"] == dt


def test_cbor2_stash_end_to_end(tmp_path):
    stash = HashStash(root_dir=str(tmp_path / "c"), serializer="cbor2")
    assert stash.serializer == "cbor2"
    stash["data"] = {"user": "alice", "scores": [1, 2, 3]}
    assert stash["data"] == {"user": "alice", "scores": [1, 2, 3]}


def test_cbor2_is_a_working_serializer():
    from hashstash.config import get_working_serializers

    assert "cbor2" in get_working_serializers()


def test_cbor2_compact_vs_hashstash(tmp_path):
    """cbor2 should produce a smaller blob than the JSON-based custom serializer
    for plain data (it's a binary format)."""
    value = {"records": [{"id": i, "name": f"item{i}"} for i in range(200)]}
    cb = serialize(value, serializer="cbor2")
    hs = serialize(value, serializer="hashstash")
    cb_len = len(cb if isinstance(cb, bytes) else cb.encode())
    hs_len = len(hs if isinstance(hs, bytes) else hs.encode())
    assert cb_len < hs_len
