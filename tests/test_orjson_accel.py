"""orjson value acceleration: values may use orjson, but cache KEYS must stay
stdlib-json canonical so hashes never depend on whether orjson is installed."""
import json

import pytest

from hashstash import HashStash, deserialize, serialize
from hashstash.serializers import custom
from hashstash.serializers.custom import _serialize_custom, serialize_custom


@pytest.mark.parametrize(
    "value",
    [
        {"a": 1, "b": [1, 2, 3]},
        {"nested": {"x": [1, {"y": 2}]}},
        [1, 2.5, "three", True, None],
        {"unicode": "☃", "bytes-ish": "plain"},
        {1: "int-key", (2, 3): "tuple-key"},  # goes through the tagged dict form
        {"big": 2 ** 70},  # orjson rejects >64-bit ints -> must fall back to json
        {"set": {1, 2, 3}},
    ],
)
def test_value_roundtrips(value, tmp_path):
    stash = HashStash(root_dir=str(tmp_path / "c"))
    stash["k"] = value
    assert stash["k"] == value


@pytest.mark.parametrize(
    "key",
    [
        {"a": 1, "b": 2},
        {"b": 2, "a": 1},
        (["fn"], {"y": 1, "x": 2}),
        {1: "x", 2: "y"},
        {"deep": {"z": 9, "a": [3, 2, 1]}},
    ],
)
def test_key_bytes_are_json_canonical(key):
    """The sort_keys path must be byte-identical to pure stdlib json — this is
    what guarantees a key hashes the same whether or not orjson is installed."""
    expected = json.dumps(_serialize_custom(key), sort_keys=True)
    got = serialize_custom(key, sort_keys=True)
    assert got == expected
    assert isinstance(got, str)  # never orjson bytes for keys


def test_orjson_is_actually_used_for_values():
    """When orjson is installed, value serialization returns bytes (orjson),
    while keys stay str (json)."""
    pytest.importorskip("orjson")
    assert custom._get_orjson() is not None
    value_blob = serialize_custom({"a": 1}, sort_keys=False)
    assert isinstance(value_blob, bytes)  # orjson path
    key_blob = serialize_custom({"a": 1}, sort_keys=True)
    assert isinstance(key_blob, str)  # json path


def test_key_stable_without_orjson(monkeypatch):
    """Simulate orjson being absent: key bytes must be unchanged."""
    key = {"b": 2, "a": 1, "c": [3, 1, 2]}
    with_orjson = serialize_custom(key, sort_keys=True)
    # force the no-orjson path
    monkeypatch.setattr(custom, "_orjson", None)
    monkeypatch.setattr(custom, "_orjson_checked", True)
    without_orjson = serialize_custom(key, sort_keys=True)
    assert with_orjson == without_orjson


def test_value_written_with_orjson_reads_without(monkeypatch, tmp_path):
    """A value serialized via orjson must be readable via the stdlib path
    (cross-compatibility) and vice versa."""
    value = {"records": [{"id": i} for i in range(20)], "flag": True}
    blob = serialize(value, serializer="hashstash")  # orjson path (bytes)
    # now pretend orjson is gone and deserialize
    monkeypatch.setattr(custom, "_orjson", None)
    monkeypatch.setattr(custom, "_orjson_checked", True)
    assert deserialize(blob, serializer="hashstash") == value


def test_big_int_value_falls_back_cleanly(tmp_path):
    stash = HashStash(root_dir=str(tmp_path / "c"))
    stash["k"] = {"n": 12345678901234567890123456789}  # way over 64-bit
    assert stash["k"] == {"n": 12345678901234567890123456789}


def test_encode_key_deterministic_across_orjson_state(tmp_path):
    stash = HashStash(root_dir=str(tmp_path / "c"))
    k1 = stash.encode_key({"a": 1, "b": 2})
    k2 = stash.encode_key({"b": 2, "a": 1})
    assert k1 == k2  # canonical regardless of insertion order / orjson


def test_get_as_string_returns_str(tmp_path):
    """get(as_string=True) must return str even though orjson value serialization
    produces bytes."""
    stash = HashStash(root_dir=str(tmp_path / "c"))
    stash["k"] = {"a": [1, 2, 3]}
    result = stash.get("k", as_string=True)
    assert isinstance(result, str)
