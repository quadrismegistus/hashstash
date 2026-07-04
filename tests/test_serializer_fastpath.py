"""Tier 3: JSON-native fast-path in serialize_custom. The fast-path must be
byte-identical to the full path and round-trip every type correctly, including
the non-finite-float fix it surfaced."""
import math
from collections import OrderedDict, namedtuple
from enum import IntEnum

import pytest

from hashstash import HashStash, deserialize, serialize
from hashstash.serializers.custom import (
    _dumps_value,
    _is_json_native,
    _serialize_custom,
    deserialize_custom,
    serialize_custom,
)


def _full_path(obj):
    """What serialize_custom would produce WITHOUT the fast-path."""
    return _dumps_value(_serialize_custom(obj))


def _nan_aware_eq(a, b):
    if isinstance(a, float) and isinstance(b, float):
        return a == b or (math.isnan(a) and math.isnan(b))
    if isinstance(a, list) and isinstance(b, list):
        return len(a) == len(b) and all(_nan_aware_eq(x, y) for x, y in zip(a, b))
    if isinstance(a, dict) and isinstance(b, dict):
        return a.keys() == b.keys() and all(_nan_aware_eq(a[k], b[k]) for k in a)
    return type(a) is type(b) and a == b


NATIVE = [
    None, True, False, 0, 42, -7, 3.14, -0.5, "", "str", "☃",
    [], {}, [1, 2, 3], {"a": 1, "b": [1, 2, {"c": None}]},
    {"deep": {"list": [1, "two", 3.0, False, None]}},
    {"big": 2 ** 70},                    # big int -> json fallback, still native-shaped
    [1.0, 2.5, {"x": [True, None]}],
]

# non-native values that DO round-trip to the exact same type
NONNATIVE_ROUNDTRIP = [
    (1, 2, 3),
    {1: "one", 2: "two"},
    {(1, 2): "tuple-key"},
    {"__py__": "x"},
    {"__pytype__": "y"},
    {"__data__": "z"},
    {"s": {1, 2, 3}},
    {"fs": frozenset([1, 2])},
    {"b": b"bytes"},
    {"t": (1, 2)},
    {"mix": [1, (2, 3)]},
]

# non-native AND pre-existingly type-coerced by the serializer (OrderedDict is
# an isinstance-dict -> plain dict; IntEnum is an isinstance-int -> int). Tier 3
# only needs to confirm these don't take the fast-path.
NONNATIVE_COERCED = [
    OrderedDict(a=1),
    IntEnum("Color", "RED GREEN")(1),
    namedtuple("P", "x y")(1, 2),
]

NONNATIVE = NONNATIVE_ROUNDTRIP + NONNATIVE_COERCED


@pytest.mark.parametrize("value", NATIVE)
def test_native_is_detected(value):
    assert _is_json_native(value) is True


@pytest.mark.parametrize("value", NONNATIVE)
def test_nonnative_is_rejected(value):
    assert _is_json_native(value) is False


@pytest.mark.parametrize("value", NATIVE)
def test_fastpath_byte_identical_to_full_path(value):
    """The whole point: the fast-path must produce exactly what the full path
    would, so deserialize (which is unchanged) reconstructs the same value."""
    assert serialize_custom(value) == _full_path(value)


@pytest.mark.parametrize("value", NATIVE + NONNATIVE_ROUNDTRIP)
def test_roundtrip_preserves_value_and_types(value):
    out = deserialize_custom(serialize_custom(value))
    assert _nan_aware_eq(out, value), f"{out!r} != {value!r}"
    assert type(out) is type(value)


# --- non-finite floats (the bug the fast-path surfaced) ----------------------


@pytest.mark.parametrize("value", [float("inf"), float("-inf"), float("nan")])
def test_non_finite_floats_roundtrip(value):
    out = deserialize(serialize(value, serializer="hashstash"), serializer="hashstash")
    if math.isnan(value):
        assert math.isnan(out)
    else:
        assert out == value


def test_non_finite_float_fixed_on_full_path_too(tmp_path):
    """The inf->null corruption (orjson emits null for inf/nan) is fixed at the
    serializer root, not just in the fast-path — nested non-finite floats that
    ride the full path (inside a non-native container) also round-trip."""
    value = {"data": (float("inf"), 1)}  # tuple -> full path
    assert not _is_json_native(value)
    out = deserialize(serialize(value, serializer="hashstash"), serializer="hashstash")
    assert out["data"][0] == float("inf")


def test_non_finite_in_stash(tmp_path):
    stash = HashStash(root_dir=str(tmp_path / "c"))
    stash["k"] = {"score": float("inf"), "vals": [1.0, float("-inf")]}
    got = stash["k"]
    assert got["score"] == float("inf")
    assert got["vals"][1] == float("-inf")


# --- keys still use the full (canonical) path --------------------------------


def test_keys_not_fastpathed(tmp_path):
    """Keys must stay on the canonical sort_keys path regardless of the value
    fast-path (identical bytes across insertion order)."""
    stash = HashStash(root_dir=str(tmp_path / "c"))
    assert stash.encode_key({"a": 1, "b": 2}) == stash.encode_key({"b": 2, "a": 1})
