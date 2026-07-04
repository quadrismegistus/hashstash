"""Guards for the serializer Tier 1+2 changes: the hot path stays undecorated,
the single-pass key check is correct, and the removed dead code stays gone."""
import pytest

from hashstash import deserialize, serialize
from hashstash.serializers import custom
from hashstash.serializers.custom import _dict_has_simple_keys


def test_serialize_custom_not_log_wrapped():
    """_serialize_custom must NOT be @log.debug-decorated — the per-recursive-
    call wrapper overhead was ~20% of serialization time (measured 1.5x)."""
    assert not hasattr(custom._serialize_custom, "__wrapped__")
    assert not hasattr(custom.serialize_custom, "__wrapped__")


@pytest.mark.parametrize(
    "d,simple",
    [
        ({"a": 1, "b": 2}, True),
        ({}, True),
        ({1: "x"}, False),            # non-str key
        ({"a": 1, 2: "b"}, False),    # mixed
        ({"__py__": "x"}, False),     # reserved marker key
        ({"__pytype__": 1}, False),
        ({"__data__": 1}, False),
        ({"normal": 1, "__py__": 2}, False),
    ],
)
def test_dict_has_simple_keys(d, simple):
    assert _dict_has_simple_keys(d) is simple


@pytest.mark.parametrize(
    "value",
    [
        {"a": 1, "b": [1, 2, 3]},
        {1: "one", 2: "two"},
        {(1, 2): "tuple-key"},
        {"__py__": "not an address", "__data__": "plain dict"},
        {"nested": {"__pytype__": "also plain"}},
        {"mixed": {"a": 1, 7: "seven"}},
    ],
)
def test_key_forms_roundtrip(value):
    assert deserialize(serialize(value, serializer="hashstash"), serializer="hashstash") == value


def test_dead_pmap_serializers_removed():
    """The unregistered PmapSerializer/PmapResultSerializer classes were deleted."""
    assert not hasattr(custom, "PmapSerializer")
    assert not hasattr(custom, "PmapResultSerializer")
