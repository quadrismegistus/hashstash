"""Regression guard for the safe-mode arbitrary-code-execution bypass.

The CUSTOM_DESERIALIZERS dispatch is keyed by the payload's own __py__, and a
few registered addresses (function/type/object/ReusableGenerator) route to
exec/compile reconstruction. That dispatch runs before the pytype refusals, so
without a guard a 2-key payload executed code under safe=True. These tests plant
a sentinel-writing payload and assert safe mode refuses it and NO code runs.
"""
import json
import os
import tempfile

import pytest

from hashstash import HashStash, safe_deserialization, SafeDeserializationError
from hashstash.serializers.custom import deserialize_custom


def _evil_source(sentinel):
    p = sentinel.replace(os.sep, "/")
    return f'import os\nopen("{p}", "w").write("pwned")\n'


def _payload(kind, sentinel):
    src = _evil_source(sentinel)
    method = {"__py__": "e.m", "__pytype__": "function", "__source__": src + "def m(self):\n    pass\n"}
    if kind == "function":
        return {"__py__": "function", "__source__": src + "def function():\n    pass\n"}
    if kind == "type":
        return {"__py__": "type", "__pytype__": "class", "__bases__": ["builtins.object"],
                "__methods__": {"m": method}, "__attrs__": {}}
    if kind == "object":
        return {"__py__": "object",
                "__cls__": {"__py__": "e.E", "__pytype__": "class", "__bases__": ["builtins.object"],
                            "__methods__": {"m": method}, "__attrs__": {}},
                "__state__": {}}
    if kind == "reusablegen":
        return {"__py__": "hashstash.utils.misc.ReusableGenerator",
                "__func__": {"__py__": "e.f", "__pytype__": "function", "__source__": src + "def f():\n    return 1\n"},
                "__args__": [], "__kwargs__": {}}
    raise ValueError(kind)


@pytest.mark.parametrize("kind", ["function", "type", "object", "reusablegen"])
def test_exec_capable_payload_refused_in_safe_mode(kind):
    sentinel = os.path.join(tempfile.mkdtemp(), "PWNED")
    payload = json.dumps(_payload(kind, sentinel))
    with safe_deserialization(True):
        with pytest.raises(SafeDeserializationError):
            deserialize_custom(payload)
    assert not os.path.exists(sentinel), f"{kind}: code executed under safe mode!"


@pytest.mark.parametrize("kind", ["function", "type", "object", "reusablegen"])
def test_same_payload_executes_without_safe_mode(kind):
    # control: the payload IS live code when safe mode is off (proves the test
    # exercises the real exec path, not an unrelated failure)
    sentinel = os.path.join(tempfile.mkdtemp(), "PWNED")
    payload = json.dumps(_payload(kind, sentinel))
    try:
        deserialize_custom(payload)
    except Exception:
        pass  # reconstruction may error after the top-level source already ran
    assert os.path.exists(sentinel)


def test_write_unsafe_read_safe_refuses_function_value(tmp_path):
    # the realistic threat: attacker writes a function value, victim reads safe
    writer = HashStash(root_dir=str(tmp_path / "c"), safe=False)
    writer.clear()
    writer["k"] = lambda x: x + 1
    victim = HashStash(root_dir=str(tmp_path / "c"), safe=True)
    with pytest.raises(SafeDeserializationError):
        _ = victim["k"]


def test_env_safe_mode_also_refuses(monkeypatch):
    monkeypatch.setenv("HASHSTASH_SAFE", "1")
    sentinel = os.path.join(tempfile.mkdtemp(), "PWNED")
    payload = json.dumps(_payload("function", sentinel))
    with pytest.raises(SafeDeserializationError):
        deserialize_custom(payload)
    assert not os.path.exists(sentinel)


def test_legit_data_still_round_trips_in_safe_mode():
    pytest.importorskip("numpy")
    import numpy as np
    from datetime import datetime
    from hashstash import serialize, deserialize

    for value in [{"a": [1, 2, 3]}, {1, 2, 3}, datetime(2020, 1, 1), np.array([1, 2, 3])]:
        with safe_deserialization(True):
            rt = deserialize(serialize(value))
        if isinstance(value, np.ndarray):
            assert np.array_equal(rt, value)
        else:
            assert rt == value
