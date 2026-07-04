"""Safe deserialization mode: data round-trips, code does not."""
import datetime
import os
import subprocess
import sys
from pathlib import Path

import pytest

from hashstash import HashStash, deserialize, serialize
from hashstash.serializers.custom import (
    SafeDeserializationError,
    safe_deserialization,
)

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _module_function():
    return 42


class _Sample:
    def __init__(self, x=1):
        self.x = x


# --- data round-trips unchanged under safe mode ------------------------------


@pytest.mark.parametrize(
    "value",
    [
        {"a": [1, 2, 3], "b": {"nested": True}},
        {1: "int-key", (2, 3): "tuple-key"},
        [1, 2.5, "three", True, None],
        {1, 2, 3},
        frozenset([4, 5]),
        b"raw bytes",
        (1, 2, 3),
        datetime.datetime(2020, 1, 1, 12, 30),
        datetime.date(2021, 6, 1),
        datetime.timedelta(hours=3),
        complex(1, 2),
        Path("/tmp/x"),
    ],
)
def test_data_roundtrips_in_safe_mode(value, tmp_path):
    stash = HashStash(root_dir=str(tmp_path / "c"), safe=True)
    stash["k"] = value
    assert stash["k"] == value


def test_numpy_pandas_allowed_in_safe_mode(tmp_path):
    np = pytest.importorskip("numpy")
    pd = pytest.importorskip("pandas")
    stash = HashStash(root_dir=str(tmp_path / "c"), safe=True)
    stash["arr"] = np.array([1, 2, 3])
    assert list(stash["arr"]) == [1, 2, 3]
    stash["df"] = pd.DataFrame({"a": [1, 2]})
    assert list(stash["df"]["a"]) == [1, 2]


# --- code is refused under safe mode -----------------------------------------


def test_safe_refuses_function(tmp_path):
    blob = serialize(_module_function, serializer="hashstash")
    assert deserialize(blob, serializer="hashstash")() == 42  # unsafe: works
    with pytest.raises(SafeDeserializationError):
        deserialize(blob, serializer="hashstash", safe=True)


def test_safe_refuses_local_function(tmp_path):
    stash = HashStash(root_dir=str(tmp_path / "c"), safe=True)
    stash["fn"] = lambda x: x + 1
    with pytest.raises(SafeDeserializationError):
        _ = stash["fn"]


def test_safe_refuses_class_and_instance(tmp_path):
    for value in (_Sample, _Sample(5)):
        blob = serialize(value, serializer="hashstash")
        with pytest.raises(SafeDeserializationError):
            deserialize(blob, serializer="hashstash", safe=True)


def test_safe_refuses_reducer_global(tmp_path):
    """A crafted reducer payload naming an arbitrary callable must be refused."""
    import json

    from hashstash.engines.base import ENVELOPE_MARKER

    payload = {"__pytype__": "reducer", "__global__": "os.system"}
    stash = HashStash(root_dir=str(tmp_path / "c"), safe=True)
    # inject the raw JSON envelope directly (encode without serialize, so the
    # crafted reducer payload survives instead of being re-serialized away)
    raw = json.dumps(
        {ENVELOPE_MARKER: True, "_values": [payload], "_written_at": [0.0]}
    )
    stash._set(stash.encode_key("evil"), stash.encode(raw, as_string=stash.string_values))
    with pytest.raises(SafeDeserializationError):
        stash.get("evil")


def test_unsafe_is_default(tmp_path):
    stash = HashStash(root_dir=str(tmp_path / "c"))
    assert stash.safe is False
    stash["fn"] = _module_function
    assert stash["fn"]() == 42


def test_safe_requires_hashstash_serializer(tmp_path):
    with pytest.raises(ValueError):
        HashStash(root_dir=str(tmp_path / "c"), serializer="pickle", safe=True)


def test_safe_context_manager():
    blob = serialize(_module_function, serializer="hashstash")
    with safe_deserialization():
        with pytest.raises(SafeDeserializationError):
            deserialize(blob, serializer="hashstash")
    # outside the context: unsafe again
    assert deserialize(blob, serializer="hashstash")() == 42


def test_env_var_enables_safe_mode(tmp_path):
    """HASHSTASH_SAFE=1 makes safe mode the default for a fresh process."""
    root = str(tmp_path / "c")
    # write a function-valued entry with an unsafe (default) process
    writer = HashStash(root_dir=root)
    writer["fn"] = _module_function

    reader_code = (
        "import sys\n"
        f"sys.path.insert(0, {REPO_ROOT!r})\n"
        "from hashstash import HashStash\n"
        "from hashstash.serializers.custom import SafeDeserializationError\n"
        f"stash = HashStash(root_dir={root!r})\n"
        "assert stash.safe is True, 'env var did not enable safe mode'\n"
        "try:\n"
        "    stash['fn']\n"
        "    print('FAIL: did not block')\n"
        "except SafeDeserializationError:\n"
        "    print('OK')\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", reader_code],
        env={**os.environ, "PYTHONPATH": REPO_ROOT, "HASHSTASH_SAFE": "1"},
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip().splitlines()[-1] == "OK"
