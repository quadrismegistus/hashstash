"""Value-envelope format versioning: new writes are stamped, old (unstamped)
envelopes read as v1, and future-version data reads its stable fields."""
import pytest

from hashstash import HashStash
from hashstash.engines.base import (
    ENVELOPE_MARKER,
    FORMAT_VERSION,
    _migrate_envelope,
    _unwrap_envelope,
    _wrap_envelope,
)


def test_new_envelope_is_stamped():
    env = _wrap_envelope(["v"], [123.0])
    assert env["_fv"] == FORMAT_VERSION


def test_old_unstamped_envelope_reads_as_v1():
    # an envelope written before versioning existed (no "_fv" field)
    legacy = {ENVELOPE_MARKER: True, "_values": ["a", "b"], "_written_at": [1.0, 2.0]}
    values, timestamps = _unwrap_envelope(legacy)
    assert values == ["a", "b"]
    assert timestamps == [1.0, 2.0]


def test_current_envelope_roundtrips_through_unwrap():
    env = _wrap_envelope(["x"], [5.0])
    values, timestamps = _unwrap_envelope(env)
    assert values == ["x"]
    assert timestamps == [5.0]


def test_future_version_reads_stable_fields():
    """Data written by a newer hashstash (higher _fv) still reads its known
    fields rather than being misinterpreted."""
    future = {
        ENVELOPE_MARKER: True,
        "_fv": FORMAT_VERSION + 5,
        "_values": ["future"],
        "_written_at": [9.0],
        "_something_new": 42,  # unknown field, ignored
    }
    values, timestamps = _unwrap_envelope(future)
    assert values == ["future"]
    assert timestamps == [9.0]


def test_migrate_envelope_is_passthrough_for_current():
    env = _wrap_envelope(["v"], [0.0])
    assert _migrate_envelope(env, FORMAT_VERSION) == env


@pytest.mark.parametrize("engine", ["sqlite", "memory"])
def test_stored_envelope_carries_version(engine, tmp_path):
    """Envelope-based engines stamp the version into stored values."""
    if engine == "sqlite":
        pytest.importorskip("sqlitedict")
    stash = HashStash(engine=engine, root_dir=str(tmp_path / engine))
    stash["k"] = {"a": 1}
    # read the raw stored envelope back and confirm the version field is present
    encoded = stash._get(stash.encode_key("k"))
    decoded = stash.decode_value(encoded)
    assert decoded.get(ENVELOPE_MARKER) is True
    assert decoded.get("_fv") == FORMAT_VERSION
    # and the value still round-trips
    assert stash["k"] == {"a": 1}


def test_value_roundtrip_unaffected(tmp_path):
    # versioning must not change what get() returns
    stash = HashStash(engine="memory", root_dir=str(tmp_path / "m"))
    stash["k"] = {"nested": [1, 2, 3]}
    assert stash["k"] == {"nested": [1, 2, 3]}
