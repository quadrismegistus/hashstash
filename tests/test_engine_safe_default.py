"""Engine-aware safe default: networked/shared engines (redis, mongo, remote
fsspec) default to safe=True reads; local engines stay code-capable. Explicit
safe= and HASHSTASH_SAFE always win."""
import os
import tempfile

import pytest

from hashstash import HashStash
from hashstash.engines.base import _is_remote_fsspec_uri


@pytest.fixture(autouse=True)
def _clear_env(monkeypatch):
    monkeypatch.delenv("HASHSTASH_SAFE", raising=False)


def _safe(**kwargs):
    return HashStash(**kwargs).safe


@pytest.mark.parametrize(
    "uri,remote",
    [
        ("s3://bucket/c", True),
        ("gcs://b/c", True),
        ("sftp://host/p", True),
        ("az://container", True),
        ("memory://c", False),
        ("file:///tmp/c", False),
        ("/plain/local/path", False),
        ("relative/path", False),
        (None, False),
    ],
)
def test_is_remote_fsspec_uri(uri, remote):
    assert _is_remote_fsspec_uri(uri) is remote


@pytest.mark.parametrize("engine", ["memory", "sqlite", "pairtree"])
def test_local_engines_default_code_capable(engine):
    if engine == "sqlite":
        pytest.importorskip("sqlitedict")
    assert _safe(engine=engine, root_dir=tempfile.mkdtemp()) is False


@pytest.mark.parametrize("engine", ["redis", "mongo"])
def test_networked_engines_default_safe(engine):
    assert _safe(engine=engine) is True


def test_remote_fsspec_defaults_safe():
    assert _safe(engine="fsspec", root_dir="s3://bucket/cache") is True


def test_local_fsspec_default_code_capable():
    assert _safe(engine="fsspec", root_dir="memory://cache") is False


def test_explicit_safe_false_overrides_networked_default():
    assert _safe(engine="redis", safe=False) is False


def test_explicit_safe_true_on_local():
    assert _safe(engine="memory", root_dir=tempfile.mkdtemp(), safe=True) is True


def test_data_only_serializer_not_auto_safed():
    """A data-only serializer is already safe; the auto-default must not set
    safe=True for it (which would trip the serializer-mismatch guard)."""
    pytest.importorskip("msgpack")
    assert _safe(engine="redis", serializer="msgpack") is False


def test_hashstash_safe_env_forces_safe(monkeypatch):
    monkeypatch.setenv("HASHSTASH_SAFE", "1")
    assert _safe(engine="memory", root_dir=tempfile.mkdtemp()) is True


def test_networked_safe_default_still_roundtrips_data(monkeypatch):
    """The safe default must not break normal data caching (only code payloads)."""
    # use a local stash but force the networked-style safe default via env
    monkeypatch.setenv("HASHSTASH_SAFE", "1")
    stash = HashStash(engine="memory", root_dir=tempfile.mkdtemp())
    assert stash.safe is True
    stash["k"] = {"user": "alice", "vals": [1, 2, 3]}
    assert stash["k"] == {"user": "alice", "vals": [1, 2, 3]}
