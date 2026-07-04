"""Session-scoped fixture: redirect the default hashstash cache away from
~/.cache/hashstash so tests that construct HashStash() with no root_dir never
touch (or pollute) the developer's real cache. See issue #9.

Config resolves its root_dir at call time from HASHSTASH_ROOT_DIR /
constants.DEFAULT_ROOT_DIR, so patching both here is effective for every
Config() created during the session — including in subprocesses, which
inherit the environment variable.
"""
import os
import tempfile

import pytest

_TMPDIR = tempfile.mkdtemp(prefix="hashstash-test-")
# set before hashstash is imported anywhere so import-time consumers see it too
os.environ["HASHSTASH_ROOT_DIR"] = _TMPDIR


@pytest.fixture(scope="session", autouse=True)
def _isolate_default_cache():
    from hashstash import constants

    saved = constants.DEFAULT_ROOT_DIR
    saved_path = constants.DEFAULT_PATH
    constants.DEFAULT_ROOT_DIR = _TMPDIR
    constants.DEFAULT_PATH = os.path.join(_TMPDIR, constants.DEFAULT_NAME)
    yield
    constants.DEFAULT_ROOT_DIR = saved
    constants.DEFAULT_PATH = saved_path
    os.environ.pop("HASHSTASH_ROOT_DIR", None)


@pytest.fixture()
def isolated_default_root():
    """The session-wide temporary default cache root."""
    return _TMPDIR
