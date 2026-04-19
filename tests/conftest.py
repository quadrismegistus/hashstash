"""Session-scoped fixture: redirect the default hashstash cache away from ~/.cache/hashstash
so tests that construct HashStash() with no root_dir don't collide on a shared path
(which breaks LMDB's per-process env-handle tracking). See issue #9."""
import os
import tempfile
import pytest


@pytest.fixture(scope="session", autouse=True)
def _isolate_default_cache():
    tmpdir = tempfile.mkdtemp(prefix="hashstash-test-")
    # Patch both the constants module and the live Config singleton if already instantiated.
    from hashstash import constants, config as _config_mod
    saved = constants.DEFAULT_ROOT_DIR
    saved_path = constants.DEFAULT_PATH
    constants.DEFAULT_ROOT_DIR = tmpdir
    constants.DEFAULT_PATH = os.path.join(tmpdir, constants.DEFAULT_NAME)
    if hasattr(_config_mod, "Config"):
        try:
            _config_mod.Config().set_root_dir(tmpdir)
        except Exception:
            pass
    yield
    constants.DEFAULT_ROOT_DIR = saved
    constants.DEFAULT_PATH = saved_path
