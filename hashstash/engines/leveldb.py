# Explicit stdlib imports: this package's `from . import *` chains are
# circular, and whether a name has landed in the package namespace yet
# depends on import order (spawn workers + editable installs order imports
# differently). Never rely on the star-chain for stdlib names.
from contextlib import contextmanager
import os

from . import *
import threading

# One plyvel.DB handle per path per process. LevelDB is single-process
# single-writer and takes an exclusive lock on its directory: opening the same
# directory twice in one process raises ("IO error: lock ... Resource
# temporarily unavailable"). A per-instance handle would do exactly that
# whenever two stash objects share a path, so we cache one handle per path —
# mirroring lmdb.py's _lmdb_envs.
_plyvel_dbs = {}
_plyvel_dbs_guard = threading.Lock()


class LevelDBHashStash(BaseHashStash):
    """Embedded key-value store backed by LevelDB via the ``plyvel`` driver.

    A single-directory alternative to LMDB that grows on demand: unlike LMDB
    there is no fixed ``map_size`` to pre-allocate — LevelDB's LSM tree expands
    as needed. LevelDB is single-process single-writer and locks its own
    directory, so no external cross-process lock is required
    (``needs_lock = False``).

    Keys and values are the raw encoded bytes from the stash encoder, stored
    directly (``db.put(encoded_key, encoded_value)``) — LevelDB is a plain
    ordered byte-keyed KV store, so no auxiliary index is needed.

    The concrete backend
    is LevelDB (``pip install plyvel``, plus the system ``leveldb`` library).
    """

    engine = "leveldb"
    filename_is_dir = True
    needs_lock = False  # LevelDB locks its own directory (single writer)
    string_keys = False  # plyvel wants bytes keys
    string_values = False  # plyvel wants bytes values

    @log.debug
    def get_db(self):
        with _plyvel_dbs_guard:
            db = _plyvel_dbs.get(self.path)
            if db is None:
                import plyvel

                os.makedirs(self.path_dirname, exist_ok=True)
                db = plyvel.DB(self.path, create_if_missing=True)
                _plyvel_dbs[self.path] = db
            return db

    def _drop_db(self):
        with _plyvel_dbs_guard:
            db = _plyvel_dbs.pop(self.path, None)
        if db is not None:
            try:
                db.close()
            except Exception as e:
                log.debug(f"error closing LevelDB db: {e}")

    @contextmanager
    def get_connection(self):
        # bypass the base connection pool: handles live in _plyvel_dbs, and
        # pooling the same object twice would let pool cleanup close a handle the
        # registry still serves (mirrors lmdb.py)
        yield self.get_db()

    def _set(self, encoded_key, encoded_value):
        self.get_db().put(encoded_key, encoded_value)

    def _get(self, encoded_key):
        return self.get_db().get(encoded_key)

    def _del(self, encoded_key):
        self.get_db().delete(encoded_key)

    def _has(self, encoded_key):
        return self.get_db().get(encoded_key) is not None

    def __len__(self):
        # LevelDB has no O(1) count; iterate keys only (values not decoded)
        return sum(1 for _ in self.get_db().iterator(include_value=False))

    def _keys(self):
        for key in self.get_db().iterator(include_value=False):
            yield key

    def _values(self):
        for value in self.get_db().iterator(include_key=False):
            yield value

    def _items(self):
        for key, value in self.get_db().iterator():
            yield key, value

    @staticmethod
    def _close_connection(connection):
        if connection is not None:
            try:
                connection.close()
            except Exception as e:
                log.debug(f"error closing LevelDB connection: {e}")

    def clear(self):
        # close the handle first so the directory isn't locked when removed
        self._drop_db()
        super().clear()
        return self

    def close(self):
        self._drop_db()
        super().close()
