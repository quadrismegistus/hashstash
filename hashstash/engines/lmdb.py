# Explicit stdlib imports: this package's `from . import *` chains are
# circular, and whether a name has landed in the package namespace yet
# depends on import order (spawn workers + editable installs order imports
# differently). Never rely on the star-chain for stdlib names.
from contextlib import contextmanager
import os

from . import *
import threading

# One lmdb.Environment per path per process. Opening the same path twice in one
# process is unsupported by LMDB ("environment already open", issue #9) — the old
# per-instance self._env did exactly that whenever two stash objects shared a path.
_lmdb_envs = {}
_lmdb_envs_guard = threading.Lock()


class LMDBHashStash(BaseHashStash):
    engine = 'lmdb'
    filename_is_dir = True
    needs_lock = False  # LMDB has its own multi-reader/single-writer locking
    to_dict_attrs = BaseHashStash.to_dict_attrs + ["map_size", "max_map_size"]

    def __init__(self, *args, map_size=None, max_map_size=None, **kwargs):
        # initial memory-map size (auto-grows on MapFullError) and the ceiling
        # for that auto-grow. A wedged write that kept raising MapFullError would
        # otherwise double map_size forever; the cap stops it and surfaces the
        # error. Raise max_map_size for caches larger than the 256 GB default.
        self.map_size = map_size if map_size is not None else DEFAULT_LMDB_MAP_SIZE
        self.max_map_size = (
            max_map_size if max_map_size is not None else DEFAULT_LMDB_MAX_MAP_SIZE
        )
        super().__init__(*args, **kwargs)

    def _env_key(self):
        # Key the process-wide env registry by REAL path so two spellings of the
        # same directory (symlink, relative-vs-absolute, trailing slash) share
        # one handle — opening the same LMDB dir twice in a process corrupts
        # reads (issue #9).
        return os.path.realpath(self.path)

    @log.debug
    def get_db(self):
        key = self._env_key()
        with _lmdb_envs_guard:
            env = _lmdb_envs.get(key)
            if env is None:
                import lmdb
                os.makedirs(self.path_dirname, exist_ok=True)
                env = lmdb.open(self.path, map_size=self.map_size)
                _lmdb_envs[key] = env
            return env

    def _drop_env(self):
        with _lmdb_envs_guard:
            env = _lmdb_envs.pop(self._env_key(), None)
        if env is not None:
            try:
                env.close()
            except Exception as e:
                log.debug(f"error closing LMDB env: {e}")

    @contextmanager
    def get_connection(self):
        # bypass the base connection pool: envs live in _lmdb_envs, and pooling the
        # same object twice would let pool cleanup close an env the registry still serves
        yield self.get_db()

    @contextmanager
    def get_transaction(self, write=False):
        import lmdb
        max_retries = 3
        for attempt in range(max_retries):
            try:
                with self.get_db().begin(write=write) as txn:
                    yield txn
                break
            except lmdb.Error as e:
                log.debug(f"LMDB transaction error (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt == max_retries - 1:
                    raise
                self._drop_env()  # force a fresh environment on the next attempt

    def _grow_map(self):
        """Double map_size (capped at max_map_size) and apply it to the live,
        registry-owned environment. LMDB has a fixed map_size and raises
        MapFullError once the map fills; set_mapsize enlarges it in place so we
        keep sharing the one env per path (issue #9) rather than reopening.

        Raises lmdb.MapFullError if the cap is already reached, so a genuinely
        unbounded write can't loop forever.
        """
        import lmdb
        if self.map_size >= self.max_map_size:
            raise lmdb.MapFullError(
                f"LMDB map is full and map_size cap ({self.max_map_size} bytes) "
                f"is reached; refusing to grow further"
            )
        self.map_size = min(self.map_size * 2, self.max_map_size)
        # set_mapsize on the current env keeps the shared registry entry valid;
        # self.map_size is in to_dict_attrs so the grown size survives pickling.
        self.get_db().set_mapsize(self.map_size)
        log.debug(f"grew LMDB map_size to {self.map_size} bytes for {self.path}")

    def _write(self, fn):
        """Run fn(txn) inside a write transaction, growing the map on MapFullError.

        A context manager can only yield once, so it can't re-run the caller's
        puts after a mid-transaction MapFullError — writes go through this
        callable-based path instead, which re-executes the whole transaction on a
        fresh txn after each grow. Generic lmdb.Error keeps the stale-env retry
        that get_transaction gives reads.
        """
        import lmdb
        max_retries = 3
        attempt = 0
        while True:
            try:
                with self.get_db().begin(write=True) as txn:
                    return fn(txn)
            except lmdb.MapFullError:
                # subclass of lmdb.Error, so this must come first. Don't drop the
                # env: grow it in place and retry the operation.
                self._grow_map()  # raises if the cap is hit
            except lmdb.Error as e:
                attempt += 1
                log.debug(f"LMDB write error (attempt {attempt}/{max_retries}): {e}")
                if attempt >= max_retries:
                    raise
                self._drop_env()  # force a fresh environment on the next attempt

    def _set(self, encoded_key, encoded_value):
        def op(txn):
            txn.put(self._encode_key_key(encoded_key), encoded_key)
            txn.put(self._encode_key_value(encoded_key), encoded_value)
        self._write(op)

    def _get(self, encoded_key):
        with self.get_transaction(write=False) as txn:
            return txn.get(self._encode_key_value(encoded_key))

    def _del(self, encoded_key):
        def op(txn):
            txn.delete(self._encode_key_key(encoded_key))
            txn.delete(self._encode_key_value(encoded_key))
        self._write(op)

    def __len__(self):
        with self.get_transaction(write=False) as txn:
            return txn.stat()['entries'] // 2

    def _has(self, encoded_key):
        with self.get_transaction(write=False) as txn:
            return txn.get(self._encode_key_key(encoded_key)) is not None
    
    def _keys(self):
        with self.get_transaction(write=False) as txn:
            cursor = txn.cursor()
            for key, value in cursor:
                if key.endswith(b'.key'):
                    yield value

    def _values(self):
        with self.get_transaction(write=False) as txn:
            cursor = txn.cursor()
            for key, value in cursor:
                if key.endswith(b'.value'):
                    yield value

    def _items(self):
        with self.get_transaction(write=False) as txn:
            cursor = txn.cursor()
            for key, value in cursor:
                if key.endswith(b'.key'):
                    yield value, txn.get(key[:-4]+b'.value')

    def _encode_key_key(self, encoded_key):
        return encode_hash(encoded_key).encode() + b'.key'
    
    def _encode_key_value(self, encoded_key):
        return encode_hash(encoded_key).encode() + b'.value'

    @staticmethod
    def _close_connection(connection):
        if connection is not None:
            connection.close()

    def clear(self):
        self._drop_env()
        super().clear()
        return self

    def close(self):
        self._drop_env()
        super().close()

