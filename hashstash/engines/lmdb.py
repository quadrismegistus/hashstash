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
    to_dict_attrs = BaseHashStash.to_dict_attrs + ["map_size"]

    def __init__(self, *args, map_size=10 * 1024**3, **kwargs):  # Default to 10GB
        self.map_size = map_size
        super().__init__(*args, **kwargs)

    @log.debug
    def get_db(self):
        with _lmdb_envs_guard:
            env = _lmdb_envs.get(self.path)
            if env is None:
                import lmdb
                os.makedirs(self.path_dirname, exist_ok=True)
                env = lmdb.open(self.path, map_size=self.map_size)
                _lmdb_envs[self.path] = env
            return env

    def _drop_env(self):
        with _lmdb_envs_guard:
            env = _lmdb_envs.pop(self.path, None)
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

    def _set(self, encoded_key, encoded_value):
        with self.get_transaction(write=True) as txn:
            txn.put(self._encode_key_key(encoded_key), encoded_key)
            txn.put(self._encode_key_value(encoded_key), encoded_value)

    def _get(self, encoded_key):
        with self.get_transaction(write=False) as txn:
            return txn.get(self._encode_key_value(encoded_key))

    def _del(self, encoded_key):
        with self.get_transaction(write=True) as txn:
            txn.delete(self._encode_key_key(encoded_key))
            txn.delete(self._encode_key_value(encoded_key))

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

