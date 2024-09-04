from . import *
import lmdb

class LMDBHashStash(BaseHashStash):
    engine = 'lmdb'
    filename_is_dir = True

    def __init__(self, *args, map_size=10 * 1024**3, **kwargs):  # Default to 10GB
        super().__init__(*args, **kwargs)
        self._env = None
        self.map_size = map_size

    @log.debug
    def get_db(self):
        if self._env is None:
            os.makedirs(self.path_dirname, exist_ok=True)
            self._env = lmdb.open(self.path, map_size=self.map_size)
        return self._env


    @contextmanager
    def get_transaction(self, write=False):
        max_retries = 3
        for attempt in range(max_retries):
            try:
                with self.get_db().begin(write=write) as txn:
                    yield txn
                break
            except lmdb.Error as e:
                log.warning(f"LMDB transaction error (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt == max_retries - 1:
                    raise
                self.close()  # Close the current environment
                self._env = None  # Reset the environment to force a new one on next attempt

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
        super().clear()
        self._env = None  # Reset the environment
        return self

    def close(self):
        if self._env is not None:
            self._env.close()
            self._env = None
        super().close()

