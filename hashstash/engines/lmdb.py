from . import *

class LMDBHashStash(BaseHashStash):
    engine = 'lmdb'
    filename_is_dir = True

    def __init__(self, *args, map_size=10 * 1024**3, **kwargs):  # Default to 10GB
        super().__init__(*args, **kwargs)
        self._env = None
        self.map_size = map_size

    @log.debug
    def get_db(self):
        import lmdb
        os.makedirs(self.path_dirname, exist_ok=True)
        return lmdb.open(self.path, map_size=self.map_size)

    @property
    def txn_w(self):
        with self.db as db:
            return db.begin(write=True)

    @property
    def txn_r(self):
        with self.db as db:
            return db.begin(write=False)

    def _encode_key_key(self, encoded_key):
        return encode_hash(encoded_key).encode() + b'.key'
    
    def _encode_key_value(self, encoded_key):
        return encode_hash(encoded_key).encode() + b'.value'

    def _set(self, encoded_key, encoded_value):
        with self.txn_w as txn:
            txn.put(self._encode_key_key(encoded_key), encoded_key)
            txn.put(self._encode_key_value(encoded_key), encoded_value)

    def _get(self, encoded_key):
        with self.txn_r as txn:
            return txn.get(self._encode_key_value(encoded_key))

    def _del(self, encoded_key):
        with self.txn_w as txn:
            txn.delete(self._encode_key_key(encoded_key))
            txn.delete(self._encode_key_value(encoded_key))

    def __len__(self):
        with self.txn_r as txn:
            return txn.stat()['entries'] // 2

    def _has(self, encoded_key):
        with self.txn_r as txn:
            return txn.get(self._encode_key_key(encoded_key)) is not None
    
    def _keys(self):
        with self.txn_r as txn:
            cursor = txn.cursor()
            for key, value in cursor:
                if key.endswith(b'.key'):
                    yield value

    def _values(self):
        with self.txn_r as txn:
            cursor = txn.cursor()
            for key, value in cursor:
                if key.endswith(b'.value'):
                    yield value

    def _items(self):
        with self.txn_r as txn:
            cursor = txn.cursor()
            for key, value in cursor:
                if key.endswith(b'.key'):
                    yield value, txn.get(key[:-4]+b'.value')