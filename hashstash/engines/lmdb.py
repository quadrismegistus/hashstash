from . import *

class LMDBHashStash(BaseHashStash):
    engine = 'lmdb'
    filename_is_dir = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._env = None

    @property
    def db(self):
        if self._env is None:
            import lmdb
            self._env = lmdb.open(self.path, map_size=1024**3)  # GB max size
        return self._env

    def get_db(self):
        return self.db

    def _set(self, encoded_key, encoded_value):
        with self.db.begin(write=True) as txn:
            txn.put(encoded_key, encoded_value)

    def _get(self, encoded_key):
        with self.db.begin() as txn:
            return txn.get(encoded_key)

    def _del(self, encoded_key):
        with self.db.begin(write=True) as txn:
            txn.delete(encoded_key)

    def __len__(self):
        with self.db.begin() as txn:
            return txn.stat()['entries']

    def _has(self, encoded_key):
        return self._get(encoded_key) is not None

    def clear(self):
        with self.db.begin(write=True) as txn:
            txn.drop(self.db.open_db())

    def _keys(self):
        with self.db.begin() as txn:
            cursor = txn.cursor()
            for key, _ in cursor:
                yield key

    def _values(self):
        with self.db.begin() as txn:
            cursor = txn.cursor()
            for _, value in cursor:
                yield value

    def _items(self):
        with self.db.begin() as txn:
            cursor = txn.cursor()
            for key, value in cursor:
                yield key, value