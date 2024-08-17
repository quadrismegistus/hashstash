from .base import *
import lmdb

class LMDBHashStash(BaseHashStash):
    engine = 'lmdb'
    filename = 'db.lmdb'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._env = None

    @property
    def db(self):
        if self._env is None:
            self._env = lmdb.open(self.path, map_size=1024**3)  # GB max size
        return self._env

    def get_db(self):
        return self.db

    def __setitem__(self, key, value):
        encoded_key = self.encode_key(key)
        encoded_value = self.encode_value(value)
        with self.db.begin(write=True) as txn:
            txn.put(encoded_key, encoded_value)

    def __getitem__(self, key):
        encoded_key = self.encode_key(key)
        with self.db.begin() as txn:
            value = txn.get(encoded_key)
        if value is None:
            raise KeyError(key)
        return self.decode_value(value)

    def __delitem__(self, key):
        encoded_key = self.encode_key(key)
        with self.db.begin(write=True) as txn:
            if not txn.delete(encoded_key):
                raise KeyError(key)

    def __len__(self):
        with self.db.begin() as txn:
            return txn.stat()['entries']

    def __contains__(self, key):
        encoded_key = self.encode_key(key)
        with self.db.begin() as txn:
            return txn.get(encoded_key) is not None

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