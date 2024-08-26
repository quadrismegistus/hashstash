from . import *

class LMDBHashStash(BaseHashStash):
    engine = 'lmdb'
    filename_is_dir = True

    def __init__(self, *args, map_size=10 * 1024**3, **kwargs):  # Default to 10GB
        super().__init__(*args, **kwargs)
        self._env = None
        self.map_size = map_size

    @property
    def db(self):
        if self._env is None:
            import lmdb
            os.makedirs(self.path_dirname, exist_ok=True)
            self._env = lmdb.open(self.path, map_size=self.map_size)
        return self._env

    def get_db(self):
        os.makedirs(self.path, exist_ok=True)
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
        self.db.close()
        self._env = None
        super().clear()

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