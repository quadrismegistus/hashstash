from . import *

class LMDBHashStash(BaseHashStash):
    engine = 'lmdb'
    filename_is_dir = True

    def __init__(self, *args, map_size=10 * 1024**3, **kwargs):  # Default to 10GB
        super().__init__(*args, **kwargs)
        self._env = None
        self.map_size = map_size

    @log.info
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

    def _set(self, encoded_key, encoded_value):
        with self.txn_w as txn:
            txn.put(encoded_key, encoded_value)

    def _get(self, encoded_key):
        with self.txn_r as txn:
            return txn.get(encoded_key)

    def _del(self, encoded_key):
        with self.txn_w as txn:
            txn.delete(encoded_key)

    def __len__(self):
        with self.txn_r as txn:
            return txn.stat()['entries']

    def _has(self, encoded_key):
        return self._get(encoded_key) is not None
    
    def _keys(self):
        with self.txn_r as txn:
            cursor = txn.cursor()
            for key, _ in cursor:
                yield key

    def _values(self):
        with self.txn_r as txn:
            cursor = txn.cursor()
            for _, value in cursor:
                yield value

    def _items(self):
        with self.txn_r as txn:
            cursor = txn.cursor()
            for key, value in cursor:
                yield key, value