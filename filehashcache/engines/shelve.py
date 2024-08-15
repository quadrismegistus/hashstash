import shelve
from typing import Any
from ..filehashcache import BaseHashCache
from functools import lru_cache

# Global shelve database
GLOBAL_SHELVE_DB = None

def get_global_shelve_db(db_path, writeback=True):
    global GLOBAL_SHELVE_DB
    if GLOBAL_SHELVE_DB is None:
        GLOBAL_SHELVE_DB = shelve.open(db_path, writeback=writeback)
    return GLOBAL_SHELVE_DB

class ShelveHashCacheModel(BaseHashCache):
    engine = 'shelve'
    filename = 'db.shelve'

    def __init__(self, root_dir: str = ".cache", compress: bool = None, b64: bool = None) -> None:
        super().__init__(root_dir=root_dir, compress=compress, b64=b64)
        self.db_path = self.path
        self._db = get_global_shelve_db(self.db_path)

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Don't close the database here, as it's now managed globally
        pass

    def __setitem__(self, key: str, value: Any) -> None:
        encoded_key = self._encode_key(key)
        encoded_value = self._encode_value(value)
        self._db[encoded_key] = encoded_value

    def __getitem__(self, key: str) -> Any:
        encoded_key = self._encode_key(key)
        try:
            encoded_value = self._db[encoded_key]
            return self._decode_value(encoded_value)
        except KeyError:
            raise KeyError(key)

    def __contains__(self, key: str) -> bool:
        encoded_key = self._encode_key(key)
        return encoded_key in self._db

    def clear(self) -> None:
        self._db.clear()

    def __len__(self) -> int:
        return len(self._db)
        
    def _keys(self):
        yield from self._db.keys()

    def __delitem__(self, key: str) -> None:
        encoded_key = self._encode_key(key)
        try:
            del self._db[encoded_key]
        except KeyError:
            raise KeyError(key)

@lru_cache(maxsize=None)
def ShelveHashCache(*args, **kwargs):
    return ShelveHashCacheModel(*args, **kwargs)