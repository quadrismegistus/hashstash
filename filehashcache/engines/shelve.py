import os
import shelve
from typing import Any
from ..filehashcache import BaseHashCache
import fcntl
import time

class ShelveHashCache(BaseHashCache):
    engine = 'shelve'
    filename = 'db.shelve'

    def __init__(self, root_dir: str = ".cache", compress: bool = True, b64: bool = True) -> None:
        super().__init__(root_dir=root_dir, compress=compress, b64=b64)
        self.db_path = self.path
        self.lock_path = self.db_path + '.lock'
        self._db = None

    def __enter__(self):
        self._db = shelve.open(self.db_path, writeback=True)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._db:
            self._db.close()
            self._db = None

    def _acquire_lock(self):
        while True:
            try:
                self.lock_file = open(self.lock_path, 'w')
                fcntl.flock(self.lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return
            except IOError:
                time.sleep(0.1)

    def _release_lock(self):
        fcntl.flock(self.lock_file, fcntl.LOCK_UN)
        self.lock_file.close()

    def __setitem__(self, key: str, value: Any) -> None:
        encoded_key = self._encode_key(key)
        encoded_value = self._encode_value(value)
        self._acquire_lock()
        try:
            with shelve.open(self.db_path, writeback=True) as db:
                db[encoded_key] = encoded_value
        finally:
            self._release_lock()

    def __getitem__(self, key: str) -> Any:
        encoded_key = self._encode_key(key)
        self._acquire_lock()
        try:
            with shelve.open(self.db_path) as db:
                try:
                    encoded_value = db[encoded_key]
                except KeyError:
                    raise KeyError(key)
            return self._decode_value(encoded_value)
        finally:
            self._release_lock()


    def __contains__(self, key: str) -> bool:
        encoded_key = self._encode_key(key)
        self._acquire_lock()
        try:
            with shelve.open(self.db_path) as db:
                return encoded_key in db
        finally:
            self._release_lock()

    def clear(self) -> None:
        self._acquire_lock()
        try:
            with shelve.open(self.db_path, writeback=True) as db:
                db.clear()
        finally:
            self._release_lock()

    def __len__(self) -> int:
        self._acquire_lock()
        try:
            with shelve.open(self.db_path) as db:
                return len(db)
        finally:
            self._release_lock()
        
    def __iter__(self):
        self._acquire_lock()
        try:
            with shelve.open(self.db_path) as db:
                yield from db.keys()
        finally:
            self._release_lock()