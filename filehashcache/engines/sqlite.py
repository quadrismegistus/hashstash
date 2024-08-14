import os
import json
import hashlib
import zlib
from base64 import b64encode, b64decode
from typing import Any, Optional
from abc import ABC, abstractmethod
from ..filehashcache import BaseHashCache
from sqlitedict import SqliteDict
from functools import cached_property

class SqliteHashCache(BaseHashCache):
    def __init__(self, root_dir: str = ".cache", compress: bool = True, b64: bool = True) -> None:
        super().__init__(compress=compress, b64=b64)
        self.db_path = root_dir+'.sqlite' if not root_dir.endswith('.sqlite') else root_dir
        self._db = None
        self._db_r = None

    @property
    def db(self):
        if not self._db:
            self._db = self.get_db()
        return self._db

    @property
    def db_r(self):
        if not self._db_r:
            self._db_r = self.get_db()
        return self._db_r

    def get_db(self, read_only=False):
        return SqliteDict(
            self.db_path, 
            flag='c' if not read_only or not os.path.exists(self.db_path) else 'r',
            autocommit=True
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        for dbkey in ['_db', '_db_r']:
            db=getattr(self,dbkey,None)
            if db is not None:
                db.close()
                setattr(self,dbkey,None)

    def __setitem__(self, key: str, value: Any) -> None:
        if self._db:
            self._db[key] = self._encode_cache(value)
        else:
            with self.get_db() as db:
                db[key] = self._encode_cache(value)

    def __getitem__(self, key: str) -> Any:
        if self._db_r:
            return self._decode_cache(self._db_r[key])
        else:
            with self.get_db(read_only=True) as db:
                return self._decode_cache(db[key])

    def __contains__(self, key: str) -> bool:
        if self.db:
            return key in self.db
        else:
            with SqliteDict(self.db_path, flag='r') as db:
                return key in db

    def get(self, key: str, default: Any = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default

    def clear(self) -> None:
        if self.db:
            self.db.clear()
        else:
            with SqliteDict(self.db_path, flag='c', autocommit=True) as db:
                db.clear()

    def __len__(self) -> int:
        if self.db:
            return len(self.db)
        else:
            with SqliteDict(self.db_path, flag='r') as db:
                return len(db)

    def __iter__(self):
        if self.db:
            yield from self.db.keys()
        else:
            with SqliteDict(self.db_path, flag='r') as db:
                yield from db.keys()