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
    engine = 'sqlite'
    filename = 'db.sqlitedict'

    def __init__(
        self,
        root_dir: str = ".cache",
        compress: bool = True,
        b64: bool = True,
    ) -> None:
        super().__init__(
            root_dir=root_dir,
            compress=compress,
            b64=b64,
        )
        self.db_path = self.path
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
        
        if read_only:
            try:
                return SqliteDict(
                    self.db_path,flag="r"
                )
            except Exception:
                pass
        
        return SqliteDict(self.db_path, flag='c', autocommit=True)

    def __exit__(self, exc_type, exc_val, exc_tb):
        for dbkey in ["_db", "_db_r"]:
            db = getattr(self, dbkey, None)
            if db is not None:
                db.close()
                setattr(self, dbkey, None)

    def __setitem__(self, key: str, value: Any) -> None:
        if self._db:
            self._db[self._encode_key(key)] = self._encode_value(value)
        else:
            with self.get_db() as db:
                db[self._encode_key(key)] = self._encode_value(value)

    def __getitem__(self, key: str) -> Any:
        try:
            if self._db_r:
                return self._decode_value(self._db_r[self._encode_key(key)])
            else:
                with self.get_db(read_only=True) as db:
                    return self._decode_value(db[self._encode_key(key)])
        except KeyError:
            raise KeyError(key)

    def __contains__(self, key: str) -> bool:
        if self._db_r:
            return key in self._db_r
        else:
            with self.get_db(read_only=True) as db:
                return key in db

    def clear(self) -> None:
        if self._db:
            self._db.clear()
        else:
            with SqliteDict(self.db_path, flag="c", autocommit=True) as db:
                db.clear()

    def __len__(self) -> int:
        if self._db_r:
            return len(self._db_r)
        else:
            with self.get_db(read_only=True) as db:
                return len(db)

    def __iter__(self):
        if self._db_r:
            yield from self._db_r.keys()
        else:
            with self.get_db(read_only=True) as db:
                yield from db.keys()
