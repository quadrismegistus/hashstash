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
    engine = "sqlite"
    filename = "db.sqlitedict"

    def get_db(self, read_only=False):
        if read_only:
            try:
                return SqliteDict(self.path, flag="r")
            except Exception:
                pass
        return SqliteDict(self.path, flag="c", autocommit=True)

    def __setitem__(self, key: str, value: Any) -> None:
        encoded_key = self._encode_key(key)
        encoded_value = self._encode_value(value)
        # db = self._db or self.get_db()
        self.db[encoded_key] = encoded_value

    def __getitem__(self, key: str) -> Any:
        encoded_key = self._encode_key(key)
        try:
            encoded_value = self.db_r[encoded_key]
            try:
                self._decode_value(encoded_value)
            except ValueError:
                return ValueError(key)
        except KeyError:
            raise KeyError(key)

    def __contains__(self, key: str) -> bool:
        encoded_key = self._encode_key(key)
        if self._db_r:
            return encoded_key in self._db_r
        else:
            with self.get_db(read_only=True) as db:
                return encoded_key in db

    def clear(self) -> None:
        self.db.clear()

    def __len__(self) -> int:
        return len(self.db_r)

    def __iter__(self):
        yield from self.db_r.keys()

    def __delitem__(self, key: str) -> None:
        encoded_key = self._encode_key(key)
        if self._db:
            del self._db[encoded_key]
        else:
            with self.get_db() as db:
                del db[encoded_key]

    def _keys(self):
        if self._db_r:
            return self._db_r.keys()
        else:
            with self.get_db(read_only=True) as db:
                return db.keys()