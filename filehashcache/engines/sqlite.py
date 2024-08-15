from .base import BaseHashCache
from sqlitedict import SqliteDict
from functools import cached_property

class SqliteHashCache(BaseHashCache):
    engine = "sqlite"
    filename = "db.sqlitedict"

    def get_db(self):
        return SqliteDict(self.path, flag="c", autocommit=True)