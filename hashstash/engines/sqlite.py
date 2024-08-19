from .base import *
from sqlitedict import SqliteDict
from functools import cached_property

class SqliteHashStash(BaseHashStash):
    engine = "sqlite"
    filename = "db.sqlite"

    def get_db(self):
        return SqliteDict(self.path, flag="c", autocommit=True)