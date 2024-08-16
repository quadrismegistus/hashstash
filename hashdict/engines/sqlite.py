from .base import BaseHashDict
from sqlitedict import SqliteDict
from functools import cached_property

class SqliteHashDict(BaseHashDict):
    engine = "sqlite"
    filename = "db.sqlite"

    def get_db(self):
        return SqliteDict(self.path, flag="c", autocommit=True)