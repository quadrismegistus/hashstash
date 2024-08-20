from . import *

class SqliteHashStash(BaseHashStash):
    engine = "sqlite"

    def get_db(self):
        from sqlitedict import SqliteDict
        return SqliteDict(self.path, flag="c", autocommit=True)