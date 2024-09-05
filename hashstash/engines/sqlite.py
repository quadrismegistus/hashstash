from . import *
import sqlite3
import os

class SqliteHashStash(BaseHashStash):
    engine = "sqlite"
    _db = None
    needs_reconnect = True

    @log.debug
    @retry_patiently()
    def get_db(self):
        log.debug(f'Path exists: {os.path.exists(self.path)}\nPath: {self.path}')
        log.debug(f'Directory exists: {os.path.exists(self.path_dirname)}\nDirectory: {self.path_dirname}')
        
        # Ensure the directory exists
        os.makedirs(self.path_dirname, exist_ok=True)
        
        from sqlitedict import SqliteDict
        
        if self._db is None or not os.path.exists(self.path):
            log.debug("Creating new SqliteDict instance")
            self._db = SqliteDict(self.path, flag='c', autocommit=True)
        
        return self._db