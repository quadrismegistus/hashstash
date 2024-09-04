from . import *
import sqlite3
import os

class SqliteHashStash(BaseHashStash):
    engine = "sqlite"
    _db = None

    @log.debug
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

    # def _set(self, encoded_key, encoded_value):
    #     try:
    #         with self as cache, cache.db as db:
    #             db[encoded_key] = encoded_value
    #     except Exception as e:
    #         log.error(f"Error in _set: {e}")
    #         log.error(f"File exists: {os.path.exists(self.path)}")
    #         if not os.path.exists(self.path):
    #             log.info("Recreating database file")
    #             self._db = None  # Force recreation of SqliteDict instance
    #         raise

    # def clear(self):
    #     super().clear()
    #     self._db = None  # Reset the database connection after clearing

    # def __del__(self):
    #     if self._db is not None:
    #         self._db.close()
