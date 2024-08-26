from . import *
import sqlite3

class SqliteHashStash(BaseHashStash):
    engine = "sqlite"

    @log.info
    def get_db(self):
        log.debug(f'Path exists: {os.path.exists(self.path)}\nPath: {self.path}')
        log.debug(f'Directory exists: {os.path.exists(self.path_dirname)}\nDirectory: {self.path_dirname}')
        
        # Ensure the directory exists
        os.makedirs(self.path_dirname, exist_ok=True)
        
        from sqlitedict import SqliteDict
        return SqliteDict(self.path, flag='c', autocommit=True, timeout=30)  # Added timeout
