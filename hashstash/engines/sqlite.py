from . import *

class SqliteHashStash(BaseHashStash):
    engine = "sqlite"

    @log.debug
    def get_db(self):
        log.info(f'{os.path.exists(self.path)}\n{self.path}')
        log.info(f'{os.path.exists(self.path_dirname)}\n{self.path_dirname}')
        from sqlitedict import SqliteDict
        return SqliteDict(self.path, flag="c", autocommit=True)