from . import *

class ShelveHashStash(BaseHashStash):
    engine = 'shelve'
    string_keys = True
    needs_reconnect = True

    @log.debug
    @retry_patiently()
    def get_db(self, writeback=True):
        import shelve
        os.makedirs(self.path_dirname, exist_ok=True)
        return shelve.open(self.path, writeback=writeback)
    