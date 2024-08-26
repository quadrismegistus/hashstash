from . import *

class DiskCacheHashStash(BaseHashStash):
    engine = 'diskcache'
    string_keys = False

    @log.info
    def get_db(self):
        from diskcache import Cache
        os.makedirs(self.path_dirname, exist_ok=True)
        return Cache(self.path)