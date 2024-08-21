from . import *

class DiskCacheHashStash(BaseHashStash):
    engine = 'diskcache'
    string_keys = False

    def get_db(self):
        from diskcache import Cache
        return DictContext(Cache(self.path))