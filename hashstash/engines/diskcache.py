from .base import *
from diskcache import Cache

class DiskCacheHashStash(BaseHashStash):
    engine = 'diskcache'
    filename = 'db.diskcache'
    string_keys = False

    def get_db(self):
        return DictContext(Cache(self.path))
    