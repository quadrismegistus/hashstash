from .base import *
from diskcache import Cache

class DiskCacheHashDict(BaseHashDict):
    engine = 'diskcache'
    filename = 'db.diskcache'
    string_keys = False

    def get_db(self):
        return DictContext(Cache(self.path))
    