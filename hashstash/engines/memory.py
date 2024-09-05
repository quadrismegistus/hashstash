from . import *
from .base import get_manager, BaseHashStash
from multiprocessing import Manager

# # Use the existing get_manager function
# manager = Manager()

SHARED_MEMORY_CACHE = None
def get_shared_memory_cache():
    global SHARED_MEMORY_CACHE
    if SHARED_MEMORY_CACHE is None:
        from UltraDict import UltraDict
        SHARED_MEMORY_CACHE = UltraDict(recursive=True)
    return SHARED_MEMORY_CACHE

class MemoryHashStash(BaseHashStash):
    engine = 'memory'
    ensure_dir = False

    @contextmanager
    def get_connection(self):
        cache = get_shared_memory_cache()
        if not self.path in cache:
            cache[self.path] = {}
        yield cache[self.path]

    def clear(self):
        cache = get_shared_memory_cache()
        cache[self.path] = {}
        return self

    @property
    def filesize(self):
        return sum(bytesize(k) + bytesize(v) for k,v in self._items())