from . import *

IN_MEMORY_CACHE = defaultdict(dict)

class MemoryDB(DictContext):
    def __init__(self, name):
        global IN_MEMORY_CACHE
        self.name = name
        self.data = IN_MEMORY_CACHE[name]

    def clear(self):
        global IN_MEMORY_CACHE
        IN_MEMORY_CACHE[self.name].clear()

class MemoryHashStash(BaseHashStash):
    engine = 'memory'
    ensure_dir = False

    @contextmanager
    def get_connection(self):
        yield MemoryDB(self.path)
    
    def clear(self):
        with self as cache, cache.db as db:
            db.clear()
        return self