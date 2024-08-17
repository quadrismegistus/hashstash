from .base import *

IN_MEMORY_CACHE = defaultdict(dict)

class MemoryDB(DictContext):
    def __init__(self, name):
        global IN_MEMORY_CACHE
        self.name = name
        self.data = IN_MEMORY_CACHE[name]

    def clear(self):
        global IN_MEMORY_CACHE
        IN_MEMORY_CACHE[self.name].clear()

class MemoryHashDict(BaseHashDict):
    engine = 'memory'
    ensure_dir = False

    def get_db(self):
        return MemoryDB(self.name)