from . import *
from .base import get_manager

# Use the existing get_manager function
manager = get_manager()

# Create a shared dictionary
SHARED_MEMORY_CACHE = manager.dict()

class MemoryDB(DictContext):
    def __init__(self, name):
        self.name = name
        if self.name not in SHARED_MEMORY_CACHE:
            SHARED_MEMORY_CACHE[self.name] = manager.dict()
        self.data = SHARED_MEMORY_CACHE[self.name]

    def clear(self):
        self.data.clear()

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