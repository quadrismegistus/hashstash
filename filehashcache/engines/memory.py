from typing import Any
from .base import BaseHashCache
from collections import defaultdict, UserDict

IN_MEMORY_CACHE = defaultdict(dict)

class MemoryDB(UserDict):
    def __init__(self, name):
        global IN_MEMORY_CACHE
        self.name = name
        self.data = IN_MEMORY_CACHE[name]

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass  # Nothing happens on close

    def clear(self):
        global IN_MEMORY_CACHE
        IN_MEMORY_CACHE[self.name].clear()

class MemoryHashCache(BaseHashCache):
    engine = 'memory'
    ensure_dir = False

    def get_db(self):
        return MemoryDB(self.name)