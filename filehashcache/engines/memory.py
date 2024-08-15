from typing import Any
from .base import BaseHashCache

IN_MEMORY_CACHE = {}

class MemoryHashCache(BaseHashCache):
    engine = 'memory'
    filename = 'in_memory'

    def get_db(self):
        global IN_MEMORY_CACHE
        return IN_MEMORY_CACHE