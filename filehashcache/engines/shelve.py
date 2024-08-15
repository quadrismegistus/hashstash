import shelve
from typing import Any
from .base import BaseHashCache
from functools import lru_cache
fcache = lru_cache(maxsize=None)

SHELVE_DB = {}


class ShelveHashCache(BaseHashCache):
    engine = 'shelve'
    filename = 'db.shelve'

    def get_db(self, writeback=True):
        global SHELVE_DB
        if SHELVE_DB is None:
            SHELVE_DB = shelve.open(self.path, writeback=writeback)
        return SHELVE_DB