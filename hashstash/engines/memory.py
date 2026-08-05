# Explicit stdlib imports: this package's `from . import *` chains are
# circular, and whether a name has landed in the package namespace yet
# depends on import order (spawn workers + editable installs order imports
# differently). Never rely on the star-chain for stdlib names.
from contextlib import contextmanager

from . import *
from .base import BaseHashStash

SHARED_MEMORY_CACHE = None
def get_shared_memory_cache():
    global SHARED_MEMORY_CACHE
    if SHARED_MEMORY_CACHE is None:
        try:
            from UltraDict import UltraDict
            SHARED_MEMORY_CACHE = UltraDict(recursive=True)
        except ImportError:
            # 'memory' is a builtin engine: without ultradict it degrades to a
            # process-local dict instead of raising at first use
            log.debug(
                "ultradict is not installed; the memory engine is process-local "
                "(pip install ultradict for shared memory across processes)"
            )
            SHARED_MEMORY_CACHE = {}
    return SHARED_MEMORY_CACHE

class MemoryHashStash(BaseHashStash):
    engine = 'memory'
    ensure_dir = False
    needs_lock = False  # UltraDict provides its own shared-memory locking

    @contextmanager
    def get_connection(self):
        cache = get_shared_memory_cache()
        if not self.path in cache:
            cache[self.path] = {}
        yield cache[self.path]

    def clear(self):
        # cascade to function-result stashes like every other engine: this
        # override skipped children entirely, so memoized results survived their
        # parent's clear() on memory alone. A caller's sub() is a separate store
        # and is spared, matching the base engine.
        for sub in self.children:
            if getattr(sub, "is_function_stash", False):
                sub.clear()
        cache = get_shared_memory_cache()
        cache[self.path] = {}
        return self

    @property
    def filesize(self):
        return sum(bytesize(k) + bytesize(v) for k,v in self._items())