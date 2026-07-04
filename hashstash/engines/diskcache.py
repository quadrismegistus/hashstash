# Explicit stdlib imports: this package's `from . import *` chains are
# circular, and whether a name has landed in the package namespace yet
# depends on import order (spawn workers + editable installs order imports
# differently). Never rely on the star-chain for stdlib names.
import os

from . import *

class DiskCacheHashStash(BaseHashStash):
    engine = 'diskcache'
    string_keys = False
    needs_lock = False  # diskcache is process- and thread-safe by design

    @log.debug
    def get_db(self):
        from diskcache import Cache
        os.makedirs(self.path_dirname, exist_ok=True)
        # eviction_policy='none' disables diskcache's default 1 GB LRS eviction —
        # no other engine silently drops entries, so this one must not either
        return Cache(self.path, eviction_policy="none")