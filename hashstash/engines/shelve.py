from . import *

class ShelveHashStash(BaseHashStash):
    engine = 'shelve'
    string_keys = True
    needs_reconnect = True

    @log.debug
    @retry_patiently()
    def get_db(self, writeback=True):
        import shelve
        os.makedirs(self.path_dirname, exist_ok=True)
        return shelve.open(self.path, writeback=writeback)

    # gdbm (the default dbm backend on Linux before Python 3.13) takes an
    # exclusive lock per open handle. The base generator implementations hold a
    # handle open while the consuming loop body opens a second one (items() ->
    # iterate keys -> get_all per key), which gdbm rejects with EAGAIN.
    # Snapshot under a single handle, then yield with the handle closed.
    def _keys(self):
        with self as cache, cache.db as db:
            keys = list(db.keys())
        yield from keys

    def _values(self):
        with self as cache, cache.db as db:
            values = list(db.values())
        yield from values

    def _items(self):
        with self as cache, cache.db as db:
            items = list(db.items())
        yield from items
