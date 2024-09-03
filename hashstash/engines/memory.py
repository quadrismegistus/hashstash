from . import *
from .base import get_manager, BaseHashStash
from multiprocessing import Manager

# Use the existing get_manager function
manager = get_manager()

# Create a shared dictionary
SHARED_MEMORY_CACHE = manager.dict()

class MemoryDB:
    def __init__(self, name):
        self.name = name

    def connect(self):
        self._manager = get_manager()
        if not hasattr(self._manager, 'shared_memory_cache'):
            self._manager.shared_memory_cache = self._manager.dict()
        if self.name not in self._manager.shared_memory_cache:
            self._manager.shared_memory_cache[self.name] = self._manager.dict()
        self.data = self._manager.shared_memory_cache[self.name]

    def clear(self):
        self.data.clear()

    # def __iter__(self):
    #     print(self.data,'???')
    #     return iter(self.data)

    # def __len__(self):
    #     return len(self.data)

    # def __getitem__(self, key):
    #     return self.data[key]

    # def __setitem__(self, key, value):
    #     self.data[key] = value

    # def __delitem__(self, key):
    #     del self.data[key]

    # def get(self, key, default=None):
    #     return self.data.get(key, default)

class MemoryHashStash(BaseHashStash):
    engine = 'memory'
    ensure_dir = False

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._db = MemoryDB(self.path)

    @contextmanager
    def get_connection(self):
        print([self._db._manager,self._db])
        yield self._db.data
    
    def clear(self):
        self._db.clear()
        return self

    # def _keys(self):
    #     print(self._db.data,'???')
    #     return iter(self._db.data)

    # def _values(self):
    #     return iter(self._db.data.values())

    # def _items(self):
    #     return iter(self._db.data.items())

    # def __len__(self):
    #     return len(self._db)

    # def __getitem__(self, key):
    #     return self._db[key]

    # def __setitem__(self, key, value):
    #     self._db[key] = value

    # def __delitem__(self, key):
    #     del self._db[key]

    # def get(self, key, default=None):
    #     return self._db.get(key, default)