from ..hashstash import *
from ..utils.encodings import encode, decode, encode_hash
from collections.abc import MutableMapping
from functools import partial
import threading

class BaseHashStash(MutableMapping):
    engine = 'base'

    root_dir = DEFAULT_ROOT_DIR
    name = DEFAULT_NAME
    filename = "db"
    dbname = "unnamed"
    compress = DEFAULT_COMPRESS
    b64 = DEFAULT_B64
    ensure_dir = True
    string_keys = False
    string_values = False

    def __init__(
        self,
        name: str = None,
        root_dir: str = None,
        compress: bool = None,
        b64: bool = None,
        filename: str = None,
        dbname: str = None,
        **kwargs
    ) -> None:
        if name is not None:
            self.name = name
        if root_dir is not None:
            self.root_dir = root_dir
        if compress is not None:
            self.compress = compress
        if b64 is not None:
            self.b64 = b64
        if filename is not None:
            self.filename = filename
        if dbname is not None:
            self.dbname = dbname

        self.path = os.path.abspath(os.path.join(self.root_dir, self.name, self.filename))
        fn,ext=os.path.splitext(self.path)
        if ext:
            self.dir = os.path.dirname(self.path)
        else:
            self.dir = self.path
        
        if self.ensure_dir:
            os.makedirs(self.dir, exist_ok=True)

        self.encode_key = partial(encode, b64=self.b64, compress=self.compress, as_string=self.string_keys)
        self.decode_key = partial(decode, b64=self.b64, compress=self.compress, as_string=self.string_keys)

        self.encode = self.encode_value = partial(encode, b64=self.b64, compress=self.compress, as_string=self.string_values)
        self.decode = self.decode_value = partial(decode, b64=self.b64, compress=self.compress, as_string=self.string_values)

    def to_dict(self):
        return {
            'engine':self.engine,
            'root_dir': self.root_dir,
            'compress': self.compress,
            'b64': self.b64,
            'name': self.name,
            'filename': self.filename,
            'dbname': self.dbname,
        }
    
    @staticmethod
    def from_dict(d:dict):
        return HashStash(**d)

    @cached_property
    def _lock(self): return threading.Lock()

    @property
    @retry_patiently()
    def db(self):
        return self.get_db()
    
    @property
    def data(self):
        return self.db
    
    def get_db(self):
        return {}

    def __enter__(self):
        if not self._lock.locked(): self._lock.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._lock.locked(): self._lock.release()

    def __setitem__(self, key: str, value: Any) -> None:
        with self as cache, cache.db as db:
            db[self.encode_key(key)] = self.encode(value)

    def __getitem__(self, key: str) -> Any:
        with self as cache, cache.db as db:
            res = db.get(self.encode_key(key))
            if res is None:
                raise KeyError(key)
            return self.decode_value(res)
    

    def __contains__(self, key: str) -> bool:
        with self as cache, cache.db as db:
            return self.encode_key(key) in db

    def clear(self) -> None:
        with self as cache, cache.db as db:
            db.clear()

    def __len__(self) -> int:
        with self as cache, cache.db as db:
            return len(db)

    def __delitem__(self, key: str) -> None:
        with self as cache, cache.db as db:
            del db[self.encode_key(key)]

    def _keys(self):
        with self as cache, cache.db as db:
            for k in db:
                yield k

    def _values(self):
        with self as cache, cache.db as db:
            for k in db:
                yield db[k]
                
    
    def _items(self):
        with self as cache, cache.db as db:
            for k in db:
                yield k, db[k]

    def get(self, key: Any, default: Any = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default

    def keys(self):
        return (self.decode_key(x) for x in self._keys())

    def values(self):
        return (self.decode_value(x) for x in self._values())
    
    def items(self):
        return ((self.decode_key(k), self.decode_value(v)) for k,v in self._items())

    def __iter__(self):
        return self.keys()
    

    def __iter__(self):
        return self.keys()

    def copy(self):
        return dict(self.items())

    def update(self, other=None, **kwargs):
        if other is not None:
            if hasattr(other, "keys"):
                for key in other:
                    self[key] = other[key]
            else:
                for key, value in other:
                    self[key] = value
        for key, value in kwargs.items():
            self[key] = value

    def setdefault(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            self[key] = default
            return default

    def pop(self, key, default=None):
        try:
            value = self[key]
            del self[key]
            return value
        except KeyError:
            return default

    def popitem(self):
        key, value = next(iter(self.items()))
        del self[key]
        return value

    
    def hash(self, data: bytes) -> str:
        return encode_hash(data)

    @property
    def cached_result(self):
        return cached_result(cache=self)

    @cached_property
    def profiler(self):
        from ..etc.profiler import HashStashProfiler
        return HashStashProfiler(self)
    
    def sub(self, **kwargs):
        new_instance = self.__class__(**{**self.__dict__, **kwargs})
        new_instance._lock = threading.Lock()  # Create a new lock for the new instance
        return new_instance
    
    def tmp(self, **kwargs):
        return TemporaryHashStash(self, **kwargs)

    def __repr__(self):
        path = str(self.path).replace(os.path.expanduser('~'), '~')
        # return f"""{self.__class__.__name__}(name="{self.name}", path="{path}")"""
        return f"""{self.__class__.__name__}({path})"""

    def __reduce__(self):
        # Return a tuple of (callable, args) that allows recreation of this object
        return (self.__class__.from_dict, (self.to_dict(),))
    

class TemporaryHashStash(BaseHashStash):
    def __init__(self, base_stash, **kwargs):
        self.base_stash = base_stash
        self.kwargs = kwargs
        self.stash = None

    def __enter__(self):
        kwargs = {
            **self.kwargs,
            **dict(
                root_dir=tempfile.mkdtemp(),
                name=f"tmp_{uuid.uuid4().hex[:10]}",
            )
        }
        self.stash = self.base_stash.sub(**kwargs)
        return self.stash

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.stash:
            threading.Thread(target=shutil.rmtree, args=(self.stash.root_dir,), daemon=True).start()
        self.stash = None
