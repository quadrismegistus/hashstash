from ..hashstash import *
from ..utils.encodings import encode, decode, encode_hash
from ..serialize import serialize, deserialize
from collections.abc import MutableMapping
from functools import partial
import threading


class BaseHashStash(MutableMapping):
    engine = "base"
    name = DEFAULT_NAME
    filename = DEFAULT_FILENAME
    dbname = DEFAULT_DBNAME
    compress = DEFAULT_COMPRESS
    b64 = DEFAULT_B64
    ensure_dir = True
    string_keys = False
    string_values = False
    serializer = DEFAULT_SERIALIZER
    root_dir = DEFAULT_ROOT_DIR

    @debug
    def __init__(
        self,
        name: str = DEFAULT_NAME,
        root_dir: str = DEFAULT_ROOT_DIR,
        compress: bool = DEFAULT_COMPRESS,
        b64: bool = DEFAULT_B64,
        serializer: Literal[
            BUILTIN_SERIALIZER,
            JSONPICKLE_SERIALIZER,
        ] = DEFAULT_SERIALIZER,
        **kwargs,
    ) -> None:
        self.name = name
        self.compress = compress
        self.b64 = b64
        self.serializer = serializer
        self.root_dir = root_dir
        self.path = os.path.join(
            (self.name if os.path.isabs(self.name) else os.path.join(self.root_dir, self.name)),
            self.engine+'_engine',
            self.serializer+'_serializer',
            self.filename,
        )
        print([self.ensure_dir, ensure_dir,self.path])
        if self.ensure_dir:
            print(self.path)
            ensure_dir(self.path)
            print(os.path.exists(os.path.dirname(self.path)))

    def encode(self, *args, **kwargs):
        return encode(*args, b64=self.b64, compress=self.compress, **kwargs)

    def decode(self, *args, **kwargs):
        return decode(*args, b64=self.b64, compress=self.compress, **kwargs)

    def serialize(self, *args, **kwargs):
        return serialize(*args, method=self.serializer, **kwargs)

    def deserialize(self, *args, **kwargs):
        return deserialize(*args, method=self.serializer, **kwargs)

    def to_dict(self):
        return {
            "engine": self.engine,
            "root_dir": self.root_dir,
            "compress": self.compress,
            "b64": self.b64,
            "name": self.name,
            "filename": self.filename,
            "dbname": self.dbname,
        }

    @staticmethod
    def from_dict(d: dict):
        return HashStash(**d)

    @cached_property
    def _lock(self):
        return threading.Lock()

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
        if not self._lock.locked():
            self._lock.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._lock.locked():
            self._lock.release()

    @debug
    def __getitem__(self, unencoded_key: str) -> Any:
        obj = self.get(unencoded_key)
        if obj is None:
            raise KeyError(unencoded_key)
        return obj

    @debug
    def __setitem__(self, unencoded_key: str, unencoded_value: Any) -> None:
        self.set(unencoded_key, unencoded_value)

    @debug
    def get(self, unencoded_key: Any, default: Any = None) -> Any:
        unencoded_value = self._get(self.encode_key(unencoded_key))
        if unencoded_value is None:
            return default
        return self.decode_value(unencoded_value)

    @debug
    def set(self, unencoded_key: Any, unencoded_value: Any) -> None:
        self._set(
            self.encode_key(unencoded_key),
            self.encode_value(unencoded_value),
        )

    @debug
    def _get(self, encoded_key: str, default: Any = None) -> Any:
        with self as cache, cache.db as db:
            return db.get(encoded_key, default)

    @debug
    def _set(self, encoded_key: str, encoded_value: Any) -> None:
        with self as cache, cache.db as db:
            db[encoded_key] = encoded_value

    @debug
    def __contains__(self, unencoded_key: Any) -> bool:
        return self.has(unencoded_key)

    def has(self, unencoded_key: Any) -> bool:
        return self._has(self.encode_key(unencoded_key))

    @debug
    def encode_key(self, unencoded_key: Any) -> Union[str, bytes]:
        return self.encode(
            self.serialize(unencoded_key),
            as_string=self.string_keys,
        )

    @debug
    def encode_value(self, unencoded_value: Any) -> Union[str, bytes]:
        return self.encode(
            self.serialize(unencoded_value),
            as_string=self.string_values,
        )

    @debug
    def decode_key(self, encoded_key: Any) -> Union[str, bytes]:
        return self.deserialize(self.decode(encoded_key))

    @debug
    def decode_value(self, encoded_value: Any) -> Union[str, bytes]:
        return self.deserialize(self.decode(encoded_value))

    @debug
    def _has(self, encoded_key: Union[str, bytes]):
        with self as cache, cache.db as db:
            return encoded_key in db

    @debug
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

    def keys(self):
        return (self.decode_key(x) for x in self._keys())

    def values(self):
        return (self.decode_value(x) for x in self._values())

    def items(self):
        return ((self.decode_key(k), self.decode_value(v)) for k, v in self._items())

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
        from ..profiler.engine_profiler import HashStashProfiler

        return HashStashProfiler(self)

    def sub(self, **kwargs):
        new_instance = self.__class__(**{**self.__dict__, **kwargs})
        new_instance._lock = threading.Lock()  # Create a new lock for the new instance
        return new_instance

    def tmp(self, **kwargs):
        return TemporaryHashStash(self, **kwargs)

    def __repr__(self):
        path = str(self.path).replace(os.path.expanduser("~"), "~")
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
            ),
        }
        self.stash = self.base_stash.sub(**kwargs)
        return self.stash

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.stash:
            threading.Thread(
                target=shutil.rmtree, args=(self.stash.root_dir,), daemon=True
            ).start()
        self.stash = None
