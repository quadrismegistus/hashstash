from . import *

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
    filename_ext = ".db"
    filename_is_dir = False

    @log.debug
    def __init__(
        self,
        name: str = None,
        root_dir: str = None,
        dbname: str = None,
        compress: bool = None,
        b64: bool = None,
        serializer: Union[SERIALIZER_TYPES, List[SERIALIZER_TYPES]] = None,
        parent: "BaseHashStash" = None,
        children: List["BaseHashStash"] = None,
        **kwargs,
    ) -> None:
        self.name = name if name is not None else self.name
        self.compress = compress if compress is not None else config.compress
        self.b64 = b64 if b64 is not None else config.b64
        self.serializer = get_working_serializers(serializer if serializer is not None else config.serializer)
        self.root_dir = root_dir if root_dir is not None else self.root_dir
        self.dbname = dbname if dbname is not None else self.dbname
        self.parent = parent
        self.children = [] if not children else children
        subnames = [f"{self.engine}"]
        if self.compress:
            subnames += ["compressed"]
        if self.b64:
            subnames += ["b64"]
        if self.filename_ext:
            subnames += [get_fn_ext(self.filename_ext)]
        self.path = os.path.join(
            (
                self.name
                if os.path.isabs(self.name)
                else os.path.join(self.root_dir, self.name)
            ),
            self.dbname,
            ".".join(subnames),
        )
        self.path_dirname = self.path if self.filename_is_dir else os.path.dirname(self.path)
        if self.ensure_dir:
            os.makedirs(self.path_dirname, exist_ok=True)

    def encode(self, *args, **kwargs):
        return encode(*args, b64=self.b64, compress=self.compress, **kwargs)

    def decode(self, *args, **kwargs):
        return decode(*args, b64=self.b64, compress=self.compress, **kwargs)

    def serialize(self, *args, **kwargs):
        return serialize(*args, serializer=self.serializer, **kwargs)

    def deserialize(self, *args, **kwargs):
        return deserialize(*args, serializer=self.serializer, **kwargs)

    def to_dict(self):
        return {
            "engine": self.engine,
            "root_dir": self.root_dir,
            "compress": self.compress,
            "b64": self.b64,
            "name": self.name,
            "filename": self.filename,
            "dbname": self.dbname,
            "serializer": self.serializer,
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
        if self.ensure_dir:
            os.makedirs(self.path_dirname, exist_ok=True)
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

    @log.debug
    def __getitem__(self, unencoded_key: str) -> Any:
        obj = self.get(unencoded_key)
        if obj is None:
            raise KeyError(unencoded_key)
        return obj

    @log.debug
    def __setitem__(self, unencoded_key: str, unencoded_value: Any) -> None:
        self.set(unencoded_key, unencoded_value)

    @log.debug
    def get(self, unencoded_key: Any, default: Any = None) -> Any:
        unencoded_value = self._get(self.encode_key(unencoded_key))
        if unencoded_value is None:
            return default
        return self.decode_value(unencoded_value)

    @log.debug
    def set(self, unencoded_key: Any, unencoded_value: Any) -> None:
        self._set(
            self.encode_key(unencoded_key),
            self.encode_value(unencoded_value),
        )

    @log.debug
    def _get(self, encoded_key: str, default: Any = None) -> Any:
        with self as cache, cache.db as db:
            res = db.get(encoded_key)
            return res if res is not None else default

    @log.debug
    def _set(self, encoded_key: str, encoded_value: Any) -> None:
        with self as cache, cache.db as db:
            db[encoded_key] = encoded_value

    @log.debug
    def __contains__(self, unencoded_key: Any) -> bool:
        return self.has(unencoded_key)

    def has(self, unencoded_key: Any) -> bool:
        return self._has(self.encode_key(unencoded_key))

    @log.debug
    def encode_key(self, unencoded_key: Any) -> Union[str, bytes]:
        return self.encode(
            self.serialize(unencoded_key),
            as_string=self.string_keys,
        )

    @log.debug
    def encode_value(self, unencoded_value: Any) -> Union[str, bytes]:
        return self.encode(
            self.serialize(unencoded_value),
            as_string=self.string_values,
        )

    @log.debug
    def decode_key(self, encoded_key: Any) -> Union[str, bytes]:
        return self.deserialize(self.decode(encoded_key))

    @log.debug
    def decode_value(self, encoded_value: Any) -> Union[str, bytes]:
        return self.deserialize(self.decode(encoded_value))

    @log.debug
    def _has(self, encoded_key: Union[str, bytes]):
        with self as cache, cache.db as db:
            return encoded_key in db

    @log.debug
    def clear(self) -> None:
        with self as cache, cache.db as db:
            db.clear()

    def __len__(self) -> int:
        with self as cache, cache.db as db:
            return len(db)

    def __delitem__(self, unencoded_key: str) -> None:
        if not self.has(unencoded_key):
            raise KeyError(unencoded_key)
        self._del(self.encode_key(unencoded_key))

    def _del(self, encoded_key: Union[str, bytes]) -> None:
        with self as cache, cache.db as db:
            del db[encoded_key]

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
        for x in self._keys():
            try:
                yield self.decode_key(x)
            except Exception as e:
                log.error(f"Error decoding key: {e}")

    def values(self):
        for x in self._values():
            try:
                yield self.decode_value(x)
            except Exception as e:
                log.error(f"Error decoding value: {e}")

    def items(self):
        for k, v in self._items():
            try:
                yield self.decode_key(k), self.decode_value(v)
            except Exception as e:
                log.error(f"Error decoding item: {e}")
    def keys_l(self):
        return list(self.keys())

    def values_l(self):
        return list(self.values())

    def items_l(self):
        return list(self.items())

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

    def pop(self, unencoded_key, default=object):
        try:
            value = self[unencoded_key]
            del self[unencoded_key]
            return value
        except KeyError:
            if default is object:
                raise
            return default

    def popitem(self):
        key, value = next(iter(self.items()))
        del self[key]
        return value

    def hash(self, data: bytes) -> str:
        return encode_hash(data)

    @property
    def stashed_result(self):
        return stashed_result(stash=self)

    @cached_property
    def profiler(self):
        from ..profilers.engine_profiler import HashStashProfiler

        return HashStashProfiler(self)

    def sub(self, **kwargs):
        kwargs = {**self.to_dict(), **kwargs, 'parent': self}
        new_instance = self.__class__(**kwargs)
        new_instance._lock = threading.Lock()  # Create a new lock for the new instance
        self.children.append(new_instance)
        return new_instance

    def tmp(self, **kwargs):
        # return TemporaryHashStash(self, **kwargs)
        kwargs = {
            **kwargs,
            **dict(
                root_dir=tempfile.mkdtemp(),
                dbname=f"{uuid.uuid4().hex[:10]}",
                name='tmp'
            ),
        }
        return self.sub(**kwargs)

    def __repr__(self):
        # path = self.path.replace(DEFAULT_ROOT_DIR+'/','').replace(os.path.expanduser("~"), "~")
        path = self.path.replace(os.path.expanduser("~"), "~")
        # path = os.path.dirname(path)
        # path = f'{self.name}{"/" + self.dbname if self.dbname != DEFAULT_DBNAME else ""}'
        return f"""{self.__class__.__name__}({path})"""

    def __reduce__(self):
        # Return a tuple of (callable, args) that allows recreation of this object
        return (self.__class__.from_dict, (self.to_dict(),))
    
    def sub_function_results(
        self,
        func,
        dbname=None,
        update_on_src_change=False,
        **kwargs
    ):
        func_name = get_obj_addr(func)
        print([func_name, func, dbname, update_on_src_change])
        if update_on_src_change:
            func_name += "/" + encode_hash(get_function_src(func))[:10]
        stash = self.sub(dbname=f'{self.dbname}/{"stashed_result" if not dbname else dbname}/{func_name}')
        func.stash = stash
        return stash

    def assemble_ld(self, incl_func=False, incl_args=True, incl_kwargs=True):
        
        ld = []
        for k, v in progress_bar(self.items(), total=len(self), desc='assembling all data from stash'):
            ind = {}
            
            if type(k) is dict:
                if incl_func and 'func' in k:
                    ind['_func'] = get_obj_addr(k['func'])
                args = k.get('args', [])
                argd = {f'_arg{i+1}': arg for i, arg in enumerate(args)}
                kwargs = {f'_{k2}':v2 for k2,v2 in k.get('kwargs', {}).items()}
                attrd = {}
                if incl_args:
                    attrd.update(argd)
                if incl_kwargs:
                    attrd.update(kwargs)
                ind.update(attrd)
            else:
                ind['_key'] = k
            
            if isinstance(v, dict):
                row = {**ind, **v}
                ld.append(row)
            elif isinstance(v, list):
                for item in progress_bar(v, desc='iterating list',progress=False):
                    if isinstance(item, dict):
                        row = {**ind, **item}
                    else:
                        row = {**ind, 'result': item}
                    ld.append(row)
            elif is_dataframe(v):
                for _,item in progress_bar(v.iterrows(), total=len(v),desc='iterating dataframe result',progress=False):
                    row = {**ind, **dict(item)}
                    ld.append(row)
            else:
                row = {**ind, 'result': v}
                ld.append(row)
        
        return ld
    
    def assemble_df(self, **kwargs):
        import pandas as pd
        ld = self.assemble_ld(**kwargs)
        if not ld: return pd.DataFrame()
        df=pd.DataFrame(ld)
        index = [k for k in df if k.startswith('_')]
        return df#.set_index(index).sort_index()
        
    
    @property
    def df(self):
        return self.assemble_df()
    
    def __hash__(self):
        # Use a combination of class name and path for hashing
        return hash(tuple(sorted(self.to_dict().items())))




# @fcache
# @log.info
def HashStash(
    name: str = None,
    engine: ENGINE_TYPES = None,
    dbname: str = None,
    compress: bool = None,
    b64: bool = None,
    serializer: Union[SERIALIZER_TYPES, List[SERIALIZER_TYPES]] = None,
    **kwargs,
) -> "BaseHashStash":
    """
    Factory function to create the appropriate cache object.

    Args:
        * args: Additional arguments to pass to the cache constructor.
        engine: The type of cache to create ("pairtree", "sqlite", "memory", "shelve", "redis", "pickledb", or "diskcache")
        **kwargs: Additional keyword arguments to pass to the cache constructor.

    Returns:
        An instance of the appropriate BaseHashStash subclass.

    Raises:
        ValueError: If an invalid engine is provided.
    """
    log.info(f'HashStash({name}/{dbname})')

    engine = engine if engine is not None else config.engine

    if engine == "pairtree":
        from .pairtree import PairtreeHashStash
        cls = PairtreeHashStash
    elif engine == "sqlite":
        from ..engines.sqlite import SqliteHashStash

        cls = SqliteHashStash
    elif engine == "memory":
        from ..engines.memory import MemoryHashStash

        cls = MemoryHashStash
    elif engine == "shelve":
        from ..engines.shelve import ShelveHashStash

        cls = ShelveHashStash
    elif engine == "redis":
        from ..engines.redis import RedisHashStash

        cls = RedisHashStash
    elif engine == "pickledb":
        from ..engines.pickledb import PickleDBHashStash

        cls = PickleDBHashStash
    elif engine == "diskcache":
        from ..engines.diskcache import DiskCacheHashStash

        cls = DiskCacheHashStash
    elif engine == "lmdb":
        from ..engines.lmdb import LMDBHashStash
        cls = LMDBHashStash
    else:
        raise ValueError(f"Invalid engine: {engine}. Options: {', '.join(ENGINES)}.")
    
    object = cls(
        name=name,
        compress=compress,
        b64=b64,
        serializer=serializer,
        dbname=dbname,
        **kwargs
    )
    return object

Stash = HashStash