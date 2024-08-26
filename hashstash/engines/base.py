from . import *
import time


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
    to_dict_attrs = [
        "engine",
        "name",
        "compress",
        "b64",
        "serializer",
        "root_dir",
        "dbname",
        "is_function_stash",
        "is_tmp",
        "append_mode",
    ]
    metadata_cols = ["_version"]

    @log.debug
    def __init__(
        self,
        name: str = None,
        root_dir: str = None,
        dbname: str = None,
        compress: bool = None,
        b64: bool = None,
        serializer: SERIALIZER_TYPES = None,
        parent: "BaseHashStash" = None,
        children: List["BaseHashStash"] = None,
        is_function_stash=None,
        is_tmp=None,
        append_mode: bool = False,
        **kwargs,
    ) -> None:
        self.name = name if name is not None else self.name
        self.compress = compress if compress is not None else config.compress
        self.b64 = b64 if b64 is not None else config.b64
        self.serializer = serializer if serializer is not None else config.serializer
        self.root_dir = root_dir if root_dir is not None else self.root_dir
        self.dbname = dbname if dbname is not None else self.dbname
        self.parent = parent
        self.children = [] if not children else children
        self.is_function_stash = is_function_stash
        self.is_tmp = is_tmp
        self.append_mode = append_mode
        encstr = "+".join(
            filter(
                None, ["zlib" if self.compress else None, "b64" if self.b64 else None]
            )
        )
        subnames = [f"{self.engine}", self.serializer]
        if encstr:
            subnames.append(encstr)
        if self.filename_ext:
            subnames.append(get_fn_ext(self.filename_ext))
        # print('subnames',subnames)
        self.path = os.path.join(
            (
                self.name
                if os.path.isabs(self.name)
                else os.path.join(self.root_dir, self.name)
            ),
            self.dbname,
            "dbs",
            ".".join(subnames),
        )
        self.path_dirname = (
            self.path if self.filename_is_dir else os.path.dirname(self.path)
        )
        # if self.ensure_dir:
        # os.makedirs(self.path_dirname, exist_ok=True)

        # if self.is_tmp:
        # self.register_cleanup()

    # def register_cleanup(self):
    # def cleanup():
    # self._remove_dir(self.path)

    # atexit.register(cleanup)

    @staticmethod
    def _remove_dir(dir_path):
        def remove():
            rmtreefn(dir_path)

        remove()
        # import subprocess
        # if os.path.isfile(dir_path):
        #     subprocess.Popen(['python', '-c', f'import os; os.remove("{dir_path}")'])
        # else:
        #     subprocess.Popen(['python', '-c', f'import shutil; shutil.rmtree("{dir_path}")'])

    @log.debug
    def encode(self, *args, **kwargs):
        return encode(*args, b64=self.b64, compress=self.compress, **kwargs)

    @log.debug
    def decode(self, *args, **kwargs):
        return decode(*args, b64=self.b64, compress=self.compress, **kwargs)

    @log.debug
    def serialize(self, *args, **kwargs):
        return serialize(*args, serializer=self.serializer, **kwargs)

    @log.debug
    def deserialize(self, *args, **kwargs):
        return deserialize(*args, serializer=self.serializer, **kwargs)

    @log.debug
    def to_dict(self):
        """

        self.name = name if name is not None else self.name
        self.compress = compress if compress is not None else config.compress
        self.b64 = b64 if b64 is not None else config.b64
        self.serializer = serializer if serializer is not None else config.serializer
        self.root_dir = root_dir if root_dir is not None else self.root_dir
        self.dbname = dbname if dbname is not None else self.dbname
        self.parent = parent
        self.children = [] if not children else children
        self.is_function_stash = is_function_stash
        self.is_tmp = is_tmp
        self.append_mode = append_mode

        Returns:
            _description_
        """
        d = {}
        for attr in self.to_dict_attrs:
            d[attr] = getattr(self, attr)
        return d

    @staticmethod
    def from_dict(d: dict):
        obj = HashStash(
            **{k: tuple(v) if isinstance(v, list) else v for k, v in d.items()}
        )
        return obj

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

    def __eq__(self, other):
        if not isinstance(other, BaseHashStash):
            return NotImplemented
        return self.to_dict() == other.to_dict()

    @log.debug
    def __enter__(self):
        if not self._lock.locked():
            self._lock.acquire()
        return self

    @log.debug
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._lock.locked():
            self._lock.release()
        # if self.is_tmp:
        # self._remove_dir(self.path)

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
    def get(
        self,
        unencoded_key: Any = None,
        default: Any = None,
        *args,
        as_function=None,
        with_metadata=False,
        as_string=False,
        all_results=False,
        **kwargs,
    ) -> Any:
        values = self.get_all(
            unencoded_key,
            *args,
            default=None,
            with_metadata=with_metadata,
            as_function=as_function,
            all_results=all_results,
            **kwargs,
        )
        value = values[-1] if values else default
        return self.serialize(value) if as_string else value

    @log.debug
    def get_all(
        self,
        unencoded_key: Any = None,
        *args,
        default: Any = None,
        as_function: bool = None,
        with_metadata: bool = None,
        all_results: bool = True,
        **kwargs,
    ) -> Any:
        unencoded_key = self.new_unencoded_key(
            unencoded_key,
            *args,
            as_function=as_function,
            **kwargs,
        )
        encoded_key = self.encode_key(unencoded_key)
        encoded_value = self._get(encoded_key)
        if encoded_value is None:
            return default

        values = self.decode_value(encoded_value)
        if with_metadata:
            values = [
                {"_version": vi + 1, "_value": value}
                for vi, value in enumerate(values)
            ]
        if not self._all_results(all_results):
            values = values[-1:]
        return values

    @staticmethod
    def get_func_key(*args, **kwargs):
        if args and args[0] == None:
            args = args[1:]
        return {"args": tuple(args), "kwargs": kwargs}

    @log.debug
    def set(self, unencoded_key: Any, unencoded_value: Any, append=None) -> None:
        encoded_key = self.encode_key(unencoded_key)
        new_unencoded_value = self.new_unencoded_value(
            unencoded_value,
            unencoded_key=unencoded_key,
            append=append,
        )
        encoded_value = self.encode_value(new_unencoded_value)
        self._set(encoded_key, encoded_value)

    @log.info
    def new_unencoded_key(self, unencoded_key: Any, *args, as_function=None, **kwargs):
        if self.is_function_stash and as_function is not False:
            if not (
                isinstance(unencoded_key, dict)
                and set(unencoded_key.keys()) == {"args", "kwargs"}
            ):
                unencoded_key = self.get_func_key(unencoded_key, *args, **kwargs)
        return unencoded_key

    @log.debug
    def new_unencoded_value(
        self,
        unencoded_value: Any,
        unencoded_key=None,
        append=None,
    ):
        if (append or self.append_mode) and unencoded_key is not None:
            oldvals = self.get_all(
                unencoded_key, all_results=True, default=[], with_metadata=False
            )
            new_unencoded_value = oldvals + [unencoded_value]
        else:
            new_unencoded_value = [unencoded_value]
        return new_unencoded_value

    @log.debug
    def _get(self, encoded_key: str, default: Any = None) -> Any:
        log.info(f'{self}: {hasattr(self,"__enter__")}')
        with self as cache, cache.db as db:
            res = db.get(encoded_key)
            return res if res is not None else default

    @log.debug
    def _set(self, encoded_key: str, encoded_value: Any) -> None:
        try:
            with self as cache, cache.db as db:
                db[encoded_key] = encoded_value
        except Exception as e:
            log.error(f"Failed to set key {encoded_key}: {e}")

    @log.debug
    def __contains__(self, unencoded_key: Any) -> bool:
        return self.has(unencoded_key)

    @log.debug
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
    def decode_key(self, encoded_key: Any, as_string=False) -> Union[str, bytes]:
        decoded_key = self.decode(encoded_key)
        return (
            self.deserialize(decoded_key)
            if not as_string
            else decoded_key.decode("utf-8")
        )

    @log.debug
    def decode_value(
        self,
        encoded_value: Any,
        as_string=False,
    ) -> Union[str, bytes, dict, list]:
        decoded_value = self.decode(encoded_value)
        return (
            self.deserialize(decoded_value)
            if not as_string
            else decoded_value.decode("utf-8")
        )

    @log.debug
    def _has(self, encoded_key: Union[str, bytes]):
        with self as cache, cache.db as db:
            return encoded_key in db

    @log.debug
    def clear(self) -> None:
        self._remove_dir(
            self.root_dir
            if str(self.root_dir).startswith("/var/")
            or str(self.root_dir).startswith("/private/var/")
            else self.path
        )

    @log.debug
    def __len__(self) -> int:
        with self as cache, cache.db as db:
            return len(db)

    @log.debug
    def __delitem__(self, unencoded_key: str) -> None:
        if not self.has(unencoded_key):
            raise KeyError(unencoded_key)
        self._del(self.encode_key(unencoded_key))

    @log.debug
    def _del(self, encoded_key: Union[str, bytes]) -> None:
        with self as cache, cache.db as db:
            del db[encoded_key]

    @log.debug
    def _keys(self):
        with self as cache, cache.db as db:
            for k in db:
                yield k

    @log.debug
    def _values(self):
        with self as cache, cache.db as db:
            for k in db:
                yield db[k]

    @log.debug
    def _items(self):
        with self as cache, cache.db as db:
            for k in db:
                yield k, db[k]

    def _all_results(self, all_results=None):
        return all_results if all_results is not None else self.append_mode

    @log.debug
    def keys(self, as_string=False):
        for x in self._keys():
            try:
                yield self.decode_key(x, as_string=as_string)
            except Exception as e:
                log.error(f"Error decoding key: {e}")
                raise e

    @log.debug
    def values(self, all_results=None, with_metadata=False, **kwargs):
        for k, v in self.items(all_results=all_results, with_metadata=with_metadata):
            yield v

    @log.debug
    def items(self, all_results=None, with_metadata=False, **kwargs):
        for key in self.keys():
            vals = self.get_all(
                key,
                all_results=all_results,
                with_metadata=with_metadata,
                **kwargs,
            )
            for val in vals:
                yield key, val

    @log.debug
    def keys_l(self, **kwargs):
        return list(self.keys(**kwargs))

    @log.debug
    def values_l(self, **kwargs):
        return list(self.values(**kwargs))

    @log.debug
    def items_l(self, **kwargs):
        return list(self.items(**kwargs))

    @log.debug
    def __iter__(self):
        return self.keys()

    @log.debug
    def copy(self):
        return dict(self.items())

    @log.debug
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

    @log.debug
    def setdefault(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            self[key] = default
            return default

    @log.debug
    def pop(self, unencoded_key, default=object):
        try:
            value = self[unencoded_key]
            del self[unencoded_key]
            return value
        except KeyError:
            if default is object:
                raise
            return default

    @log.debug
    def popitem(self):
        key, value = next(iter(self.items()))
        del self[key]
        return value

    @log.debug
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
        kwargs = {**self.to_dict(), **kwargs, "parent": self}
        new_instance = self.__class__(**kwargs)
        new_instance._lock = threading.Lock()  # Create a new lock for the new instance
        self.children.append(new_instance)
        return new_instance

    @contextmanager
    def tmp(self, use_tempfile=True, **kwargs):
        kwargs = {
            **kwargs,
            **dict(
                dbname=f"tmp/{uuid.uuid4().hex[:10]}",
                is_tmp=True,
            ),
        }
        if use_tempfile:
            kwargs["root_dir"] = tempfile.mkdtemp()
        temp_stash = self.sub(**kwargs)
        try:
            yield temp_stash
        finally:
            temp_stash.clear()
            # temp_stash._remove_dir(temp_stash.root_dir)
            temp_stash._remove_dir(
                temp_stash.root_dir if use_tempfile else temp_stash.path
            )

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
        self, func, dbname=None, update_on_src_change=False, **kwargs
    ):
        # import types
        func_name = get_obj_addr(func).replace('<','_').replace('>','_')
        if update_on_src_change or not can_import_object(func):
            func_name += "/" + encode_hash(get_function_src(func))[:10]
        stash = self.sub(
            dbname=f'{self.dbname}/{"stashed_result" if not dbname else dbname}/{func_name}',
            is_function_stash=True,
        )
        func.stash = stash
        log.info(f'func.stash: {func.stash}')

        # Bind the new methods to the instance
        # stash.get = types.MethodType(get_func, stash)
        # stash.encode_key = types.MethodType(encode_key_func, stash)

        return stash

    def assemble_ld(
        self,
        all_results=None,
        with_metadata=None,
        flatten=True,
        progress=True,
    ):
        ld = []
        iterr = self.items(
            all_results=self._all_results(all_results),
            with_metadata=True,
        )
        if progress:
            iterr = progress_bar(iterr)
        for key, value_d in iterr:
            key_d = {"_key": key} if not isinstance(key, dict) else key
            if flatten:
                value = value_d.pop("_value")
                value_ld = flatten_ld(value)
                for value_d2 in value_ld:
                    ld.append({**key_d, **value_d, **value_d2})
            else:
                ld.append({**key_d, **value_d})
        return filter_ld(ld, no_nan=False, no_meta=not with_metadata)

    def assemble_df(
        self,
        index_cols=None,
        index=True,
        all_results=None,
        with_metadata=None,
        df_engine="pandas",
        **kwargs,
    ):
        ld = self.assemble_ld(
            all_results=all_results,
            with_metadata=with_metadata,
            **kwargs,
        )
        if not ld:
            return MetaDataFrame([], df_engine=df_engine)
        mdf = MetaDataFrame(ld, df_engine=df_engine)
        return mdf.set_index()

    @property
    def df(self):
        return self.assemble_df()

    @property
    def ld(self):
        return self.assemble_ld()

    def __hash__(self):
        # Use a combination of class name and path for hashing
        return hash(tuple(sorted(self.to_dict().items())))


# @fcache
def HashStash(
    name: str = None,
    engine: ENGINE_TYPES = None,
    dbname: str = None,
    compress: bool = None,
    b64: bool = None,
    serializer: SERIALIZER_TYPES = None,
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
    elif engine == "mongo":
        from ..engines.mongo import MongoHashStash

        cls = MongoHashStash
    elif engine == "dataframe":
        from .dataframe import DataFrameHashStash

        cls = DataFrameHashStash
    else:
        raise ValueError(f"Invalid engine: {engine}. Options: {', '.join(ENGINES)}.")

    object = cls(
        name=name,
        compress=compress,
        b64=b64,
        serializer=serializer,
        dbname=dbname,
        **kwargs,
    )
    return object


Stash = HashStash
