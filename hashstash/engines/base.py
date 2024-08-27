from . import *
import time
import threading
from contextlib import contextmanager

_connection_pool = {}


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
        "root_dir",
        "name",
        "dbname",
        "engine",
        "serializer",
        "compress",
        "b64",
        "append_mode",
        "is_function_stash",
        "is_tmp",
    ]
    metadata_cols = ["_version"]
    _connection_pool = _connection_pool
    _connection_lock = threading.Lock()
    _last_used = {}
    CONNECTION_TIMEOUT = 60  # Close connections after 60 seconds of inactivity
    append_mode = DEFAULT_APPEND_MODE
    is_tmp = False
    is_function_stash = False

    @log.trace
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
        append_mode: bool = None,
        **kwargs,
    ) -> None:
        self.name = name if name is not None else self.name
        self.compress = compress if compress is not None else config.compress
        self.b64 = b64 if b64 is not None else config.b64
        self.serializer = serializer if serializer is not None else config.serializer
        self.root_dir = os.path.expanduser(
            root_dir if root_dir is not None else self.root_dir
        )
        self.dbname = dbname if dbname is not None else self.dbname
        self.parent = parent
        self.children = [] if not children else children
        self.is_function_stash = (
            is_function_stash
            if is_function_stash is not None
            else self.is_function_stash
        )
        self.is_tmp = is_tmp if is_tmp is not None else self.is_tmp
        self.append_mode = append_mode if append_mode is not None else self.append_mode
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
        self.filename = ".".join(subnames)
        self.path = os.path.join(
            (
                self.name
                if os.path.isabs(self.name)
                else os.path.join(self.root_dir, self.name)
            ),
            self.dbname,
            "dbs",
            self.filename,
        )
        self.path_dirname = (
            self.path if self.filename_is_dir else os.path.dirname(self.path)
        )

    @staticmethod
    def _remove_dir(dir_path):
        rmtreefn(dir_path)

    @log.trace
    def encode(self, *args, **kwargs):
        return encode(*args, b64=self.b64, compress=self.compress, **kwargs)

    @log.trace
    def decode(self, *args, **kwargs):
        return decode(*args, b64=self.b64, compress=self.compress, **kwargs)

    @log.trace
    def serialize(self, *args, **kwargs):
        return serialize(*args, serializer=self.serializer, **kwargs)

    @log.trace
    def deserialize(self, *args, **kwargs):
        return deserialize(*args, serializer=self.serializer, **kwargs)

    @log.trace
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
        d["filename"] = self.filename
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
        return self.get_connection()

    def get_db(self):
        # This method should be implemented by subclasses
        raise NotImplementedError("Subclasses must implement get_db method")

    @contextmanager
    @retry_patiently()
    def get_connection(self):
        if self.path not in self._connection_pool:
            log.debug(f"Opening {self.engine} at {self.path}")
            self._connection_pool[self.path] = self.get_db()
            self._last_used[self.path] = time.time()
        try:
            yield _connection_pool[self.path]
        finally:
            self._cleanup_connections()
            pass

    @classmethod
    def _cleanup_connections(cls):
        current_time = time.time()
        with cls._connection_lock:
            for path, last_used in list(cls._last_used.items()):
                if current_time - last_used > cls.CONNECTION_TIMEOUT:
                    cls._close_connection_path(path)

    def close(self):
        self._close_connection_path(self.path)

    @classmethod
    def _close_connection_path(cls, path):
        conn = cls._connection_pool.get(path)
        if conn is not None:
            cls._close_connection(conn)
            cls._connection_pool.pop(path, None)
            cls._last_used.pop(path, None)

    @staticmethod
    def _close_connection(connection):
        # Default implementation, can be overridden by subclasses
        if hasattr(connection, "close"):
            try:
                connection.close()
            except Exception as e:
                log.warn(f"error closing connection: {e}")
        else:
            log.warn(f"how does one close connection of type {connection}?")

    @property
    def data(self):
        return self.db

    def __eq__(self, other):
        if not isinstance(other, BaseHashStash):
            return False
        return self.to_dict() == other.to_dict()

    @log.trace
    def __enter__(self):
        # if not self._lock.locked():
        #     self._lock.acquire()
        return self

    @log.trace
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
        # if self._lock.locked():
        # self._lock.release()
        # if self.is_tmp:
        # self._remove_dir(self.path)

    @log.trace
    def __getitem__(self, unencoded_key: str) -> Any:
        obj = self.get(unencoded_key)
        if obj is None:
            raise KeyError(unencoded_key)
        return obj

    @log.trace
    def __setitem__(self, unencoded_key: str, unencoded_value: Any) -> None:
        self.set(unencoded_key, unencoded_value)

    @log.trace
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

    @log.trace
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
                {"_version": vi + 1, "_value": value} for vi, value in enumerate(values)
            ]
        if not self._all_results(all_results):
            values = values[-1:]
        return values

    @staticmethod
    @log.info
    def new_function_key(*args, store_args=True, **kwargs):
        # if args and args[0] == None:
            # args = args[1:]
        kwargs = {k: v for k, v in kwargs.items() if k and k[0] != "_"}
        key = {"args": tuple(args), "kwargs": kwargs}
        if not store_args:
            key = encode_hash(serialize(key))
        return key

    @log.trace
    def set(self, unencoded_key: Any, unencoded_value: Any, append=None) -> None:
        encoded_key = self.encode_key(unencoded_key)
        new_unencoded_value = self.new_unencoded_value(
            unencoded_value,
            unencoded_key=unencoded_key,
            append=append,
        )
        encoded_value = self.encode_value(new_unencoded_value)
        self._set(encoded_key, encoded_value)

    @log.trace
    def run(
        self,
        func,
        *args,
        _store_args=True,
        _force=False,
        **kwargs,
    ):
        stash = self.attach_func(func)# if getattr(func,'stash',None) is None else func.stash
        unencoded_key = stash.new_function_key(
            *args, 
            store_args=_store_args, 
            **kwargs
        )
        log.debug(f'key = {unencoded_key}')
        log.debug(f'key = {serialize(unencoded_key)}')
        log.debug(f'key = {stash.encode_key(unencoded_key)}')
        if not _force:
            res = stash.get(
                unencoded_key,
                as_function=False,
                **kwargs,
            )
            if res is not None:
                log.debug(f"Stash hit for {func.__name__}. Returning stashed result.")
                return res

        # didn't find
        note = "Forced execution" if _force else "Stash miss"
        log.debug(f"{note} for {func.__name__}. Executing function.")

        # call func
        result = func(*args, **kwargs)
        result = list(result) if is_generator(result) else result
        log.info(f"Caching result for {func.__name__}")
        stash.set(unencoded_key, result)
        return result
    
    def attach_func(self, func):
        local_stash = self.sub_function_results(func)
        func.stash = local_stash
        funcx=func
        while getattr(funcx,'__wrapped__',None):
            funcx=funcx.__wrapped__
            funcx.stash = self
        return local_stash
        

    @log.info
    def new_unencoded_key(self, unencoded_key: Any, *args, as_function=None, **kwargs):
        if self.is_function_stash and as_function is not False:
            if not (
                isinstance(unencoded_key, dict)
                and set(unencoded_key.keys()) == {"args", "kwargs"}
            ):
                unencoded_key = self.new_function_key(unencoded_key, *args, **kwargs)
        return unencoded_key

    @log.trace
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

    @log.trace
    def _get(self, encoded_key: str, default: Any = None) -> Any:
        with self as cache, cache.db as db:
            res = db.get(encoded_key)
            return res if res is not None else default

    @log.trace
    def _set(self, encoded_key: str, encoded_value: Any) -> None:
        try:
            with self as cache, cache.db as db:
                db[encoded_key] = encoded_value
        except Exception as e:
            log.error(f"Failed to set key {encoded_key}: {e}")

    @log.trace
    def __contains__(self, unencoded_key: Any) -> bool:
        return self.has(unencoded_key)

    @log.trace
    def has(self, unencoded_key: Any) -> bool:
        return self._has(self.encode_key(unencoded_key))

    @log.trace
    def encode_key(self, unencoded_key: Any) -> Union[str, bytes]:
        return self.encode(
            self.serialize(unencoded_key),
            as_string=self.string_keys,
        )

    @log.trace
    def encode_value(self, unencoded_value: Any) -> Union[str, bytes]:
        return self.encode(
            self.serialize(unencoded_value),
            as_string=self.string_values,
        )

    @log.trace
    def decode_key(self, encoded_key: Any, as_string=False) -> Union[str, bytes]:
        decoded_key = self.decode(encoded_key)
        return (
            self.deserialize(decoded_key)
            if not as_string
            else decoded_key.decode("utf-8")
        )

    @log.trace
    def decode_value(
        self,
        encoded_value: Any,
        as_string=False,
    ) -> Union[str, bytes, dict, list]:
        log.debug('Decoding value')
        decoded_value = self.decode(encoded_value)
        log.debug(f'Decoded value of {len(decoded_value):,}B')
        return (
            self.deserialize(decoded_value)
            if not as_string
            else decoded_value.decode("utf-8")
        )

    @log.trace
    def _has(self, encoded_key: Union[str, bytes]):
        with self as cache, cache.db as db:
            return encoded_key in db

    @log.trace
    def clear(self) -> "BaseHashStash":
        self.close()
        self._remove_dir(
            self.root_dir
            if str(self.root_dir).startswith("/var/")
            or str(self.root_dir).startswith("/private/var/")
            else self.path
        )
        return self

    @log.trace
    def __len__(self) -> int:
        with self as cache, cache.db as db:
            return len(db)

    @log.trace
    def __delitem__(self, unencoded_key: str) -> None:
        if not self.has(unencoded_key):
            raise KeyError(unencoded_key)
        self._del(self.encode_key(unencoded_key))

    @log.trace
    def _del(self, encoded_key: Union[str, bytes]) -> None:
        with self as cache, cache.db as db:
            del db[encoded_key]

    @log.trace
    def _keys(self):
        with self as cache, cache.db as db:
            for k in db:
                yield k

    @log.trace
    def _values(self):
        with self as cache, cache.db as db:
            for k in db:
                yield db[k]

    @log.trace
    def _items(self):
        with self as cache, cache.db as db:
            for k in db:
                yield k, db[k]

    def _all_results(self, all_results=None):
        return all_results if all_results is not None else self.append_mode

    @log.trace
    def keys(self, as_string=False):
        for x in self._keys():
            try:
                yield self.decode_key(x, as_string=as_string)
            except Exception as e:
                log.error(f"Error decoding key: {e}")
                raise e

    @log.trace
    def values(self, all_results=None, with_metadata=False, **kwargs):
        for k, v in self.items(all_results=all_results, with_metadata=with_metadata):
            yield v

    @log.trace
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

    @log.trace
    def keys_l(self, **kwargs):
        return list(self.keys(**kwargs))

    @log.trace
    def values_l(self, **kwargs):
        return list(self.values(**kwargs))

    @log.trace
    def items_l(self, **kwargs):
        return list(self.items(**kwargs))

    @log.trace
    def __iter__(self):
        return self.keys()

    @log.trace
    def copy(self):
        return dict(self.items())

    @log.trace
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

    @log.trace
    def setdefault(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            self[key] = default
            return default

    @log.trace
    def pop(self, unencoded_key, default=object):
        try:
            value = self[unencoded_key]
            del self[unencoded_key]
            return value
        except KeyError:
            if default is object:
                raise
            return default

    @log.trace
    def popitem(self):
        key, value = next(iter(self.items()))
        del self[key]
        return value

    @log.trace
    def hash(self, data: bytes) -> str:
        return encode_hash(data)

    @property
    def stashed_result(self):
        return stashed_result(stash=self)

    @cached_property
    def profiler(self):
        from ..profilers.engine_profiler import HashStashProfiler

        return HashStashProfiler(self)

    @cached_property
    def profile(self):
        return self.profiler.profile

    @log.trace
    def sub(self, **kwargs):
        kwargs = {**self.to_dict(), **kwargs, "parent": self}
        new_instance = self.__class__(**kwargs)
        # new_instance._lock = threading.Lock()  # Create a new lock for the new instance
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
        # return f"""{self.__class__.__name__}({path})"""
        pathstr = os.path.join(self.name, self.dbname)
        pathstr = pathstr.replace(os.path.sep, " " + os.path.sep + " ")
        return f"""{self.__class__.__name__}({pathstr})"""

    def _repr_html_(self):
        selfstr = repr(self)
        dict_items = self.to_dict()
        dict_items["len"] = len(self)
        attr_groups = {
            "path": ["root_dir", "name", "dbname", "filename"],
            "engine": [
                "engine",
                "serializer",
                "compress",
                "b64",
                "df_engine",
                "io_engine",
            ],
            "misc": ["append_mode", "is_function_stash", "is_tmp", "is_sub"],
            "stats": ["len"],
        }

        html = [
            '<table border="1" class="dataframe">',
            "<thead><tr><th>Config</th><th>Param</th><th>Value</th></tr></thead>",
            "<tbody>",
        ]

        for group, attrs in attr_groups.items():
            group_seen = False
            for attr in attrs:
                if attr in dict_items and dict_items[attr]:
                    html.append(
                        f'<tr><td><b>{group.title() if not group_seen else ""}</b></td>'
                        f'<td>{attr.replace("_", " ").title()}</td>'
                        f"<td><i>{dict_items[attr]}</i></td></tr>"
                    )
                    group_seen = True

        html.append("</tbody></table>")
        return f'<pre>{self.__class__.__name__}</pre>{"".join(html)}'

    def __reduce__(self):
        # Return a tuple of (callable, args) that allows recreation of this object
        return (self.__class__.from_dict, (self.to_dict(),))

    @log.trace
    def sub_function_results(
        self, func, dbname=None, update_on_src_change=False, **kwargs
    ):
        # import types
        func_name = get_obj_addr(func).replace("<", "_").replace(">", "_")
        if update_on_src_change:  # or not can_import_object(func):
            # logger.info(f'updating on src change because can import object? {can_import_object(func)} --> {func}')
            func_name += "/" + encode_hash(get_function_src(func))[:10]
        # new_dbname = f'{self.dbname}/{"stashed_result" if not dbname else dbname}/{func_name}'
        new_dbname = f'{"stashed_result" if not dbname else dbname}/{func_name}'
        log.debug(f"Sub-function results stash: {new_dbname}")
        stash = self.sub(
            dbname=new_dbname,
            is_function_stash=True,
        )
        func.stash = stash

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
        engine: The type of cache to create ("pairtree", "sqlite", "memory", "shelve", "redis", or "diskcache")
        **kwargs: Additional keyword arguments to pass to the cache constructor.

    Returns:
        An instance of the appropriate BaseHashStash subclass.

    Raises:
        ValueError: If an invalid engine is provided.
    """
    engine = engine if engine is not None else config.engine

    working_engines = set(BUILTIN_ENGINES)

    if engine == "pairtree":
        from .pairtree import PairtreeHashStash

        cls = PairtreeHashStash
    elif engine in {"sqlite", "sqlitedict"}:
        try:
            import sqlitedict

            working_engines.add("sqlite")
            from ..engines.sqlite import SqliteHashStash

            cls = SqliteHashStash
        except ImportError:
            pass
    elif engine == "memory":
        from ..engines.memory import MemoryHashStash

        cls = MemoryHashStash
    elif engine == "shelve":
        from ..engines.shelve import ShelveHashStash

        cls = ShelveHashStash
    elif engine == "redis":
        try:
            import redis
            import redis_dict

            working_engines.add("redis")
            from ..engines.redis import RedisHashStash

            cls = RedisHashStash
        except ImportError:
            pass
    elif engine == "diskcache":
        try:
            import diskcache

            working_engines.add("diskcache")
            from ..engines.diskcache import DiskCacheHashStash

            cls = DiskCacheHashStash
        except ImportError:
            pass
    elif engine == "lmdb":
        try:
            import lmdb

            working_engines.add("lmdb")
            from ..engines.lmdb import LMDBHashStash

            cls = LMDBHashStash
        except ImportError:
            pass
    elif engine == "mongo":
        try:
            import pymongo

            working_engines.add("mongo")
            from ..engines.mongo import MongoHashStash

            cls = MongoHashStash
        except ImportError:
            pass
    elif engine == "dataframe":
        try:
            import pandas as pd
            import numpy as np

            working_engines.add("dataframe")
            from .dataframe import DataFrameHashStash

            cls = DataFrameHashStash
        except ImportError:
            pass
    else:
        raise ValueError(
            f"\n\nInvalid HashStash engine: {engine}.\n\nOptions available given current install: {', '.join(working_engines)}\nAll options: {', '.join(ENGINES)}"
        )

    if engine in ENGINES and engine not in working_engines:
        raise ValueError(
            f"\n\nEngine {engine} is available but not installed. Please run: pip install hashstash[{engine}]\n\nOptions available given current install: {', '.join(working_engines)}\nAll options: {', '.join(ENGINES)}"
        )

    object = cls(
        name=name,
        compress=compress,
        b64=b64,
        serializer=serializer,
        dbname=dbname,
        **kwargs,
    )
    return object

def attach_stash_to_function(func, stash = None, **stash_kwargs):
    if stash is None:
        stash = HashStash(**stash_kwargs)
    local_stash = stash.sub_function_results(func)
    func.stash = local_stash
    return stash



Stash = HashStash
