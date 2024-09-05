from . import *
import time
import threading
from contextlib import contextmanager
from multiprocessing import Manager, Lock as mp_Lock
from multiprocessing.managers import SyncManager
from ..serializers import serialize, deserialize

_manager = Manager()
_connection_lock = _manager.dict()
_connection_pool = {}
_last_used = {}


def get_manager():
    global _manager, _connection_lock
    if _manager is None:
        _manager = SyncManager()
        _manager.start()
        _connection_lock = _manager.dict()
    return _manager


# Function to get or create a lock for a given path
def get_lock(path):
    global _connection_lock
    manager = get_manager()
    if path not in _connection_lock:
        _connection_lock[path] = manager.Lock()
    return _connection_lock[path]



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
    CONNECTION_TIMEOUT = 60  # Close connections after 60 seconds of inactivity
    append_mode = DEFAULT_APPEND_MODE
    is_tmp = False
    is_function_stash = False
    needs_lock = True
    needs_reconnect = False

    @log.debug
    def __init__(
        self,
        name: str = None,
        root_dir: str = None,
        dbname: str = None,
        compress: str = None,
        b64: bool = None,
        serializer: SERIALIZER_TYPES = None,
        parent: "BaseHashStash" = None,
        children: List["BaseHashStash"] = None,
        is_function_stash=None,
        is_tmp=None,
        append_mode: bool = False,
        **kwargs,
    ) -> None:
        config = Config()
        self.name = name if name is not None else self.name
        self.compress = get_compresser(
            compress if compress is not None else config.compress
        )
        self.b64 = b64 if b64 is not None else config.b64
        if self.compress and (self.string_keys or self.string_values):
            self.b64 = True
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
        self._tmp = None
        self.append_mode = append_mode if append_mode is not None else self.append_mode
        encstr = "+".join(
            filter(
                None,
                [
                    self.compress if self.compress else "raw",
                    "b64" if self.b64 else None,
                ],
            )
        )
        subnames = [f"{self.engine}", self.serializer]
        if encstr:
            subnames.append(encstr)
        if self.filename_ext:
            subnames.append(get_fn_ext(self.filename_ext))
        self.filename = ".".join(subnames)
        self.path = os.path.join(
            (
                self.name
                if os.path.isabs(self.name)
                else os.path.join(self.root_dir, self.name)
            ),
            self.dbname,
            self.filename,
        )
        self.path_dirname = (
            self.path if self.filename_is_dir else os.path.dirname(self.path)
        )

    @staticmethod
    def _remove_dir(dir_path):
        if os.path.exists(dir_path):
            rmtreefn(dir_path)

    @log.debug
    def encode(self, *args, b64=None, compress=None, **kwargs):
        return encode(
            *args,
            b64=self.b64 if b64 is None else b64,
            compress=self.compress if compress is None else compress,
            **kwargs,
        )

    @log.debug
    def decode(self, *args, b64=None, compress=None, **kwargs):
        return decode(
            *args,
            b64=self.b64 if b64 is None else b64,
            compress=self.compress if compress is None else compress,
            **kwargs,
        )

    @log.debug
    def serialize(self, *args, **kwargs):
        return serialize(*args, serializer=self.serializer, **kwargs)

    @log.debug
    def deserialize(self, *args, **kwargs):
        return deserialize(*args, serializer=self.serializer, **kwargs)

    @log.debug
    def to_dict(self):
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

    @property
    @retry_patiently()
    def db(self):
        return self.get_connection()

    def get_db(self):
        # This method should be implemented by subclasses
        raise NotImplementedError("Subclasses must implement get_db method")

    @log.debug
    def __enter__(self):
        if not self.needs_lock:
            return self
        
        log.debug(f"locking {self}")
        self._lock = get_lock(self.path)
        try:
            # Attempt to acquire the lock without blocking
            acquired = self._lock.acquire(False)
            if not acquired:
                log.debug(f"Lock already held for {self}")
        except TypeError:
            # If acquire(False) is not supported, fall back to blocking acquire
            self._lock.acquire()
        return self

    @log.debug
    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.needs_lock:
            return
        if hasattr(self, "_lock"):
            log.debug(f"unlocking {self}")
            try:
                self._lock.release()
            except (ValueError, RuntimeError) as e:
                log.debug(e)
                # Lock was already released or not held
                pass

    @contextmanager
    @retry_patiently()
    def get_connection(self):
        global _connection_pool, _last_used

        if self.needs_reconnect:
            with self.get_db() as db:
                yield db
        else:
            if not self.path in _connection_pool:
                log.debug(f"Opening {self.engine} at {self.path}")
                conn = self.get_db()
                _connection_pool[self.path] = conn
            else:
                conn = _connection_pool[self.path]
                _last_used[self.path] = time.time()
            try:
                yield conn
            finally:
                self._cleanup_connections()

    def _cleanup_connections(self):
        global _connection_pool, _last_used
        current_time = time.time()
        for path, last_used in list(_last_used.items()):
            if current_time - last_used > self.CONNECTION_TIMEOUT:
                self._close_connection_path(path)

    def close(self):
        self._close_connection_path(self.path)

    def _close_connection_path(self, path):
        with self:
            conn = _connection_pool.pop(path, None)
            if conn is not None:
                self._close_connection(conn)
                _last_used.pop(path, None)

    def connect(self):
        with self.get_connection() as db:
            return True

    def query(self, test_func_key=bool, test_func_val=bool, return_vals=None, **kwargs):
        return_vals = return_vals or test_func_val is not bool
        for k in progress_bar(
            self.keys(),
            total=len(self),
            desc=f"querying by key = {test_func_key.__name__} and value = {test_func_val.__name__}",
        ):
            if test_func_key(k):
                if not return_vals:
                    yield k
                else:
                    v = self.get(k)
                    if test_func_val(v):
                        yield (k, v)

    @classmethod
    def _cleanup_connections(cls):
        current_time = time.time()
        for path, last_used in list(_last_used.items()):
            if current_time - last_used > cls.CONNECTION_TIMEOUT:
                cls._close_connection_path(path)

    def close(self):
        self._close_connection_path(self.path)

    @classmethod
    def _close_connection_path(cls, path):
        global _connection_pool
        conn = _connection_pool.get(path)
        if conn is not None:
            with get_lock(path):
                try:
                    cls._close_connection(conn)
                except Exception as e:
                    log.debug(e)
                _connection_pool.pop(path, None)
                _last_used.pop(path, None)

    @staticmethod
    def _close_connection(connection):
        # Default implementation, can be overridden by subclasses
        if hasattr(connection, "close"):
            try:
                connection.close()
            except Exception as e:
                log.debug(f"error closing connection: {e}")
        # else:
        # log.warn(f"how does one close connection of type {connection}?")

    @property
    def data(self):
        return self.db

    def __eq__(self, other):
        if not isinstance(other, BaseHashStash):
            return False
        return self.to_dict() == other.to_dict()

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
    def get_func(self, *args, func=None, _dbname=None, **kwargs):
        fstash = (
            self.sub_function_results(func, dbname=_dbname)
            if not self.is_function_stash
            else self
        )
        return fstash.get(
            self.new_function_key(
                *args,
                **kwargs,
            )
        )

    # @log.debug
    # def set_func(self, unencoded_value, *args, _dbname=None, **kwargs):
    #     fstash = self.sub_function_results(func, dbname=_dbname)
    #     return fstash.set(self.new_function_key(*args, **kwargs), unencoded_value)

    # def get_set_func(self, func, setter, *args, _dbname=None, **kwargs):
    #     fstash = self.sub_function_results(func, dbname=_dbname)
    #     setter_func = lambda: setter(*args, **kwargs)
    #     return fstash.get_set(self.new_function_key(func, *args, **kwargs), setter_func)

    def get_set(
        self,
        unencoded_key,
        unencoded_value_setter,
        default=None,
        _force=False,
        **kwargs,
    ):
        unencoded_value = None
        if not _force:
            unencoded_value = self.get(unencoded_key, default=None, **kwargs)
        if unencoded_value is None:
            log.debug("setting")
            unencoded_value = unencoded_value_setter()
            if unencoded_value is not None:
                self.set(unencoded_key, unencoded_value)
        else:
            log.debug("getting")
        return unencoded_value if unencoded_value is not None else default

    @log.debug
    def get(
        self,
        unencoded_key: Any = None,
        default: Any = None,
        with_metadata=False,
        as_dataframe=None,
        as_string=False,
        all_results=None,
        **kwargs,
    ) -> Any:
        values = self.get_all(
            unencoded_key,
            default=None,
            with_metadata=with_metadata,
            all_results=all_results,
            as_dataframe=as_dataframe,
            **kwargs,
        )
        value = values[-1] if values else default
        return self.serialize(value) if as_string else value

    @log.debug
    def get_all(
        self,
        unencoded_key: Any = None,
        default: Any = None,
        with_metadata: bool = None,
        all_results: bool = True,
        **kwargs,
    ) -> Any:
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

    @log.debug
    def set(self, unencoded_key: Any, unencoded_value: Any, append=None) -> None:
        encoded_key = self.encode_key(unencoded_key)
        # log.info(encoded_key)
        new_unencoded_value = self.new_unencoded_value(
            unencoded_value,
            unencoded_key=unencoded_key,
            append=append,
        )

        encoded_value = self.encode_value(new_unencoded_value)
        self._set(encoded_key, encoded_value)

    @log.debug
    def run(
        self,
        func,
        *args,
        _force=False,
        **kwargs,
    ):
        fstash = (
            self.attach_func(func)
            # if getattr(func, "stash", None) is None
            # else func.stash
        )
        args = list(args)
        if get_pytype(func) == "instancemethod":
            args = [get_object_from_method(func)] + args
        elif get_pytype(func) == "classmethod":
            args = [get_class_from_method(func)] + args
        # func = unwrap_func(func)
        unencoded_key = fstash.new_function_key(
            *args,
            store_args=kwargs.get("store_args", True),
            **{k: v for k, v in kwargs.items() if k and k[0] != "_"},
        )
        # #pprint(unencoded_key)
        # #print('run',meta_kwargs)
        if not _force:
            res = fstash.get(unencoded_key, default=None, **kwargs)
            if res is not None:
                log.debug(
                    f"Stash hit for {func.__name__} in {fstash}. Returning stashed result"
                )
                # return unencoded_key
                return res

        # didn't find
        note = "Forced execution" if _force else "Stash miss"
        log.debug(f"{note} for {func.__name__}. Executing function.")

        # call func
        # args = [obj] + list(args) if obj else list(args)
        # result = unwrap_func(func)(*args, **kwargs)
        funcx = unwrap_func(func)
        result = call_function_politely(funcx, *args, **kwargs, _force=_force)
        result = list(result) if is_generator(result) else result
        log.debug(
            f"Caching result for {func.__name__} under {serialize(unencoded_key)}"
        )
        fstash.set(unencoded_key, result)
        # return unencoded_key
        return result

    def map(
        self,
        func,
        objects=[],
        options=[],
        num_proc=1,
        total=None,
        desc=None,
        progress=True,
        ordered=True,
        preload=True,
        precompute=True,
        stash_runs=True,
        stash_map=True,
        _force=False,
        **common_kwargs,
    ):
        pmap = None
        self.attach_func(func)
        key = StashMap.get_stash_key(func, objects, options, total=total)
        #pprint(key)
        if not _force:
            pmap = self.get(key)

        if pmap is None:
            return StashMap(
                func,
                objects=objects,
                options=options,
                num_proc=num_proc,
                total=total,
                desc=desc,
                progress=progress,
                ordered=ordered,
                stash=self,
                preload=preload,
                precompute=precompute,
                stash_runs=stash_runs,
                stash_map=stash_map,
                _force=_force,
                _stash_key=key,
                **common_kwargs,
            )
        else:
            pmap.desc = desc
            pmap.progress = progress
            pmap.ordered = ordered
            pmap.num_proc = num_proc
            pmap.stash = self
            pmap._stash_key = key

            if not pmap._preload and preload:
                pmap._preload = preload
                pmap._precompute = precompute
                pmap.preload()
            elif not pmap._precompute and precompute:
                pmap.compute()
                pmap._precompute = True

            return pmap

    def attach_func(self, func):
        funcx = unwrap_func(func)
        local_stash = self.sub_function_results(funcx)
        func.__dict__["stash"] = funcx.__dict__["stash"] = local_stash
        return local_stash

    @log.debug
    def new_function_key(self, *args, store_args=True, **kwargs):
        key = {
            "args": args,
            "kwargs": kwargs,
        }
        return encode_hash(self.serialize(key)) if not store_args else key

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
            # compress=False
        )

    @log.debug
    def encode_value(self, unencoded_value: Any) -> Union[str, bytes]:
        return self.encode(
            self.serialize(unencoded_value),
            as_string=self.string_values,
        )

    @log.debug
    def decode_key(self, encoded_key: Any, as_string=False) -> Union[str, bytes]:
        decoded_key = self.decode(
            encoded_key,
            # compress=False,
        )
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
        log.debug("Decoding value")
        decoded_value = self.decode(encoded_value)
        log.debug(f"Decoded value of {len(decoded_value):,}B")
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
    def clear(self) -> "BaseHashStash":
        self.close()
        self._remove_dir(
            self.root_dir
            if str(self.root_dir).startswith("/var/")
            or str(self.root_dir).startswith("/private/var/")
            else self.path
        )

        return self

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
            if vals is not None:
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

    @property
    def stashed_dataframe(self):
        return stashed_dataframe(stash=self)

    @cached_property
    def profiler(self):
        from ..profilers.engine_profiler import HashStashProfiler

        return HashStashProfiler(self)

    @cached_property
    def profile(self):
        return self.profiler.profile

    @log.debug
    def sub(self, **kwargs):
        kwargs = {**self.to_dict(), **kwargs, "parent": self}
        new_instance = self.__class__(**kwargs)
        self.children.append(new_instance)
        return new_instance

    @contextmanager
    def tmp(self, use_tempfile=True, dbname=None, **kwargs):
        kwargs = {
            **kwargs,
            **dict(
                dbname=f"tmp/{uuid.uuid4().hex[:10]}" if dbname is None else dbname,
                is_tmp=True,
            ),
        }
        if use_tempfile:
            kwargs["root_dir"] = tempfile.mkdtemp()
        temp_stash = self.sub(**kwargs)
        self._tmp = temp_stash
        try:
            yield temp_stash
        finally:
            try:
                self._tmp = None
                temp_stash.clear()
                temp_stash._remove_dir(
                    temp_stash.root_dir if use_tempfile else temp_stash.path
                )
            except Exception as e:
                log.error(f"Error clearing temp stash: {e}")

    def __repr__(self):
        path = self.path.replace(os.path.expanduser("~"), "~")
        return f"""{self.__class__.__name__}({path})"""

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

    @log.debug
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
        func.__dict__["stash"] = stash
        stash.__dict__["func"] = func
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
    

    @property
    def filesize(self):
        """
        Get the total size of self.path in bytes, whether it's a file or directory.
        
        Returns:
            int: Total size in bytes
        """
        if not os.path.exists(self.path):
            return 0
        
        if os.path.isfile(self.path):
            return os.path.getsize(self.path)
        
        total_size = 0
        for dirpath, dirnames, filenames in os.walk(self.path):
            for filename in filenames:
                file_path = os.path.join(dirpath, filename)
                total_size += os.path.getsize(file_path)
        
        return total_size


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
    config = Config()
    engine = get_engine(engine if engine is not None else config.engine)

    if engine == "pairtree":
        from .pairtree import PairtreeHashStash

        cls = PairtreeHashStash
    elif engine in {"sqlite", "sqlitedict"}:
        try:
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
            from ..engines.redis import RedisHashStash

            cls = RedisHashStash
        except ImportError:
            pass
    elif engine == "diskcache":
        try:
            from ..engines.diskcache import DiskCacheHashStash

            cls = DiskCacheHashStash
        except ImportError:
            pass
    elif engine == "lmdb":
        try:
            from ..engines.lmdb import LMDBHashStash

            cls = LMDBHashStash
        except ImportError:
            pass
    elif engine == "mongo":
        try:
            from ..engines.mongo import MongoHashStash

            cls = MongoHashStash
        except ImportError:
            pass
    elif engine == "dataframe":
        try:
            from .dataframe import DataFrameHashStash

            cls = DataFrameHashStash
        except ImportError:
            pass
    else:
        raise ValueError(
            f"\n\nInvalid HashStash engine: {engine}.\n\nOptions available given current install: {', '.join(get_working_engines())}\nAll options: {', '.join(ENGINES)}"
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


def attach_stash_to_function(func, stash=None, **stash_kwargs):
    if stash is None:
        stash = HashStash(**stash_kwargs)
    local_stash = stash.sub_function_results(func)
    func.stash = local_stash
    return stash


Stash = HashStash
