# Explicit stdlib imports: this package's `from . import *` chains are
# circular, and whether a name has landed in the package namespace yet
# depends on import order (spawn workers + editable installs order imports
# differently). Never rely on the star-chain for stdlib names.
from collections import Counter
from collections.abc import MutableMapping
from functools import cached_property
from typing import Any
from typing import List
from typing import Union
import importlib
import json
import os
import tempfile
import uuid

from . import *
import time
import threading
from contextlib import contextmanager
from datetime import datetime, timezone, timedelta
from pathlib import Path
from ..serializers import serialize, deserialize

_connection_pool = {}
_last_used = {}
_conn_refcount = {}
_pool_guard = threading.Lock()

_path_locks = {}
_path_locks_guard = threading.Lock()

if os.name == "nt":
    import msvcrt

    def _lock_fileno(fileno):
        # msvcrt.locking(LK_LOCK) retries ~10s then raises; loop until acquired
        while True:
            try:
                msvcrt.locking(fileno, msvcrt.LK_LOCK, 1)
                return
            except OSError:
                continue

    def _unlock_fileno(fileno):
        msvcrt.locking(fileno, msvcrt.LK_UNLCK, 1)

else:
    import fcntl

    def _lock_fileno(fileno):
        fcntl.flock(fileno, fcntl.LOCK_EX)

    def _unlock_fileno(fileno):
        fcntl.flock(fileno, fcntl.LOCK_UN)


class _PathLock:
    """Reentrant lock scoped to a stash path that actually excludes other
    processes: a threading.RLock coordinates threads in-process while an
    advisory file lock (flock/msvcrt on <path>.lock) excludes other processes.
    The OS releases the file lock automatically if the holder dies."""

    def __init__(self, path):
        self.lock_path = path + ".lock"
        self._rlock = threading.RLock()
        self._file = None
        self._depth = 0

    def acquire(self):
        self._rlock.acquire()
        if self._depth == 0:
            try:
                lock_dir = os.path.dirname(self.lock_path)
                if lock_dir:
                    os.makedirs(lock_dir, exist_ok=True)
                self._file = open(self.lock_path, "a+b")
                _lock_fileno(self._file.fileno())
            except Exception:
                if self._file is not None:
                    self._file.close()
                    self._file = None
                self._rlock.release()
                raise
        self._depth += 1
        return True

    def release(self):
        if self._depth <= 0:
            raise RuntimeError(f"release of unheld lock {self.lock_path}")
        self._depth -= 1
        if self._depth == 0 and self._file is not None:
            try:
                if os.name == "nt":
                    self._file.seek(0)
                _unlock_fileno(self._file.fileno())
            finally:
                self._file.close()
                self._file = None
        self._rlock.release()

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()


def get_lock(path):
    """Process-wide registry of per-path locks (one _PathLock per stash path)."""
    with _path_locks_guard:
        lock = _path_locks.get(path)
        if lock is None:
            lock = _path_locks[path] = _PathLock(path)
        return lock

ENVELOPE_MARKER = "__hs_v1__"

# Engines where the cache may be written by a party you don't control (a shared
# server, a remote bucket): these default to safe=True reads so a malicious value
# can't execute code on load. Local file engines you own stay code-capable.
NETWORKED_ENGINES = frozenset({"redis", "mongo"})


def _is_remote_fsspec_uri(root_dir):
    """True if an fsspec root_dir points at a remote/shared filesystem (s3://,
    gcs://, sftp://, ...). A plain path or the local/memory protocols are not
    remote and pose no untrusted-writer risk beyond a local directory."""
    if not isinstance(root_dir, str) or "://" not in root_dir:
        return False
    scheme = root_dir.split("://", 1)[0].lower()
    return scheme not in ("", "file", "local", "memory")


# Explicit version stamped into every value envelope (the "_fv" field). Bump this
# ONLY when an incompatible change is made to how values are wrapped, and add a
# branch to _migrate_envelope() for the old version. Envelopes written before
# versioning existed have no "_fv" and read as version 1; the field is plain data,
# so it costs a couple of bytes and is ignored by older hashstash versions
# (forward-compatible). Keys are never versioned — they must stay canonical.
#
# Scope: this versions the ENVELOPE used by the key-value engines (sqlite, lmdb,
# redis, mongo, memory, diskcache, shelve). The file-layout engines (pairtree,
# dataframe, jsonl) store bare values and version through their on-disk layout,
# so they don't carry "_fv". Serializer-format changes (custom.py __pytype__
# tags) are handled by tag-dispatch — the deserializer recognizes both old and
# new tag forms — which is why most format evolution needs no version bump; this
# field is for changes to the envelope that a structure check alone can't tell
# apart.
FORMAT_VERSION = 1

# single-flight key locks are striped across this many lock files per stash:
# bounded lock-file count, negligible collision odds between distinct keys
SINGLE_FLIGHT_STRIPES = 256

# access counters shared by every stash instance pointing at the same path
# (run()/attach_func create fresh sub-stash instances per call; their traffic
# must land on the same counters the user reads via func.stash.stats)
_stats_by_path = {}
_stats_guard = threading.Lock()


def _get_stats(path):
    with _stats_guard:
        stats = _stats_by_path.get(path)
        if stats is None:
            stats = _stats_by_path[path] = Counter()
        return stats


class _MissingType:
    """Sentinel distinguishing 'key absent' from a stored None value."""

    def __repr__(self):
        return "<MISSING>"

    def __bool__(self):
        return False


_MISSING = _MissingType()


class HashStashWarning(UserWarning):
    """Category for hashstash's user-actionable, data-integrity warnings (an
    unreadable pre-1.0 cache, a wrong-layout migrate). Emitted via warnings.warn
    AND the logger: the logger keeps it loud on stderr even when a consumer has
    filterwarnings('ignore'), while the warning lets programmatic consumers catch
    it with warnings.catch_warnings(record=True)."""


class HashStashCachedError(Exception):
    """Raised for a cached exception whose original type couldn't be
    reconstructed (e.g. a non-importable custom exception)."""


# Cached exceptions are stored as a plain-data marker dict (not a serialized
# exception object), so they read back even under safe mode and carry their own
# optional expiry. The marker key is deliberately long to avoid colliding with
# a user value that happens to be a dict.
_CACHED_EXC_MARKER = "__hashstash_cached_exception_v1__"


def _make_cached_exc(exc, expires_at):
    # only keep exc.args if they are simple data — otherwise reconstruction falls
    # back to the message, and we never risk the exc-cache write itself failing
    raw_args = getattr(exc, "args", ())
    if all(isinstance(a, (str, int, float, bool, type(None))) for a in raw_args):
        args = list(raw_args)
    else:
        args = None
    return {
        _CACHED_EXC_MARKER: True,
        "type": f"{type(exc).__module__}.{type(exc).__qualname__}",
        "message": str(exc),
        "args": args,
        "expires_at": expires_at,
    }


def _reconstruct_cached_exc(info):
    type_name = info.get("type") or "builtins.Exception"
    message = info.get("message", "")
    args = info.get("args")
    exc_type = None
    try:
        mod, _, name = type_name.rpartition(".")
        exc_type = getattr(importlib.import_module(mod), name) if mod else None
    except Exception:
        exc_type = None
    if isinstance(exc_type, type) and issubclass(exc_type, BaseException):
        try:
            return exc_type(*(args if args is not None else ([message] if message else [])))
        except Exception:
            return exc_type(message) if message else exc_type()
    return HashStashCachedError(f"{type_name}: {message}")


def _is_cached_exc(res):
    return isinstance(res, dict) and res.get(_CACHED_EXC_MARKER) is True


def _coerce_timestamp(dt):
    """Normalize a datetime or unix timestamp to a float unix timestamp (UTC)."""
    if dt is None:
        return None
    if isinstance(dt, (int, float)):
        return float(dt)
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            log.warning(f"naive datetime {dt!r} passed to hashstash; interpreting as UTC")
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    raise TypeError(
        f"before/after must be a datetime or unix timestamp, got {type(dt).__name__}"
    )


def _unwrap_envelope(decoded):
    """Return (values, timestamps). Pre-envelope (legacy) entries read as timestamp=0.

    A bare (non-envelope) value — including a bare list — is ONE value: treating
    legacy lists as multiple versions silently corrupted list-valued caches
    (get() returned the last element instead of the list)."""
    if isinstance(decoded, dict) and decoded.get(ENVELOPE_MARKER) is True:
        # envelopes written before versioning have no "_fv" -> treat as version 1
        version = decoded.get("_fv", 1)
        if version != FORMAT_VERSION:
            decoded = _migrate_envelope(decoded, version)
        values = decoded.get("_values", [])
        timestamps = decoded.get("_written_at", [])
        if len(timestamps) < len(values):
            timestamps = list(timestamps) + [0.0] * (len(values) - len(timestamps))
        return list(values), list(timestamps)
    return [decoded], [0.0]


def _migrate_envelope(decoded, version):
    """Bring an envelope written by an older FORMAT_VERSION up to the current one.

    Currently a no-op passthrough (only version 1 exists) — this is the single
    place to add migration logic when FORMAT_VERSION is bumped, so old caches
    keep reading instead of silently misinterpreting. A newer-than-known version
    (data written by a future hashstash) is returned untouched: the envelope
    fields we read (_values/_written_at) are stable, so forward reads still work.
    """
    if version > FORMAT_VERSION:
        log.debug(
            f"reading envelope format v{version} with hashstash format "
            f"v{FORMAT_VERSION}; reading known fields only"
        )
        return decoded
    # if version < FORMAT_VERSION: add per-version migration steps here
    return decoded


def _wrap_envelope(values, timestamps):
    return {
        ENVELOPE_MARKER: True,
        "_fv": FORMAT_VERSION,
        "_values": list(values),
        "_written_at": list(timestamps),
    }


def _filter_by_time(values, timestamps, before=None, after=None):
    """Keep only (value, timestamp) pairs with after <= t <= before (inclusive on both sides,
    matching pandas.Series.between and SQL BETWEEN). Missing timestamps (0.0) count as epoch —
    included by ``before=...``, excluded by ``after=...`` (when after > 0)."""
    if before is None and after is None:
        return values, timestamps
    before_ts = _coerce_timestamp(before) if before is not None else float("inf")
    after_ts = _coerce_timestamp(after) if after is not None else float("-inf")
    kept = [(v, t) for v, t in zip(values, timestamps) if after_ts <= t <= before_ts]
    if not kept:
        return [], []
    vs, ts = zip(*kept)
    return list(vs), list(ts)


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
        "dbname",
        "engine",
        "serializer",
        "compress",
        "b64",
        "append_mode",
        "is_function_stash",
        "is_tmp",
        "ttl",
        "safe",
        "max_entries",
        "_root_is_dir",
    ]
    metadata_cols = ["_version", "_written_at"]
    CONNECTION_TIMEOUT = 60  # Close connections after 60 seconds of inactivity
    append_mode = DEFAULT_APPEND_MODE
    is_tmp = False
    is_function_stash = False
    needs_lock = True
    needs_reconnect = False

    @log.debug
    def __init__(
        self,
        root_dir: str = None,
        dbname: str = None,
        compress: str = None,
        b64: bool = None,
        serializer: SERIALIZER_TYPES = None,
        parent: "BaseHashStash" = None,
        children: List["BaseHashStash"] = None,
        is_function_stash:bool=None,
        is_tmp:bool=None,
        append_mode: bool = False,
        clear: bool = False,
        ttl: Union[int, float, timedelta] = None,
        safe: bool = None,
        max_entries: int = None,
        legacy_read: bool = False,
        _root_is_dir: bool = None,
        **kwargs,
    ) -> None:
        # read caches whose keys were stored under an older encoding: on a get()
        # miss, fall back to a decode-and-match scan (see _legacy_find). Read-only
        self.legacy_read = legacy_read
        config = Config()
        # self.name = name if name is not None else self.name

        self.compress = get_compresser(
            compress if compress is not None else config.compress
        )
        self.b64 = b64 if b64 is not None else config.b64
        if b64 is None and (self.string_keys or self.string_values):
            self.b64 = True
        self.serializer = serializer if serializer is not None else config.serializer
        self.dbname = dbname if dbname is not None else self.dbname
        self.parent = parent
        self.children = [] if not children else children
        # ttl (seconds or timedelta): entries older than this read as absent.
        # Enforced on read, engine-agnostic; use prune() to reclaim storage.
        if isinstance(ttl, timedelta):
            ttl = ttl.total_seconds()
        if ttl is not None and ttl <= 0:
            raise ValueError(f"ttl must be positive, got {ttl!r}")
        self.ttl = ttl
        # safe mode: deserialization refuses payloads that would execute code
        # (see serializers.custom.safe_deserialization). Resolution order:
        #   explicit safe= arg > HASHSTASH_SAFE=1 (whole process) > engine default.
        # Networked/shared engines (redis/mongo, remote fsspec) default to safe
        # reads because their writer may be untrusted; local engines you own stay
        # code-capable. Only applies to the code-executing hashstash serializer
        # (data-only serializers are already safe). Override with safe=False.
        if safe is not None:
            self.safe = safe
        elif os.environ.get("HASHSTASH_SAFE"):
            self.safe = True
        elif self.serializer == "hashstash" and (
            self.engine in NETWORKED_ENGINES
            or (self.engine == "fsspec" and _is_remote_fsspec_uri(root_dir))
        ):
            self.safe = True
        else:
            self.safe = False
        if self.safe and self.serializer != "hashstash":
            raise ValueError(
                f"safe=True requires the 'hashstash' serializer; "
                f"{self.serializer!r} deserialization can always execute code"
            )
        # max_entries: soft cap; oldest entries are evicted (LRS) when exceeded
        if max_entries is not None and max_entries < 1:
            raise ValueError(f"max_entries must be >= 1, got {max_entries!r}")
        self.max_entries = max_entries
        self.is_function_stash = (
            is_function_stash
            if is_function_stash is not None
            else self.is_function_stash
        )
        self.is_tmp = is_tmp if is_tmp is not None else self.is_tmp
        self._tmp = None
        self.append_mode = append_mode if append_mode is not None else self.append_mode



        # _root_is_dir overrides the name-based heuristic: internal callers (sub())
        # know their root is a directory even when its name contains dots (the
        # param folder 'engine.serializer.encoding' always does — the old heuristic
        # silently collapsed every sub-stash onto its parent's directory)
        root_is_dir = _root_is_dir if _root_is_dir is not None else (
            root_dir is None or is_dir(root_dir)
        )
        self._root_is_dir = root_is_dir
        if root_is_dir:
            if root_dir is None:
                self.root_dir = os.path.join(config.root_dir,DEFAULT_NAME)
            else:
                root_dir = str(root_dir)
                if "://" in root_dir:
                    # an fsspec URL (s3://bucket/x, memory://cache): already
                    # absolute, and abspath would mangle the protocol
                    self.root_dir = root_dir
                elif os.path.isabs(root_dir) or root_dir.startswith("~"):
                    self.root_dir = os.path.expanduser(root_dir)
                elif os.sep in root_dir or "/" in root_dir or root_dir.startswith("."):
                    # a relative *path* (contains separators or leading dot):
                    # resolve from the current directory, like any file API would
                    self.root_dir = os.path.abspath(root_dir)
                else:
                    # a bare *name*: nest under the configured cache root
                    self.root_dir = os.path.join(config.root_dir, root_dir)

            folders = [self.root_dir]
            if self.dbname: folders.append(self.dbname)
            param_folder_name = f"{self.engine}.{self.serializer}.{get_encoding_str(self.compress, self.b64)}"
            folders.append(param_folder_name)
            self.path_dirname = os.path.join(*folders)
            self.path = os.path.join(self.path_dirname, self.filename)
            self._owns_dir = True
        else:
            path = Path(root_dir).expanduser().resolve()
            self.root_dir = str(path.parent)
            self.filename = str(path.name)
            self.path_dirname = str(path.parent)
            self.path = str(path)
            # path_dirname is a pre-existing directory we share with other files;
            # clear() must never remove it (see _owns_dir check there)
            self._owns_dir = False

        self._stats = _get_stats(self.path)

        if clear:
            self.clear()

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
        kwargs.setdefault("safe", self.safe)
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
    def db(self):
        return self.get_connection()

    def get_db(self):
        # This method should be implemented by subclasses
        raise NotImplementedError("Subclasses must implement get_db method")

    @log.debug
    def __enter__(self):
        # Blocking, reentrant, cross-process. The old implementation acquired
        # non-blocking and proceeded into the critical section on failure, and
        # released other holders' locks on exit — it excluded nothing.
        if self.needs_lock:
            get_lock(self.path).acquire()
        return self

    @log.debug
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.needs_lock:
            get_lock(self.path).release()

    @contextmanager
    def get_connection(self):
        if self.needs_reconnect:
            with self.get_db() as db:
                yield db
            return

        with _pool_guard:
            conn = _connection_pool.get(self.path)
            if conn is None:
                log.debug(f"Opening {self.engine} at {self.path}")
                conn = self.get_db()
                _connection_pool[self.path] = conn
            _conn_refcount[self.path] = _conn_refcount.get(self.path, 0) + 1
            _last_used[self.path] = time.time()
        try:
            yield conn
        finally:
            with _pool_guard:
                _conn_refcount[self.path] = _conn_refcount.get(self.path, 1) - 1
                _last_used[self.path] = time.time()
            self._cleanup_connections()

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
        with _pool_guard:
            stale = [
                path
                for path, last_used in _last_used.items()
                if current_time - last_used > cls.CONNECTION_TIMEOUT
                and _conn_refcount.get(path, 0) <= 0
            ]
        for path in stale:
            cls._close_connection_path(path)

    def close(self):
        self._close_connection_path(self.path)

    @classmethod
    def _close_connection_path(cls, path):
        with _pool_guard:
            conn = _connection_pool.pop(path, None)
            _last_used.pop(path, None)
            _conn_refcount.pop(path, None)
        if conn is not None:
            try:
                cls._close_connection(conn)
            except Exception as e:
                log.debug(e)

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
        obj = self.get(unencoded_key, default=_MISSING)
        if obj is _MISSING:
            raise KeyError(unencoded_key)
        return obj

    @log.debug
    def __setitem__(self, unencoded_key: str, unencoded_value: Any) -> None:
        self.set(unencoded_key, unencoded_value)

    @log.debug
    def get_func(self, *args, func=None, _dbname=None, default=None, **kwargs):
        fstash = (
            self.sub_function_results(func, dbname=_dbname)
            if not self.is_function_stash
            else self
        )
        return fstash.get(
            self.new_function_key(
                *args,
                **kwargs,
            ),
            default=default,
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
        single_flight=True,
        cache_exceptions=False,
        exception_ttl=None,
        **kwargs,
    ):
        unencoded_value = _MISSING
        if not _force:
            unencoded_value = self.get(unencoded_key, default=_MISSING, **kwargs)
            if unencoded_value is not _MISSING:
                unencoded_value = self._resolve_hit(unencoded_value)  # may re-raise
            if unencoded_value is _MISSING and single_flight:
                # single-flight: concurrent callers missing on the same key wait
                # for one compute instead of all running the setter
                with self.key_lock(unencoded_key):
                    unencoded_value = self.get(unencoded_key, default=_MISSING, **kwargs)
                    if unencoded_value is not _MISSING:
                        unencoded_value = self._resolve_hit(unencoded_value)
                    if unencoded_value is _MISSING:
                        log.debug("setting")
                        unencoded_value = self._call_setter(
                            unencoded_key, unencoded_value_setter,
                            cache_exceptions, exception_ttl,
                        )
                return unencoded_value if unencoded_value is not _MISSING else default
        if unencoded_value is _MISSING:
            log.debug("setting")
            unencoded_value = self._call_setter(
                unencoded_key, unencoded_value_setter, cache_exceptions, exception_ttl,
            )
        else:
            log.debug("getting")
        return unencoded_value if unencoded_value is not _MISSING else default

    def _call_setter(self, unencoded_key, setter, cache_exceptions, exception_ttl):
        try:
            value = setter()
        except Exception as e:
            if cache_exceptions:
                try:
                    expires_at = (time.time() + exception_ttl) if exception_ttl else None
                    self.set(unencoded_key, _make_cached_exc(e, expires_at))
                except Exception:
                    log.debug("could not negative-cache exception")
            raise
        self.set(unencoded_key, value)
        return value

    def key_lock(self, unencoded_key):
        """Cross-process lock scoped to one key, for single-flight computes.

        Locks are striped: the key hashes to one of SINGLE_FLIGHT_STRIPES file
        locks, so lock files stay bounded. Two different keys occasionally share
        a stripe and serialize their computes — harmless. Reentrant within a
        thread; spans processes on one machine (a file lock cannot span hosts, so
        multi-host redis/mongo callers may still race).

        The lock file is always LOCAL, derived from a hash of self.path under the
        temp dir — self.path can be a remote URL (s3://..., memory://...) for the
        fsspec engine, and building the lock at that path materialized a bogus
        `./s3:/...` lock tree in the working directory. Two processes on one
        machine hash to the same path, so cross-process single-flight still
        works."""
        import tempfile
        encoded_key = self.encode_key(unencoded_key)
        stripe = int(encode_hash(encoded_key), 16) % SINGLE_FLIGHT_STRIPES
        lock_base = os.path.join(
            tempfile.gettempdir(), "hashstash-locks", encode_hash(str(self.path))
        )
        return get_lock(f"{lock_base}.sf{stripe}")

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
        found = values is not None and (not isinstance(values, list) or bool(values))
        if (
            not found
            and self.legacy_read
            and unencoded_key is not None
            and not with_metadata
            and as_dataframe is None
        ):
            # the key may have been stored under an older encoding: find it by
            # decode-and-match and read its value (read-only, no rewrite)
            legacy = self._legacy_find(unencoded_key)
            if legacy is not _MISSING:
                self._stats["hits"] += 1
                return self.serialize(legacy, as_string=True) if as_string else legacy
        self._stats["hits" if found else "misses"] += 1
        value = values[-1] if values else default
        return self.serialize(value, as_string=True) if as_string else value

    @log.debug
    def get_all(
        self,
        unencoded_key: Any = None,
        default: Any = None,
        with_metadata: bool = None,
        all_results: bool = True,
        before=None,
        after=None,
        **kwargs,
    ) -> Any:
        encoded_key = self.encode_key(unencoded_key)
        encoded_value = self._get(encoded_key)
        if encoded_value is None:
            return default

        decoded = self.decode_value(encoded_value)
        values, timestamps = _unwrap_envelope(decoded)
        after = self._ttl_after(after, kwargs)
        values, timestamps = _filter_by_time(values, timestamps, before=before, after=after)
        if not values:
            return default
        if with_metadata:
            values = [
                {"_version": vi + 1, "_value": v, "_written_at": t}
                for vi, (v, t) in enumerate(zip(values, timestamps))
            ]
        if not self._all_results(all_results):
            values = values[-1:]
        return values

    @log.debug
    def set(self, unencoded_key: Any, unencoded_value: Any, append=None) -> None:
        # Append is a read-modify-write: it reads the old envelope, appends, and
        # writes it back. `with self:` locks that RMW for needs_lock=True engines
        # (sqlite/pairtree/shelve). The needs_lock=False KV engines (lmdb, redis,
        # mongo, diskcache, duckdb, leveldb, fsspec, memory) rely on their own
        # per-op locking, which does NOT span the two ops of an RMW — so an
        # unlocked concurrent append lost versions. Hold a per-key cross-process
        # lock around the RMW for append on those engines. (A file lock only
        # coordinates one machine; multi-host redis/mongo appends can still race.)
        is_append = append if append is not None else self.append_mode
        rmw_lock = self.key_lock(unencoded_key) if (is_append and not self.needs_lock) else None
        if rmw_lock is not None:
            rmw_lock.acquire()
        try:
            with self:
                encoded_key = self.encode_key(unencoded_key)
                new_unencoded_value = self.new_unencoded_value(
                    unencoded_value,
                    unencoded_key=unencoded_key,
                    append=append,
                )

                encoded_value = self.encode_value(new_unencoded_value)
                self._set(encoded_key, encoded_value)
        finally:
            if rmw_lock is not None:
                rmw_lock.release()
        self._stats["sets"] += 1
        if self.max_entries is not None:
            self._enforce_max_entries()

    def _enforce_max_entries(self):
        """Evict oldest entries (by latest write time) when over capacity.

        Amortized: eviction only fires when len exceeds max_entries, and then
        trims down to ~90% of the limit, so it runs about once every
        max_entries/10 writes rather than on every write. Enforcement scans all
        keys' timestamps (O(n)) when it fires — best for engines with cheap
        len (sqlite/lmdb/redis/mongo/memory) or moderate caches."""
        try:
            n = len(self)
        except Exception:
            return
        if n <= self.max_entries:
            return
        target = max(1, int(self.max_entries * 0.9))
        to_evict = n - target
        # (key, latest_ts) without decoding values where possible
        aged = self._entries_by_age()
        for key, _ts in aged[:to_evict]:
            try:
                self.delete(key)
            except KeyError:
                pass

    def _entries_by_age(self):
        """List of (key, latest_written_at) sorted oldest-first. Default
        implementation reads metadata via get_all; engines with timestamped
        filenames (pairtree) can override to avoid decoding values."""
        aged = []
        for key in list(self.keys()):
            entries = self.get_all(
                key, default=None, with_metadata=True, all_results=True,
                as_dataframe=False, as_list=True, apply_ttl=False,
            )
            if not entries:
                continue
            aged.append((key, entries[-1].get("_written_at", 0.0)))
        aged.sort(key=lambda kt: kt[1])
        return aged

    def _resolve_hit(self, res):
        """Interpret a cache hit. A normal value is returned as-is. A cached
        exception that is still valid is re-raised; an expired one returns
        _MISSING so the caller recomputes."""
        if _is_cached_exc(res):
            expires_at = res.get("expires_at")
            if expires_at is not None and time.time() > expires_at:
                return _MISSING
            raise _reconstruct_cached_exc(res)
        return res

    @log.debug
    def run(
        self,
        func,
        *args,
        _force=False,
        _store_args=True,
        _single_flight=True,
        _cache_exceptions=False,
        _exception_ttl=None,
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
        # underscore-prefixed kwargs are stash-control options: excluded from the
        # cache key and never passed to the function or to get()
        func_kwargs = {k: v for k, v in kwargs.items() if k and k[0] != "_"}
        unencoded_key = fstash.new_function_key(
            *args,
            store_args=_store_args,
            **func_kwargs,
        )
        if not _force:
            res = fstash.get(unencoded_key, default=_MISSING)
            if res is not _MISSING:
                res = fstash._resolve_hit(res)  # re-raises a cached exception
                if res is not _MISSING:
                    log.debug(
                        f"Stash hit for {func.__name__} in {fstash}. Returning stashed result"
                    )
                    return res
            if _single_flight:
                # single-flight: concurrent callers missing on the same key wait
                # for one compute instead of all executing the function (opt out
                # per call with _single_flight=False)
                with fstash.key_lock(unencoded_key):
                    res = fstash.get(unencoded_key, default=_MISSING)
                    if res is not _MISSING:
                        res = fstash._resolve_hit(res)
                        if res is not _MISSING:
                            log.debug(
                                f"Stash hit for {func.__name__} after waiting on "
                                f"another caller's compute"
                            )
                            return res
                    return self._run_and_store(
                        fstash, func, unencoded_key, args, func_kwargs,
                        _cache_exceptions, _exception_ttl,
                    )

        note = "Forced execution" if _force else "Stash miss"
        log.debug(f"{note} for {func.__name__}. Executing function.")
        return self._run_and_store(
            fstash, func, unencoded_key, args, func_kwargs,
            _cache_exceptions, _exception_ttl,
        )

    @staticmethod
    def _run_and_store(
        fstash, func, unencoded_key, args, func_kwargs,
        cache_exceptions=False, exception_ttl=None,
    ):
        funcx = unwrap_func(func)
        try:
            result = call_function_politely(funcx, *args, **func_kwargs)
        except Exception as e:
            if cache_exceptions:
                # negative caching: a later call re-raises this instead of
                # re-executing. Guard the write so a serialization hiccup on the
                # marker never masks the real exception.
                try:
                    expires_at = (time.time() + exception_ttl) if exception_ttl else None
                    fstash.set(unencoded_key, _make_cached_exc(e, expires_at))
                except Exception:
                    log.debug("could not negative-cache exception")
            raise
        result = list(result) if is_generator(result) else result
        log.debug(
            f"Caching result for {func.__name__} under {serialize(unencoded_key)}"
        )
        fstash.set(unencoded_key, result)
        return result

    # --- async API ---------------------------------------------------------
    # Storage engines are synchronous; these run the blocking work in a thread
    # so an event loop isn't stalled. arun additionally awaits coroutine
    # functions (caching the awaited result, not the coroutine object) — this
    # is what @stashed_result uses to wrap `async def`.

    async def aget(self, *args, **kwargs):
        import asyncio
        return await asyncio.to_thread(self.get, *args, **kwargs)

    async def aset(self, *args, **kwargs):
        import asyncio
        return await asyncio.to_thread(self.set, *args, **kwargs)

    async def ahas(self, *args, **kwargs):
        import asyncio
        return await asyncio.to_thread(self.has, *args, **kwargs)

    async def arun(self, func, *args, _force=False, _store_args=True,
                   _cache_exceptions=False, _exception_ttl=None, **kwargs):
        import asyncio

        funcx = unwrap_func(func)
        if not asyncio.iscoroutinefunction(funcx):
            # plain callable: just run() off-thread (which handles exception
            # caching itself)
            return await asyncio.to_thread(
                self.run, func, *args,
                _force=_force, _store_args=_store_args,
                _cache_exceptions=_cache_exceptions, _exception_ttl=_exception_ttl,
                **kwargs,
            )

        fstash = self.attach_func(func)
        func_kwargs = {k: v for k, v in kwargs.items() if k and k[0] != "_"}
        unencoded_key = fstash.new_function_key(
            *list(args), store_args=_store_args, **func_kwargs
        )
        if not _force:
            res = await asyncio.to_thread(fstash.get, unencoded_key, default=_MISSING)
            if res is not _MISSING:
                # re-raise a still-valid cached exception; expired -> _MISSING ->
                # recompute (parity with sync run())
                res = fstash._resolve_hit(res)
                if res is not _MISSING:
                    return res
        # await the coroutine; negative-cache a failure like run() does so a
        # later await replays it instead of re-executing
        try:
            result = await funcx(*args, **func_kwargs)
        except Exception as e:
            if _cache_exceptions:
                try:
                    expires_at = (time.time() + _exception_ttl) if _exception_ttl else None
                    await asyncio.to_thread(
                        fstash.set, unencoded_key, _make_cached_exc(e, expires_at)
                    )
                except Exception:
                    log.debug("could not negative-cache exception")
            raise
        await asyncio.to_thread(fstash.set, unencoded_key, result)
        return result

    def map(
        self,
        func,
        objects=[],
        options=[],
        num_proc=None,
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
        if stash_map and getattr(self, "safe", False):
            # a stored StashMap embeds the mapped function, which safe-mode
            # deserialize refuses — so caching the whole map on a safe stash
            # (redis/mongo default) makes it unreadable: a repeat map(), or even
            # keys()/items(), would raise SafeDeserializationError. Skip the
            # map-level cache under safe mode; per-item results still cache.
            stash_map = False
        # common_kwargs are merged into every call's options, so they must be part
        # of the map's identity or maps differing only in kwargs return stale results
        key = StashMap.get_stash_key(
            func, objects, options, total=total, **common_kwargs
        )
        #pprint(key)
        if stash_map and not _force and self.has(key):
            log.info(f"Stash hit for {func.__name__} in {self}. Returning stashed StashMap")
            pmap = self.get(key)
            log.info(f"Returned stashed StashMap")

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
            pmap.num_proc = get_num_proc(num_proc)
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
        for f in (func, funcx):
            try:
                f.__dict__["stash"] = local_stash
            except (AttributeError, TypeError):
                pass  # builtins have no writable __dict__
        return local_stash

    @log.debug
    def new_function_key(self, *args, store_args=True, **kwargs):
        # key = {
        #     "args": args,
        #     "kwargs": kwargs,
        # }
        key = (args,kwargs)
        return encode_hash(self.serialize(key, sort_keys=True)) if not store_args else key

    @log.debug
    def new_unencoded_value(
        self,
        unencoded_value: Any,
        unencoded_key=None,
        append=None,
    ):
        now_ts = time.time()
        if (append or self.append_mode) and unencoded_key is not None:
            encoded_key = self.encode_key(unencoded_key)
            encoded_value = self._get(encoded_key)
            if encoded_value is not None:
                decoded = self.decode_value(encoded_value)
                oldvals, oldts = _unwrap_envelope(decoded)
            else:
                oldvals, oldts = [], []
            values = oldvals + [unencoded_value]
            timestamps = oldts + [now_ts]
        else:
            values = [unencoded_value]
            timestamps = [now_ts]
        return _wrap_envelope(values, timestamps)

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
            raise

    @log.debug
    def __contains__(self, unencoded_key: Any) -> bool:
        return self.has(unencoded_key)

    @log.debug
    def has(self, unencoded_key: Any) -> bool:
        if self.ttl is not None:
            # storage-level existence isn't enough: an expired entry must read
            # as absent everywhere, or get_set/run would trust a dead key
            return self.get(unencoded_key, default=_MISSING) is not _MISSING
        if self._has(self.encode_key(unencoded_key)):
            return True
        if self.legacy_read:
            # the hot path is `if key in stash: stash[key]` — legacy_read must
            # cover membership too, or old-format entries still read as absent
            return self._legacy_find(unencoded_key) is not _MISSING
        return False

    @log.debug
    def encode_key(self, unencoded_key: Any) -> Union[str, bytes]:
        # sort_keys: equal dicts (and equal kwargs) must encode to identical bytes
        # regardless of insertion order, or lookups silently miss
        return self.encode(
            self.serialize(unencoded_key, sort_keys=True),
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
            b64=self.b64,
            compress=self.compress,
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
        decoded_value = self.decode(
            encoded_value,
            b64=self.b64,
            compress=self.compress,
        )
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
        for sub in self.children:
            sub.clear()

        self.close()
        if getattr(self, "_owns_dir", True):
            self._remove_dir(self.path_dirname)
        else:
            self._remove_dir(self.path)
        return self

    @log.debug
    def __len__(self) -> int:
        # Physical count of stored keys. TTL is applied LAZILY: an expired entry
        # is hidden by `in`/`get` and skipped by iteration, but still counted
        # here until it is overwritten/deleted (or compacted). So on a TTL'd
        # stash, len() can exceed the number of live entries — matching how disk
        # caches generally treat expiry (no eager background sweep).
        with self as cache, cache.db as db:
            return len(db)

    @log.debug
    def __delitem__(self, unencoded_key: str) -> None:
        self.delete(unencoded_key)
    
    def delete(self, unencoded_key: str) -> None:
        if not self.has(unencoded_key):
            raise KeyError(unencoded_key)
        self._del(self.encode_key(unencoded_key))
        self._stats["deletes"] += 1

    def invalidate(self, *args, **kwargs) -> bool:
        """Delete the cached result for one call signature.

        On a function stash (``func.stash``), pass the same arguments as the
        original call: ``func.stash.invalidate(2, 3)``. On a plain stash, pass
        the key itself: ``stash.invalidate(key)``. Returns True if an entry was
        deleted, False if there was nothing cached for that signature."""
        if self.is_function_stash:
            func_kwargs = {k: v for k, v in kwargs.items() if k and k[0] != "_"}
            key = self.new_function_key(
                *args, store_args=kwargs.get("_store_args", True), **func_kwargs
            )
        else:
            if len(args) != 1 or kwargs:
                raise TypeError(
                    "invalidate() on a non-function stash takes exactly one "
                    "argument: the key"
                )
            key = args[0]
        try:
            self.delete(key)
            return True
        except KeyError:
            return False

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

    # --- legacy-cache recovery -------------------------------------------------
    # A cache written by an OLDER hashstash whose key encoding differs (the
    # serialized/canonical form of a key changed across versions) still
    # decodes its keys — keys() works and len() is right — but get()/`in`/items()
    # silently miss, because encode_key(key) now hashes to a DIFFERENT address
    # than where the entry was stored. These read each entry via its STORED
    # encoded key (from _keys()), which _get() hashes to the correct address,
    # so they never depend on encode_key(key) still matching.

    def _raw_items(self):
        """Raw (encoded_key, encoded_value) pairs including ALL stored versions.
        Engines whose _items() takes all_results (pairtree) default to latest-only,
        which silently dropped history during recovery — ask for everything."""
        try:
            return self._items(all_results=True)
        except TypeError:
            return self._items()

    def iter_recovered(self):
        """Yield (key, value) for every stored version by reading the engine's raw
        (encoded_key, encoded_value) pairs and unwrapping the value envelope — the
        read path never calls encode_key(key), so it works on a cache from any
        hashstash version whose keys/values this version can still decode. Yields
        every version (oldest-first); migrate() re-appends them so history
        survives. Streams; holds nothing in memory."""
        for enc_key, enc_val in self._raw_items():
            try:
                key = self.decode_key(enc_key)
                values, _ = _unwrap_envelope(self.decode_value(enc_val))
            except Exception as e:
                log.debug(f"recover: skipping an unreadable entry: {e}")
                continue
            for value in values:
                yield key, value

    def _legacy_items(self, all_results=None):
        """items() under legacy_read: every version (oldest-first) when
        all_results, else the latest per key."""
        if self._all_results(all_results):
            yield from self.iter_recovered()
            return
        latest = {}  # canonical-key -> (key, value); iter_recovered is oldest-first
        for key, value in self.iter_recovered():
            latest[serialize(key, as_string=True, sort_keys=True)] = (key, value)
        for key, value in latest.values():
            yield key, value

    def _legacy_find(self, unencoded_key):
        """Read the latest value whose key was stored under an older encoding, by
        scanning the raw entries and matching the decoded key (streaming,
        read-only, no rewrite). O(n) per call — enabled per-stash with
        legacy_read=True; for many reads or a large cache, migrate() once."""
        want = serialize(unencoded_key, as_string=True, sort_keys=True)
        for enc_key, enc_val in self._items():
            try:
                if serialize(self.decode_key(enc_key), as_string=True, sort_keys=True) == want:
                    values, _ = _unwrap_envelope(self.decode_value(enc_val))
                    if values:
                        return values[-1]
            except Exception:
                continue
        return _MISSING

    def _ttl_after(self, after, kwargs=None):
        """Effective 'after' floor for reads: an explicit after wins; otherwise
        the ttl floor applies unless apply_ttl=False was passed (prune() needs
        raw access or expired entries could never be reclaimed)."""
        if after is not None:
            return after
        if kwargs is not None and kwargs.pop("apply_ttl", None) is False:
            return None
        if self.ttl:
            return time.time() - self.ttl
        return None

    def _all_results(self, all_results=None):
        # Default to LATEST-per-key — consistent with len() and stash[key].
        # Pass all_results=True for every appended version (append_mode history).
        # This used to default to self.append_mode, so items()/values() in an
        # append_mode stash yielded ALL versions while len()/getitem were latest,
        # and DataFrames built from items() double-counted rewritten keys.
        return all_results if all_results is not None else False

    @log.debug
    def keys(self, as_string=False):
        for x in self._keys():
            try:
                yield self.decode_key(x, as_string=as_string)
            except Exception as e:
                log.error(f"Error decoding key: {e}")
                raise e

    def filter_keys(self, _subdict=None, **field_values):
        """Yield stored keys that are dicts containing all the given field=value
        pairs — a convenience over scanning ``keys()`` yourself when you use
        structured dict keys (``stash[{"model": m, "prompt": p}] = ...``).

            for key in stash.filter_keys(model="gpt-4"):
                ...

        This is an O(n) scan (there is no per-field key index); it just saves the
        boilerplate. Pass fields as kwargs or a dict: ``filter_keys({"model": m})``.
        """
        query = {**(_subdict or {}), **field_values}
        for key in self.keys():
            if isinstance(key, dict) and all(key.get(k) == v for k, v in query.items()):
                yield key

    @log.debug
    def values(self, all_results=None, with_metadata=False, **kwargs):
        for k, v in self.items(all_results=all_results, with_metadata=with_metadata):
            yield v

    @log.debug
    def items(self, all_results=None, with_metadata=False, **kwargs):
        if self.legacy_read and not with_metadata:
            # read old-format entries the normal path can't address; reads raw, so
            # it also covers any new-format entries (no double-yield)
            yield from self._legacy_items(all_results=all_results)
            return
        n_keys = n_yield = 0
        for key in self.keys():
            n_keys += 1
            vals = self.get_all(
                key,
                all_results=all_results,
                with_metadata=with_metadata,
                **kwargs,
            )
            if vals is not None:
                for val in vals:
                    n_yield += 1
                    yield key, val
        if n_keys and not n_yield and not self.legacy_read and not self.ttl:
            # keys enumerate but NONE resolve to a value: the tell-tale sign of a
            # cache written by an older hashstash whose key encoding differs.
            # Warn loudly rather than silently look empty (which risks re-spending
            # the budget that built the cache).
            self._warn_unaddressable(n_keys)

    def _warn_unaddressable(self, n):
        if getattr(self, "_warned_unaddressable", False):
            return
        self._warned_unaddressable = True
        self._warn_data_integrity(
            f"{type(self).__name__}: {n} stored keys enumerate but NONE could be "
            f"read — almost certainly a cache written by an OLDER hashstash whose "
            f"key encoding differs. Recover it with stash.migrate(dest=...) "
            f"(dry_run=True to count first), or open the stash with "
            f"legacy_read=True. Fresh writes are unaffected."
        )

    @staticmethod
    def _warn_data_integrity(msg):
        # both channels on purpose: the logger stays loud on stderr even under
        # filterwarnings('ignore') (common in ML stacks); the warning lets
        # programmatic consumers catch it via warnings.catch_warnings.
        import warnings

        log.warning(msg)
        warnings.warn(msg, HashStashWarning, stacklevel=3)

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
        if hasattr(other, "items"):
            for key, value in other.items():
                self[key] = value
        for key, value in kwargs.items():
            self[key] = value

    @log.debug
    def setdefault(self, key, default=None):
        val = self.get(key, default=_MISSING)
        if val is not _MISSING: return val
        self.set(key,default)
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
        try:
            key, value = next(iter(self.items()))
        except StopIteration:
            raise KeyError("popitem(): stash is empty") from None
        del self[key]
        return (key,value)

    @log.debug
    def hash(self, data: bytes) -> str:
        return encode_hash(data)

    @property
    def stats(self):
        """Counters for this stash instance: hits, misses, sets, deletes (all
        four keys always present). Per-instance and in-memory only (not shared
        across processes). Note: `stash.run(...)` / `@stashed_result` count on
        the function's own sub-stash (`func.stash.stats`), not on this one."""
        base = {"hits": 0, "misses": 0, "sets": 0, "deletes": 0}
        base.update(self._stats)
        return base

    def reset_stats(self):
        self._stats.clear()
        return self

    @property
    def stashed_result(self):
        return stashed_result(stash=self)
    
    @property
    def stashed(self):
        return self.stashed_result

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
    def sub(self, root_dir:str=None, dbname=DEFAULT_SUB_DBNAME, **kwargs):
        kwargs = {
            **self.to_dict(),
            **kwargs,
            "parent": self,
            'root_dir': root_dir if root_dir is not None else self.path_dirname,
            'dbname': dbname
        }
        if root_dir is None:
            # our own path_dirname is definitionally a directory, even though its
            # dotted param-folder name would fail the is_dir extension heuristic
            kwargs['_root_is_dir'] = True
        new_instance = self.__class__(**kwargs)
        self.children.append(new_instance)
        return new_instance

    def graph(self, name="graph"):
        from ..graph import GraphStash
        return GraphStash(self, name=name)

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
        # append a compact summary of any non-zero activity counters
        st = self.stats
        active = " ".join(f"{k}={st[k]}" for k in ("hits", "misses", "sets", "deletes") if st[k])
        suffix = f" [{active}]" if active else ""
        return f"{self.__class__.__name__}({path}){suffix}"

    def _repr_html_(self):
        selfstr = repr(self)
        dict_items = self.to_dict()
        dict_items["len"] = len(self)
        attr_groups = {
            "path": ["root_dir", "dbname", "filename"],
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
        func_name = get_obj_addr(func).replace("<", "_").replace(">", "_")
        if update_on_src_change or not self._function_is_stable_identity(func):
            # closures, lambdas, and non-importable functions can't be identified
            # by address alone (every lambda is '__main__.<lambda>'; two closures
            # from one factory share an address): include source + closure values
            # in the namespace so different functions never share cached results
            func_name += "/" + encode_hash(self._function_identity_sig(func))[:10]
        new_dbname = f'{"stashed_result" if not dbname else dbname}/{func_name}'
        log.debug(f"Sub-function results stash: {new_dbname}")
        stash = self.sub(
            dbname=new_dbname,
            is_function_stash=True,
        )
        try:
            func.__dict__["stash"] = stash
        except (AttributeError, TypeError):
            pass  # builtins have no writable __dict__
        stash.__dict__["func"] = func
        return stash

    @staticmethod
    def _function_is_stable_identity(func):
        """True if the function's import address alone identifies it: importable,
        not a lambda, and not a closure (whose cell values the address can't see)."""
        if getattr(func, "__name__", "") == "<lambda>":
            return False
        if getattr(unwrap_func(func), "__closure__", None):
            return False
        return can_import_object(func)

    @staticmethod
    def _function_identity_sig(func):
        from ..serializers.custom import get_function_closure

        sig = get_function_src(func) or getattr(func, "__qualname__", repr(func))
        closure = get_function_closure(unwrap_func(func))
        if closure:
            sig += json.dumps(closure, sort_keys=True, default=str)
        return sig

    def assemble_ld(
        self,
        all_results=None,
        with_metadata=None,
        flatten=True,
        progress=False,
    ):
        ld = []
        iterr = self.items_l(
            all_results=self._all_results(all_results),
            with_metadata=True,
        )
        if progress:
            iterr = progress_bar(iterr, desc='Assembling cached contents')
        for key, value_d in iterr:
            if self.is_function_stash:
                args,kwargs = key
                key_d = {f'_arg{n+1}':arg for n,arg in enumerate(args)}
                key_d.update({f'_{k}':v for k,v in kwargs.items()})
            else:
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
        import pandas as pd
        from ..utils.dataframes import set_index

        ld = self.assemble_ld(
            all_results=all_results,
            with_metadata=with_metadata,
            **kwargs,
        )
        if not ld:
            return pd.DataFrame()
        # key columns are '_'-prefixed; promote them to the index
        return set_index(pd.DataFrame(ld), prefix_columns="_")

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

    def size_bytes(self):
        """Alias for filesize — total on-disk bytes used by this stash."""
        return self.filesize

    def last_modified(self):
        """Most recent modification time (unix timestamp) across this stash's files, or None if empty/missing."""
        if not os.path.exists(self.path):
            return None
        if os.path.isfile(self.path):
            return os.path.getmtime(self.path)
        latest = None
        for dirpath, _, filenames in os.walk(self.path):
            for filename in filenames:
                mtime = os.path.getmtime(os.path.join(dirpath, filename))
                if latest is None or mtime > latest:
                    latest = mtime
        return latest

    def filter(self, predicate):
        """Yield (key, value) pairs where predicate(key) is truthy. Keys are iterated lazily and values
        are only decoded for matches, so this skips value-decode cost for non-matches."""
        for key in self.keys():
            if predicate(key):
                yield key, self[key]

    def migrate(self, dest=None, dry_run=False, **kwargs):
        """Copy every entry into ``dest``, reading via the raw (encoded) entries
        so it also RECOVERS a cache written by an older hashstash that
        ``items()``/``get()`` now silently miss (the key encoding changed across
        versions — keys still decode, but ``encode_key(key)`` hashes elsewhere).

        dest: destination HashStash; if None, kwargs build one (e.g.
        ``engine='jsonl'``). dry_run: count only, no writes — safe on a huge
        stash, and lets you diff the counts against an expected total first.

        Returns a report ``{'total', 'migrated', 'failed', 'dest'}`` (previously
        returned the dest stash — it is now ``report['dest']``).
        """
        if not dry_run and dest is None:
            dest = HashStash(**kwargs)
        total = migrated = failed = 0
        for enc_key, enc_val in self._raw_items():
            total += 1
            try:
                key = self.decode_key(enc_key)
                values, _ = _unwrap_envelope(self.decode_value(enc_val))
            except Exception as e:
                log.debug(f"migrate: skipping an unreadable entry: {e}")
                failed += 1
                continue
            for value in values:  # each stored version (oldest-first)
                if dry_run:
                    migrated += 1
                    continue
                try:
                    # append so multi-version (append-mode) source history survives
                    # regardless of dest's append_mode; a single-version key just
                    # lands once, so a plain latest-only cache migrates unchanged.
                    dest.set(key, value, append=True)
                    migrated += 1
                except Exception as e:
                    log.debug(f"migrate: could not re-store an entry: {e}")
                    failed += 1
        if not total:
            self._warn_empty_migrate()
        log.info(
            f"migrate(dry_run={dry_run}): {migrated} migrated, {failed} failed "
            f"/ {total} entries"
        )
        return {"total": total, "migrated": migrated, "failed": failed, "dest": dest}

    def _warn_empty_migrate(self):
        """Nothing to migrate — but if a SIBLING layout dir (same parent, different
        engine/serializer/encoding suffix) holds data, the stash was almost
        certainly opened with the wrong kwargs (e.g. b64 omitted): the layout is
        encoded in the dirname, so this path resolves empty while the data sits
        one dir over. Point the user at it instead of silently reporting 0."""
        try:
            here = str(self.path)
            parent = os.path.dirname(os.path.dirname(here))  # .../<layout>/data.db
            layout_dir = os.path.dirname(here)
            if not os.path.isdir(parent):
                return
            siblings = [
                d for d in os.listdir(parent)
                if os.path.join(parent, d) != layout_dir
                and os.path.isdir(os.path.join(parent, d))
            ]
            if siblings:
                self._warn_data_integrity(
                    f"migrate found 0 entries at {layout_dir}, but sibling layout "
                    f"dir(s) exist: {siblings}. The engine/serializer/encoding "
                    f"(e.g. b64) is part of the path — reopen the source with the "
                    f"kwargs matching the intended layout dir, then migrate."
                )
        except Exception:
            pass

    def prune(self, older_than=None, dry_run=True):
        """Delete entries where the latest-write timestamp is older than ``older_than`` (a
        ``datetime`` or ``timedelta``; a timedelta is interpreted as "older than now - delta").

        Entries without a recorded timestamp (pre-feature data) are left alone — we don't delete
        what we can't date.

        Returns the count of entries matched. When ``dry_run=True`` (the default) nothing is
        actually deleted — use ``dry_run=False`` to perform the deletion.

        ``older_than`` is required: calling ``prune()`` with no age filter raises ``ValueError``
        to prevent accidentally nuking the whole cache."""
        if older_than is None:
            raise ValueError(
                "prune() requires older_than= (a datetime or timedelta). Refusing to run "
                "without an age filter — pass older_than=timedelta(days=0) if you really want "
                "to delete everything stamped."
            )
        if isinstance(older_than, timedelta):
            cutoff = time.time() - older_than.total_seconds()
        else:
            cutoff = _coerce_timestamp(older_than)
        matched = []
        total = 0
        for key in list(self.keys()):
            total += 1
            # as_dataframe/as_list are honored by the dataframe engine (and harmlessly
            # ignored elsewhere): prune needs plain dicts to read _written_at from.
            # apply_ttl=False: prune must see expired entries or it could never
            # reclaim them
            entries = self.get_all(
                key, default=None, with_metadata=True, all_results=True,
                as_dataframe=False, as_list=True, apply_ttl=False,
            )
            if not entries:
                continue
            latest_ts = entries[-1].get("_written_at", 0.0)
            if latest_ts <= 0:
                continue  # unstamped — skip
            if latest_ts <= cutoff:
                matched.append(key)
        mode = "dry_run" if dry_run else "delete"
        log.info(
            f"prune(older_than={older_than!r}, dry_run={dry_run}): matched {len(matched)} / {total} entries [{mode}]"
        )
        if not dry_run:
            for key in matched:
                # delete the physical entry directly: `del self[key]` goes
                # through has(), which is TTL-aware and would raise KeyError on a
                # TTL-expired-but-present key — exactly the entries prune targets.
                # prune already iterated keys(), so existence is not in question.
                self._del(self.encode_key(key))
                self._stats["deletes"] += 1
        return len(matched)


# @fcache
def HashStash(
    root_dir: str = None,
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
    if serializer is not None:
        serializer = get_serializer_type(serializer)

    engine_registry = {
        "pairtree": ("hashstash.engines.pairtree", "PairtreeHashStash"),
        "sqlite": ("hashstash.engines.sqlite", "SqliteHashStash"),
        "sqlitedict": ("hashstash.engines.sqlite", "SqliteHashStash"),
        "memory": ("hashstash.engines.memory", "MemoryHashStash"),
        "shelve": ("hashstash.engines.shelve", "ShelveHashStash"),
        "redis": ("hashstash.engines.redis", "RedisHashStash"),
        "diskcache": ("hashstash.engines.diskcache", "DiskCacheHashStash"),
        "lmdb": ("hashstash.engines.lmdb", "LMDBHashStash"),
        "mongo": ("hashstash.engines.mongo", "MongoHashStash"),
        "dataframe": ("hashstash.engines.dataframe", "DataFrameHashStash"),
        "jsonl": ("hashstash.engines.jsonl", "JSONLHashStash"),
        "fsspec": ("hashstash.engines.fsspec", "FsspecHashStash"),
        "duckdb": ("hashstash.engines.duckdb_engine", "DuckDBHashStash"),
        "leveldb": ("hashstash.engines.leveldb", "LevelDBHashStash"),
    }
    module_name, class_name = engine_registry[engine]
    try:
        module = importlib.import_module(module_name)
    except ImportError as e:
        hint = ENGINE_INSTALL_HINTS.get(engine, engine)
        raise ImportError(
            f"HashStash engine {engine!r} failed to import ({e}). "
            f"Install its dependencies with: pip install {hint}"
        ) from e
    cls = getattr(module, class_name)

    # name= is a friendly alias for dbname= (the real param): several users
    # reached for name= and had it silently swallowed into a shared default stash
    if "name" in kwargs and dbname is None:
        dbname = kwargs.pop("name")
    elif "name" in kwargs:
        kwargs.pop("name")

    # Reject-by-warning on unknown kwargs instead of silently dropping them: a
    # typo like dir= / ttll= / compres= otherwise sends data to the default
    # cache or disables a setting with no signal — a data-safety footgun.
    _warn_unknown_stash_kwargs(cls, kwargs)

    return cls(
        root_dir=root_dir,
        compress=compress,
        b64=b64,
        serializer=serializer,
        dbname=dbname,
        **kwargs,
    )


def _accepted_kwarg_names(cls):
    import inspect
    names = set()
    for klass in cls.__mro__:
        init = klass.__dict__.get("__init__")
        if init is None:
            continue
        try:
            for pname, p in inspect.signature(init).parameters.items():
                if pname != "self" and p.kind in (
                    p.POSITIONAL_OR_KEYWORD, p.KEYWORD_ONLY
                ):
                    names.add(pname)
        except (ValueError, TypeError):
            pass
    return names


def _warn_unknown_stash_kwargs(cls, kwargs):
    known = _accepted_kwarg_names(cls)
    known |= set(getattr(cls, "to_dict_attrs", []))
    # factory params + common engine params that flow through as **kwargs
    known |= {
        "root_dir", "engine", "dbname", "name", "compress", "b64", "serializer",
        "filename", "df_engine", "io_engine", "map_size", "max_map_size", "flat",
    }
    # underscore kwargs are internal (from_dict round-trips) — never flag them
    unknown = [k for k in kwargs if k not in known and not k.startswith("_")]
    if unknown:
        import warnings
        warnings.warn(
            f"HashStash: ignoring unrecognized argument(s) {unknown} — check for "
            f"typos (e.g. root_dir not dir, dbname/name, ttl, compress). An "
            f"unrecognized argument is dropped, which can silently write to the "
            f"default cache or leave a setting off.",
            stacklevel=3,
        )


def attach_stash_to_function(func, stash=None, **stash_kwargs):
    if stash is None:
        stash = HashStash(**stash_kwargs)
    local_stash = stash.sub_function_results(func)
    func.stash = local_stash
    return stash


Stash = HashStash
