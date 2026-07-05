# Explicit stdlib imports: this package's `from . import *` chains are
# circular, and whether a name has landed in the package namespace yet
# depends on import order (spawn workers + editable installs order imports
# differently). Never rely on the star-chain for stdlib names.
from typing import List
from typing import Set
from typing import Union
import importlib
import os

from . import *
from . import constants as _constants

class Config:
    def __init__(
        self,
        serializer: Union[SERIALIZER_TYPES, List[SERIALIZER_TYPES]] = None,
        engine: ENGINE_TYPES = None,
        compress: bool = None,
        b64: bool = DEFAULT_B64,
        root_dir: str = None,
        **kwargs,
    ):
        self.serializer = get_serializer_type(serializer)
        self.engine = get_engine(engine)
        self.compress = get_compresser(compress)
        self.b64 = b64
        if root_dir is None:
            # resolved at call time, not bound at import: HASHSTASH_ROOT_DIR and
            # patched constants (e.g. test isolation) must take effect for
            # Config() instances created later
            root_dir = (
                os.environ.get("HASHSTASH_ROOT_DIR")
                or _constants.DEFAULT_ROOT_DIR
            )
        self.root_dir = root_dir


    def to_dict(self):
        return {
            "serializer": self.serializer,
            "engine": self.engine,
            "compress": self.compress,
            "b64": self.b64,
            "root_dir": self.root_dir,
        }

    def __repr__(self):
        return f"hashstash.Config({self.to_dict()})"

    def set_serializer(
        self, serializer: Union[SERIALIZER_TYPES, List[SERIALIZER_TYPES]]
    ):
        # check if serializer is allowable
        if not serializer in set(SERIALIZER_TYPES.__args__):
            raise ValueError(
                f"Invalid serializer: {serializer}. Options: {', '.join(SERIALIZER_TYPES.__args__)}."
            )
        self.serializer = serializer

    def set_engine(self, engine: ENGINE_TYPES):
        # check if engine is allowable
        if engine not in set(ENGINE_TYPES.__args__):
            raise ValueError(
                f"Invalid engine: {engine}. Options: {', '.join(ENGINE_TYPES.__args__)}."
            )
        self.engine = engine

    def set_compress(self, compress: bool):
        self.compress = compress

    def set_root_dir(self, root_dir: str):
        self.root_dir = root_dir

    def set_b64(self, b64: bool):
        self.b64 = b64

    def disable_compression(self):
        self.compress = False

    def disable_b64(self):
        self.b64 = False

    def enable_compression(self):
        self.compress = True

    def enable_b64(self):
        self.b64 = True






# --- lazy, per-backend availability ------------------------------------------
#
# Optional-backend availability is resolved LAZILY and cached PER BACKEND, so
# constructing a Config (which resolves serializer/engine/compress) never imports
# a backend it isn't actually asked for. The old get_working_* helpers eagerly
# probed EVERY optional backend on their first call — importing pandas/blosc/
# duckdb/pymongo/redis/jsonpickle/... — which made the first serialize() in a
# process pay ~300ms for imports it never used. Now each backend is import-probed
# only when it is requested (get_engine/get_serializer_type/get_compresser), and
# the full working-set is enumerated only when genuinely needed (listings /
# error messages). Correctness is unchanged: the same modules gate the same
# backends, just imported on demand.

@fcache
def _can_import(module_name):
    """Cached: is this module importable? Imports ONLY that module, not the world."""
    try:
        importlib.import_module(module_name)
        return True
    except ImportError:
        return False


# each optional engine -> the module(s) that must import for it to be usable
_ENGINE_REQUIRES = {
    "sqlite": ("sqlitedict",),
    "redis": ("redis", "redis_dict"),
    "diskcache": ("diskcache",),
    "lmdb": ("lmdb",),
    "mongo": ("pymongo",),
    "dataframe": ("pandas", "numpy"),
    "fsspec": ("fsspec",),
    "duckdb": ("duckdb",),
    "leveldb": ("plyvel",),
}


@fcache
def _engine_available(engine):
    # builtins (incl. jsonl) are stdlib-only and always available
    if engine in BUILTIN_ENGINES:
        return True
    reqs = _ENGINE_REQUIRES.get(engine)
    if reqs is None:
        return False  # unknown engine name
    return all(_can_import(m) for m in reqs)


@fcache
def get_working_engines():
    working_engines = set(BUILTIN_ENGINES)
    for engine in _ENGINE_REQUIRES:
        if _engine_available(engine):
            working_engines.add(engine)
    return working_engines


def get_engine(engine):
    # Fail loudly: the old silent fallback to pairtree meant a typo'd engine name
    # (or a missing dependency) quietly wrote to the wrong store
    if engine is None:
        engine = OPTIMAL_ENGINE_TYPE
    if not _engine_available(engine):
        if engine in ENGINES:
            hint = ENGINE_INSTALL_HINTS.get(engine, engine)
            raise ImportError(
                f"HashStash engine {engine!r} is not installed. "
                f"Install it with: pip install {hint}"
            )
        raise ValueError(
            f"Unknown HashStash engine {engine!r}. Choose one of: {', '.join(ENGINES)}"
        )
    return engine





# hashstash/pickle are always available (stdlib); the rest are optional imports
_ALWAYS_SERIALIZERS = ("hashstash", "pickle")
_SERIALIZER_REQUIRES = {
    "jsonpickle": ("jsonpickle",),
    "msgpack": ("msgpack",),
    "cbor2": ("cbor2",),
}


@fcache
def _serializer_available(serializer):
    if serializer in _ALWAYS_SERIALIZERS:
        return True
    reqs = _SERIALIZER_REQUIRES.get(serializer)
    if reqs is None:
        return False
    return all(_can_import(m) for m in reqs)


@fcache
def get_working_serializers():
    working_serializers = list(_ALWAYS_SERIALIZERS)
    for serializer in _SERIALIZER_REQUIRES:
        if _serializer_available(serializer):
            working_serializers.append(serializer)
    return working_serializers

def get_serializer_type(serializer):
    if serializer is None:
        serializer = OPTIMAL_SERIALIZER
    if not _serializer_available(serializer):
        if serializer in SERIALIZERS:
            raise ImportError(
                f"HashStash serializer {serializer!r} is not installed. "
                f"Install it with: pip install {serializer}"
            )
        raise ValueError(
            f"Unknown HashStash serializer {serializer!r}. "
            f"Choose one of: {', '.join(SERIALIZERS)}"
        )
    return serializer




@fcache
def get_working_io_engines():
    """
    Determine which I/O engines are available based on current pip installations.

    Returns:
        list: A list of available I/O engine names.
    """
    working_engines = ["csv", "json", "pickle"]

    # Check for specific engines
    try:
        import pyarrow

        working_engines.extend(["parquet", "feather"])
    except ImportError:
        pass

    return set(working_engines)


def get_io_engine(io_engine=None):
    if io_engine is None:
        if check_io_engine(OPTIMAL_DATAFRAME_IO_ENGINE):
            return OPTIMAL_DATAFRAME_IO_ENGINE
        return DEFAULT_DATAFRAME_IO_ENGINE
    if check_io_engine(io_engine):
        return io_engine
    raise ValueError(
        f"IO engine {io_engine} not found or installed. Please choose one of: {get_working_io_engines()}"
    )


def check_io_engine(io_engine):
    return io_engine in get_working_io_engines()


def get_working_df_engines() -> Set[str]:
    working_engines = set()
    try:
        import pandas

        working_engines.add("pandas")
    except ImportError:
        pass

    try:
        import polars

        working_engines.add("polars")
    except ImportError:
        pass

    return working_engines


def check_df_engine(df_engine):
    return df_engine in get_working_df_engines()


def get_df_engine(df_engine=None):
    if df_engine is None:
        if check_df_engine(OPTIMAL_DATAFRAME_DF_ENGINE):
            return OPTIMAL_DATAFRAME_DF_ENGINE
        return DEFAULT_DATAFRAME_DF_ENGINE
    if check_df_engine(df_engine):
        return df_engine
    raise ValueError(
        f"DF engine {df_engine} not found or installed. Please choose one of: {get_working_df_engines()}"
    )


def get_dataframe_engine(df):
    from .utils.misc import is_dataframe
    from .utils.addrs import get_obj_addr
    if not is_dataframe(df):
        return
    return get_obj_addr(df).split(".")[0]




# raw/zlib/gzip/bz2 are stdlib; blosc/lz4 are optional imports
_ALWAYS_COMPRESSERS = (RAW_NO_COMPRESS, 'zlib', 'gzip', 'bz2')
_COMPRESSER_REQUIRES = {
    'blosc': ('blosc',),
    'lz4': ('lz4.block',),
}


@fcache
def _compresser_available(compress):
    if compress in _ALWAYS_COMPRESSERS:
        return True
    reqs = _COMPRESSER_REQUIRES.get(compress)
    if reqs is None:
        return False
    return all(_can_import(m) for m in reqs)


@fcache
def get_working_compressers():
    compressers = list(_ALWAYS_COMPRESSERS)
    for compress in _COMPRESSER_REQUIRES:
        if _compresser_available(compress):
            compressers.append(compress)
    return set(compressers)

@fcache
def get_compresser(compress):
    from .utils.logs import log
    if compress in {False,RAW_NO_COMPRESS}:
        return RAW_NO_COMPRESS
    if compress in {True, None}:
        compress = OPTIMAL_COMPRESS
    if not _compresser_available(compress):
        if compress in COMPRESSERS:
            log.debug(f'Compression library {compress} is not installed. Defaulting to zlib. To install {compress}, run: pip install {compress}')
        else:
            log.debug(f'Compression library {compress} is not recognized. Defaulting to zlib. Choose one of: {", ".join(COMPRESSERS)}')
        compress = DEFAULT_COMPRESS
    return compress


