from . import *

class Config:
    def __init__(
        self,
        serializer: Union[SERIALIZER_TYPES, List[SERIALIZER_TYPES]] = None,
        engine: ENGINE_TYPES = None,
        compress: bool = None,
        b64: bool = DEFAULT_B64,
        **kwargs,
    ):
        self.serializer = get_serializer_type(serializer)
        self.engine = get_engine(engine)
        self.compress = get_compresser(compress)
        self.b64 = b64

    def to_dict(self):
        return {
            "serializer": self.serializer,
            "engine": self.engine,
            "compress": self.compress,
            "b64": self.b64,
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






@fcache
def get_working_engines():
    working_engines = set(BUILTIN_ENGINES)

    try:
        import sqlitedict

        working_engines.add("sqlite")
    except ImportError:
        pass

    try:
        import redis
        import redis_dict

        working_engines.add("redis")
    except ImportError:
        pass

    try:
        import diskcache

        working_engines.add("diskcache")
    except ImportError:
        pass

    try:
        import lmdb

        working_engines.add("lmdb")
    except ImportError:
        pass

    try:
        import pymongo

        working_engines.add("mongo")
    except ImportError:
        pass

    try:
        import pandas as pd
        import numpy as np

        working_engines.add("dataframe")
    except ImportError:
        pass

    return working_engines


def get_engine(engine):
    from .utils.logs import log
    if engine is None:
        engine = OPTIMAL_ENGINE_TYPE
    if engine not in get_working_engines():
        if engine in ENGINES:
            log.warning(
                f"Engine {engine} is not installed. Defaulting to {DEFAULT_ENGINE_TYPE}. To install {engine}, run: pip install {engine}"
            )
        else:
            log.warning(
                f'Engine {engine} is not recognized. Defaulting to {DEFAULT_ENGINE_TYPE}. Choose one of: {", ".join(ENGINES)}'
            )
        engine = DEFAULT_ENGINE_TYPE
    return engine





@fcache
def get_working_serializers():
    from .utils.logs import log
    working_serializers = ['hashstash','pickle']
    try:
        import jsonpickle
        working_serializers.append('jsonpickle')
    except ImportError:
        pass
    return working_serializers

def get_serializer_type(serializer):
    if serializer is None:
        serializer = OPTIMAL_SERIALIZER
    return serializer if serializer in get_working_serializers() else DEFAULT_SERIALIZER




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
    from .utils.dataframes import MetaDataFrame
    from .utils.addrs import get_obj_addr
    if not is_dataframe(df):
        return
    if isinstance(df, MetaDataFrame):
        return df.df_engine
    return get_obj_addr(df).split(".")[0]




@fcache
def get_working_compressers():
    compressers = [RAW_NO_COMPRESS, 'zlib', 'gzip', 'bz2']
    try:
        import blosc
        compressers.append('blosc')
    except ImportError:
        pass

    try:
        import lz4.block
        compressers.append('lz4')
    except ImportError:
        pass
    return set(compressers)

@fcache
def get_compresser(compress):
    from .utils.logs import log
    if compress in {False,RAW_NO_COMPRESS}:
        return RAW_NO_COMPRESS
    if compress in {True, None}:
        compress = OPTIMAL_COMPRESS
    if not compress in get_working_compressers():
        if compress in COMPRESSERS:
            log.warning(f'Compression library {compress} is not installed. Defaulting to zlib. To install {compress}, run: pip install {compress}')
        else:
            log.warning(f'Compression library {compress} is not recognized. Defaulting to zlib. Choose one of: {", ".join(COMPRESSERS)}')
        compress = DEFAULT_COMPRESS
    return compress


