import warnings
warnings.filterwarnings('ignore')
import sys

sys.path.insert(0, "/Users/ryan/github/prosodic")
import logging
from typing import *
import os
import multiprocessing as mp
from typing import Literal
import time
import random

DEFAULT_ROOT_DIR = os.path.expanduser("~/.cache/hashstash")
DEFAULT_NAME = "default_stash"
DEFAULT_PATH = os.path.join(DEFAULT_ROOT_DIR, DEFAULT_NAME)
DEFAULT_REDIS_DIR = os.path.join(DEFAULT_ROOT_DIR, ".redis")
DEFAULT_MONGO_DIR = os.path.join(DEFAULT_ROOT_DIR, ".mongo")

OPTIMAL_DATAFRAME_IO_ENGINE = 'feather'
DEFAULT_DATAFRAME_IO_ENGINE = 'csv'
OPTIMAL_DATAFRAME_DF_ENGINE = 'pandas'
DEFAULT_DATAFRAME_DF_ENGINE = 'pandas'

DEFAULT_APPEND_MODE = True

RAW_NO_COMPRESS= 'raw'

DEFAULT_DBNAME = "main"
DEFAULT_FILENAME = "db"

DEFAULT_LOG_LEVEL = logging.INFO

# Default settings
OPTIMAL_COMPRESS = 'lz4'
DEFAULT_COMPRESS = RAW_NO_COMPRESS
DEFAULT_B64 = False

COMPRESSERS = ['zlib','lz4','blosc','gzip','bz2']

# Cache engines
ENGINE_TYPES = Literal[
    "memory", 
    "pairtree", 
    # "dataframe",
    # "shelve",
    "lmdb",
    "sqlite",
    "diskcache", 
    "redis", 
    "mongo",
]
ENGINES = ENGINE_TYPES.__args__
BUILTIN_ENGINES = ['memory', 'pairtree', 'shelve']
EXT_ENGINES = [e for e in ENGINES if e not in BUILTIN_ENGINES]

# Performance testing constants
DEFAULT_NUM_PROC = 1# mp.cpu_count() - 2 if mp.cpu_count() > 2 else 1
DEFAULT_DATA_SIZE = 1_000_00

DEFAULT_ENGINE_TYPE = "pairtree"
OPTIMAL_ENGINE_TYPE = "pairtree"
INITIAL_SIZE = 1024
DEFAULT_ITERATIONS = 1000
GROUPBY = ["Engine", "Encoding", "Operation", "write_num"]
SORTBY = "Speed (MB/s)"
DEFAULT_INDEX = ["Encoding", "Engine"]


# Profile sizes
NUM_PROFILE_SIZES = 6
PROFILE_SIZE_MULTIPLIER = 10
INITIAL_PROFILE_SIZE = 10
PROFILE_SIZES = [1, 10, 100, 1_000, 10_000, 100_000, 1_000_000]

# Redis settings
REDIS_HOST = "localhost"
REDIS_PORT = 6379
# REDIS_PORT = 6739 # not 6379
REDIS_DB = 0

# MongoDB settings
MONGO_HOST = "localhost"
MONGO_PORT = 27017


OBJ_ADDR_KEY = "__py__"
OBJ_ARGS_KEY = "__py_args__"
OBJ_KWARGS_KEY = "__py_kwargs__"
OBJ_SRC_KEY = "__py_src__"

SERIALIZER_TYPES = Literal[
    "hashstash",          # flexible, but not as fast as jsonpickle
    "jsonpickle",      # pretty flexible json replacement for pickle
    "pickle",          # fastest but not platform independent
]
DEFAULT_SERIALIZER = "hashstash"
OPTIMAL_SERIALIZER = "hashstash"
SERIALIZERS = list(SERIALIZER_TYPES.__args__)

DATA_TYPES = ('pandas_df', 'dict')
DEFAULT_DATA_TYPE = 'pandas_df'


## objects
from functools import lru_cache
fcache = lru_cache(maxsize=None)

