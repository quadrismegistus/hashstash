from typing import *
import os
import multiprocessing as mp
from typing import Literal


DEFAULT_ROOT_DIR = os.path.expanduser('~/.cache/hashstash')
DEFAULT_REDIS_DIR = os.path.join(DEFAULT_ROOT_DIR, '_redis','data','db')
DEFAULT_NAME = 'default_cache'


# Default settings
DEFAULT_COMPRESS = True
DEFAULT_B64 = True

# Cache engines
ENGINES = ("memory", "file", "sqlite", "redis", "diskcache", "lmdb", "shelve") #"pickledb"

# Performance testing constants
DEFAULT_NUM_PROC = mp.cpu_count() - 2 if mp.cpu_count() > 2 else 1
DEFAULT_DATA_SIZE = 1_000_000
ENGINE_TYPES = Literal["memory", "file", "sqlite", "redis", "diskcache", "lmdb", "shelve", "pickledb"]
DEFAULT_ENGINE_TYPE = "file"
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
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0