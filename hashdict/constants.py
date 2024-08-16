from typing import *
import os
import multiprocessing as mp
from typing import Literal


DEFAULT_ROOT_DIR = os.path.expanduser('~/.cache/hashdict')
DEFAULT_NAME = 'default'


# Default settings
DEFAULT_COMPRESS = True
DEFAULT_B64 = True

# Cache engines
ENGINES = ("memory", "file", "sqlite", "redis", "diskcache", "lmdb") # "shelve",  "pickledb"

# Performance testing constants
DEFAULT_NUM_PROC = (1, 2, mp.cpu_count())
DEFAULT_DATA_SIZE = 1_000_000
ENGINE_TYPES = Literal["memory", "file", "sqlite", "redis", "diskcache", "lmdb", "shelve", "pickledb"]
DEFAULT_ENGINE_TYPE = "file"
INITIAL_SIZE = 1024
DEFAULT_ITERATIONS = 1
GROUPBY = ["Engine", "Encoding", "Operation", "write_num"]
SORTBY = "Speed (MB/s)"
DEFAULT_INDEX = ["Encoding", "Engine"]


# Profile sizes
NUM_PROFILE_SIZES = 6
PROFILE_SIZE_MULTIPLIER = 10
INITIAL_PROFILE_SIZE = 10
PROFILE_SIZES = None

# Redis settings
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0