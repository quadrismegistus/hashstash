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
DEFAULT_NAME = "default_cache"
DEFAULT_PATH = os.path.join(DEFAULT_ROOT_DIR, DEFAULT_NAME)
DEFAULT_REDIS_DIR = os.path.join(DEFAULT_ROOT_DIR, "_redis", "data", "db")
DEFAULT_DBNAME = "main"
DEFAULT_FILENAME = "db"

DEFAULT_LOG_LEVEL = logging.INFO

# Default settings
DEFAULT_COMPRESS = True
DEFAULT_B64 = True

# Cache engines
ENGINES = (
    "memory",
    "pairtree",
    "sqlite",
    "redis",
    "diskcache",
    "lmdb",
    "shelve",
    # "pickledb"
)

# Performance testing constants
DEFAULT_NUM_PROC = mp.cpu_count() - 2 if mp.cpu_count() > 2 else 1
DEFAULT_DATA_SIZE = 1_000_000
ENGINE_TYPES = Literal[
    "memory", "pairtree", "sqlite", "redis", "diskcache", "lmdb", "shelve", "pickledb"
]
DEFAULT_ENGINE_TYPE = "pairtree"
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
REDIS_DB = 0

OBJ_ADDR_KEY = "__py__"
OBJ_ARGS_KEY = "__py_args__"
OBJ_KWARGS_KEY = "__py_kwargs__"
OBJ_SRC_KEY = "__py_src__"

# DEFAULT_SERIALIZER = "custom"
SERIALIZER_TYPES = Literal[
    "custom",          # flexible, but not as fast as jsonpickle
    "jsonpickle_ext",  # if fails to decode a value, so will jsonpickle
    "jsonpickle",      # will work as backup if numpy and pandas are not installed
    "pickle",          # fastest but not platform independent
    "orjson",          # cannot handle pandas etc
    "json",            # cannot handle pandas and numpy etc
]
DEFAULT_SERIALIZER = list(SERIALIZER_TYPES.__args__)


class Dog:
    goestoheaven = True

    def __init__(self, name):
        self.name = name
        self.good = True

    def bark(self):
        print("woof ", end="", flush=True)
        time.sleep(random.random() / 2)
        print("woof")


dog = Dog("rex")
