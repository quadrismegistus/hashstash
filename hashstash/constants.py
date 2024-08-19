import sys; sys.path.insert(0,'/Users/ryan/github/prosodic')

from typing import *
import os
import multiprocessing as mp
from typing import Literal
import time
import random

DEFAULT_ROOT_DIR = os.path.expanduser('~/.cache/hashstash')
DEFAULT_NAME = 'default_cache'
DEFAULT_PATH = os.path.join(DEFAULT_ROOT_DIR, DEFAULT_NAME)
DEFAULT_REDIS_DIR = os.path.join(DEFAULT_ROOT_DIR, '_redis','data','db')
DEFAULT_DBNAME = 'hashstash'
DEFAULT_FILENAME = "db"

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

OBJ_ADDR_KEY='__py__'
OBJ_ARGS_KEY='__py_args__'
OBJ_KWARGS_KEY='__py_kwargs__'
OBJ_SRC_KEY = '__py_src__'

BUILTIN_SERIALIZER = 'builtin'
JSONPICKLE_SERIALIZER = 'jsonpickle'
DEFAULT_SERIALIZER = BUILTIN_SERIALIZER

class Dog:
    goestoheaven = True

    def __init__(self, name):
        self.name = name
        self.good = True

    def bark(self):
        print("woof ", end="", flush=True)
        time.sleep(random.random() / 2)
        print("woof")

dog = Dog('rex')
