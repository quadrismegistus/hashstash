# HashStash

HashStash is a versatile caching library for Python that supports multiple storage engines, serializers, and encoding options. It provides a simple dictionary-like interface for caching data with various backend options. HashStash is designed to be easy to use, flexible, and efficient.

## Features

### Convenient usage
- Dictionary interface: but absolutely anything can be either a key or value (even functions, dataframes, etc)

- Multiprocessing support: connection pooling, multiprocessing locks, etc

- Function decorators like `@stashed_result`, which cache the results of function calls

- Context managers for temporary caches

- Easy dataframe assembly from cached contents

### Multiple storage engines

- File-based
    - "__pairtree__" (no dependencies, no database; just organized folder and file structure; very fast)
    - "__[lmdb](https://pypi.org/project/lmdb/)__" (single file, very efficient, slightly faster than pairtree)
    - "__[diskcache](https://pypi.org/project/diskcache/)__" (similar to pairtree, but slower)
    - "__sqlite__" (using [sqlitedict](https://pypi.org/project/sqlitedict/))

- Server-based
    - "__redis__" (using [redis-py](https://pypi.org/project/redis/))
    - "__mongo__" (using [pymongo](https://pypi.org/project/pymongo/))

- In-memory
    - "__memory__" (shared memory, using [ultradict](https://pypi.org/project/ultradict/))

### Multiple serializers

- Transportable between Python versions
    - "__hashstash__"
        - Custom, no dependencies
        - Can serialize nearly anything, even lambdas or functions defined within functions
        - Serializes pandas dataframes using pyarrow if available
        - Faster than jsonpickle but with larger file sizes
        - Mostly JSON-based, with some binary data
    - "__[jsonpickle](https://pypi.org/project/jsonpickle/)__"
        - Flexible, battle-tested, but slowest

- Not transportable between Python versions
    - "__pickle__"
        - Standard library
        - By far the fastest
        - But dangerous to use when sharing data across projects or Python versions 

### Compression and encoding options
- External compressors (with depedencies):
    - "__[lz4](<https://pypi.org/project/python-lz4/)>)__" (fastest)
    - "__[blosc](https://pypi.org/project/blosc/)__"

- Built-in compressors (no dependencies):
    - "__zlib__"
    - "__gzip__"
    - "__bz2__" (smallest file size, but slowest)

## Installation

HashStash requires no dependencies by default, but you can install optional dependencies to get the best performance.

* Default installation: `pip install hashstash`

* Installation with only the optimal dependencies (lmdb + lz4 + pyarrow): `pip install hashstash[best]`

* Full installation with all optional dependencies: `pip install hashstash[all]`

* Installation with specific optional dependencies:

    - For pandas dataframe serialization and support: `pip install hashstash[dataframe]`

    - For file-based engines: `pip install hashstash[filebased]`

    - For server-based engines: `pip install hashstash[servers]`

    - For all engine types: `pip install hashstash[engines]`

    - For specific engines:
        - `pip install hashstash[redis]`
        - `pip install hashstash[mongo]`
        - `pip install hashstash[lmdb]`
        - `pip install hashstash[sqlite]`
        - `pip install hashstash[diskcache]`
        - `pip install hashstash[memory]`

    - For development: `pip install hashstash[dev]`

Note: You can combine multiple optional dependencies, e.g., `pip install hashstash[dataframe,filebased]`

## Usage

Here's a quick example of how to use HashStash. 

### Creating a stash

```python
from hashstash import HashStash

# Create a stash instance
stash = HashStash()

# or customize:
stash = HashStash(
    # naming
    root_dir="project_cache",    # root directory of the stash (default: default_stash)
                                 # if not an absolute path, will be ~/.cache/hashstash/[root_dir]
    dbname="sub_cache",          # name of "database" or subfolder (default: main)
    
    # engines
    engine="pairtree",           # or lmdb, sqlite, diskcache, redis, mongo, or memory
    serializer="hashstash",      # or jsonpickle or pickle
    compress='lz4',              # or blosc, bz2, gzip, zlib, or raw
    b64=False,                   # base64 encode keys and values

    # storage options
    append_mode=False,           # store all versions of a key/value pair
)

# clear for this readme
stash.clear()

# show stash config
stash
```

<pre>PairtreeHashStash</pre><table border="1" class="dataframe"><thead><tr><th>Config</th><th>Param</th><th>Value</th></tr></thead><tbody><tr><td><b>Path</b></td><td>Root Dir</td><td><i>/Users/ryan/.cache/hashstash/project_cache</i></td></tr><tr><td><b></b></td><td>Dbname</td><td><i>sub_cache</i></td></tr><tr><td><b></b></td><td>Filename</td><td><i>pairtree.hashstash.lz4.db</i></td></tr><tr><td><b>Engine</b></td><td>Engine</td><td><i>pairtree</i></td></tr><tr><td><b></b></td><td>Serializer</td><td><i>hashstash</i></td></tr><tr><td><b></b></td><td>Compress</td><td><i>lz4</i></td></tr></tbody></table>

### Stashing objects

Literally anything can be a key or value, including functions, dataframes, dictionaries, etc.

```python
# store arbitrary objects in keys and values, even unhashable ones
stash["cat"] = {"goodness":"good"}
stash[{"goodness":"bad"}] = 'dog'

for key, value in stash.items():
    print(f'KEY: {key} >>> VALUE: {value}')
```

↓

    KEY: cat >>> VALUE: {'goodness': 'good'}
    KEY: {'goodness': 'bad'} >>> VALUE: dog

```python
# Even dataframes can be a key or value
import pandas as pd
df = pd.DataFrame({
    "name":["cat","dog"],
    "goodness":["good","bad"]
})
stash["cat-dog"] = df
stash[df] = "cat-dog"

stash[df] == "cat-dog", stash["cat-dog"].equals(df)
```

↓

    (True, True)

### Other dictionary operations

HashStash fully implements the dictionary-like `MutableMapping` interface, providing the following methods:

- `__setitem__(key: Any, value: Any)`: Set an item in the cache

- `__getitem__(key: Any) -> Any`: Get an item from the cache

- `__contains__(key: Any) -> bool`: Check if a key exists in the cache

- `get(key: Any, default: Any = None) -> Any`: Get an item with a default value

- `clear() -> None`: Clear all items from the cache

- `__len__() -> int`: Return the number of items in the cache

- `__iter__()`: Iterate over all keys in the cache

- `keys()`: Return an iterator over the cache keys

- `values()`: Return an iterator over the cache values

- `items()`: Return an iterator over the cache key-value pairs

- `update(other=None, **kwargs)`: Update the cache with key-value pairs from another dictionary or keyword arguments

- `setdefault(key, default=None)`: Set a key with a default value if it doesn't exist, and return the value

- `pop(key, default=None)`: Remove and return the value for a key, or return the default if the key doesn't exist

- `popitem()`: Remove and return a (key, value) pair from the cache

There are also extra dictionary-like functions for convenience:

- `keys_l()`: Return a list of all keys in the cache

- `values_l()`: Return a list of all values in the cache

- `items_l()`: Return a list of all key-value pairs in the cache

### Append mode

```python
# Append mode
stash = HashStash(engine="memory", append_mode=True).clear()

stash["cat"] = "good"
stash["cat"] = "bad"
stash.get_all("cat")
```

↓

    ['good', 'bad']

```python
# Not append mode
stash = HashStash(engine="memory", append_mode=False)
stash["cat"] = "good"
stash["cat"] = "bad"
stash.get_all("cat")
```

↓

    ['bad']

### Function decorators

HashStash provides a `@stashed_result` decorator for caching the results of function calls.

```python
from hashstash import stashed_result

@stashed_result(engine="memory")          # or @my_stash.stashed_result
def expensive_computation(x):
    print('performing some time consuming calculation')
    return x * 2

# you can now access the stash directly as an attribute of the function
expensive_computation.stash

# clear it for this readme
expensive_computation.stash.clear()

# first call will compute and cache the result
expensive_computation(5)

# subsequent calls will return the cached result -- will not print
expensive_computation(5)

# you can iterate over results like any other stash
for key,value in expensive_computation.stash.items():
    print(f'KEY: {key} >>> VALUE: {value}')
```

↓

    performing some time consuming calculation
    KEY: {'args': (5,), 'kwargs': {}} >>> VALUE: 10

### Temporary Caches

HashStash provides a `tmp` method for creating temporary caches that are automatically cleaned up. The temporary cache is automatically cleared and removed after the with block:

```python
stash = HashStash()

with stash.tmp() as tmp_stash:
    tmp_stash["key"] = "value"
    print("key" in tmp_stash)
    
print("key" in tmp_stash)
```

↓

    True
    False

### Utilities

#### Serialization

HashStash supports multiple serialization methods:

- `serialize`: Serializes Python objects
- `deserialize`: Deserializes data back into Python objects

```python
from hashstash import serialize, deserialize

data = pd.DataFrame({"name": ["kitty", "doggy"], "goodness": ["good", "bad"]})
serialized_data = serialize(data)
deserialized_data = deserialize(serialized_data)

data.equals(deserialized_data)
```

↓

    True

You can also specify the serializer. The custom "hashstash" serializer is the default, but you can also use "jsonpickle" or "pickle":

```python
serialized_data = serialize(data, serializer="jsonpickle")
deserialized_data = deserialize(serialized_data, serializer="jsonpickle")

data.equals(deserialized_data)
```

↓

    True

### Encoding and Compression

HashStash provides functions for encoding and compressing data:

- `encode`: Encodes and optionally compresses data
- `decode`: Decodes and decompresses data

These functions are used internally by HashStash but can also be used directly:

```python
from hashstash import encode, decode

data = b"Hello, World!"
encoded_data = encode(data, compress='lz4', b64=True)
decoded_data = decode(encoded_data, compress='lz4', b64=True)

data == decoded_data
```

↓

    True

## Profiling

### Engines

LMDB is the fastest engine, followed by the custom "pairtree" implementation.

![Engines](./figures/fig.comparing_engines.png)

### Serializers

Pickle is by far the fastest serializer, but it is not transportable between Python versions. HashStash is generally faster than jsonpickle, and can serialize more data types (including lambdas and functions within functions), but it produces larger file sizes.

![Serializers](./figures/fig.comparing_serializers_size_speed.png)

### Encodings

LZ4 is the fastest compressor, but it requires an external dependency. BZ2 is the slowest, but it provides the best compression ratio.

![Compressors](./figures/fig.comparing_encodings_size_speed.png)

### All together

LMDB engine, with pickle serializer, with no compression (raw) or LZ4 or blosc compression is the fastest combination of parameters; followed by pairtree with the same. 

![All together](./figures/fig.comparing_engines_serializers_encodings.png)

## Development

### Tests

To run the tests, clone this repository and run  `pytest` in the root project directory.

### Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

### License

This project is licensed under the GNU License.
